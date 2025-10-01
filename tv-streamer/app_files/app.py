import os
import signal
from datetime import datetime
from subprocess import Popen, PIPE
from typing import Generator

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, jsonify, Response
)
from google.cloud import storage
from google.cloud.sql.connector import Connector

from .models import db, Stream


# -----------------------------------------------------------------------------
# Flask app (templates live in ../templates)
# -----------------------------------------------------------------------------
app = Flask(__name__, template_folder="../templates")
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "change-me")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False


# -----------------------------------------------------------------------------
# Cloud SQL (Postgres) via Connector (pg8000)
# -----------------------------------------------------------------------------
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")  # project:region:instance
DB_USER = os.environ.get("DB_USER", "tvuser")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME", "tvdb")

if not INSTANCE_CONNECTION_NAME:
    raise RuntimeError("INSTANCE_CONNECTION_NAME env var is required (project:region:instance)")
if not DB_PASS:
    raise RuntimeError("DB_PASS env var is required (map Secret Manager secret as env var)")

_connector = Connector()

def getconn():
    return _connector.connect(
        INSTANCE_CONNECTION_NAME, "pg8000",
        user=DB_USER, password=DB_PASS, db=DB_NAME
    )

app.config["SQLALCHEMY_DATABASE_URI"] = "postgresql+pg8000://"
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"creator": getconn}
db.init_app(app)
with app.app_context():
    db.create_all()  # tvuser must have CREATE/USAGE on schema public


# -----------------------------------------------------------------------------
# (Optional) GCS for channel photos
# -----------------------------------------------------------------------------
BUCKET_NAME = os.environ.get("BUCKET_NAME")
_storage_client = storage.Client() if BUCKET_NAME else None
_bucket = _storage_client.bucket(BUCKET_NAME) if BUCKET_NAME else None

def upload_photo_to_gcs(file_path: str, stream_id: int) -> str:
    if not _bucket:
        return ""
    blob = _bucket.blob(f"{stream_id}/photos/{os.path.basename(file_path)}")
    blob.upload_from_filename(file_path)
    blob.make_public()  # demo; prefer signed URLs in production
    return blob.public_url


# -----------------------------------------------------------------------------
# MJPEG generator (FFmpeg → mpjpeg muxer → multipart)
# -----------------------------------------------------------------------------
def mjpeg_generator(
    hls_url: str,
    width: int = 640,
    fps: int = 20,
    jpeg_quality: int = 6,
    referer: str | None = None,
    extra_headers: str | None = None,
) -> Generator[bytes, None, None]:
    """
    Use FFmpeg's mpjpeg muxer to emit a compliant multipart/x-mixed-replace stream.
    We stream bytes through; FFmpeg handles boundaries and part headers.
    """

    # Prepare optional HTTP headers (each line must end with \r\n)
    headers_value = ""
    if referer:
        headers_value += f"Referer: {referer}\r\n"
    headers_value += "User-Agent: Mozilla/5.0\r\n"
    if extra_headers:  # e.g., "Origin: ...\r\nCookie: token=...\r\n"
        headers_value += extra_headers

    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-re",                      # pace in real time
        "-fflags", "nobuffer",
        "-flags", "low_delay",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "2",
    ]

    # Inject headers if set (preferred over -user_agent)
    if headers_value:
        cmd += ["-headers", headers_value]

    cmd += [
        "-i", hls_url,
        "-an",                      # no audio in MJPEG
        "-vf", f"fps={fps},scale={width}:-1",
        "-vcodec", "mjpeg",
        "-q:v", str(jpeg_quality),  # 2(best)..31(worst)
        "-f", "mpjpeg",
        "-boundary_tag", "frame",   # <- matches response boundary
        "-"
    ]

    proc = Popen(cmd, stdout=PIPE, stderr=PIPE, bufsize=0)

    try:
        while True:
            chunk = proc.stdout.read(16384)
            if not chunk:
                err = proc.stderr.read().decode("utf-8", "ignore").strip()
                if err:
                    app.logger.error("FFmpeg exited: %s", err)
                break
            yield chunk  # FFmpeg already emits correct multipart with boundary=frame
    except GeneratorExit:
        pass
    except Exception as e:
        app.logger.exception("MJPEG generator error: %r", e)
    finally:
        try:
            if proc.poll() is None:
                proc.terminate()
                proc.wait(timeout=3)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    streams = Stream.query.order_by(Stream.id).all()
    return render_template("index.html", streams=streams)


@app.route("/admin")
def admin_list():
    streams = Stream.query.order_by(Stream.id).all()
    return render_template("admin_list.html", streams=streams)


@app.route("/admin/add", methods=["GET", "POST"])
def admin_add():
    if request.method == "POST":
        name = request.form["name"].strip()
        hls_url = request.form["hls_url"].strip()
        photo = request.files.get("photo")

        stream = Stream(name=name, hls_url=hls_url)
        db.session.add(stream)
        db.session.commit()

        if photo and photo.filename and _bucket:
            tmp = f"/tmp/{photo.filename}"
            photo.save(tmp)
            stream.photo_url = upload_photo_to_gcs(tmp, stream.id)
            db.session.commit()
            os.remove(tmp)

        flash("Stream added!", "success")
        return redirect(url_for("admin_list"))
    return render_template("admin_form.html", stream=None)


@app.route("/admin/edit/<int:stream_id>", methods=["GET", "POST"])
def admin_edit(stream_id):
    stream = Stream.query.get_or_404(stream_id)
    if request.method == "POST":
        stream.name = request.form["name"].strip()
        stream.hls_url = request.form["hls_url"].strip()
        photo = request.files.get("photo")
        if photo and photo.filename and _bucket:
            tmp = f"/tmp/{photo.filename}"
            photo.save(tmp)
            stream.photo_url = upload_photo_to_gcs(tmp, stream.id)
            os.remove(tmp)
        stream.last_processed = datetime.utcnow()
        db.session.commit()
        flash("Stream updated!", "success")
        return redirect(url_for("admin_list"))
    return render_template("admin_form.html", stream=stream)


@app.route("/admin/delete/<int:stream_id>")
def admin_delete(stream_id):
    Stream.query.filter_by(id=stream_id).delete()
    db.session.commit()
    flash("Stream deleted!", "info")
    return redirect(url_for("admin_list"))


# ---- MJPEG stream ------------------------------------------------------------
@app.route("/mjpeg/<int:stream_id>")
def mjpeg(stream_id):
    stream = Stream.query.get_or_404(stream_id)
    # If your provider requires a specific Referer/Origin/Cookie, set here:
    referer = request.args.get("referer") or None
    extra = request.args.get("extra") or None  # raw header lines like "Origin: ...\r\nCookie: ...\r\n"
    gen = mjpeg_generator(stream.hls_url, width=640, fps=20, jpeg_quality=6,
                          referer=referer, extra_headers=extra)
    return Response(
        gen,
        mimetype="multipart/x-mixed-replace; boundary=frame",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        },
    )


# ---- Player: MJPEG + HLS audio ----------------------------------------------
@app.route("/player/<int:stream_id>")
def player(stream_id):
    stream = Stream.query.get_or_404(stream_id)
    return render_template("player.html", stream=stream, hls_url=stream.hls_url)


# ---- Health ------------------------------------------------------------------
@app.route("/health")
def health():
    return {"ok": True}, 200