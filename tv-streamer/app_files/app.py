import os
import subprocess
import shutil
import glob
import mimetypes
import logging
from datetime import datetime, timedelta

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from google.cloud import storage
from google.cloud.sql.connector import Connector

import google.auth
from google.auth.transport.requests import Request

from .logging_setup import configure_logging, memory_handler
from .models import db, Stream

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
configure_logging()
logger = logging.getLogger("tv-streamer")

# -----------------------------------------------------------------------------
# Flask app (templates are ../templates)
# -----------------------------------------------------------------------------
app = Flask(__name__, template_folder="../templates")
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "change-me")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Optional HLS headers
HLS_USER_AGENT = os.environ.get("HLS_USER_AGENT", "Mozilla/5.0")
HLS_REFERER = os.environ.get("HLS_REFERER")

# Tunables
CAPTURE_SECONDS = int(os.environ.get("CAPTURE_SECONDS", "60"))
AUDIO_SEGMENT_SECONDS = int(os.environ.get("AUDIO_SEGMENT_SECONDS", "10"))
SIGNED_URL_TTL_SECONDS = int(os.environ.get("SIGNED_URL_TTL_SECONDS", "3600"))

# -----------------------------------------------------------------------------
# Cloud SQL (Postgres) via Connector (pg8000)
# -----------------------------------------------------------------------------
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")
DB_USER = os.environ.get("DB_USER", "tvuser")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME", "tvdb")

if not INSTANCE_CONNECTION_NAME:
    raise RuntimeError("INSTANCE_CONNECTION_NAME env var is required (project:region:instance).")
if not DB_PASS:
    raise RuntimeError("DB_PASS env var is required (map Secret Manager secret as env var).")

logger.info("DB settings loaded (user=%s, db=%s, instance=%s)", DB_USER, DB_NAME, INSTANCE_CONNECTION_NAME)

_connector = Connector()
def getconn():
    return _connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
    )

app.config["SQLALCHEMY_DATABASE_URI"] = "postgresql+pg8000://"
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {"creator": getconn}

db.init_app(app)
with app.app_context():
    db.create_all()
    logger.info("Ensured DB tables exist")

# -----------------------------------------------------------------------------
# Google Cloud Storage
# -----------------------------------------------------------------------------
BUCKET_NAME = os.environ.get("BUCKET_NAME")
if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME env var is required.")
logger.info("Using bucket=%s", BUCKET_NAME)

_storage_client = storage.Client()
_bucket = _storage_client.bucket(BUCKET_NAME)

# Keyless signing (IAM) config
SIGNING_SERVICE_ACCOUNT = os.environ.get("SIGNING_SERVICE_ACCOUNT")
if not SIGNING_SERVICE_ACCOUNT:
    raise RuntimeError("Set SIGNING_SERVICE_ACCOUNT to your Cloud Run service account email.")

# Get ADC and ensure it can fetch an access token (for IAM signBlob)
_ADC, _ = google.auth.default(scopes=[
    "https://www.googleapis.com/auth/iam",
    "https://www.googleapis.com/auth/devstorage.read_only",
])
def _access_token() -> str:
    if not _ADC.valid:
        _ADC.refresh(Request())
    return _ADC.token

def _upload(file_path: str, dest_name: str) -> None:
    """Upload file; no ACL changes (works with PAP/UBLA)."""
    blob = _bucket.blob(dest_name)
    ctype, _ = mimetypes.guess_type(file_path)
    if file_path.endswith(".webp"):
        ctype = "image/webp"
    elif file_path.endswith(".mp3"):
        ctype = "audio/mpeg"
    if not ctype:
        ctype = "application/octet-stream"
    blob.cache_control = "public, max-age=3600"
    blob.upload_from_filename(file_path, content_type=ctype)
    logger.info("Uploaded name=%s type=%s size=%s", dest_name, ctype, os.path.getsize(file_path))

def _sign(name: str, response_type: str | None = None) -> str:
    """Return a V4 signed URL using IAM (no private key needed)."""
    blob = _bucket.blob(name)
    return blob.generate_signed_url(
        version="v4",
        method="GET",
        expiration=timedelta(seconds=SIGNED_URL_TTL_SECONDS),
        response_type=response_type,
        service_account_email=SIGNING_SERVICE_ACCOUNT,
        access_token=_access_token(),
    )

def _max_index(names) -> int:
    m = 0
    for name in names:
        try:
            m = max(m, int(os.path.basename(name).split("_")[-1].split(".")[0]))
        except Exception:
            pass
    return m

def _list_names(stream_id: int):
    """Return sorted object NAMES for frames and audio."""
    prefix = f"{stream_id}/"
    blobs = list(_bucket.list_blobs(prefix=prefix))

    def idx(name: str) -> int:
        try:
            return int(os.path.basename(name).split("_")[-1].split(".")[0])
        except Exception:
            return 0

    frame_names = sorted((b.name for b in blobs if b.name.endswith(".webp")), key=idx)
    audio_names = sorted((b.name for b in blobs if b.name.endswith(".mp3")), key=idx)
    logger.info("Listed assets stream_id=%s frames=%s audio=%s", stream_id, len(frame_names), len(audio_names))
    return frame_names, audio_names

def _list_signed(stream_id: int):
    frames, audios = _list_names(stream_id)
    frame_urls = [_sign(n, "image/webp") for n in frames]
    audio_urls = [_sign(n, "audio/mpeg") for n in audios]
    return frame_urls, audio_urls

# -----------------------------------------------------------------------------
# ffmpeg helpers
# -----------------------------------------------------------------------------
def _net_flags():
    flags = ["-user_agent", HLS_USER_AGENT]
    if HLS_REFERER:
        flags += ["-headers", f"Referer: {HLS_REFERER}\r\n"]
    flags += ["-reconnect", "1", "-reconnect_at_eof", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5"]
    return flags

def _detect_audio_index(hls_url: str):
    cmd = ["ffprobe", "-v", "error", *_net_flags(), "-select_streams", "a",
           "-show_entries", "stream=index", "-of", "csv=p=0", hls_url]
    try:
        out = subprocess.run(cmd, capture_output=True, check=True, text=True, timeout=20)
        lines = [ln.strip() for ln in out.stdout.splitlines() if ln.strip()]
        indices = []
        for ln in lines:
            try: indices.append(int(ln))
            except ValueError: pass
        idx = min(indices) if indices else None
        logger.info("ffprobe audio indices=%s picked=%s", indices, idx)
        return idx
    except subprocess.CalledProcessError as e:
        logger.error("ffprobe failed: %s", (e.stderr or "").strip())
    except Exception as e:
        logger.exception("ffprobe exception: %s", repr(e))
    return None

def _capture_once(stream: Stream, duration: int) -> tuple[int, int]:
    hls_url = stream.hls_url
    sid = str(stream.id)
    tmp_frame_dir = f"/tmp/{sid}_frames"
    tmp_audio_dir = f"/tmp/{sid}_audio"
    os.makedirs(tmp_frame_dir, exist_ok=True)
    os.makedirs(tmp_audio_dir, exist_ok=True)

    frame_names, audio_names = _list_names(stream.id)
    max_frame = _max_index(frame_names)
    max_audio = _max_index(audio_names)

    # Frames (20 fps → ~duration*20 webp)
    frame_cmd = [
        "ffmpeg", "-y", *_net_flags(), "-i", hls_url, "-t", str(duration),
        "-vf", "scale=480:-1,fps=20", "-c:v", "libwebp", "-quality", "50", "-compression_level", "6",
        "-start_number", str(max_frame + 1),
        f"{tmp_frame_dir}/frame_%04d.webp",
    ]

    # Audio (10s segments, map first audio if detectable)
    audio_index = _detect_audio_index(hls_url)
    base_audio_cmd = [
        "ffmpeg", "-y", *_net_flags(), "-i", hls_url, "-t", str(duration),
        "-vn", "-c:a", "libmp3lame", "-b:a", "64k", "-ar", "44100",
        "-f", "segment", "-segment_time", str(AUDIO_SEGMENT_SECONDS),
        "-segment_start_number", str(max_audio + 1),
        f"{tmp_audio_dir}/audio_%03d.mp3",
    ]
    audio_cmd = base_audio_cmd if audio_index is None else [
        "ffmpeg", "-y", *_net_flags(), "-i", hls_url, "-t", str(duration),
        "-map", f"0:a:{audio_index}",
        "-vn", "-c:a", "libmp3lame", "-b:a", "64k", "-ar", "44100",
        "-f", "segment", "-segment_time", str(AUDIO_SEGMENT_SECONDS),
        "-segment_start_number", str(max_audio + 1),
        f"{tmp_audio_dir}/audio_%03d.mp3",
    ]

    subprocess.run(frame_cmd, capture_output=True, check=True)
    try:
        subprocess.run(audio_cmd, capture_output=True, check=True)
    except subprocess.CalledProcessError:
        subprocess.run(base_audio_cmd, capture_output=True, check=True)

    # Upload
    frame_paths = sorted(glob.glob(f"{tmp_frame_dir}/*.webp"))
    audio_paths = sorted(glob.glob(f"{tmp_audio_dir}/*.mp3"))
    for f in frame_paths:
        _upload(f, f"{sid}/frames/{os.path.basename(f)}")
    for f in audio_paths:
        _upload(f, f"{sid}/audio/{os.path.basename(f)}")

    shutil.rmtree(tmp_frame_dir, ignore_errors=True)
    shutil.rmtree(tmp_audio_dir, ignore_errors=True)
    return len(frame_paths), len(audio_paths)

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    streams = Stream.query.order_by(Stream.id).all()
    return render_template("index.html", streams=streams)

@app.route("/favicon.ico")
def favicon():
    return "", 204

@app.route("/logs")
def logs_page():
    entries = memory_handler.get(200)
    return render_template("logs.html", entries=list(reversed(entries)))

@app.route("/logs.json")
def logs_json():
    return jsonify(memory_handler.get(200))

@app.route("/logs/clear")
def logs_clear():
    memory_handler.clear()
    return "OK", 200

@app.route("/get_assets/<int:stream_id>")
def get_assets_json(stream_id):
    frames, audios = _list_signed(stream_id)
    return jsonify({"frames": frames, "audios": audios})

@app.route("/streams/<int:stream_id>")
def player(stream_id):
    stream = Stream.query.get_or_404(stream_id)
    frames, audios = _list_signed(stream_id)
    return render_template("player.html", stream=stream, frames=frames, audios=audios)

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
        logger.info("Added stream id=%s name=%s", stream.id, stream.name)

        if photo and photo.filename:
            tmp = f"/tmp/{photo.filename}"
            photo.save(tmp)
            _upload(tmp, f"{stream.id}/photos/{os.path.basename(tmp)}")
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
        if photo and photo.filename:
            tmp = f"/tmp/{photo.filename}"
            photo.save(tmp)
            _upload(tmp, f"{stream.id}/photos/{os.path.basename(tmp)}")
            os.remove(tmp)

        stream.last_processed = datetime.utcnow()
        db.session.commit()
        logger.info("Updated stream id=%s", stream.id)
        flash("Stream updated!", "success")
        return redirect(url_for("admin_list"))

    return render_template("admin_form.html", stream=stream)

@app.route("/admin/delete/<int:stream_id>")
def admin_delete(stream_id):
    Stream.query.filter_by(id=stream_id).delete()
    db.session.commit()
    logger.info("Deleted stream id=%s", stream_id)
    flash("Stream deleted!", "info")
    return redirect(url_for("admin_list"))

@app.route("/process/<int:stream_id>")
def process_stream(stream_id):
    stream = Stream.query.get_or_404(stream_id)
    logger.info("PROCESS start stream_id=%s url=%s", stream_id, stream.hls_url)
    try:
        f, a = _capture_once(stream, CAPTURE_SECONDS)
    except subprocess.CalledProcessError as e:
        err = (e.stderr or b"").decode("utf-8", "ignore")
        logger.error("FFMPEG_ERROR: %s", err)
        return jsonify({"error": "ffmpeg failed", "detail": err}), 500
    except Exception as e:
        logger.exception("PROCESS_ERROR stream_id=%s", stream_id)
        return jsonify({"error": "processing failed", "detail": str(e)}), 500

    stream.last_processed = datetime.utcnow()
    db.session.commit()
    logger.info("PROCESS end stream_id=%s uploaded_frames=%s uploaded_audio=%s", stream_id, f, a)
    return jsonify({"status": "OK", "uploaded_frames": f, "uploaded_audio": a})

@app.errorhandler(Exception)
def on_error(e):
    logger.exception("UNHANDLED %s %s", request.method, request.path)
    return "Internal Server Error", 500

@app.route("/health")
def health():
    return {"ok": True}, 200
