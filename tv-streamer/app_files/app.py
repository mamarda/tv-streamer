import os
import subprocess
import shutil
import glob
import mimetypes
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from google.cloud import storage
from google.cloud.sql.connector import Connector

from .models import db, Stream

# -----------------------------------------------------------------------------
# Flask app  (templates live one level up in ../templates)
# -----------------------------------------------------------------------------
app = Flask(__name__, template_folder="../templates")
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "change-me")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# -----------------------------------------------------------------------------
# Cloud SQL (Postgres) via Connector (pg8000)
# -----------------------------------------------------------------------------
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")  # "project:region:instance"
DB_USER = os.environ.get("DB_USER", "tvuser")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME", "tvdb")

if not INSTANCE_CONNECTION_NAME:
    raise RuntimeError("INSTANCE_CONNECTION_NAME env var is required (project:region:instance).")
if not DB_PASS:
    raise RuntimeError("DB_PASS env var is required (map Secret Manager secret as env var).")

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
    db.create_all()  # requires tvuser to have CREATE on schema public

# -----------------------------------------------------------------------------
# Google Cloud Storage
# -----------------------------------------------------------------------------
BUCKET_NAME = os.environ.get("BUCKET_NAME")
if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME env var is required.")

_storage_client = storage.Client()
_bucket = _storage_client.bucket(BUCKET_NAME)

def upload_to_gcs(file_path: str, stream_id: int, prefix: str) -> str:
    """
    Upload a local file to GCS under {stream_id}/{prefix}/{file}, with correct Content-Type.
    """
    blob = _bucket.blob(f"{stream_id}/{prefix}/{os.path.basename(file_path)}")

    # Determine proper Content-Type
    ctype, _ = mimetypes.guess_type(file_path)
    if file_path.endswith(".webp"):
        ctype = "image/webp"
    elif file_path.endswith(".mp3"):
        ctype = "audio/mpeg"
    if not ctype:
        ctype = "application/octet-stream"

    blob.upload_from_filename(file_path, content_type=ctype)

    # Cache for 1 hour (optional)
    blob.cache_control = "public, max-age=3600"
    blob.patch()

    # Public for demo (consider signed URLs in production)
    blob.make_public()
    return blob.public_url

def _max_index(names) -> int:
    max_idx = 0
    for name in names:
        base = os.path.basename(name)
        parts = base.split("_")
        if len(parts) < 2:
            continue
        try:
            max_idx = max(max_idx, int(parts[-1].split(".")[0]))
        except ValueError:
            pass
    return max_idx

def list_assets(stream_id: int):
    prefix = f"{stream_id}/"
    blobs = list(_bucket.list_blobs(prefix=prefix))

    def idx(name: str) -> int:
        try:
            return int(os.path.basename(name).split("_")[-1].split(".")[0])
        except Exception:
            return 0

    frame_blobs = sorted((b for b in blobs if b.name.endswith(".webp")), key=lambda b: idx(b.name))
    audio_blobs = sorted((b for b in blobs if b.name.endswith(".mp3")), key=lambda b: idx(b.name))
    return [b.public_url for b in frame_blobs], [b.public_url for b in audio_blobs]

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    streams = Stream.query.order_by(Stream.id).all()
    return render_template("index.html", streams=streams)

@app.route("/get_assets/<int:stream_id>")
def get_assets_json(stream_id):
    frames, audios = list_assets(stream_id)
    return jsonify({"frames": frames, "audios": audios})

@app.route("/streams/<int:stream_id>")
def player(stream_id):
    stream = Stream.query.get_or_404(stream_id)
    frames, audios = list_assets(stream_id)
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

        if photo and photo.filename:
            tmp = f"/tmp/{photo.filename}"
            photo.save(tmp)
            stream.photo_url = upload_to_gcs(tmp, stream.id, "photos")
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
            stream.photo_url = upload_to_gcs(tmp, stream.id, "photos")
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

@app.route("/process/<int:stream_id>")
def process_stream(stream_id):
    """
    Grab ~10s from HLS: frames (webp @20fps) + audio (mp3 segments), append indexes, upload to GCS.
    Includes UA header and structured error reporting.
    """
    stream = Stream.query.get_or_404(stream_id)
    hls_url = stream.hls_url

    sid = str(stream_id)
    tmp_frame_dir = f"/tmp/{sid}_frames"
    tmp_audio_dir = f"/tmp/{sid}_audio"
    os.makedirs(tmp_frame_dir, exist_ok=True)
    os.makedirs(tmp_audio_dir, exist_ok=True)

    # Determine current max indexes in GCS
    existing = list(_bucket.list_blobs(prefix=f"{sid}/"))
    existing_frames = [b.name for b in existing if b.name.endswith(".webp")]
    existing_audio = [b.name for b in existing if b.name.endswith(".mp3")]
    max_frame = _max_index(existing_frames)
    max_audio = _max_index(existing_audio)

    # Many HLS servers require a UA; keep capture short for quick tests
    net_flags = ["-user_agent", "Mozilla/5.0", "-loglevel", "error"]

    frame_cmd = [
        "ffmpeg", "-y", *net_flags, "-i", hls_url, "-t", "10",
        "-vf", "scale=480:-1,fps=20",
        "-c:v", "libwebp", "-quality", "50", "-compression_level", "6",
        "-start_number", str(max_frame + 1),
        f"{tmp_frame_dir}/frame_%04d.webp",
    ]
    audio_cmd = [
        "ffmpeg", "-y", *net_flags, "-i", hls_url, "-t", "10",
        "-vn", "-c:a", "libmp3lame", "-b:a", "32k",
        "-f", "segment", "-segment_time", "5",
        "-segment_start_number", str(max_audio + 1),
        f"{tmp_audio_dir}/audio_%03d.mp3",
    ]

    try:
        subprocess.run(frame_cmd, capture_output=True, check=True)
        subprocess.run(audio_cmd, capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        err = (e.stderr or b"").decode("utf-8", "ignore")
        print("FFMPEG_ERROR:", err)
        shutil.rmtree(tmp_frame_dir, ignore_errors=True)
        shutil.rmtree(tmp_audio_dir, ignore_errors=True)
        return jsonify({"error": "ffmpeg failed", "detail": err}), 500
    except Exception as e:
        print("PROCESS_ERROR:", repr(e))
        shutil.rmtree(tmp_frame_dir, ignore_errors=True)
        shutil.rmtree(tmp_audio_dir, ignore_errors=True)
        return jsonify({"error": "processing failed", "detail": str(e)}), 500

    # Upload to GCS with correct Content-Type
    for fpath in sorted(glob.glob(f"{tmp_frame_dir}/*.webp")):
        upload_to_gcs(fpath, stream_id, "frames")
    for fpath in sorted(glob.glob(f"{tmp_audio_dir}/*.mp3")):
        upload_to_gcs(fpath, stream_id, "audio")

    shutil.rmtree(tmp_frame_dir, ignore_errors=True)
    shutil.rmtree(tmp_audio_dir, ignore_errors=True)

    stream.last_processed = datetime.utcnow()
    db.session.commit()
    return jsonify({"status": "Processed and appended ~10s chunk."})

@app.route("/health")
def health():
    return {"ok": True}, 200
