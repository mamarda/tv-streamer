import os
import subprocess
import shutil
import glob
import mimetypes
import logging
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from google.cloud import storage
from google.cloud.sql.connector import Connector

from .logging_setup import configure_logging, memory_handler
from .models import db, Stream

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
configure_logging()
logger = logging.getLogger("tv-streamer")

# -----------------------------------------------------------------------------
# Flask app  (templates live one level up in ../templates)
# -----------------------------------------------------------------------------
app = Flask(__name__, template_folder="../templates")
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "change-me")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Optional headers for HLS (set in Cloud Run env if your source requires them)
HLS_USER_AGENT = os.environ.get("HLS_USER_AGENT", "Mozilla/5.0")
HLS_REFERER = os.environ.get("HLS_REFERER")  # e.g. https://example.com

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
    db.create_all()  # tvuser must have CREATE/USAGE on schema public
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

def upload_to_gcs(file_path: str, stream_id: int, prefix: str) -> str:
    """
    Upload a local file to GCS under {stream_id}/{prefix}/{file}, with correct Content-Type.
    Assumes bucket allows public ACLs (PAP disabled, access control = fine-grained).
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
    blob.cache_control = "public, max-age=3600"
    blob.patch()
    blob.make_public()

    logger.info("Uploaded to GCS url=%s content_type=%s size=%s", blob.public_url, ctype, os.path.getsize(file_path))
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

    logger.info("Listed assets stream_id=%s frames=%s audio=%s", stream_id, len(frame_blobs), len(audio_blobs))
    return [b.public_url for b in frame_blobs], [b.public_url for b in audio_blobs]

# -----------------------------------------------------------------------------
# Networking flags for ffmpeg/ffprobe
# -----------------------------------------------------------------------------
def _net_flags(for_probe: bool = False):
    flags = ["-user_agent", HLS_USER_AGENT]
    if HLS_REFERER:
        flags += ["-headers", f"Referer: {HLS_REFERER}\r\n"]
    # reconnect flags (harmless for ffprobe)
    flags += ["-reconnect", "1",
              "-reconnect_at_eof", "1",
              "-reconnect_streamed", "1",
              "-reconnect_delay_max", "5"]
    return flags

def _detect_audio_index(hls_url: str):
    """Return first audio stream index using ffprobe, or None if not found."""
    cmd = ["ffprobe", "-v", "error",
           *_net_flags(for_probe=True),
           "-select_streams", "a",
           "-show_entries", "stream=index",
           "-of", "csv=p=0",
           hls_url]
    try:
        out = subprocess.run(cmd, capture_output=True, check=True, text=True, timeout=20)
        lines = [ln.strip() for ln in out.stdout.splitlines() if ln.strip()]
        indices = []
        for ln in lines:
            try:
                indices.append(int(ln))
            except ValueError:
                pass
        idx = min(indices) if indices else None
        logger.info("ffprobe audio indices=%s picked=%s", indices, idx)
        return idx
    except subprocess.CalledProcessError as e:
        logger.error("ffprobe failed: %s", (e.stderr or "").strip())
    except Exception as e:
        logger.exception("ffprobe exception: %s", repr(e))
    return None

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.route("/")
def index():
    streams = Stream.query.order_by(Stream.id).all()
    return render_template("index.html", streams=streams)

@app.route("/favicon.ico")
def favicon():
    # Stop noisy 404s in logs
    return "", 204

@app.route("/logs")
def logs_page():
    entries = memory_handler.get(200)
    entries = list(reversed(entries))  # newest first
    return render_template("logs.html", entries=entries)

@app.route("/logs.json")
def logs_json():
    return jsonify(memory_handler.get(200))

@app.route("/logs/clear")
def logs_clear():
    memory_handler.clear()
    return "OK", 200

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
        logger.info("Added stream id=%s name=%s", stream.id, stream.name)

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
    hls_url = stream.hls_url
    logger.info("PROCESS start stream_id=%s url=%s", stream_id, hls_url)

    sid = str(stream_id)
    tmp_frame_dir = f"/tmp/{sid}_frames"
    tmp_audio_dir = f"/tmp/{sid}_audio"
    os.makedirs(tmp_frame_dir, exist_ok=True)
    os.makedirs(tmp_audio_dir, exist_ok=True)

    # Determine current max indexes in GCS to append
    existing = list(_bucket.list_blobs(prefix=f"{sid}/"))
    existing_frames = [b.name for b in existing if b.name.endswith(".webp")]
    existing_audio = [b.name for b in existing if b.name.endswith(".mp3")]
    max_frame = _max_index(existing_frames)
    max_audio = _max_index(existing_audio)

    # Build network flags
    net_flags = _net_flags()

    # --- Frames: aim ~12s @20fps ≈ 240 frames
    frame_cmd = [
        "ffmpeg", "-y",
        *net_flags,
        "-i", hls_url,
        "-t", "12",
        "-vf", "scale=480:-1,fps=20",
        "-c:v", "libwebp", "-quality", "50", "-compression_level", "6",
        "-start_number", str(max_frame + 1),
        f"{tmp_frame_dir}/frame_%04d.webp",
    ]

    # --- Audio: detect audio stream, segment into two ~6s MP3s
    audio_index = _detect_audio_index(hls_url)
    audio_cmd = [
        "ffmpeg", "-y",
        *net_flags,
        "-i", hls_url,
        "-t", "12",
        "-vn",
        "-c:a", "libmp3lame", "-b:a", "64k", "-ar", "44100",
        "-f", "segment", "-segment_time", "6",
        "-segment_start_number", str(max_audio + 1),
        f"{tmp_audio_dir}/audio_%03d.mp3",
    ]
    if audio_index is not None:
        audio_cmd = [
            "ffmpeg", "-y",
            *net_flags,
            "-i", hls_url,
            "-t", "12",
            "-map", f"0:a:{audio_index}",
            "-vn",
            "-c:a", "libmp3lame", "-b:a", "64k", "-ar", "44100",
            "-f", "segment", "-segment_time", "6",
            "-segment_start_number", str(max_audio + 1),
            f"{tmp_audio_dir}/audio_%03d.mp3",
        ]

    # Run both; collect stderr on failure
    try:
        subprocess.run(frame_cmd, capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        err = (e.stderr or b"").decode("utf-8", "ignore")
        logger.error("FFMPEG_FRAME_ERROR: %s", err)
        shutil.rmtree(tmp_frame_dir, ignore_errors=True)
        shutil.rmtree(tmp_audio_dir, ignore_errors=True)
        return jsonify({"error": "ffmpeg frames failed", "detail": err}), 500

    try:
        subprocess.run(audio_cmd, capture_output=True, check=True)
    except subprocess.CalledProcessError as e:
        # Try a fallback without explicit -map
        err1 = (e.stderr or b"").decode("utf-8", "ignore")
        logger.warning("FFMPEG_AUDIO_PRIMARY_FAILED, retrying without -map: %s", err1)
        alt_cmd = [
            "ffmpeg", "-y",
            *net_flags,
            "-i", hls_url,
            "-t", "12",
            "-vn",
            "-c:a", "libmp3lame", "-b:a", "64k", "-ar", "44100",
            "-f", "segment", "-segment_time", "6",
            "-segment_start_number", str(max_audio + 1),
            f"{tmp_audio_dir}/audio_%03d.mp3",
        ]
        try:
            subprocess.run(alt_cmd, capture_output=True, check=True)
        except subprocess.CalledProcessError as e2:
            err2 = (e2.stderr or b"").decode("utf-8", "ignore")
            logger.error("FFMPEG_AUDIO_ERROR: %s", err2)
            shutil.rmtree(tmp_frame_dir, ignore_errors=True)
            shutil.rmtree(tmp_audio_dir, ignore_errors=True)
            return jsonify({"error": "ffmpeg audio failed", "detail": err2, "first_error": err1}), 500

    # Upload to GCS with correct Content-Type
    frame_paths = sorted(glob.glob(f"{tmp_frame_dir}/*.webp"))
    audio_paths = sorted(glob.glob(f"{tmp_audio_dir}/*.mp3"))

    uploaded_frames = 0
    uploaded_audio = 0

    for fpath in frame_paths:
        upload_to_gcs(fpath, stream_id, "frames")
        uploaded_frames += 1
    for fpath in audio_paths:
        upload_to_gcs(fpath, stream_id, "audio")
        uploaded_audio += 1

    shutil.rmtree(tmp_frame_dir, ignore_errors=True)
    shutil.rmtree(tmp_audio_dir, ignore_errors=True)

    stream.last_processed = datetime.utcnow()
    db.session.commit()

    logger.info("PROCESS end stream_id=%s uploaded_frames=%s uploaded_audio=%s", stream_id, uploaded_frames, uploaded_audio)
    return jsonify({
        "status": "OK",
        "uploaded_frames": uploaded_frames,
        "uploaded_audio": uploaded_audio
    })

# --- global error handler -----------------------------------------------------
@app.errorhandler(Exception)
def on_error(e):
    logger.exception("UNHANDLED %s %s", request.method, request.path)
    return "Internal Server Error", 500

@app.route("/health")
def health():
    return {"ok": True}, 200
