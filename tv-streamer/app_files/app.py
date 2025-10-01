import os
import subprocess
import shutil
import glob
from datetime import datetime

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from google.cloud import storage
from sqlalchemy import create_engine
from cloud_sql_python_connector import Connector

from .models import db, Stream

# ----------------------------------------------------------------------------
# Flask app
# ----------------------------------------------------------------------------
app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "change-me")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# ----------------------------------------------------------------------------
# Cloud SQL (Postgres) via connector (pg8000)
# ----------------------------------------------------------------------------
INSTANCE_CONNECTION_NAME = os.environ.get("INSTANCE_CONNECTION_NAME")  # project:region:instance
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "Tmamarda1!")
DB_NAME = os.environ.get("DB_NAME", "postgres")

if not INSTANCE_CONNECTION_NAME:
    raise RuntimeError("INSTANCE_CONNECTION_NAME env var is required")

if not DB_PASS:
    raise RuntimeError("DB_PASS env var is required (store it in Secret Manager)")

connector = Connector()

def getconn():
    return connector.connect(
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

# ----------------------------------------------------------------------------
# Google Cloud Storage
# ----------------------------------------------------------------------------
BUCKET_NAME = os.environ.get("BUCKET_NAME")
if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME env var is required")

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)


def upload_to_gcs(file_path: str, stream_id: int, prefix: str) -> str:
    blob = bucket.blob(f"{stream_id}/{prefix}/{os.path.basename(file_path)}")
    blob.upload_from_filename(file_path)
    blob.make_public()
    return blob.public_url


def _max_index(names):
    max_idx = 0
    for name in names:
        base = os.path.basename(name)
        parts = base.split("_")
        if len(parts) < 2:
            continue
        idx_str = parts[-1].split(".")[0]
        try:
            max_idx = max(max_idx, int(idx_str))
        except ValueError:
            continue
    return max_idx


def list_assets(stream_id: int):
    prefix = f"{stream_id}/"
    blobs = list(bucket.list_blobs(prefix=prefix))
    frame_blobs = [b for b in blobs if b.name.endswith(".webp")]
    audio_blobs = [b for b in blobs if b.name.endswith(".mp3")]

    def idx(name: str) -> int:
        try:
            return int(os.path.basename(name).split("_")[-1].split(".")[0])
        except Exception:
            return 0

    frame_blobs.sort(key=lambda b: idx(b.name))
    audio_blobs.sort(key=lambda b: idx(b.name))

    frame_urls = [b.public_url for b in frame_blobs]
    audio_urls = [b.public_url for b in audio_blobs]
    return frame_urls, audio_urls


# ----------------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------------
@app.route("/")
def index():
    streams = Stream.query.order_by(Stream.id).all()
    return render_template("index.html", streams=streams)


@app.route("/get_assets/<int:stream_id>")
def get_assets_json(stream_id):
    frame_urls, audio_urls = list_assets(stream_id)
    return jsonify({"frames": frame_urls, "audios": audio_urls})


@app.route("/streams/<int:stream_id>")
def player(stream_id):
    stream = Stream.query.get_or_404(stream_id)
    frame_urls, audio_urls = list_assets(stream_id)
    return render_template("player.html", stream=stream, frames=frame_urls, audios=audio_urls)


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
    stream = Stream.query.get_or_404(stream_id)
    hls_url = stream.hls_url

    stream_id_str = str(stream_id)
    tmp_frame_dir = f"/tmp/{stream_id_str}_frames"
    tmp_audio_dir = f"/tmp/{stream_id_str}_audio"
    os.makedirs(tmp_frame_dir, exist_ok=True)
    os.makedirs(tmp_audio_dir, exist_ok=True)

    existing = list(bucket.list_blobs(prefix=f"{stream_id_str}/"))
    existing_frames = [b.name for b in existing if b.name.endswith(".webp")]
    existing_audio = [b.name for b in existing if b.name.endswith(".mp3")]

    max_frame = _max_index(existing_frames)
    max_audio = _max_index(existing_audio)

    frame_cmd = [
        "ffmpeg", "-y", "-i", hls_url,
        "-vf", "scale=480:-1,fps=20",
        "-c:v", "libwebp", "-quality", "50", "-compression_level", "6",
        "-start_number", str(max_frame + 1),
        f"{tmp_frame_dir}/frame_%04d.webp",
    ]

    audio_cmd = [
        "ffmpeg", "-y", "-i", hls_url, "-t", "30",
        "-vn", "-c:a", "libmp3lame", "-b:a", "32k",
        "-f", "segment", "-segment_time", "5",
        "-segment_start_number", str(max_audio + 1),
        f"{tmp_audio_dir}/audio_%03d.mp3",
    ]

    subprocess.run(frame_cmd, capture_output=True, check=True)
    subprocess.run(audio_cmd, capture_output=True, check=True)

    for fpath in sorted(glob.glob(f"{tmp_frame_dir}/*.webp")):
        upload_to_gcs(fpath, stream_id, "frames")
    for fpath in sorted(glob.glob(f"{tmp_audio_dir}/*.mp3")):
        upload_to_gcs(fpath, stream_id, "audio")

    shutil.rmtree(tmp_frame_dir, ignore_errors=True)
    shutil.rmtree(tmp_audio_dir, ignore_errors=True)

    stream.last_processed = datetime.utcnow()
    db.session.commit()
    return jsonify({"status": "Processed and appended new chunk."})


@app.route("/health")
def health():
    return {"ok": True}, 200
