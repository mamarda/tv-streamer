# app_files/app.py
import os
import json
import time
import shutil
import logging
import tempfile
import subprocess
from datetime import timedelta
from urllib.parse import quote

from flask import Flask, jsonify, request, redirect, url_for, render_template_string, abort

# our logging config + in-memory handler used by /logs
from .logging_setup import configure_logging, memory_handler

# ---- GCS + signing (works on Cloud Run without a key) ----
import google.auth
from google.auth.transport.requests import Request as GARequest
from google.auth import iam as ga_iam
from google.oauth2 import service_account
from google.cloud import storage

# ---- Optional: simple DB (Cloud SQL Postgres over Unix socket) ----
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

APP_NAME = "tv-streamer"
logger = configure_logging(APP_NAME)

app = Flask(__name__)

# --------------------------
# Configuration / Environment
# --------------------------
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "")
BUCKET_NAME = os.getenv("BUCKET_NAME", "")

# capture tunables
DEFAULT_CAPTURE_SECONDS = int(os.getenv("CAPTURE_SECONDS", "60"))
DEFAULT_FPS = int(os.getenv("FPS", "1"))
DEFAULT_AUDIO_SEG = int(os.getenv("AUDIO_SEGMENT_SECONDS", "10"))
DEFAULT_AUDIO_BR = os.getenv("AUDIO_BITRATE", "64k")
DEFAULT_AUDIO_CODEC = os.getenv("AUDIO_CODEC", "libopus")
DEFAULT_FRAME_SCALE = os.getenv("FRAME_SCALE", "640:-2")  # width:-2 maintains AR

# optional headers for HLS
HLS_REFERER = os.getenv("HLS_REFERER", "")
HLS_USER_AGENT = os.getenv("HLS_USER_AGENT", "")

# service account used to sign URLs (Cloud Run service account by default)
SIGNING_SERVICE_ACCOUNT = os.getenv(
    "SIGNING_SERVICE_ACCOUNT",
    os.getenv("GOOGLE_SERVICE_ACCOUNT_EMAIL", os.getenv("K_SERVICE_ACCOUNT", "")),
)
# If still empty, fall back to the Cloud Run runtime service account
if not SIGNING_SERVICE_ACCOUNT:
    SIGNING_SERVICE_ACCOUNT = os.getenv("K_SERVICE")  # best-effort


# --------------------------------
# Database (very small, streams 1..)
# --------------------------------
_engine = None


def _db_url():
    """Prefer Cloud SQL Postgres over Unix socket if configured."""
    if INSTANCE_CONNECTION_NAME and DB_USER and DB_PASS and DB_NAME:
        # Postgres default socket path on Cloud Run
        socket_dir = f"/cloudsql/{INSTANCE_CONNECTION_NAME}"
        return f"postgresql+pg8000://{DB_USER}:{quote(DB_PASS)}@/{DB_NAME}?unix_sock={quote(socket_dir)}/.s.PGSQL.5432"
    return ""


def _get_engine():
    global _engine
    if _engine is None:
        url = _db_url()
        if url:
            try:
                _engine = create_engine(url, pool_pre_ping=True)
                logger.info(
                    "DB settings loaded (user=%s, db=%s, instance=%s)",
                    DB_USER,
                    DB_NAME,
                    INSTANCE_CONNECTION_NAME,
                )
            except SQLAlchemyError as e:
                logger.error("DB engine create failed: %s", e)
                _engine = None
        else:
            logger.info("DB not configured; admin will show editable URL field only.")
    return _engine


def ensure_tables():
    eng = _get_engine()
    if not eng:
        logger.info("Ensured DB tables exist (no-op; DB not configured)")
        return
    with eng.begin() as conn:
        conn.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS streams (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL DEFAULT 'Stream',
                url TEXT NOT NULL DEFAULT ''
            )
        """
            )
        )
        # ensure row 1 exists
        row = conn.execute(text("SELECT id FROM streams WHERE id=1")).first()
        if not row:
            conn.execute(
                text("INSERT INTO streams (id,name,url) VALUES (1,'Stream 1','')")
            )
    logger.info("Ensured DB tables exist")


def get_stream_url(stream_id: int) -> str:
    eng = _get_engine()
    if not eng:
        # fallback env/default when DB missing
        return os.getenv("STREAM_URL_DEFAULT", "")
    with eng.begin() as conn:
        row = conn.execute(text("SELECT url FROM streams WHERE id=:i"), {"i": stream_id}).first()
        return row[0] if row and row[0] else ""


def save_stream_url(stream_id: int, url: str):
    eng = _get_engine()
    if not eng:
        return
    with eng.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO streams(id, name, url) VALUES(:i, :n, :u) "
                "ON CONFLICT (id) DO UPDATE SET url=EXCLUDED.url"
            ),
            {"i": stream_id, "n": f"Stream {stream_id}", "u": url},
        )


# -----------------------
# GCS helpers (signed URL)
# -----------------------
_gcs_client = None
_signing_credentials = None


def _gcs():
    global _gcs_client
    if _gcs_client is None:
        _gcs_client = storage.Client()
    return _gcs_client


def _build_signing_credentials():
    """
    Create a 'signing' credentials object using IAM Credentials API.
    This lets us sign URLs without having a private key file.
    """
    global _signing_credentials
    if _signing_credentials is not None:
        return _signing_credentials

    base_creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    # When running on Cloud Run, base_creds is a Compute Engine creds (no private key).
    # Wrap with an IAM Signer so google-cloud-storage can sign V4 URLs.
    signer = ga_iam.Signer(GARequest(), base_creds, SIGNING_SERVICE_ACCOUNT)
    _signing_credentials = service_account.Credentials(
        signer=signer,
        service_account_email=SIGNING_SERVICE_ACCOUNT,
        token_uri=base_creds.token_uri,
    )
    return _signing_credentials


def upload_to_gcs(fpath: str, stream_id: int, kind: str):
    # kind is "frames" or "audio"
    bucket = _gcs().bucket(BUCKET_NAME)
    name = os.path.basename(fpath)
    blob = bucket.blob(f"{stream_id}/{kind}/{name}")
    blob.upload_from_filename(fpath)  # do NOT make_public; we use signed URLs
    logger.debug("Uploaded %s to %s/%s", name, BUCKET_NAME, blob.name)


def _sign(object_name: str, content_type: str, minutes: int = 60) -> str:
    """
    V4 signed URL using IAM Signer (no private key in container).
    """
    creds = _build_signing_credentials()
    bucket = _gcs().bucket(BUCKET_NAME)
    blob = bucket.blob(object_name)

    url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=minutes),
        method="GET",
        response_type=content_type,
        credentials=creds,
    )
    return url


def _list_signed(stream_id: int):
    """
    List frame and audio objects for stream and return signed URLs.
    """
    bucket = _gcs().bucket(BUCKET_NAME)

    frames_prefix = f"{stream_id}/frames/"
    audio_prefix = f"{stream_id}/audio/"

    # list frames
    frames = [
        b.name for b in bucket.list_blobs(prefix=frames_prefix)
        if b.name.endswith(".webp")
    ]
    frames.sort()

    # list audio
    audios = [
        b.name for b in bucket.list_blobs(prefix=audio_prefix)
        if b.name.endswith((".ogg", ".mp3", ".m4a"))
    ]
    audios.sort()

    frame_urls = [_sign(n, "image/webp") for n in frames]
    # Pick content-type to match extension for best playback
    def _ctype(n: str) -> str:
        if n.endswith(".ogg"):
            return "audio/ogg"
        if n.endswith(".mp3"):
            return "audio/mpeg"
        if n.endswith(".m4a"):
            return "audio/mp4"
        return "application/octet-stream"

    audio_urls = [_sign(n, _ctype(n)) for n in audios]

    logger.info(
        "Listed assets stream_id=%s frames=%d audio=%d",
        stream_id,
        len(frame_urls),
        len(audio_urls),
    )
    return frame_urls, audio_urls


# ----------------------
# FFprobe / FFmpeg logic
# ----------------------
def _header_args_for_hls():
    header_lines = []
    if HLS_REFERER:
        header_lines.append(f"Referer: {HLS_REFERER}")
    if HLS_USER_AGENT:
        header_lines.append(f"User-Agent: {HLS_USER_AGENT}")
    blob = "\\r\\n".join(header_lines) + "\\r\\n" if header_lines else ""
    return (["-headers", blob] if blob else []), blob


def _ffprobe_has_audio(url: str, header_args: list[str]) -> bool:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        *header_args,
        "-select_streams",
        "a",
        "-show_streams",
        "-of",
        "json",
        url,
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=25)
        info = json.loads(out.decode("utf-8", "ignore"))
        return bool(info.get("streams"))
    except Exception as e:
        logger.warning("FFPROBE failed (%s); assuming no audio", e)
        return False


def capture_stream(stream_id: int, url: str, seconds: int | None = None) -> tuple[int, int]:
    """
    Capture frames (webp sequence) and optional audio segments.
    Returns (uploaded_frames, uploaded_audio_segments).
    """
    cap_secs = int(seconds or DEFAULT_CAPTURE_SECONDS)
    fps = DEFAULT_FPS
    seg = DEFAULT_AUDIO_SEG
    abitrate = DEFAULT_AUDIO_BR
    acodec = DEFAULT_AUDIO_CODEC
    scale = DEFAULT_FRAME_SCALE

    header_args, _ = _header_args_for_hls()
    has_audio = _ffprobe_has_audio(url, header_args)
    logger.info("PROBE stream_id=%s audio_present=%s", stream_id, has_audio)

    tmp = tempfile.mkdtemp(prefix=f"cap_{stream_id}_")
    frames_dir = os.path.join(tmp, "frames")
    audio_dir = os.path.join(tmp, "audio")
    os.makedirs(frames_dir, exist_ok=True)
    if has_audio:
        os.makedirs(audio_dir, exist_ok=True)

    # ffmpeg common
    common = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "warning",
        *header_args,
        "-i",
        url,
        "-t",
        str(cap_secs),
    ]

    # video output (place per-output options before that sink)
    vid_part = [
        "-map",
        "0:v:0",
        "-vf",
        f"fps={fps},scale={scale}",
        "-start_number",
        "1",
        "-f",
        "image2",
        os.path.join(frames_dir, "frame_%04d.webp"),
    ]

    if has_audio:
        aud_part = [
            "-map",
            "0:a:0",
            "-c:a",
            acodec,
            "-b:a",
            abitrate,
            "-ar",
            "48000",
            "-ac",
            "1",
            "-f",
            "segment",
            "-segment_time",
            str(seg),
            "-reset_timestamps",
            "1",
            os.path.join(audio_dir, "chunk_%03d.ogg"),
        ]
        cmd = [*common, *vid_part, *aud_part]
    else:
        cmd = [*common, *vid_part]

    logger.info("FFMPEG start stream_id=%s", stream_id)
    proc = subprocess.run(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    if proc.returncode != 0:
        logger.error("FFMPEG_ERROR rc=%s out=%s", proc.returncode, proc.stdout[:4000])

    # upload produced media
    up_frames = 0
    up_audio = 0
    for name in sorted(os.listdir(frames_dir)):
        if not name.endswith(".webp"):
            continue
        fpath = os.path.join(frames_dir, name)
        upload_to_gcs(fpath, stream_id, "frames")
        up_frames += 1

    if has_audio and os.path.isdir(audio_dir):
        for name in sorted(os.listdir(audio_dir)):
            if not name.endswith((".ogg", ".mp3", ".m4a")):
                continue
            fpath = os.path.join(audio_dir, name)
            upload_to_gcs(fpath, stream_id, "audio")
            up_audio += 1

    shutil.rmtree(tmp, ignore_errors=True)
    logger.info(
        "PROCESS end stream_id=%s uploaded_frames=%s uploaded_audio=%s",
        stream_id,
        up_frames,
        up_audio,
    )
    return up_frames, up_audio


# --------------
# Flask endpoints
# --------------
@app.route("/health")
def health():
    return "ok", 200


@app.route("/favicon.ico")
def favicon():
    # stop the repetitive 404 stacktraces
    return ("", 204)


ADMIN_HTML = """
<!doctype html>
<title>TV Streamer Admin</title>
<style>
body{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px;}
input[type=text]{width:560px;padding:8px;}
button{padding:8px 14px;cursor:pointer;}
a{color:#0a58ca;text-decoration:none;margin-right:10px;}
small{color:#666}
</style>
<h2>Admin</h2>
<p>
  <a href="{{ url_for('logs_page') }}">Logs</a>
  <a href="{{ url_for('player', stream_id=1) }}">Player</a>
</p>
<form method="post">
  <label><b>Stream URL (id=1)</b></label><br>
  <input type="text" name="url" value="{{ url }}" placeholder="https://example.com/master.m3u8">
  <button type="submit">Save</button>
</form>
<p>
  <a href="{{ url_for('process_stream', stream_id=1) }}"><button>Process (capture {{cap}}s)</button></a>
  <small>FPS={{fps}}, Audio segment={{seg}}s</small>
</p>
"""


@app.route("/admin", methods=["GET", "POST"])
def admin():
    ensure_tables()
    url = get_stream_url(1)
    if request.method == "POST":
        new_url = request.form.get("url", "").strip()
        if new_url:
            save_stream_url(1, new_url)
            url = new_url
    return render_template_string(
        ADMIN_HTML, url=url, cap=DEFAULT_CAPTURE_SECONDS, fps=DEFAULT_FPS, seg=DEFAULT_AUDIO_SEG
    )


@app.route("/process/<int:stream_id>")
def process_stream(stream_id: int):
    url = get_stream_url(stream_id)
    if not url:
        abort(400, "Stream URL is empty. Set it in /admin first.")
    logger.info("PROCESS start stream_id=%s url=%s", stream_id, url)
    try:
        capture_stream(stream_id, url)
    except Exception as e:
        logger.exception("PROCESS failed stream_id=%s: %s", stream_id, e)
        abort(500, f"Processing failed: {e}")
    return redirect(url_for("player", stream_id=stream_id))


@app.route("/get_assets/<int:stream_id>")
def get_assets_json(stream_id: int):
    frames, audios = _list_signed(stream_id)
    return jsonify({"frames": frames, "audios": audios})


PLAYER_HTML = """
<!doctype html>
<title>TV Streamer Player</title>
<style>
body{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:20px;}
#frame{max-width:90vw;max-height:75vh;display:block;border:1px solid #ddd;border-radius:8px}
.ctrl{margin:12px 0}
small{color:#666}
</style>
<h2>Player (stream {{stream_id}})</h2>
<p>
  <a href="{{ url_for('admin') }}">Admin</a>
  <a href="{{ url_for('logs_page') }}">Logs</a>
</p>
<div class="ctrl">
  <button id="btn-audio">Start audio</button>
  <small id="meta"></small>
</div>
<img id="frame" src="" alt="frame">
<audio id="audio" controls preload="none"></audio>

<script>
const streamId = {{stream_id}};
let frames = [];
let audios = [];
let idx = 0;
let playing = false;

const img = document.getElementById('frame');
const meta = document.getElementById('meta');
const btn = document.getElementById('btn-audio');
const audioEl = document.getElementById('audio');

btn.onclick = () => {
  if (!audios.length) { alert('No audio segments found (source may be video-only).'); return; }
  if (!playing) { playing = true; playAudioQueue(); btn.textContent='Stop audio'; }
  else { playing = false; audioEl.pause(); btn.textContent='Start audio'; }
};

function playAudioQueue() {
  if (!playing) return;
  if (!audios.length) return;
  let current = 0;
  function next() {
    if (!playing) return;
    current = (current + 1) % audios.length;
    audioEl.src = audios[current];
    audioEl.play().catch(()=>{});
  }
  audioEl.onended = next;
  audioEl.onerror = next;
  audioEl.src = audios[0];
  audioEl.play().catch(()=>{});
}

async function refresh() {
  try {
    const res = await fetch(`/get_assets/${streamId}`);
    const j = await res.json();
    frames = j.frames || [];
    audios = j.audios || [];
    meta.textContent = `frames=${frames.length} audio_segments=${audios.length}`;
  } catch(e) {
    console.error(e);
  }
}
function tick() {
  if (frames.length) {
    idx = (idx + 1) % frames.length;
    img.src = frames[idx];
  }
}
refresh();
setInterval(refresh, 5000); // poll for new assets
setInterval(tick, 1000/ {{fps}} ); // show at ~fps
</script>
"""

@app.route("/streams/<int:stream_id>")
def player(stream_id: int):
    frames, audios = _list_signed(stream_id)  # for initial count in logs
    return render_template_string(PLAYER_HTML, stream_id=stream_id, fps=max(1, DEFAULT_FPS))


LOGS_HTML = """
<!doctype html>
<title>Recent Logs</title>
<style>
body{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:24px;}
table{border-collapse:collapse;width:100%;}
th,td{border:1px solid #eee;padding:6px 8px;font-size:13px;vertical-align:top}
th{background:#fafafa;text-align:left}
.level-ERROR{color:#c00;font-weight:600}
.level-INFO{color:#333}
pre{white-space:pre-wrap;margin:0}
</style>
<h2>Recent Logs</h2>
<p><small>Newest first. This is per-container memory; refresh to update.</small></p>
<table>
  <thead><tr><th>Time (epoch)</th><th>Level</th><th>Logger</th><th>Message</th></tr></thead>
  <tbody>
    {% for rec in records %}
      <tr>
        <td>{{ rec.created }}</td>
        <td class="level-{{ rec.levelname }}">{{ rec.levelname }}</td>
        <td>{{ rec.name }}</td>
        <td><pre>{{ rec.getMessage() }}</pre></td>
      </tr>
    {% endfor %}
  </tbody>
</table>
"""

@app.route("/logs")
def logs_page():
    # newest first
    records = list(getattr(memory_handler, "buffer", []))[-500:]
    records.reverse()
    return render_template_string(LOGS_HTML, records=records)


# ----------------
# App entry / init
# ----------------
with app.app_context():
    logger.info("Using bucket=%s", BUCKET_NAME)
    ensure_tables()


# -----------
# WSGI target
# -----------
# gunicorn command should point to "app_files.app:app"
# Example: gunicorn -b :8080 -w 2 app_files.app:app
