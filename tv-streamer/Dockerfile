FROM python:3.12-slim

# OS deps for ffmpeg and webp
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg libwebp-dev \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app and templates
COPY app_files/ app_files/
COPY templates/ templates/

# Expose for Cloud Run
ENV PORT=8080

# Start with gunicorn; use module path (package import style)
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app_files.app:app"]
