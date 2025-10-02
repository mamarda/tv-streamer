import logging
import sys
import os
import threading
import re
from collections import deque

# Build redaction patterns so secrets never appear in the in-app log view.
def _build_redaction_patterns():
    patterns = []

    # redact specific env values if present
    for key in ("DB_PASS", "SECRET_KEY"):
        val = os.getenv(key)
        if val:
            try:
                patterns.append((re.compile(re.escape(val)), f"[REDACTED_{key}]"))
            except re.error:
                pass

    # redact generic tokens in query strings/headers
    patterns.append(
        (re.compile(r"(?i)(token|sig|signature|key|pass|password)=([^\s&#]+)"),
         r"\1=[REDACTED]")
    )
    patterns.append(
        (re.compile(r"(?i)Authorization:\s*Bearer\s+[A-Za-z0-9\-\._~\+\/]+=*"),
         "Authorization: Bearer [REDACTED]")
    )
    return patterns


class MemoryLogHandler(logging.Handler):
    """In-memory ring buffer of recent log lines (per container instance)."""
    def __init__(self, capacity: int = 800,
                 fmt: str = "%(asctime)s %(levelname)s %(name)s %(message)s"):
        super().__init__()
        self._buf = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._fmt = fmt
        self._patterns = _build_redaction_patterns()
        self.setFormatter(logging.Formatter(self._fmt))

    def _sanitize(self, text: str) -> str:
        out = text
        for pat, repl in self._patterns:
            out = pat.sub(repl, out)
        return out

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            msg = self._sanitize(msg)
            item = {
                "ts": record.created,          # epoch seconds
                "level": record.levelname,
                "logger": record.name,
                "message": msg,
            }
            with self._lock:
                self._buf.append(item)
        except Exception:
            # logging must never raise
            pass

    def get(self, limit: int | None = None):
        with self._lock:
            data = list(self._buf)
        return data[-limit:] if limit else data

    def clear(self) -> None:
        with self._lock:
            self._buf.clear()


# exported singleton used by the app
memory_handler = MemoryLogHandler()


def configure_logging() -> None:
    """Send logs to stdout (Cloud Logging) AND to the in-memory buffer."""
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    stream = logging.StreamHandler(stream=sys.stdout)
    stream.setFormatter(logging.Formatter(fmt))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(stream)
    root.addHandler(memory_handler)
    root.setLevel(level)

    # keep gunicorn visible
    logging.getLogger("gunicorn.error").setLevel(level)
    logging.getLogger("gunicorn.access").setLevel(level)
