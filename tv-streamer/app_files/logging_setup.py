import logging
import sys
import os
import threading
import re
from datetime import datetime
from collections import deque

# --- redaction helpers --------------------------------------------------------

def _build_redaction_patterns() -> list[tuple[re.Pattern, str]]:
    pats: list[tuple[re.Pattern, str]] = []

    # redact known env var values if present in logs
    for k in ("DB_PASS", "SECRET_KEY"):
        v = os.getenv(k)
        if v:
            try:
                pats.append((re.compile(re.escape(v)), f"[REDACTED_{k}]"))
            except re.error:
                pass

    # redact generic key/value tokens in URLs or text
    generic = [
        r"(?i)(token|sig|signature|key|pass|password)=([^\s&#]+)",
        r"(?i)Authorization:\s*Bearer\s+[A-Za-z0-9\-\._~\+\/]+=*",
    ]
    for pat in generic:
        pats.append((re.compile(pat), r"\1=[REDACTED]"))

    return pats


class MemoryLogHandler(logging.Handler):
    """
    In-memory ring buffer of recent log lines.
    NOTE: Per-instance
