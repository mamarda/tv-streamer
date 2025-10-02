import logging
import sys
import os

def configure_logging() -> None:
    """
    Send structured app logs to stdout so Cloud Run/Cloud Logging capture them.
    No extra GCP libraries needed.
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    fmt = "%(asctime)s %(levelname)s %(name)s %(message)s"
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(fmt))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Make sure Gunicorn loggers are not overly chatty but still visible
    logging.getLogger("gunicorn.error").setLevel(level)
    logging.getLogger("gunicorn.access").setLevel(level)
