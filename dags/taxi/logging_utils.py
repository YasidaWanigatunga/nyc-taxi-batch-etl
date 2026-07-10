from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Render every log record as one line of JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "context"):
            payload["context"] = record.context
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers: # guard against duplicate handlers
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


@contextmanager
def log_stage(logger: logging.Logger, stage: str, **context):
    """Log START / SUCCESS / FAILED for a stage, with duration. Re-raises."""
    started = time.perf_counter()
    logger.info(f"{stage} :: START", extra={"context": context})
    try:
        yield
    except Exception as exc:
        logger.error(
            f"{stage} :: FAILED",
            exc_info=True,
            extra={"context": {**context, "error": str(exc)}},
        )
        raise # never swallow — Airflow must see it
    else:
        elapsed = round(time.perf_counter() - started, 3)
        logger.info(
            f"{stage} :: SUCCESS",
            extra={"context": {**context, "duration_seconds": elapsed}},
        )