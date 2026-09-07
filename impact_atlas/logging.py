"""Structured logging setup shared by command-line entrypoints."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, UTC
from pathlib import Path


class JsonFormatter(logging.Formatter):
    """Render operational logs as compact JSON records suitable for aggregation."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize a log record while preserving exception details when present."""
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(name: str, log_path: Path, *, verbose: bool = False) -> logging.Logger:
    """Configure an idempotent JSON file logger and human-readable console logger."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger

    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(JsonFormatter())

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s | %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
