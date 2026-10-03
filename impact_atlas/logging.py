"""Bounded, credential-aware logging for local application processes."""

from __future__ import annotations

import json
import copy
import logging
import os
import re
import sys
import threading
from collections.abc import Iterable
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TYPE_CHECKING, TextIO
from urllib.parse import urlsplit

_CONFIGURATION_LOCK = threading.RLock()
_SECRET_KEY = re.compile(r"(?:API_?KEY|TOKEN|PASSWORD|PASSWD|SECRET|AUTHORIZATION)$", re.IGNORECASE)
_CREDENTIAL = re.compile(
    r"\b(api[_-]?key|access[_-]?token|refresh[_-]?token|password|passwd|client_secret|secret|token)"
    r"([\"']?\s*[:=]\s*)(\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\s,;&}]+)", re.IGNORECASE
)
_AUTHORIZATION = re.compile(r"\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_URL = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_REDACTED = "[REDACTED]"


class CredentialRedactor:
    """Remove configured secrets, credential fields and URL payloads from log text.

    This is defense in depth, not a general PII detector. Callers must still
    avoid logging raw records, credentials, or confidential request bodies.
    """

    def __init__(self, secrets: Iterable[str] = ()) -> None:
        """Capture explicit and currently configured environment credentials."""
        values = set(secrets)
        values.update(value for key, value in os.environ.items() if _SECRET_KEY.search(key))
        self._secrets = tuple(sorted((value for value in values if value), key=len, reverse=True))

    @staticmethod
    def _redact_url(match: re.Match[str]) -> str:
        """Keep the provider hostname while removing userinfo, path and query."""
        try:
            parsed = urlsplit(match.group())
            host = parsed.hostname
            if host:
                return f"{parsed.scheme}://{host}/{_REDACTED}"
        except ValueError:
            pass
        return "[REDACTED URL]"

    def redact(self, text: str) -> str:
        """Redact message/traceback text and cap the rendered field length."""
        text = _URL.sub(self._redact_url, text)
        text = _AUTHORIZATION.sub(lambda match: f"{match[1]} {_REDACTED}", text)
        text = _CREDENTIAL.sub(lambda match: f"{match[1]}{match[2]}{_REDACTED}", text)
        for value in self._secrets:
            text = text.replace(value, _REDACTED)
        limit = 32768
        return text if len(text) <= limit else text[:limit] + " [TRUNCATED]"


class JsonFormatter(logging.Formatter):
    """Render structured log records without exposing credential values."""

    def __init__(self, redactor: CredentialRedactor | None = None) -> None:
        """Use an explicit redactor or capture current environment credentials."""
        super().__init__()
        self._redactor = redactor or CredentialRedactor()

    def format(self, record: logging.LogRecord) -> str:
        """Serialize without mutating the record shared by other handlers."""
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": self._redactor.redact(record.getMessage()),
        }
        if record.exc_info:
            payload["exception"] = self._redactor.redact(self.formatException(record.exc_info))
        elif record.exc_text:
            payload["exception"] = self._redactor.redact(record.exc_text)
        if record.stack_info:
            payload["stack"] = self._redactor.redact(record.stack_info)
        return json.dumps(payload, ensure_ascii=False)


class RedactedConsoleFormatter(logging.Formatter):
    """Apply the same credential policy to human-readable console output."""

    def __init__(self, redactor: CredentialRedactor) -> None:
        """Bind a shared redactor to the console format."""
        super().__init__("%(asctime)s %(levelname)s %(name)s | %(message)s")
        self._redactor = redactor

    def format(self, record: logging.LogRecord) -> str:
        """Redact the fully formatted output, including any cached traceback."""
        return self._redactor.redact(super().format(copy.copy(record)))


class _OwnedFileHandler(RotatingFileHandler):
    """Identify file handlers managed by configure_logging."""


if TYPE_CHECKING:
    _ConsoleBase = logging.StreamHandler[TextIO]
else:
    _ConsoleBase = logging.StreamHandler


class _OwnedConsoleHandler(_ConsoleBase):
    """Identify console handlers managed by configure_logging."""


def configure_logging(
    name: str,
    log_path: Path,
    *,
    verbose: bool = False,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 3,
    secrets: Iterable[str] = (),
) -> logging.Logger:
    """Configure rotating file and console handlers without accumulating duplicates.

    Only this function's handlers are replaced; caller-installed handlers are
    left alone and must supply their own redaction policy. Rotation is intended
    for one process per log file, not concurrent processes sharing a file.
    """
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer")
    if isinstance(backup_count, bool) or not isinstance(backup_count, int) or backup_count <= 0:
        raise ValueError("backup_count must be a positive integer")
    path = log_path.resolve()
    level = logging.DEBUG if verbose else logging.INFO
    redactor = CredentialRedactor(secrets)
    with _CONFIGURATION_LOCK:
        logger = logging.getLogger(name)
        owned = [h for h in logger.handlers if isinstance(h, (_OwnedFileHandler, _OwnedConsoleHandler))]
        existing_file = next((h for h in owned if isinstance(h, _OwnedFileHandler)), None)
        if (len(owned) != 2 or existing_file is None or Path(existing_file.baseFilename) != path
                or existing_file.maxBytes != max_bytes or existing_file.backupCount != backup_count):
            path.parent.mkdir(parents=True, exist_ok=True)
            # Create the replacement before detaching working handlers, so an
            # unwritable destination does not disable the existing logger.
            file_handler = _OwnedFileHandler(path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
            console_handler = _OwnedConsoleHandler(sys.stdout)
            for handler in owned:
                logger.removeHandler(handler)
                handler.close()
            owned = [file_handler, console_handler]
            for handler in owned:
                logger.addHandler(handler)
        for handler in owned:
            handler.setLevel(level)
            handler.setFormatter(JsonFormatter(redactor) if isinstance(handler, _OwnedFileHandler)
                                 else RedactedConsoleFormatter(redactor))
        logger.setLevel(level)
        logger.propagate = False
        return logger
