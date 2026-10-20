"""General-purpose safeguards for writing text files and spreadsheet CSV cells."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO


def csv_safe_value(value: object) -> object:
    """Return a CSV cell that treats formula-like source strings as literal text.

    Numeric Python values remain numeric. Formula-looking strings, including
    those prefixed by whitespace or a BOM, gain a leading apostrophe. This is
    intended for spreadsheet-facing CSV and changes the raw string value.
    """
    if not isinstance(value, str):
        return value
    candidate = value.lstrip("\ufeff \t\r\n")
    if value.startswith(("\t", "\r", "\n")) or candidate.startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


@contextmanager
def atomic_text_writer(path: Path, *, newline: str | None = None) -> Iterator[IO[str]]:
    """Replace one UTF-8 file only after the entire write succeeds.

    A sibling temporary file ensures replacement stays on one filesystem.
    Serialization, flush or replacement failures preserve the previous file.
    This is single-file atomic visibility, not a multi-file transaction or a
    guarantee of directory-entry durability after sudden power loss. A hard
    process kill may leave a temporary file. Destination paths are trusted
    caller configuration, not arbitrary user-supplied paths.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pending_path: Path | None = None
    original_error: BaseException | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline=newline, dir=path.parent,
            prefix=f".{path.name}.", suffix=".tmp", delete=False,
        ) as stream:
            pending_path = Path(stream.name)
            yield stream.file
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pending_path, path)
        pending_path = None
    except BaseException as error:
        original_error = error
        raise
    finally:
        if pending_path is not None:
            try:
                pending_path.unlink(missing_ok=True)
            except OSError as cleanup_error:
                if original_error is None:
                    raise
                original_error.add_note(f"Temporary-file cleanup also failed: {type(cleanup_error).__name__}")
