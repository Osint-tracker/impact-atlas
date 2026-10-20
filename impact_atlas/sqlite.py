"""Safe SQLite connection and transaction primitives."""

from __future__ import annotations

import sqlite3
import math
from contextlib import contextmanager
from pathlib import Path
from collections.abc import Iterator


class SQLiteDatabase:
    """Owns SQLite connection policy and explicit commit/rollback semantics."""

    def __init__(self, path: Path, *, timeout_seconds: float = 30.0) -> None:
        """Validate lock timeout before acquiring any filesystem resources."""
        if (isinstance(timeout_seconds, bool) or not math.isfinite(timeout_seconds)
                or not 0 < timeout_seconds <= (2**31 - 1) / 1000):
            raise ValueError("timeout_seconds must be finite, positive and fit SQLite's millisecond timeout.")
        self.path = path
        self.timeout_seconds = timeout_seconds
        self._busy_timeout_ms = max(1, math.ceil(timeout_seconds * 1000))

    def connect(self) -> sqlite3.Connection:
        """Open a configured connection with WAL, FK checks, and row mappings enabled."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=self.timeout_seconds)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        except BaseException as error:
            self._close(connection, error)
            raise
        return connection

    @staticmethod
    def _close(connection: sqlite3.Connection, original_error: BaseException | None) -> None:
        """Close deterministically, preserving an earlier failure if cleanup also fails."""
        try:
            connection.close()
        except sqlite3.Error as cleanup_error:
            if original_error is None:
                raise
            original_error.add_note(f"SQLite close also failed: {type(cleanup_error).__name__}")

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        """Own a configured connection; closing rolls back any uncommitted work."""
        connection = self.connect()
        original_error: BaseException | None = None
        try:
            yield connection
        except BaseException as error:
            original_error = error
            raise
        finally:
            self._close(connection, original_error)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Commit on success; roll back data and DDL on errors or cancellation.

        Callers must not issue their own COMMIT, ROLLBACK or executescript inside
        this scope, since those operations can end the owned transaction early.
        """
        with self.connection() as connection:
            try:
                connection.execute("BEGIN")
                yield connection
                connection.commit()
            except BaseException as error:
                try:
                    connection.rollback()
                except sqlite3.Error as rollback_error:
                    error.add_note(f"SQLite rollback also failed: {type(rollback_error).__name__}")
                raise
