"""Safe SQLite connection and transaction primitives."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from collections.abc import Iterator


class SQLiteDatabase:
    """Owns SQLite connection policy and explicit commit/rollback semantics."""

    def __init__(self, path: Path, *, timeout_seconds: float = 30.0) -> None:
        """Create a database adapter for a project-local file path."""
        self.path = path
        self.timeout_seconds = timeout_seconds

    def connect(self) -> sqlite3.Connection:
        """Open a configured connection with WAL, FK checks, and row mappings enabled."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=self.timeout_seconds)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(f"PRAGMA busy_timeout={int(self.timeout_seconds * 1000)}")
        return connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Yield a connection that commits on success and rolls back on failure."""
        connection = self.connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
