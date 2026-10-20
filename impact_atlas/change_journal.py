"""Transactional undo for bounded SQLite row changes, with conflict detection.

The caller owns the transaction. This module never commits, selects records to
combine, or interprets their contents. Snapshots stay in the private database.
"""

from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import sqlite3
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TypeAlias

SQLValue: TypeAlias = str | int | float | bytes | None
Snapshot: TypeAlias = dict[str, dict[str, SQLValue]]
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_CHANGE_ID = re.compile(r"[0-9a-f]{32}\Z")


class JournalConflict(ValueError):
    """The stored change cannot be restored without risking data loss."""


class ChangeNotFound(LookupError):
    """No recorded change exists for this resource and identifier."""


@dataclass(frozen=True)
class UndoResult:
    """Stable undo result, including an idempotent repeated request."""

    change_id: str
    record_count: int
    already_reverted: bool


@dataclass(frozen=True)
class ChangeSummary:
    """Recovery metadata without duplicating source text into API responses."""

    change_id: str
    record_count: int
    created_at: str
    reverted_at: str | None


def _identifier(value: str) -> str:
    """Quote a conservative SQL identifier; identifiers are trusted configuration."""
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError("Invalid journal resource identifier")
    return '"' + value + '"'


def _encode(value: SQLValue) -> object:
    """Preserve binary values without pickle or executable deserialization."""
    if isinstance(value, bytes):
        return {"bytes": base64.b64encode(value).decode("ascii")}
    if isinstance(value, float) and not math.isfinite(value):
        raise JournalConflict("Non-finite database value cannot be journaled")
    return value


def _decode(value: object) -> SQLValue:
    """Reject malformed journal values before any restoration writes."""
    if isinstance(value, dict) and set(value) == {"bytes"} and isinstance(value["bytes"], str):
        return base64.b64decode(value["bytes"], validate=True)
    if value is None or (isinstance(value, (str, int, float)) and not isinstance(value, bool)):
        if isinstance(value, float) and not math.isfinite(value):
            raise JournalConflict("Non-finite journal value")
        return value
    raise JournalConflict("Unsupported journal value")


def _fingerprint(row: dict[str, SQLValue]) -> str:
    """Hash every column so unrelated edits and schema changes block stale undo."""
    encoded = {name: _encode(value) for name, value in row.items()}
    return hashlib.sha256(json.dumps(encoded, sort_keys=True, allow_nan=False).encode("utf-8")).hexdigest()


class RowChangeJournal:
    """Journal selected columns while detecting changes across the whole row.

    Use within BEGIN IMMEDIATE on the same connection as the mutation. Keep
    network requests outside that transaction. Reverted entries are retained;
    journal retention and database backups are the application's responsibility.
    """

    def __init__(self, connection: sqlite3.Connection, *, table: str,
                 key: str, columns: tuple[str, ...]) -> None:
        """Bind to a resource; never accept table/column names from HTTP input."""
        self.connection = connection
        self.table = table
        self.key = key
        self.columns = columns
        self._table = _identifier(table)
        self._key = _identifier(key)
        if not columns or len(set(columns)) != len(columns) or key in columns:
            raise ValueError("Journal columns must be unique and exclude the record key")
        for column in columns:
            _identifier(column)

    def _require_transaction(self) -> None:
        """Prevent separating data updates from their recovery record."""
        if not self.connection.in_transaction:
            raise RuntimeError("Row changes and journal writes require one explicit transaction")

    def _ensure_schema(self) -> None:
        """Create the independently versioned journal inside the caller's transaction."""
        self._require_transaction()
        self.connection.execute("""
            CREATE TABLE IF NOT EXISTS _atlas_row_changes (
                change_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                resource TEXT NOT NULL,
                key_column TEXT NOT NULL,
                columns_json TEXT NOT NULL,
                before_json TEXT NOT NULL,
                after_hashes_json TEXT NOT NULL,
                record_count INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                reverted_at TEXT
            )
        """)
        self.connection.execute("""
            CREATE INDEX IF NOT EXISTS _atlas_row_changes_recent
            ON _atlas_row_changes (resource, created_at DESC, change_id DESC)
        """)

    def history(self, *, limit: int = 50) -> list[ChangeSummary]:
        """Read a bounded recent history; never create schema during a read request."""
        if not 1 <= limit <= 200:
            raise ValueError("History limit must be 1..200")
        exists = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_atlas_row_changes'"
        ).fetchone()
        if not exists:
            return []
        rows = self.connection.execute("""
            SELECT change_id, record_count, created_at, reverted_at
            FROM _atlas_row_changes WHERE resource = ?
            ORDER BY created_at DESC, change_id DESC LIMIT ?
        """, (self.table, limit)).fetchall()
        return [ChangeSummary(*row) for row in rows]

    def capture(self, keys: Sequence[str]) -> Snapshot:
        """Read bounded, distinct records and fail if any requested row is missing."""
        self._require_transaction()
        if (not 1 <= len(keys) <= 500 or any(not isinstance(key, str) or not key for key in keys)
                or len(set(keys)) != len(keys)):
            raise JournalConflict("Expected 1..500 distinct record keys")
        placeholders = ",".join("?" for _ in keys)
        # Identifiers pass a strict allowlist; all record keys are bound values.
        cursor = self.connection.execute(
            f"SELECT * FROM {self._table} WHERE {self._key} IN ({placeholders})", keys  # noqa: S608
        )
        names = [column[0] for column in cursor.description]
        result: Snapshot = {}
        for raw in cursor.fetchall():
            row = dict(zip(names, raw, strict=True))
            key = row.get(self.key)
            if not isinstance(key, str) or not all(column in row for column in self.columns):
                raise JournalConflict("Database resource schema changed")
            result[key] = row
        if len(result) != len(keys):
            raise JournalConflict("A journaled record is missing")
        return result

    def record(self, before: Snapshot) -> str:
        """Save recovery values and fingerprints after a successful in-transaction write."""
        self._ensure_schema()
        after = self.capture(list(before))
        for key, row in before.items():
            if set(row) != set(after[key]) or any(
                row[name] != after[key][name] for name in row if name not in self.columns
            ):
                raise JournalConflict("Mutation changed columns outside the declared journal boundary")
        originals = {key: {name: _encode(row[name]) for name in self.columns} for key, row in before.items()}
        fingerprints = {key: _fingerprint(row) for key, row in after.items()}
        change_id = uuid.uuid4().hex
        self.connection.execute("""
            INSERT INTO _atlas_row_changes
            (change_id, version, resource, key_column, columns_json, before_json,
             after_hashes_json, record_count, created_at)
            VALUES (?, 1, ?, ?, ?, ?, ?, ?, ?)
        """, (change_id, self.table, self.key, json.dumps(self.columns),
              json.dumps(originals, allow_nan=False), json.dumps(fingerprints),
              len(before), datetime.now(UTC).isoformat()))
        return change_id

    def restore(self, change_id: str) -> UndoResult:
        """Restore all recorded columns atomically, rejecting later edits or missing rows."""
        self._require_transaction()
        if not _CHANGE_ID.fullmatch(change_id):
            raise ChangeNotFound("Unknown change identifier")
        self._ensure_schema()
        entry = self.connection.execute("""
            SELECT version, key_column, columns_json, before_json, after_hashes_json,
                   record_count, reverted_at FROM _atlas_row_changes
            WHERE change_id = ? AND resource = ?
        """, (change_id, self.table)).fetchone()
        if entry is None:
            raise ChangeNotFound("No recovery history exists for this change")
        try:
            version, key_column, columns_json, before_json, hashes_json, count, reverted_at = entry
            if version != 1 or key_column != self.key or json.loads(columns_json) != list(self.columns):
                raise JournalConflict("Unsupported journal schema or resource")
            originals = json.loads(before_json)
            fingerprints = json.loads(hashes_json)
            if (not isinstance(originals, dict) or not isinstance(fingerprints, dict)
                    or set(originals) != set(fingerprints) or len(originals) != count
                    or not 1 <= count <= 500):
                raise JournalConflict("Invalid journal record set")
            decoded: Snapshot = {}
            for key, values in originals.items():
                if (not isinstance(key, str) or not key or not isinstance(values, dict)
                        or set(values) != set(self.columns) or not isinstance(fingerprints[key], str)
                        or not _DIGEST.fullmatch(fingerprints[key])):
                    raise JournalConflict("Invalid journal record")
                decoded[key] = {name: _decode(value) for name, value in values.items()}
            if reverted_at is not None:
                return UndoResult(change_id, count, already_reverted=True)
            current = self.capture(list(decoded))
            if any(_fingerprint(current[key]) != fingerprints[key] for key in decoded):
                raise JournalConflict("Records changed after this operation; automatic undo was refused")
        except (TypeError, ValueError, KeyError) as error:
            if isinstance(error, JournalConflict):
                raise
            raise JournalConflict("Invalid recovery history") from error

        assignments = ", ".join(f"{_identifier(name)} = ?" for name in self.columns)
        # The only interpolated identifiers are validated constructor configuration.
        sql = f"UPDATE {self._table} SET {assignments} WHERE {self._key} = ?"  # noqa: S608
        changes_before_restore = self.connection.total_changes
        for key, values in decoded.items():
            self.connection.execute(sql, (*[values[name] for name in self.columns], key))
        if self.connection.total_changes - changes_before_restore != len(decoded):
            raise JournalConflict("Database triggers or cascading writes exceeded the recovery boundary")
        restored = self.capture(list(decoded))
        if any(restored[key] != {**current[key], **values} for key, values in decoded.items()):
            raise JournalConflict("Database constraints or triggers prevented exact restoration")
        self.connection.execute("UPDATE _atlas_row_changes SET reverted_at = ? WHERE change_id = ?",
                                (datetime.now(UTC).isoformat(), change_id))
        return UndoResult(change_id, count, already_reverted=False)
