"""Typed repository for normalized raw OSINT events.

``save_raw_events`` remains available as a compatibility function for existing
ingestors. New code should depend on :class:`RawEventRepository` directly.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from collections.abc import Iterable, Mapping

from impact_atlas.config import ProjectPaths
from impact_atlas.sqlite import SQLiteDatabase

logger = logging.getLogger(__name__)
DB_PATH = ProjectPaths.discover().raw_events_database


def get_hash(text: str) -> str:
    """Return the legacy deterministic hash used to de-duplicate raw event text."""
    return hashlib.md5(text.encode("utf-8"), usedforsecurity=False).hexdigest()  # noqa: S324


@dataclass(frozen=True, slots=True)
class RawEvent:
    """Validated raw signal accepted by the persistence layer."""

    text: str
    source_type: str | None
    source_name: str | None
    published_at: str | None
    media_urls: str
    url: str | None

    @classmethod
    def from_mapping(cls, event: Mapping[str, Any]) -> RawEvent | None:
        """Normalize a legacy event dictionary, returning ``None`` if text is absent."""
        text = event.get("text")
        if not isinstance(text, str) or not text.strip():
            return None
        media_urls = event.get("media_urls", "[]")
        return cls(
            text=text,
            source_type=_as_optional_text(event.get("type")),
            source_name=_as_optional_text(event.get("source")),
            published_at=_as_optional_text(event.get("date")),
            media_urls=media_urls if isinstance(media_urls, str) else str(media_urls),
            url=_as_optional_text(event.get("url")),
        )


def _as_optional_text(value: object) -> str | None:
    """Coerce a non-empty value to text while preserving missing values as ``None``."""
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class RawEventRepository:
    """Transactional repository for the ``raw_signals`` SQLite table."""

    def __init__(self, db_path: Path) -> None:
        """Bind the repository to one database file without opening it eagerly."""
        self._database = SQLiteDatabase(db_path)

    def save_many(self, events: Iterable[Mapping[str, Any]]) -> int:
        """Persist valid events in one transaction and return newly inserted rows."""
        normalized_events: list[RawEvent] = []
        for event in events:
            normalized = RawEvent.from_mapping(event)
            if normalized is None:
                logger.warning("Skipping raw event without text content.")
                continue
            normalized_events.append(normalized)
        if not normalized_events:
            return 0

        with self._database.transaction() as connection:
            self._initialize_schema(connection)
            changes_before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO raw_signals (
                    event_hash, source_type, source_name, date_published,
                    text_content, media_urls, url
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        get_hash(event.text),
                        event.source_type,
                        event.source_name,
                        event.published_at,
                        event.text,
                        event.media_urls,
                        event.url,
                    )
                    for event in normalized_events
                ],
            )
            return connection.total_changes - changes_before

    @staticmethod
    def _initialize_schema(connection: sqlite3.Connection) -> None:
        """Create the table and apply the additive ``url`` migration when required."""
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_signals (
                event_hash TEXT PRIMARY KEY,
                source_type TEXT,
                source_name TEXT,
                date_published DATETIME,
                text_content TEXT,
                tie_score REAL DEFAULT 0,
                processed BOOLEAN DEFAULT 0,
                media_urls TEXT,
                url TEXT
            )
            """
        )
        columns = {row[1] for row in connection.execute("PRAGMA table_info(raw_signals)")}
        if "url" not in columns:
            connection.execute("ALTER TABLE raw_signals ADD COLUMN url TEXT")

def save_raw_events(events_list: Iterable[Mapping[str, Any]]) -> int:
    """Persist legacy event mappings through the transactional repository."""
    return RawEventRepository(Path(DB_PATH)).save_many(events_list)
