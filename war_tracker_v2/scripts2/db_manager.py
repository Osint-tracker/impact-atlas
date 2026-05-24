"""
Optimized Database Manager for raw_signals ingestion.
- Persistent WAL-mode connection (singleton pattern)
- Batch INSERT OR IGNORE via executemany
- atexit hook for graceful shutdown
"""

import sqlite3
import hashlib
import os
import atexit
import threading

# Percorso del DB Gigante
DB_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', 'data', 'raw_events.db'
)
DB_PATH = os.path.normpath(DB_PATH)

# --- Persistent Connection Singleton ---
_conn = None
_lock = threading.Lock()


def _get_connection():
    """Return a persistent SQLite connection (WAL mode, singleton)."""
    global _conn
    if _conn is not None:
        return _conn

    with _lock:
        if _conn is not None:
            return _conn

        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, timeout=30.0)
        _conn.execute("PRAGMA journal_mode=WAL;")
        _conn.execute("PRAGMA synchronous=NORMAL;")
        _conn.execute("PRAGMA cache_size=-8000;")  # 8MB cache
        _conn.execute("""
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
        """)
        _conn.commit()
        return _conn


def _close_connection():
    """atexit hook: commit and close the persistent connection."""
    global _conn
    if _conn is not None:
        try:
            _conn.commit()
            _conn.close()
        except Exception:
            pass
        _conn = None


atexit.register(_close_connection)


def get_hash(text):
    """Generate MD5 hash of text content."""
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def save_raw_events(events_list):
    """
    Batch-insert events using executemany + INSERT OR IGNORE.
    Expects list of dicts: {'text', 'source', 'type', 'date', 'url'?, 'media_urls'?}
    Returns count of newly inserted rows.
    """
    if not events_list:
        return 0

    conn = _get_connection()

    rows = []
    for ev in events_list:
        ev_hash = get_hash(ev['text'])
        media_urls = ev.get('media_urls', '[]')
        url = ev.get('url')
        rows.append((
            ev_hash,
            ev['type'],
            ev['source'],
            ev['date'],
            ev['text'],
            media_urls,
            url
        ))

    with _lock:
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT OR IGNORE INTO raw_signals
                (event_hash, source_type, source_name, date_published, text_content, media_urls, url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rows)
        saved = cursor.rowcount
        conn.commit()

    return max(saved, 0)
