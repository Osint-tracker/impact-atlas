import sqlite3
import hashlib
import os
from datetime import datetime

# Percorso del DB Gigante
DB_PATH = os.path.join('war_tracker_v2', 'data', 'raw_events.db')


def get_hash(text):
    return hashlib.md5(text.encode('utf-8')).hexdigest()


def save_raw_events(events_list):
    """
    Riceve una lista di dict: {'text': ..., 'source': ..., 'date': ...}
    """
    if not events_list:
        return 0

    # Assicuriamoci che la cartella esista
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Crea tabella se non esiste (Schema raw_signals)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_signals (
            event_hash TEXT PRIMARY KEY,
            source_type TEXT,
            source_name TEXT,
            date_published DATETIME,
            text_content TEXT,
            tie_score REAL DEFAULT 0,
            processed BOOLEAN DEFAULT 0,
            media_urls TEXT
        )
    """)

    saved_count = 0
    for ev in events_list:
        ev_hash = get_hash(ev['text'])
        media_urls = ev.get('media_urls', '[]')

        try:
            cursor.execute("""
                INSERT INTO raw_signals (event_hash, source_type, source_name, date_published, text_content, media_urls, url)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (ev_hash, ev['type'], ev['source'], ev['date'], ev['text'], media_urls, ev.get('url')))
            saved_count += 1
        except sqlite3.IntegrityError:
            continue  # Già esiste, saltiamo

    conn.commit()
    conn.close()
    return saved_count
