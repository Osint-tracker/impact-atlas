import sqlite3
import os
from datetime import datetime

# Percorsi assoluti per evitare confusione
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'raw_events.db')


def get_db_connection():
    """Crea connessione al DB SQLite."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Per accedere alle colonne per nome
    return conn


def init_db():
    """Inizializza le tabelle se non esistono."""
    conn = get_db_connection()
    c = conn.cursor()

    # Tabella UNICA per tutti i segnali grezzi (Web e Telegram)
    # event_hash è la chiave primaria per evitare duplicati esatti
    c.execute('''
        CREATE TABLE IF NOT EXISTS raw_signals (
            event_hash TEXT PRIMARY KEY,
            source_type TEXT,      -- 'GDELT', 'TELEGRAM', 'RSS'
            source_name TEXT,      -- es. 'Reuters', 'Rybar'
            date_published TEXT,   -- YYYY-MM-DD HH:MM:SS
            text_content TEXT,
            url TEXT,
            lat REAL,              -- Se disponibile (GDELT)
            lon REAL,              -- Se disponibile (GDELT)
            media_has_video INTEGER DEFAULT 0,
            
            -- Stati di lavorazione
            is_embedded INTEGER DEFAULT 0,  -- 0=No, 1=Sì (In Chroma)
            cluster_id TEXT                -- ID del cluster assegnato
        )
    ''')

    # Indici per velocizzare le ricerche
    c.execute("CREATE INDEX IF NOT EXISTS idx_date ON raw_signals(date_published)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_embedded ON raw_signals(is_embedded)")

    conn.commit()
    conn.close()
    print(f"✅ Database inizializzato in: {DB_PATH}")


def save_batch_signals(signals_list):
    """
    Salva una lista di dizionari nel DB ignorando i duplicati (Hash collision).
    Molto veloce.
    """
    if not signals_list:
        return 0

    conn = get_db_connection()
    c = conn.cursor()

    inserted_count = 0

    # Prepariamo la query
    sql = '''
        INSERT OR IGNORE INTO raw_signals 
        (event_hash, source_type, source_name, date_published, text_content, url, lat, lon)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    '''

    # Prepariamo i dati (Lista di tuple)
    data_tuples = []
    for s in signals_list:
        data_tuples.append((
            s['hash'], s['type'], s['source'], s['date'],
            s['text'], s['url'], s.get('lat'), s.get('lon')
        ))

    try:
        c.executemany(sql, data_tuples)
        conn.commit()
        inserted_count = c.rowcount
    except Exception as e:
        print(f"❌ Errore salvataggio batch: {e}")
    finally:
        conn.close()

    return inserted_count


# Se lanciato direttamente, inizializza
if __name__ == "__main__":
    init_db()
