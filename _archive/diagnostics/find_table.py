"""Diagnostic to find which database and table contains our problematic event."""
import sqlite3
import os

databases = ['impact_atlas.db', 'osint_tracker.db', 'raw_events.db', 'test_debug.db', 'test_raw_events.db']
target_id = 'b947642e-d87d-4a4c-9405-5c003ccb413f'

for db in databases:
    if not os.path.exists(db):
        continue
    try:
        conn = sqlite3.connect(db)
        tables = [t[0] for t in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        print(f"Database: {db} | Tables: {tables}")
        for t in tables:
            try:
                # Check columns
                cur = conn.cursor()
                cur.execute(f"PRAGMA table_info({t})")
                cols = [c[1] for c in cur.fetchall()]
                
                # Check for event_id
                if 'event_id' in cols or 'id' in cols:
                    id_col = 'event_id' if 'event_id' in cols else 'id'
                    row = conn.execute(f"SELECT * FROM {t} WHERE {id_col} = ?", (target_id,)).fetchone()
                    if row:
                        print(f"  [FOUND] In table '{t}':")
                        cur.execute(f"SELECT * FROM {t} WHERE {id_col} = ?", (target_id,))
                        desc = cur.description
                        for col, val in zip(desc, row):
                            print(f"    {col[0]}: {repr(val)}")
            except Exception as e:
                pass
        conn.close()
    except Exception as e:
        print(f"Error reading {db}: {e}")
