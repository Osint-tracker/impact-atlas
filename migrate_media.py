import sqlite3
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, 'war_tracker_v2', 'data', 'raw_events.db')

def migrate():
    print(f"Migrating {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("DB does not exist.")
        return
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if media_urls exists in raw_signals
    try:
        cursor.execute("PRAGMA table_info(raw_signals);")
        columns = [c[1] for c in cursor.fetchall()]
        if 'media_urls' not in columns:
            cursor.execute("ALTER TABLE raw_signals ADD COLUMN media_urls TEXT;")
            print("Added media_urls to raw_signals")
        else:
            print("media_urls already exists in raw_signals")
    except Exception as e:
        print("Error on raw_signals:", e)
        
    # Check if media_urls exists in unique_events
    try:
        cursor.execute("PRAGMA table_info(unique_events);")
        columns = [c[1] for c in cursor.fetchall()]
        if 'media_urls' not in columns:
            cursor.execute("ALTER TABLE unique_events ADD COLUMN media_urls TEXT;")
            print("Added media_urls to unique_events")
            
        else:
            print("media_urls already exists in unique_events")
    except Exception as e:
        print("Error on unique_events:", e)
        
    conn.commit()
    conn.close()

if __name__ == "__main__":
    migrate()
