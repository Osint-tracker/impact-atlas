import sqlite3
import os
import sys

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

# The database path based on our findings
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Note: we run from osint-tracker/scripts2/ meaning parent is osint-tracker
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

def migrate_db():
    print(f"[*] Accessing database at: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("[!] Database not found. Please ensure the path is correct.")
        sys.exit(1)
        
    try:
        conn = sqlite3.connect(DB_PATH)
        # WAL mode is active according to event_builder.py
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        
        # Check if column already exists
        cursor.execute("PRAGMA table_info(unique_events)")
        columns = [info[1] for info in cursor.fetchall()]
        
        if 'operational_sector' not in columns:
            print("[*] Adding 'operational_sector' column to 'unique_events' table...")
            cursor.execute("ALTER TABLE unique_events ADD COLUMN operational_sector TEXT;")
            conn.commit()
            print("[+] Migration successful: 'operational_sector' column added.")
        else:
            print("[*] Column 'operational_sector' already exists. No migration needed.")
            
        conn.close()
    except Exception as e:
        print(f"[!] Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate_db()
