import sqlite3
import os
import json
import time

# DB Path configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

def migrate_gdelt():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found: {DB_PATH}")
        return

    print("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Check Source Counts
    cursor.execute("SELECT count(*) FROM raw_signals")
    total_raw = cursor.fetchone()[0]
    print(f"Total Raw Signals (GDELT + Others): {total_raw}")

    # 2. Select Candidates (Avoiding duplicates if run multiple times)
    print("Starting Migration (Raw -> Unique)...")
    
    # We fetch in batches to avoid RAM explosion
    BATCH_SIZE = 5000
    offset = 640000
    migrated_count = 0
    skipped_count = 0

    while True:
        cursor.execute(f"""
            SELECT event_hash, source_name, date_published, text_content 
            FROM raw_signals 
            LIMIT {BATCH_SIZE} OFFSET {offset}
        """)
        rows = cursor.fetchall()

        if not rows:
            break

        to_insert = []
        for r in rows:
            evt_hash, src, date, text = r
            
            # Prepare fields
            sources_list = json.dumps([src]) if src else '["GDELT"]'
            
            # Tuple for insertion
            to_insert.append((
                evt_hash,          # event_id
                date,             # first_seen
                date,             # last_seen
                sources_list,     # sources_list
                text,             # full_text_dossier
                'PENDING'         # ai_analysis_status
            ))

        # Bulk Insert with IGNORE to skip existing IDs
        cursor.executemany("""
            INSERT OR IGNORE INTO unique_events (
                event_id, first_seen_date, last_seen_date, sources_list, full_text_dossier, ai_analysis_status
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, to_insert)
        
        # Calculate extracted count (rowcount might be unreliable with OR IGNORE in some versions, but usually works)
        # Instead we count loops
        migrated_count += cursor.rowcount  # This captures actual inserts
        offset += BATCH_SIZE
        conn.commit()
        
        print(f"   Processed {offset}/{total_raw}...")

    conn.close()
    print(f"Migration Complete. Rows Inserted: {migrated_count}")

if __name__ == "__main__":
    migrate_gdelt()
