import sqlite3
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

def fix_status():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("--- Fixing DB Status ---")
    
    # Check how many to fix
    cursor.execute("SELECT COUNT(*) FROM unique_events WHERE ai_analysis_status = 'PENDING' AND ai_report_json IS NOT NULL")
    count = cursor.fetchone()[0]
    print(f"Found {count} events to rescue (PENDING -> COMPLETED).")

    if count > 0:
        cursor.execute("UPDATE unique_events SET ai_analysis_status = 'COMPLETED' WHERE ai_analysis_status = 'PENDING' AND ai_report_json IS NOT NULL")
        conn.commit()
        print(f"✅ Successfully updated {cursor.rowcount} events to COMPLETED.")
    else:
        print("No events needed fixing.")

    conn.close()

if __name__ == "__main__":
    fix_status()
