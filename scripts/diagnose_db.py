import sqlite3
import os
import sys

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

def diagnose():
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("--- Event Status Counts ---")
    cursor.execute("SELECT ai_analysis_status, COUNT(*) FROM unique_events GROUP BY ai_analysis_status")
    for row in cursor.fetchall():
        print(f"Status '{row[0]}': {row[1]}")

    print("\n--- Completed Events Source Check ---")
    cursor.execute("SELECT COUNT(*) FROM unique_events WHERE ai_analysis_status = 'COMPLETED'")
    total_completed = cursor.fetchone()[0]
    print(f"Total COMPLETED: {total_completed}")

    cursor.execute("SELECT COUNT(*) FROM unique_events WHERE ai_analysis_status = 'COMPLETED' AND (urls_list IS NULL OR urls_list = '')")
    completed_no_source = cursor.fetchone()[0]
    print(f"COMPLETED with Empty/Null Sources: {completed_no_source}")

    print("\n--- Anomaly Check ---")
    cursor.execute("SELECT COUNT(*) FROM unique_events WHERE ai_analysis_status = 'PENDING' AND ai_report_json IS NOT NULL")
    pending_with_data = cursor.fetchone()[0]
    print(f"PENDING but have ai_report_json: {pending_with_data}")

    conn.close()

if __name__ == "__main__":
    diagnose()
