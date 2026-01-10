import sqlite3
import os

DB_PATH = 'war_tracker_v2/data/raw_events.db'

def check_db():
    if not os.path.exists(DB_PATH):
        print(f"❌ DB not found at {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Count Total
        cursor.execute("SELECT COUNT(*) FROM unique_events")
        total = cursor.fetchone()[0]
        print(f"📊 Total Rows in unique_events: {total}")

        # Count Statuses
        cursor.execute("SELECT ai_analysis_status, COUNT(*) FROM unique_events GROUP BY ai_analysis_status")
        statuses = cursor.fetchall()
        print("📊 Status Breakdown:")
        for s in statuses:
            print(f"   - {s[0]}: {s[1]}")

        conn.close()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_db()
