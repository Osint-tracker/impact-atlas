import sqlite3
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

def inspect():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT event_id, ai_report_json FROM unique_events WHERE ai_analysis_status = 'PENDING' AND ai_report_json IS NOT NULL LIMIT 3")
    rows = cursor.fetchall()
    
    print(f"Found {len(rows)} samples:")
    for row in rows:
        print(f"ID: {row[0]}")
        print(f"JSON Preview: {str(row[1])[:200]}...")
        print("-" * 40)
        
    conn.close()

if __name__ == "__main__":
    inspect()
