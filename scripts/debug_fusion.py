import sqlite3
import os
from datetime import datetime, timedelta
import json

DB_PATH = 'war_tracker_v2/data/raw_events.db'

def check_db_state():
    if not os.path.exists(DB_PATH):
        print(f"❌ DB not found at {DB_PATH}")
        return

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # 1. Check Total Events in last 96h
        cutoff_date = (datetime.now() - timedelta(hours=96)).isoformat()
        print(f"Time Cutoff: {cutoff_date}")
        
        cursor.execute("SELECT COUNT(*) FROM unique_events WHERE last_seen_date >= ?", (cutoff_date,))
        recent_total = cursor.fetchone()[0]
        print(f"Events in last 96h: {recent_total}")

        # 2. Check Vectors in last 96h
        cursor.execute("""
            SELECT COUNT(*) FROM unique_events 
            WHERE last_seen_date >= ? 
            AND embedding_vector IS NOT NULL
        """, (cutoff_date,))
        recent_vectors = cursor.fetchone()[0]
        print(f"Events with Vectors (last 96h): {recent_vectors}")

        # 3. Check Vectors Total (Any time)
        cursor.execute("SELECT COUNT(*) FROM unique_events WHERE embedding_vector IS NOT NULL")
        total_vectors = cursor.fetchone()[0]
        print(f"Total Events with Vectors (All time): {total_vectors}")
        
        # 4. Check Status Breakdown for recent items
        if recent_total > 0:
            print("\nRecent Events Status:")
            cursor.execute("""
                SELECT ai_analysis_status, COUNT(*) 
                FROM unique_events 
                WHERE last_seen_date >= ? 
                GROUP BY ai_analysis_status
            """, (cutoff_date,))
            for row in cursor.fetchall():
                print(f"   - {row[0]}: {row[1]}")

        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_db_state()
