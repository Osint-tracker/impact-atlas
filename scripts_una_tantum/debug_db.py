"""Quick debug script to check DB values"""
import sqlite3

conn = sqlite3.connect('war_tracker_v2/data/raw_events.db')
cursor = conn.cursor()

cursor.execute("""
    SELECT event_id, urls_list, ai_summary, reliability, bias_score 
    FROM unique_events 
    WHERE ai_analysis_status = 'COMPLETED' 
    LIMIT 3
""")

for row in cursor.fetchall():
    print(f"ID: {row[0][:30]}...")
    print(f"  urls_list: {str(row[1])[:80] if row[1] else 'EMPTY'}")
    print(f"  ai_summary: {str(row[2])[:80] if row[2] else 'EMPTY'}")
    print(f"  reliability: {row[3]}")
    print(f"  bias_score: {row[4]}")
    print()

conn.close()
