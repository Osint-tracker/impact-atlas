"""Diagnostic DB script"""
import sqlite3

conn = sqlite3.connect('war_tracker_v2/data/raw_events.db')
cursor = conn.cursor()

# Check Status Counts
cursor.execute("SELECT ai_analysis_status, COUNT(*) FROM unique_events GROUP BY ai_analysis_status")
print("=== Status Counts ===")
for row in cursor.fetchall():
    print(f"{row[0]}: {row[1]}")

# Check sample data for COMPLETED
print("\n=== Sample Completed Data ===")
cursor.execute("SELECT event_id, urls_list FROM unique_events WHERE ai_analysis_status = 'COMPLETED' LIMIT 3")
for row in cursor.fetchall():
    print(f"ID: {row[0][:8]}... | URLs: {row[1]}")

conn.close()
