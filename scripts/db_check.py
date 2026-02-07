import sqlite3

conn = sqlite3.connect('war_tracker_v2/data/raw_events.db')
cur = conn.cursor()

# Check status breakdown
cur.execute("SELECT ai_analysis_status, COUNT(*) FROM unique_events GROUP BY ai_analysis_status")
print("Status breakdown:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

# Check completed with embeddings
cur.execute("SELECT COUNT(*) FROM unique_events WHERE ai_analysis_status = 'COMPLETED' AND embedding_vector IS NOT NULL")
print(f"\nCOMPLETED with embeddings: {cur.fetchone()[0]}")

# Check date range
cur.execute("SELECT MIN(last_seen_date), MAX(last_seen_date) FROM unique_events")
row = cur.fetchone()
print(f"\nDate range: {row[0]} to {row[1]}")

conn.close()
