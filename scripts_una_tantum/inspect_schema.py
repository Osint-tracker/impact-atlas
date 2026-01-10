import sqlite3
import os

DB_PATH = 'war_tracker_v2/data/raw_events.db'
# Handle path relative to root or script
if not os.path.exists(DB_PATH) and os.path.exists('osint_tracker.db'):
     DB_PATH = 'osint_tracker.db' # fallback

print(f"Checking DB: {DB_PATH}")
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("PRAGMA table_info(unique_events)")
columns = cursor.fetchall()

print(f"{'CID':<5} {'Name':<25} {'Type':<10}")
print("-" * 40)
for col in columns:
    print(f"{col[0]:<5} {col[1]:<25} {col[2]:<10}")

conn.close()
