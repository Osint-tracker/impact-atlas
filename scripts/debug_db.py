
import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("SELECT * FROM units_registry LIMIT 5")
rows = cursor.fetchall()

print("--- UNITS REGISTRY DUMP ---")
for row in rows:
    print(dict(row))

print("\n--- RAW EVENTS UNITS DUMP (Sample) ---")
cursor.execute("SELECT ai_report_json FROM unique_events LIMIT 1")
row = cursor.fetchone()
if row:
    import json
    data = json.loads(row['ai_report_json'])
    print(data.get('military_units_detected', []))

conn.close()
