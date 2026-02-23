import sqlite3
import json

conn = sqlite3.connect('c:/Users/lucag/.vscode/cli/osint-tracker/war_tracker_v2/data/raw_events.db')
cursor = conn.cursor()
cursor.execute("SELECT sources_list, urls_list FROM unique_events LIMIT 5")
rows = cursor.fetchall()
for r in rows:
    print(f"Sources: {r[0]}")
    print(f"URLs: {r[1]}")
    print("-" * 40)
conn.close()
