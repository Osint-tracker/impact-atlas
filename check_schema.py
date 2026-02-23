import sqlite3

conn = sqlite3.connect('c:/Users/lucag/.vscode/cli/osint-tracker/war_tracker_v2/data/raw_events.db')
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(raw_signals);")
print("raw_signals:", [c[1] for c in cursor.fetchall()])

cursor.execute("PRAGMA table_info(unique_events);")
print("unique_events:", [c[1] for c in cursor.fetchall()])
conn.close()
