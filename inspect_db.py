import sqlite3
import os

DB_PATH = 'impact_atlas.db'

if not os.path.exists(DB_PATH):
    print(f"Error: {DB_PATH} not found.")
    exit(1)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print("=== Tables ===")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
for t in tables:
    print(f"- {t[0]}")
    cursor.execute(f"PRAGMA table_info({t[0]});")
    cols = cursor.fetchall()
    for c in cols:
        print(f"  - {c[1]} ({c[2]})")

conn.close()
