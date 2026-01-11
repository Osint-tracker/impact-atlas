import sqlite3
import os

DB_PATH = os.path.join('war_tracker_v2', 'data', 'raw_events.db')
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Conta i totali
cursor.execute(
    "SELECT is_embedded, COUNT(*) FROM raw_signals GROUP BY is_embedded")
results = cursor.fetchall()

print("📊 STATO LAVORI:")
for status, count in results:
    if status == 0:
        label = "🟠 DA FARE (Coda)"
    elif status == 1:
        label = "🟢 COMPLETATI (Embeddati)"
    elif status == 2:
        label = "🔴 SCARTATI (Irrilevanti)"
    else:
        label = "❓ SCONOSCIUTO"
    print(f"{label}: {count}")

conn.close()
