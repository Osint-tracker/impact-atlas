import sqlite3
import json

conn = sqlite3.connect('war_tracker_v2/data/raw_events.db')
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur.execute("SELECT event_id, ai_report_json FROM unique_events WHERE urls_list LIKE '%noel%' OR ai_report_json LIKE '%noel%'")
rows = cur.fetchall()

updated = 0
for row in rows:
    event_id = row['event_id']
    ai_report_str = row['ai_report_json']
    if not ai_report_str: continue
    
    try:
        data = json.loads(ai_report_str)
        
        # Override bias to be strictly Pro-Ukrainian (+3.0)
        data['bias_score'] = 3.0
        data['dominant_bias'] = "Pro-Ukraine"
        
        new_json = json.dumps(data)
        cur.execute("UPDATE unique_events SET ai_report_json = ?, bias_score = 3.0 WHERE event_id = ?", (new_json, event_id))
        updated += cur.rowcount
    except Exception as e:
        print(f"Error {event_id}: {e}")

conn.commit()
print(f"Updated {updated} Noel Report events with strong Pro-Ukrainian bias.")
conn.close()
