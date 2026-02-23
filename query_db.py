import sqlite3
import json
import datetime

conn = sqlite3.connect('impact_atlas.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

try:
    # Get today's events based on ingestion or date? Let's just use rowid to get the latest 500
    cursor.execute("SELECT * FROM kinetic_events ORDER BY rowid DESC LIMIT 500")
    recent = cursor.fetchall()
    
    qwen_count = 0
    today_count = 0
    
    # Let's count how many have 'visionary' or 'qwen' in their raw_data
    # Also find out if there's any other field.
    for r in recent:
        row_dict = dict(r)
        
        raw_data = row_dict.get('raw_data') or "{}"
        
        if 'qwen' in raw_data.lower() or 'visionary' in raw_data.lower() or 'visual_confirmation' in raw_data.lower():
            qwen_count += 1
            if qwen_count <= 3:
                print(f"Sample match [event_id={row_dict.get('event_id')}]: {raw_data[:200]}...")
                
    print(f"Among the last 500 events (in kinetic_events), found {qwen_count} analyzed with vision/qwen.")
    
except Exception as e:
    print("Error querying kinetic_events:", e)

# Also let's check if there are other tables like 'raw_events' or 'processed_reports'
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [t['name'] for t in cursor.fetchall()]
print("\nAll Tables in DB:", tables)

if 'events' not in tables and 'reports' not in tables:
    # Maybe the main DB is somewhere else? 
    pass

conn.close()
