import os
import json
import sqlite3

DB_PATH = 'war_tracker_v2/data/raw_events.db'
MEDIA_DIR = 'war_tracker_v2/data/media'

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("SELECT event_id, media_urls FROM unique_events WHERE media_urls IS NOT NULL AND media_urls != '[]'")
rows = cursor.fetchall()

updated = 0
for event_id, m_urls in rows:
    try:
        urls = json.loads(m_urls)
        new_urls = []
        changed = False
        for u in urls:
            if 't.me' in u:
                # e.g. https://t.me/MAKS23_NAFO/86099
                parts = u.split('t.me/')[1].split('/')
                if len(parts) >= 2:
                    chan = parts[0]
                    msg_id = parts[1]
                    
                    found = False
                    for ext in ['.mp4', '.jpg']:
                        local_path = os.path.abspath(os.path.join(MEDIA_DIR, f"{chan}_{msg_id}{ext}"))
                        if os.path.exists(local_path):
                            new_urls.append(local_path)
                            changed = True
                            found = True
                            break
                    if not found:
                        new_urls.append(u)
                else:
                    new_urls.append(u)
            else:
                new_urls.append(u)
                
        if changed:
            cursor.execute("UPDATE unique_events SET media_urls = ? WHERE event_id = ?", (json.dumps(new_urls), event_id))
            updated += 1
    except:
        pass

conn.commit()
conn.close()
print(f"Updated {updated} events with local media paths.")
