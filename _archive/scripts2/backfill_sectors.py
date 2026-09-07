import sqlite3
import os
import sys
import json

sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

from geolocator_agent import geolocator

def backfill_sectors():
    print(f"[*] Accessing database at: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("[!] Database not found.")
        sys.exit(1)
        
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        
        print("[*] Selecting events...")
        # ai_report_json holds the geography
        cursor.execute("SELECT event_id, ai_report_json FROM unique_events WHERE ai_report_json IS NOT NULL")
        rows = cursor.fetchall()
        
        updates = []
        for row in rows:
            event_id, ai_json = row
            lat = 0.0
            lon = 0.0
            try:
                data = json.loads(ai_json)
                tactics = data.get('tactics', {})
                geo = tactics.get('geo_location', {}).get('explicit', {})
                lat = geo.get('lat')
                lon = geo.get('lon')
                if not lat or not lon:
                    inferred = tactics.get('geo_location', {}).get('inferred', {})
                    lat = inferred.get('lat')
                    lon = inferred.get('lon')
                
                if lat is not None and lon is not None:
                    lat = float(lat)
                    lon = float(lon)
                else:
                    lat, lon = 0.0, 0.0
            except:
                pass
            
            if lat != 0.0 and lon != 0.0:
                sector = geolocator.assign_sector(lon, lat)
                updates.append((sector, event_id))
            
        print(f"[*] Updating {len(updates)} database rows...")
        cursor.executemany("UPDATE unique_events SET operational_sector = ? WHERE event_id = ?", updates)
        conn.commit()
        
        print(f"[+] Successfully backfilled operational sectors for {len(updates)} events.")
        conn.close()
    except Exception as e:
        print(f"[!] Backfill failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    backfill_sectors()
