import sys
import os
import io

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import asyncio
import sqlite3
import json

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
sys.path.insert(0, PROJECT_ROOT)

from scripts2.geolocator_agent import gazetteer, geolocator

DB_PATH = os.path.join(PROJECT_ROOT, "war_tracker_v2", "data", "raw_events.db")

async def main():
    print(f"Connecting to DB: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()
    
    # Check if locations_registry was successfully created
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='locations_registry';")
    if not cursor.fetchone():
        print("Initializing GazetteerCache DB...")
        gazetteer._init_db()
        print("Initialized.")
        
    cursor.execute("""
        SELECT event_id, ai_report_json
        FROM unique_events
        WHERE ai_analysis_status = 'COMPLETED' 
          AND lat IS NULL 
          AND first_seen_date >= '2026-05-05'
    """)
    rows = cursor.fetchall()
    
    print(f"Found {len(rows)} events to fix.")
    
    fixed_count = 0
    for row in rows:
        event_id, ai_report_json = row
        try:
            data = json.loads(ai_report_json) if ai_report_json else {}
            tactics = data.get('tactics', {})
            geo = tactics.get('geo_location', {})
            
            location_name = geo.get('inferred', {}).get('toponym_raw')
            region_name = geo.get('inferred', {}).get('region', '')
            
            if location_name and location_name != "Unknown":
                banned_locations = ["Ukraine", "Russia", "Europe", "NATO", "EU", "Border", "Frontline", "Front", "Zone"]
                if location_name.strip() not in banned_locations:
                    print(f"Geocoding: {location_name}, {region_name} for event {event_id}")
                    lat, lon, canonical = await gazetteer.get_coordinates(location_name, region_name)
                    
                    if lat and lon:
                        print(f"  -> Found: {lat}, {lon} [{canonical}]")
                        
                        operational_sector = geolocator.assign_sector(float(lon), float(lat))
                        
                        cursor.execute("""
                            UPDATE unique_events 
                            SET lat = ?, lon = ?, operational_sector = ?
                            WHERE event_id = ?
                        """, (lat, lon, operational_sector, event_id))
                        
                        geo['explicit'] = geo.get('explicit', {})
                        geo['explicit']['lat'] = lat
                        geo['explicit']['lon'] = lon
                        data['tactics']['geo_location'] = geo
                        cursor.execute("""
                            UPDATE unique_events
                            SET ai_report_json = ?
                            WHERE event_id = ?
                        """, (json.dumps(data, ensure_ascii=False), event_id))
                        
                        conn.commit()
                        fixed_count += 1
                        
                        await asyncio.sleep(1.0) # Rate limit protection
                    else:
                        print(f"  -> Could not geocode")
        except Exception as e:
            print(f"Error processing {event_id}: {e}")
            
    conn.close()
    print(f"Done. Fixed {fixed_count} out of {len(rows)} events.")

if __name__ == "__main__":
    asyncio.run(main())
