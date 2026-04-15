import sqlite3
import json
import os
import sys

sys.stdout.reconfigure(encoding='utf-8')

dblist = [
    'war_tracker_v2/data/raw_events_snapshot.db',
    'war_tracker_v2/data/raw_events.db',
    'impact_atlas.db'
]

removed_total = 0

for db in dblist:
    if os.path.exists(db):
        try:
            conn = sqlite3.connect(db)
            c = conn.cursor()
            
            # Check if units_registry exists
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='units_registry'")
            if c.fetchone():
                c.execute("DELETE FROM units_registry WHERE faction='UNKNOWN' OR faction='' OR faction IS NULL")
                if c.rowcount > 0:
                    print(f"Removed {c.rowcount} UNKNOWN units from {db}")
                    removed_total += c.rowcount
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error checking {db}: {e}")

# Clean up units.json
units_path = 'assets/data/units.json'
if os.path.exists(units_path):
    with open(units_path, 'r', encoding='utf-8') as f:
        units = json.load(f)
    
    clean_units = [u for u in units if u.get('faction') not in ['UNKNOWN', '', None]]
    
    if len(units) != len(clean_units):
        print(f"Removed {len(units) - len(clean_units)} UNKNOWN units from {units_path}")
        with open(units_path, 'w', encoding='utf-8') as f:
            json.dump(clean_units, f, indent=2, ensure_ascii=False)

print("DB cleanup complete!")
