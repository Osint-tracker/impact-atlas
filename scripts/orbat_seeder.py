import sqlite3
import json
import csv
import os
import sys

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Path to the main RAW EVENTS database (as used by event_builder.py)
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
ASSETS_DATA_DIR = os.path.join(BASE_DIR, '../assets/data')

UA_CSV_PATH = os.path.join(ASSETS_DATA_DIR, 'orbat_ua.csv')
RU_JSON_PATH = os.path.join(ASSETS_DATA_DIR, 'orbat_ru.json')

def init_db():
    print(f"[*] Connecting to database: {DB_PATH}")
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS units_registry (
            unit_id TEXT PRIMARY KEY,       -- e.g., "UA_47_MECH"
            display_name TEXT,              -- e.g., "47th Separate Mechanized Brigade"
            faction TEXT,                   -- "UA" or "RU"
            type TEXT,                      -- "ARMORED", "INFANTRY", "ARTILLERY", "AIRBORNE"
            subordination TEXT,             -- e.g., "Operational Command North"
            last_seen_lat REAL,
            last_seen_lon REAL,
            last_seen_date DATETIME,
            status TEXT DEFAULT 'ACTIVE',   -- "ACTIVE", "REGROUPING", "DESTROYED", "ENGAGED"
            linked_event_id TEXT            -- ID of the event that updated this position
        );
    """)
    conn.commit()
    return conn

def clean_unit_id(raw_name, faction):
    """Generates a normalized ID better than random strings."""
    # Simple normalization: "47th Separate Mechanized Brigade" -> "UA_47_MECH_BDE"
    slug = raw_name.upper().replace('SEPARATE ', '').replace('GUARDS ', '').replace('  ', ' ')
    slug = slug.replace(' ', '_').replace('.', '')
    return f"{faction}_{slug}"[:50] # Cap length

def seed_ukraine(conn):
    if not os.path.exists(UA_CSV_PATH):
        print(f"   [SKIP] UA CSV not found: {UA_CSV_PATH}")
        return

    print("[*] Seeding Ukrainian Units...")
    cursor = conn.cursor()
    count = 0
    
    with open(UA_CSV_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Format depends on actual CSV columns from uawardata
            # Expected: name, parent, type, etc.
            # We'll try to adapt common columns
            
            raw_name = row.get('name') or row.get('unit')
            if not raw_name: continue

            # Determine ID
            unit_id = clean_unit_id(raw_name, 'UA')
            
            # Map type
            raw_type = str(row.get('type_text', '')).upper()
            unit_type = "INFANTRY"
            if "TANK" in raw_type or "ARMORED" in raw_type: unit_type = "ARMORED"
            elif "ARTILLERY" in raw_type or "MISSILE" in raw_type: unit_type = "ARTILLERY"
            elif "MARINE" in raw_type: unit_type = "NAVAL_INFANTRY"
            elif "AIR ASSAULT" in raw_type or "AIRBORNE" in raw_type: unit_type = "AIRBORNE"
            elif "DRONE" in raw_type or "UAV" in raw_type: unit_type = "DRONE_OPS"
            elif "SPECIAL" in raw_type: unit_type = "SPECIAL_FORCES"
            
            # Subordination
            sub = row.get('parent') or row.get('command')
            
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO units_registry (unit_id, display_name, faction, type, subordination, status)
                    VALUES (?, ?, 'UA', ?, ?, 'ACTIVE')
                """, (unit_id, raw_name, unit_type, sub))
                count += 1
            except Exception as e:
                print(f"Error inserting {raw_name}: {e}")

    conn.commit()
    print(f"   [OK] Seeded {count} Ukrainian units.")

def seed_russia(conn):
    if not os.path.exists(RU_JSON_PATH):
        print(f"   [SKIP] RU JSON not found: {RU_JSON_PATH}")
        return

    print("[*] Seeding Russian Units...")
    cursor = conn.cursor()
    count = 0
    
    with open(RU_JSON_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for u in data:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO units_registry (unit_id, display_name, faction, type, status)
                    VALUES (?, ?, 'RU', ?, 'ACTIVE')
                """, (u['unit_id'], u['name'], u['type']))
                count += 1
            except Exception as e:
                print(f"Error inserting {u['name']}: {e}")
    
    conn.commit()
    print(f"   [OK] Seeded {count} Russian units.")

def main():
    conn = init_db()
    seed_ukraine(conn)
    seed_russia(conn)
    conn.close()
    print("[*] Seeding Complete.")

if __name__ == "__main__":
    main()
