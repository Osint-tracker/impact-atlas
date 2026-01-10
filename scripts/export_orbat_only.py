import sqlite3
import json
import os
import sys

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
UNITS_JSON_PATH = os.path.join(BASE_DIR, '../assets/data/units.json')

def export_units():
    print("[DB] Exporting ORBAT Units...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='units_registry'")
        if not cursor.fetchone():
            print("   [ERR] Table units_registry not found.")
            return

        cursor.execute("SELECT * FROM units_registry ORDER BY last_seen_date DESC")
        rows = cursor.fetchall()
        
        units = []
        for row in rows:
            u = dict(row)
            if u.get('last_seen_date'): u['last_seen_date'] = str(u['last_seen_date'])
            units.append(u)

        with open(UNITS_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(units, f, indent=2, ensure_ascii=False)
            
        print(f"   [DONE] Exported {len(units)} units to {UNITS_JSON_PATH}")
        conn.close()
    except Exception as e:
        print(f"   [ERR] Failed to export units: {e}")

if __name__ == "__main__":
    export_units()
