"""
parabellum_sync.py - Sync Unit Positions from Parabellum Think Tank Map
========================================================================
Fetches real-time military unit positions from the Parabellum Maps WFS API
and updates the local units_registry table.

Features:
- Fetches all units in GeoJSON format
- Maps factions (Ukraine, Russian Federation) to standard codes
- Updates last_seen coordinates and timestamps
- Can be run periodically (e.g., daily cron job)

API Source: https://geo.parabellumthinktank.com
License: Check Parabellum's terms of use before production usage
"""

import sqlite3
import requests
import json
import os
import sys
from datetime import datetime, timezone
import argparse

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

# Parabellum WFS API Endpoint
WFS_URL = (
    "https://geo.parabellumthinktank.com/index.php/lizmap/service"
    "?repository=russoukrainianwar"
    "&project=russian_invasion_of_ukraine"
    "&SERVICE=WFS"
    "&REQUEST=GetFeature"
    "&TYPENAME=Unit%C3%A0"
    "&VERSION=1.1.0"
    "&OUTPUTFORMAT=GeoJSON"
)

# Faction mapping
FACTION_MAP = {
    "Ukraine": "UA",
    "Ukrainian": "UA",
    "Russian Federation": "RU",
    "Russia": "RU",
    "DPR": "RU_PROXY",
    "LPR": "RU_PROXY",
    "Wagner": "RU_PMC",
}

# Unit type normalization
TYPE_MAP = {
    "infantry motorized": "INFANTRY_MOT",
    "infantry mechanized": "INFANTRY_MECH",
    "security mechanized": "SECURITY_MECH",
    "tank": "ARMOR",
    "armor": "ARMOR",
    "artillery": "ARTILLERY",
    "airborne": "AIRBORNE",
    "air assault": "AIR_ASSAULT",
    "naval infantry": "NAVAL_INFANTRY",
    "special forces": "SOF",
    "sof": "SOF",
    "drone": "DRONE_OPS",
    "uav": "DRONE_OPS",
    "reconnaissance": "RECON",
}


def normalize_faction(raw_faction):
    """Convert faction name to standard code."""
    if not raw_faction:
        return "UNKNOWN"
    for key, code in FACTION_MAP.items():
        if key.lower() in raw_faction.lower():
            return code
    return "UNKNOWN"


def normalize_type(raw_type):
    """Convert unit type to standard code."""
    if not raw_type:
        return "INFANTRY"
    raw_lower = raw_type.lower()
    for key, code in TYPE_MAP.items():
        if key in raw_lower:
            return code
    return "INFANTRY"


def generate_unit_id(unit_name, echelon, faction_code):
    """Generate a normalized unit ID."""
    if not unit_name:
        return None
    
    # Clean up the name
    clean = unit_name.upper().strip()
    clean = clean.replace("'", "").replace('"', "")
    clean = clean.replace(" ", "_").replace("-", "_")
    clean = clean[:30]  # Limit length
    
    # Add echelon suffix if available
    echelon_suffix = ""
    if echelon:
        echelon_map = {
            "brigade": "BDE",
            "battalion": "BN",
            "regiment": "RGT",
            "division": "DIV",
            "company": "COY",
        }
        for key, suffix in echelon_map.items():
            if key in echelon.lower():
                echelon_suffix = f"_{suffix}"
                break
    
    return f"{faction_code}_{clean}{echelon_suffix}"


def ensure_table(conn):
    """Ensure units_registry table exists with required columns."""
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS units_registry (
            unit_id TEXT PRIMARY KEY,
            display_name TEXT,
            faction TEXT,
            type TEXT,
            subordination TEXT,
            echelon TEXT,
            last_seen_lat REAL,
            last_seen_lon REAL,
            last_seen_date DATETIME,
            status TEXT DEFAULT 'ACTIVE',
            source TEXT DEFAULT 'PARABELLUM',
            linked_event_id TEXT
        )
    """)
    
    # Add missing columns if they don't exist
    try:
        cursor.execute("ALTER TABLE units_registry ADD COLUMN echelon TEXT")
    except:
        pass
    try:
        cursor.execute("ALTER TABLE units_registry ADD COLUMN source TEXT DEFAULT 'MANUAL'")
    except:
        pass
    
    conn.commit()


def fetch_parabellum_data():
    """Fetch unit data from Parabellum WFS API."""
    print("Fetching data from Parabellum Maps WFS API...")
    
    try:
        response = requests.get(WFS_URL, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        features = data.get('features', [])
        print(f"   Retrieved {len(features)} features")
        return features
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return []


def sync_units(features, conn, dry_run=False):
    """Sync fetched features to the database."""
    cursor = conn.cursor()
    
    stats = {'inserted': 0, 'updated': 0, 'skipped': 0}
    
    for feature in features:
        props = feature.get('properties', {})
        geometry = feature.get('geometry', {})
        
        # Extract data
        unit_name = props.get('unit')
        if not unit_name:
            stats['skipped'] += 1
            continue
        
        faction_raw = props.get('insignia_name')
        echelon = props.get('echelon')
        unit_type = props.get('unit_type')
        date_update = props.get('date_update')
        
        # Normalize
        faction_code = normalize_faction(faction_raw)
        type_code = normalize_type(unit_type)
        unit_id = generate_unit_id(unit_name, echelon, faction_code)
        
        if not unit_id:
            stats['skipped'] += 1
            continue
        
        # Extract coordinates (GeoJSON format: [lon, lat])
        lat, lon = None, None
        if geometry:
            coords = geometry.get('coordinates', [])
            geom_type = geometry.get('type', '')
            
            # Handle different geometry types
            if geom_type == 'Point' and coords and len(coords) >= 2:
                lon, lat = coords[0], coords[1]
            elif geom_type == 'Polygon' and coords and len(coords) > 0:
                # Use first point of first ring as centroid approximation
                ring = coords[0]
                if ring and len(ring) > 0 and len(ring[0]) >= 2:
                    lon, lat = ring[0][0], ring[0][1]
            elif geom_type == 'MultiPoint' and coords and len(coords) > 0:
                if len(coords[0]) >= 2:
                    lon, lat = coords[0][0], coords[0][1]
        
        # Parse update date
        if date_update:
            try:
                last_seen = datetime.fromisoformat(date_update.replace('Z', '+00:00'))
            except:
                last_seen = datetime.now(timezone.utc)
        else:
            last_seen = datetime.now(timezone.utc)
        
        if dry_run:
            print(f"   [DRY] {unit_id}: {unit_name} ({faction_code}) @ {lat:.4f}, {lon:.4f}" if lat else f"   [DRY] {unit_id}: {unit_name} ({faction_code}) - no coords")
            stats['inserted'] += 1
            continue
        
        # Upsert into database
        try:
            cursor.execute("""
                INSERT INTO units_registry 
                    (unit_id, display_name, faction, type, echelon, 
                     last_seen_lat, last_seen_lon, last_seen_date, source, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PARABELLUM', 'ACTIVE')
                ON CONFLICT(unit_id) DO UPDATE SET
                    display_name = excluded.display_name,
                    last_seen_lat = excluded.last_seen_lat,
                    last_seen_lon = excluded.last_seen_lon,
                    last_seen_date = excluded.last_seen_date,
                    source = 'PARABELLUM'
            """, (unit_id, unit_name, faction_code, type_code, echelon, lat, lon, last_seen.isoformat()))
            
            if cursor.rowcount > 0:
                stats['updated'] += 1
            else:
                stats['inserted'] += 1
                
        except Exception as e:
            print(f"   Error inserting {unit_id}: {e}")
            stats['skipped'] += 1
    
    if not dry_run:
        conn.commit()
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Sync unit positions from Parabellum Maps")
    parser.add_argument("--dry-run", action="store_true", help="Preview without making changes")
    args = parser.parse_args()
    
    print("=" * 60)
    print("PARABELLUM UNIT SYNC")
    print("=" * 60)
    
    if args.dry_run:
        print("[DRY RUN MODE - No changes will be made]")
    
    # Connect to database
    print(f"\nConnecting to: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH, timeout=60)
    ensure_table(conn)
    
    # Get current count
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM units_registry")
    before_count = cursor.fetchone()[0]
    print(f"Current units in registry: {before_count}")
    
    # Fetch and sync
    features = fetch_parabellum_data()
    
    if not features:
        print("No features to sync!")
        conn.close()
        return
    
    stats = sync_units(features, conn, dry_run=args.dry_run)
    
    # Final count
    if not args.dry_run:
        cursor.execute("SELECT COUNT(*) FROM units_registry")
        after_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM units_registry WHERE source = 'PARABELLUM'")
        parabellum_count = cursor.fetchone()[0]
    else:
        after_count = before_count
        parabellum_count = 0
    
    conn.close()
    
    # Summary
    print("\n" + "=" * 60)
    print("SYNC COMPLETE")
    print("=" * 60)
    print(f"   Processed: {len(features)}")
    print(f"   Inserted:  {stats['inserted']}")
    print(f"   Updated:   {stats['updated']}")
    print(f"   Skipped:   {stats['skipped']}")
    print(f"   Total units (before): {before_count}")
    print(f"   Total units (after):  {after_count}")
    print(f"   From Parabellum:      {parabellum_count}")


if __name__ == "__main__":
    main()
