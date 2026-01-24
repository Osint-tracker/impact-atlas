#!/usr/bin/env python3
"""
Ingest ORBAT Data from External WFS
-----------------------------------
Fetches military unit data from the Parabellum WFS service,
normalizes the fields, and saves it to a local JSON file for the frontend.
"""

import sys
import os
import json
import requests
import time
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================
WFS_URL = (
    "https://geo.parabellumthinktank.com/index.php/lizmap/service"
    "?repository=russoukrainianwar&project=russian_invasion_of_ukraine"
    "&SERVICE=WFS&VERSION=1.0.0&REQUEST=GetFeature"
    "&TYPENAME=russian_invasion_of_ukraine_p_2&OUTPUTFORMAT=GeoJSON"
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(BASE_DIR, '../assets/data/orbat_units.json')

# =============================================================================
# NORMALIZATION HELPERS
# =============================================================================
def normalize_faction(insignia_name):
    """Normalize insignia name to 'UA', 'RU', or 'UNKNOWN'."""
    if not insignia_name:
        return 'UNKNOWN'
    
    name_lower = insignia_name.lower()
    if 'ukraine' in name_lower:
        return 'UA'
    if 'russian' in name_lower or 'federation' in name_lower:
        return 'RU'
    return 'UNKNOWN'

def normalize_unit_entry(feature):
    """Convert raw WFS feature to standard unit dict."""
    props = feature.get('properties', {})
    # Handle geometry (can be None)
    geometry = feature.get('geometry')
    coords = geometry.get('coordinates', []) if geometry else []
    
    # Handle MultiPoint vs Point
    lat, lon = None, None
    if coords and geometry:
        if geometry['type'] == 'MultiPoint':
            # Take first point
            lon, lat = coords[0][0], coords[0][1]
        elif feature['geometry']['type'] == 'Point':
            lon, lat = coords[0], coords[1]

    # Construct clean object
    return {
        "orbat_id": str(props.get('id', '')),
        "unit_name": props.get('unit'),           # e.g. "47th"
        "full_name_en": props.get('name_en'),     # Sometimes null
        "faction": normalize_faction(props.get('insignia_name')),
        "type": props.get('unit_type'),           # e.g. "Infantry Mechanized"
        "echelon": props.get('echelon'),          # e.g. "Brigade"
        "echelon_symbol": props.get('echelon_symbol'), # e.g. "X"
        "branch": props.get('branch'),            # e.g. "National Guard"
        "sub_branch": props.get('sub_branch'),
        "garrison": props.get('garrison'),
        "district": props.get('district_of_origin'),
        "commander": props.get('commander'),
        "superior": props.get('unit_superior'),
        "lat": lat,
        "lon": lon,
        "updated_at": props.get('modification_date') or datetime.now().isoformat()
    }

# =============================================================================
# MAIN EXECUTION
# =============================================================================
def main():
    print(f"[INFO] Fetching ORBAT data from WFS...")
    try:
        response = requests.get(WFS_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[ERROR] Failed to fetch WFS data: {e}")
        sys.exit(1)

    features = data.get('features', [])
    print(f"[INFO] Found {len(features)} features. Processing...")

    processed_units = []
    for feat in features:
        unit = normalize_unit_entry(feat)
        # Basic validation: Must have a name or ID
        if unit['unit_name'] or unit['orbat_id']:
            processed_units.append(unit)

    print(f"[INFO] Processed {len(processed_units)} valid units.")

    # Save to JSON
    try:
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(processed_units, f, indent=2, ensure_ascii=False)
        print(f"[SUCCESS] Saved to {OUTPUT_PATH}")
    except Exception as e:
        print(f"[ERROR] Failed to write output file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
