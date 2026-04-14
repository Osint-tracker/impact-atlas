"""
Build ORBAT from KMZ v2.0 — Rich Intelligence Extraction
==========================================================
Parses owl_layer.geojson (with ExtendedData) to build enriched orbat_full.json.
Extracts per-unit:
  - name, side, coordinates (last known position)
  - description (operational notes, subordination)
  - military_unit_number (в/ч)
  - last_known_location (with source URL and date)
  - older_geolocations (full movement timeline)
  - emblem_url (unit insignia from Google MyMaps)
  - sidc (APP-6 symbology code, when available)
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding='utf-8')

INPUT_GEOJSON = os.path.join("assets", "data", "owl_layer.geojson")
OUTPUT_ORBAT = os.path.join("assets", "data", "orbat_full.json")


def parse_geolocation_entries(raw_text):
    """
    Parses a geolocation text block into structured entries.
    Input format (newline-separated):
      https://x.com/user/status/1234 Vuhledar 07/11/22
      https://x.com/user/status/5678 Pokrovsk direction 01/03/25
    Returns list of dicts: [{url, location, date, raw}]
    """
    if not raw_text:
        return []

    entries = []
    lines = [l.strip() for l in raw_text.split('\n') if l.strip()]

    for line in lines:
        entry = {"raw": line}

        # Extract URL
        url_match = re.search(r'(https?://\S+)', line)
        if url_match:
            entry["url"] = url_match.group(1)
            # Remainder after URL is location + date
            remainder = line[url_match.end():].strip()
        else:
            remainder = line

        # Try to extract date (DD/MM/YY or DD/MM/YYYY at end of string)
        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})\s*$', remainder)
        if date_match:
            entry["date"] = date_match.group(1)
            remainder = remainder[:date_match.start()].strip()

        # What's left is the location description
        if remainder:
            entry["location"] = remainder

        entries.append(entry)

    return entries


def classify_unit_type(name):
    """Classify unit echelon/type from name keywords."""
    lower = name.lower()

    if any(k in lower for k in ['army', 'corps', 'grouping']):
        return "Army/Corps"
    elif 'division' in lower:
        return "Division"
    elif 'brigade' in lower:
        return "Brigade"
    elif 'regiment' in lower:
        return "Regiment"
    elif 'battalion' in lower:
        return "Battalion"
    elif 'company' in lower:
        return "Company"
    elif any(k in lower for k in ['group', 'detachment', 'task force']):
        return "Group/Detachment"
    elif any(k in lower for k in ['volunteer', 'legion']):
        return "Volunteer"
    elif any(k in lower for k in ['[uav]', 'drone', 'fpv']):
        return "UAV"
    elif any(k in lower for k in ['[territorial]', 'territorial']):
        return "Territorial"
    else:
        return "Unit"


def classify_branch(name, description):
    """Classify branch from name and description."""
    text = f"{name} {description}".lower()

    if any(k in text for k in ['air force', 'aviation', 'helicopter', 'pilot']):
        return "Air Force"
    elif any(k in text for k in ['navy', 'naval', 'marine', 'fleet', 'coastal']):
        return "Navy/Marines"
    elif any(k in text for k in ['airborne', 'parachute', 'airmobile', 'air assault']):
        return "Airborne"
    elif any(k in text for k in ['special', 'spetsnaz', 'sof', 'commando']):
        return "Special Forces"
    elif any(k in text for k in ['national guard', 'rosgvard', 'border guard']):
        return "National Guard"
    elif any(k in text for k in ['territorial', 'tdf']):
        return "Territorial Defense"
    elif any(k in text for k in ['artillery', 'rocket', 'mlrs', 'missile']):
        return "Artillery"
    elif any(k in text for k in ['tank', 'armor', 'armour']):
        return "Armor"
    elif any(k in text for k in ['mechanized', 'motor rifle', 'motorized', 'infantry']):
        return "Mechanized/Infantry"
    elif any(k in text for k in ['engineer', 'pontoon', 'sapper']):
        return "Engineer"
    elif any(k in text for k in ['signal', 'comms', 'communication', 'electronic warfare', 'ew']):
        return "Signals/EW"
    elif any(k in text for k in ['logistics', 'supply', 'maintenance']):
        return "Logistics"
    elif any(k in text for k in ['drone', 'uav', 'fpv', 'unmanned']):
        return "UAV"
    else:
        return "Ground Forces"


def determine_side_from_features(features):
    """Build a side-assignment map based on parent Folder context in KML.
    We use style_url patterns + name heuristics since KML folder hierarchy
    isn't preserved in our flat GeoJSON."""

    # Style URL patterns observed:
    # UA icons tend to use blue (#icon-ci-1, #icon-ci-2, etc.)
    # RU icons tend to use red (#icon-ci-3, #icon-ci-4, etc.)
    # But this isn't reliable enough alone.
    # We rely more on name-based heuristics.
    pass


def build_orbat():
    print("🏗️  ORBAT Builder v2.0 — Rich KMZ Intelligence Extraction")
    print(f"   Timestamp: {datetime.now(timezone.utc).isoformat()}")

    if not os.path.exists(INPUT_GEOJSON):
        print(f"❌ Input file not found: {INPUT_GEOJSON}")
        print("   Run ingest_owl_total.py first.")
        return

    with open(INPUT_GEOJSON, 'r', encoding='utf-8') as f:
        data = json.load(f)

    features = data.get('features', [])
    print(f"   Loaded {len(features)} features from {INPUT_GEOJSON}")

    # Filter to Point features (units)
    unit_features = [f for f in features if f.get('geometry', {}).get('type') == 'Point']
    print(f"   Point features (units): {len(unit_features)}")

    orbat = []
    stats = {"has_description": 0, "has_mil_number": 0, "has_location": 0,
             "has_geolocations": 0, "has_emblem": 0, "ru": 0, "ua": 0}

    for feat in unit_features:
        props = feat.get('properties', {})
        geom = feat.get('geometry', {})
        coords = geom.get('coordinates', [])

        name = props.get('name', '').strip()
        if not name or name.lower() in ('unknown', 'untitled'):
            continue

        description = props.get('description', '').strip()
        side = props.get('side', 'NEUTRAL')
        mil_unit_number = props.get('military_unit_number', '').strip()
        last_location_raw = props.get('last_known_location', '').strip()
        older_geo_raw = props.get('older_geolocations', '').strip()
        older_geo_2_raw = props.get('older_geolocations_2', '').strip()
        emblem = props.get('emblem_url', '').strip()
        sidc = props.get('sidc', '').strip()

        # Parse last known location
        last_location = None
        if last_location_raw:
            entries = parse_geolocation_entries(last_location_raw)
            if entries:
                last_location = entries[0]  # First entry is the latest

        # Parse geolocation timeline (merge both fields, recent first)
        geo_timeline = []
        if older_geo_2_raw:
            geo_timeline.extend(parse_geolocation_entries(older_geo_2_raw))
        if older_geo_raw:
            geo_timeline.extend(parse_geolocation_entries(older_geo_raw))

        # Auto-classify
        unit_type = classify_unit_type(name)
        branch = classify_branch(name, description)

        unit_entry = {
            "name": name,
            "side": side,
            "type": unit_type,
            "branch": branch,
            "lat": coords[1] if len(coords) >= 2 else None,
            "lon": coords[0] if len(coords) >= 2 else None,
        }

        # Add fields only if they have content (keep JSON lean)
        if description:
            unit_entry["description"] = description
            stats["has_description"] += 1
        if mil_unit_number:
            unit_entry["military_unit_number"] = mil_unit_number
            stats["has_mil_number"] += 1
        if last_location:
            unit_entry["last_known_location"] = last_location
            stats["has_location"] += 1
        if geo_timeline:
            unit_entry["geolocation_timeline"] = geo_timeline
            stats["has_geolocations"] += 1
        if emblem:
            unit_entry["emblem_url"] = emblem
            stats["has_emblem"] += 1
        if sidc:
            unit_entry["sidc"] = sidc

        # Stats
        if side == "RU":
            stats["ru"] += 1
        elif side == "UA":
            stats["ua"] += 1

        orbat.append(unit_entry)

    # Sort by side (RU first), then by name
    orbat.sort(key=lambda u: (0 if u['side'] == 'RU' else 1, u['name']))

    # Save
    os.makedirs(os.path.dirname(OUTPUT_ORBAT), exist_ok=True)
    with open(OUTPUT_ORBAT, 'w', encoding='utf-8') as f:
        json.dump(orbat, f, indent=2, ensure_ascii=False)

    # Summary
    print(f"\n{'='*60}")
    print(f"  ORBAT EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"  Total Units:          {len(orbat)}")
    print(f"  RU:                   {stats['ru']}")
    print(f"  UA:                   {stats['ua']}")
    print(f"  NEUTRAL/Unknown:      {len(orbat) - stats['ru'] - stats['ua']}")
    print(f"  With Description:     {stats['has_description']}")
    print(f"  With Mil Unit №:      {stats['has_mil_number']}")
    print(f"  With Last Location:   {stats['has_location']}")
    print(f"  With Geo Timeline:    {stats['has_geolocations']}")
    print(f"  With Emblem:          {stats['has_emblem']}")
    print(f"{'='*60}")
    print(f"  Output: {OUTPUT_ORBAT}")
    print(f"  Size: {os.path.getsize(OUTPUT_ORBAT)/1024/1024:.1f} MB")
    print(f"{'='*60}")


if __name__ == "__main__":
    build_orbat()
