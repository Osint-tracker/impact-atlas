"""
Generate Output Script (v2.0 - Column-Based)
Exports events from SQLite to GeoJSON and CSV.
Reads directly from dedicated columns for reliability.
"""
import sqlite3
import json
import os
import csv
import sys
from urllib.parse import urlparse

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
GEOJSON_PATH = os.path.join(BASE_DIR, '../assets/data/events.geojson')
CSV_PATH = os.path.join(BASE_DIR, '../assets/data/events_export.csv')
UNITS_JSON_PATH = os.path.join(BASE_DIR, '../assets/data/units.json')
ORBAT_JSON_PATH = os.path.join(BASE_DIR, '../assets/data/orbat_units.json')


def parse_sources_to_list(sources_str):
    """Parse sources string to structured list of {name, url}."""
    if not sources_str:
        return []
    
    # Handle both ' | ' and ' ||| ' separators
    if ' ||| ' in str(sources_str):
        urls = [u.strip() for u in str(sources_str).split(' ||| ') if u.strip()]
    elif ' | ' in str(sources_str):
        urls = [u.strip() for u in str(sources_str).split(' | ') if u.strip()]
    else:
        urls = [str(sources_str).strip()] if sources_str else []
    
    result = []
    for url in urls:
        url = str(url).strip()
        # Filter out invalid URLs
        if len(url) < 5 or url.lower() in ['none', 'null', 'unknown', '[null]']:
            continue
        try:
            domain = urlparse(url).netloc.replace('www.', '')
            if not domain:
                domain = "Source"
        except:
            domain = "Source"
        result.append({"name": domain, "url": url})
    
    return result


def load_orbat_data():
    """Load external ORBAT data for enrichment."""
    try:
        if os.path.exists(ORBAT_JSON_PATH):
            with open(ORBAT_JSON_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to load ORBAT data: {e}")
    return []


def enrich_units(ai_units, orbat_data):
    """Fuzzy match AI units with ORBAT data to add metadata."""
    if not ai_units or not orbat_data:
        return ai_units

    for u in ai_units:
        # Match logic
        best_match = None
        # Heuristic: Name containment + Faction match
        u_name = (u.get('unit_name') or '').lower()
        u_id = (u.get('unit_id') or '').lower()
        u_faction = (u.get('faction') or 'UNKNOWN').upper()
        
        # Simple scorer
        best_score = 0
        
        for ob in orbat_data:
            score = 0
            # Safe faction check
            ob_faction = (ob.get('faction') or '').upper()
            if ob_faction != u_faction:
                continue
                
            ob_name = (ob.get('unit_name') or '').lower()
            if not ob_name: continue
            
            # Exact match
            if ob_name == u_name or ob_name == u_id:
                score = 100
            # Strong containment (e.g. "47th" in "47th Brigade")
            elif ob_name in u_name or ob_name in u_id:
                score = 80
            
            if score > best_score:
                best_score = score
                best_match = ob
        
        # Apply enrichment
        if best_match and best_score >= 80:
            u['orbat_id'] = best_match.get('orbat_id')
            u['display_name'] = best_match.get('display_name') # Add this line
            u['echelon'] = best_match.get('echelon')
            u['echelon_symbol'] = best_match.get('echelon_symbol')
            u['type'] = best_match.get('type') # Use standardized type
            u['branch'] = best_match.get('branch')
            u['sub_branch'] = best_match.get('sub_branch')
            u['garrison'] = best_match.get('garrison')
            u['district'] = best_match.get('district')
            u['commander'] = best_match.get('commander')
            u['superior'] = best_match.get('superior')
            
            # Helper to track usage (for merging later)
            best_match['_used'] = True
            
    return ai_units


def get_marker_style(tie_score, effect_score):
    """Calculate marker radius and color based on TIE metrics."""
    try:
        tie_score = float(tie_score or 0)
        effect_score = float(effect_score or 0)
    except:
        tie_score = 0
        effect_score = 0
    
    radius = 4 + (tie_score / 10)
    
    if effect_score >= 8:
        color = "#ef4444"  # Red - Critical
    elif effect_score >= 5:
        color = "#f59e0b"  # Orange - Significant
    elif effect_score >= 3:
        color = "#eab308"  # Yellow - Moderate
    else:
        color = "#64748b"  # Gray - Minor/Unknown
    
    return radius, color



def update_unit_stats(stats_acc, unit, event_data):
    """Accumulate statistics for AI Triage."""
    # Key by ORBAT ID (Priority 1) -> Unit ID -> Unit Name
    key = unit.get('orbat_id')
    if not key:
        key = unit.get('unit_id') or unit.get('unit_name') or 'UNKNOWN'
    
    key = str(key).lower()
    
    if key not in stats_acc:
        stats_acc[key] = {
            "engagement_count": 0,
            "last_active": "2000-01-01",
            "total_tie": 0,
            "tactics_hist": {}, # Histogram of event classifications
            "roles_hist": {},
            "orbat_id": unit.get('orbat_id')
        }
        
    entry = stats_acc[key]
    entry["engagement_count"] += 1
    
    # Date Update
    evt_date = event_data.get('date', '2000-01-01')
    if evt_date > entry["last_active"]:
        entry["last_active"] = evt_date
        
    # Scores
    entry["total_tie"] += event_data.get('tie_score', 0)
    
    # Tactics (Event Classification)
    cls = event_data.get('classification', 'UNKNOWN')
    entry["tactics_hist"][cls] = entry["tactics_hist"].get(cls, 0) + 1


def export_units(unit_stats=None, orbat_data=None):
    print("[DB] Exporting ORBAT Units (with AI Triage)...")
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='units_registry'")
        if not cursor.fetchone():
            print("   [SKIP] units_registry table does not exist.")
            conn.close()
            return


        cursor.execute("SELECT * FROM units_registry ORDER BY last_seen_date DESC")
        rows = cursor.fetchall()
        
        units = []
        for row in rows:
            u = dict(row)
            # Ensure proper types
            if u.get('last_seen_date'):
               u['last_seen_date'] = str(u['last_seen_date'])
            
            # === AI TRIAGE MERGE ===
            if unit_stats:
                # Key Match: Registry Unit ID uses orbat_id if available
                key = (u.get('unit_id') or u.get('unit_name') or '').lower()
                
                stats = unit_stats.get(key)
                
                if stats:
                    u['engagement_count'] = stats['engagement_count']
                    u['last_active'] = stats['last_active']
                    u['avg_tie'] = round(stats['total_tie'] / stats['engagement_count'], 1)
                    
                    # Top Tactic
                    sorted_tactics = sorted(stats['tactics_hist'].items(), key=lambda x: x[1], reverse=True)
                    u['primary_tactic'] = sorted_tactics[0][0] if sorted_tactics else 'UNKNOWN'
                else:
                    u['engagement_count'] = 0
                    u['avg_tie'] = 0
            
            units.append(u)
        
        # === MERGE ORBAT UNITS (Unmatched) ===
        if orbat_data:
            print(f"[INFO] Merging unmatched ORBAT units...")
            orbat_count = 0
            for ob in orbat_data:
                # If NOT matched in enrich_units (no _used flag), add it now
                if not ob.get('_used'):
                    new_u = {
                        "unit_id": ob.get('orbat_id') or ob.get('unit_name'),
                        "display_name": ob.get('unit_name'),
                        "faction": ob.get('faction'),
                        "type": ob.get('type') or 'UNKNOWN',
                        "echelon": ob.get('echelon'),
                        "branch": ob.get('branch'),
                        "sub_branch": ob.get('sub_branch'),
                        "garrison": ob.get('garrison'),
                        "district": ob.get('district'),
                        "commander": ob.get('commander'),
                        "superior": ob.get('superior'),
                        "last_seen_lat": ob.get('lat'),
                        "last_seen_lon": ob.get('lon'),
                        "last_seen_date": ob.get('updated_at'),
                        "status": "ACTIVE", 
                        "source": "PARABELLUM",
                        "engagement_count": 0,
                        "avg_tie": 0
                    }
                    if new_u['last_seen_lat'] and new_u['last_seen_lon']:
                         units.append(new_u)
                         orbat_count += 1
            
            print(f"   [MERGE] Added {orbat_count} unmatched units from Parabellum.")

        # Save to JSON
        with open(UNITS_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(units, f, indent=2, ensure_ascii=False)
            
        print(f"   [DONE] Exported {len(units)} units to {UNITS_JSON_PATH}")
            
        print(f"   [DONE] Exported {len(units)} units to {UNITS_JSON_PATH}")
        conn.close()
    except Exception as e:
        print(f"   [ERR] Failed to export units: {e}")



def main():
    print("[DB] Connecting to database...")
    
    if not os.path.exists(DB_PATH):
        print(f"[ERR] Database not found: {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Load ORBAT Data
    orbat_data = load_orbat_data()
    print(f"[INFO] Loaded {len(orbat_data)} ORBAT units for enrichment.")
    sys.stdout.flush()
    
    # Query ALL columns directly
    cursor.execute("""
        SELECT 
            event_id,
            last_seen_date,
            -- Direct columns (no JSON parsing needed!)
            title,
            description,
            tie_score,
            tie_status,
            kinetic_score,
            target_score,
            effect_score,
            reliability,
            bias_score,
            ai_summary,
            has_video,
            urls_list,
            -- JSON blob for coordinates fallback
            ai_report_json
        FROM unique_events 
        WHERE ai_analysis_status = 'COMPLETED'
          AND urls_list IS NOT NULL 
          AND urls_list != ''
    """)
    
    rows = cursor.fetchall()
    print(f"[INFO] Found {len(rows)} completed events")
    
    print(f"[INFO] Found {len(rows)} completed events")
    
    # 3. AI Triage Accumulator
    unit_stats_acc = {}
    
    geojson_features = []
    csv_rows = []
    csv_headers = [
        "ID", "Date", "Title", "Lat", "Lon", "TIE", "K", "T", "E", 
        "Reliability", "Bias", "HasVideo", "Sources"
    ]
    
    for db_row in rows:
        try:
            row = dict(db_row)
            ai_data = {}  # CRITICAL: Reset for each iteration to avoid stale data

            # === DIRECT COLUMN READS (No JSON parsing!) ===
            event_id = row['event_id']
            
            # Robust date handling - prevent NaT
            date = row['last_seen_date']
            if not date or str(date).lower() in ['none', 'nat', 'null', '']:
                # Fallback to ai_report_json timestamp
                if row.get('ai_report_json'):
                    try:
                        ai_data = json.loads(row['ai_report_json'])
                        date = ai_data.get('timestamp_generated', '')
                        if date:
                            # Extract just the date part (YYYY-MM-DD)
                            date = date[:10] if len(date) >= 10 else date
                    except:
                        pass
                if not date:
                    date = 'Unknown'
            
            # Title/Description
            title = row.get('title') or ''
            description = row.get('description') or ''
            
            # TIE Metrics
            tie_score = float(row.get('tie_score') or 0)
            k_score = float(row.get('kinetic_score') or 0)
            t_score = float(row.get('target_score') or 0)
            e_score = float(row.get('effect_score') or 0)
            
            # Quality Metrics
            reliability = int(row.get('reliability') or 0)
            bias_score = float(row.get('bias_score') or 0)
            
            # AI Content
            ai_summary = row.get('ai_summary') or ''
            has_video = bool(row.get('has_video'))
            
            # Sources
            sources_str = row.get('urls_list') or ''
            structured_sources = parse_sources_to_list(sources_str)
            
            # Coordinates (extracted from JSON blob - no DB column exists)
            lat = None
            lon = None
            
            if row.get('ai_report_json'):
                try:
                    if not ai_data: # If not already loaded above
                        ai_data = json.loads(row['ai_report_json'])
                    
                    tactics = ai_data.get('tactics', {})
                    geo = tactics.get('geo_location', {}).get('explicit', {})
                    lat = geo.get('lat')
                    lon = geo.get('lon')
                    
                    # Fallback for title
                    if not title:
                        editorial = ai_data.get('editorial', {})
                        title = editorial.get('title_en', '')
                    
                    # Multiple fallback sources for description
                    if not description:
                        editorial = ai_data.get('editorial', {})
                        description = editorial.get('description_en', '')
                    
                    # Fallback 2: Use Soldier's summary
                    if not description:
                        event_analysis = tactics.get('event_analysis', {})
                        description = event_analysis.get('summary_en', '')
                    
                    # Fallback 3: Use Brain's strategic assessment
                    if not description:
                        strategy = ai_data.get('strategy', {})
                        description = strategy.get('strategic_value_assessment', '')
                    
                    # Fallback 4: Use AI summary (strategist output)
                    if not description and ai_summary:
                        # Extract just the English part (before [IT])
                        en_part = ai_summary.split('[IT]')[0].replace('[EN]', '').strip()
                        if len(en_part) > 20:  # Only use if substantial
                            description = en_part[:300]
                            
                except Exception as e:
                    print(f"[WARN] Error parsing ai_report_json for {event_id}: {e}")
            
            # Skip if no coordinates
            if not lat or not lon or float(lat) == 0 or float(lon) == 0:
                continue

            # Skip if no valid sources (Python-level check)
            if not structured_sources:
                continue
            
            # Calculate marker style
            radius, color = get_marker_style(tie_score, e_score)
            
            # Enrich Units
            raw_units = ai_data.get('military_units_detected', [])
            enriched_units = enrich_units(raw_units, orbat_data)
            
            # Build GeoJSON Feature
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)]
                },
                "properties": {
                    "id": event_id,
                    "title": title,
                    "description": description,
                    "date": date,
                    
                    # TIE Metrics
                    "tie_total": round(tie_score, 1),
                    "vec_k": k_score,
                    "vec_t": t_score,
                    "vec_e": e_score,
                    
                    # Quality Metrics
                    "reliability": reliability,
                    "bias_score": bias_score,

                    # Tactical Data (Extracted for Dashboard)
                    "classification": ai_data.get('classification', ai_data.get('event_analysis', {}).get('classification', 'UNKNOWN')),
                    "target_type": ai_data.get('target_type', ai_data.get('titan_assessment', {}).get('target_type_category', 'UNKNOWN')),
                    "intensity_score": k_score, # Mapping Kinetic Score to Intensity
                    
                    # AI Content
                    "ai_reasoning": ai_summary,
                    "has_video": has_video,
                    
                    # Sources (JSON serialized for JS)
                    "sources_list": json.dumps(structured_sources),
                    
                    "units": json.dumps(enriched_units),
                    
                    # Marker Style
                    "marker_radius": radius,
                    "marker_color": color
                }
            }
            geojson_features.append(feature)
            
            # --- AI TRIAGE UPDATE ---
            for u in enriched_units:
                update_unit_stats(unit_stats_acc, u, {
                    "date": date,
                    "tie_score": tie_score,
                    "classification": ai_data.get('classification', 'UNKNOWN')
                })
            
            # Build CSV Row
            csv_rows.append({
                "ID": event_id,
                "Date": date,
                "Title": title[:50],
                "Lat": lat,
                "Lon": lon,
                "TIE": round(tie_score, 1),
                "K": k_score,
                "T": t_score,
                "E": e_score,
                "Reliability": reliability,
                "Bias": bias_score,
                "HasVideo": 1 if has_video else 0,
                "Sources": len(structured_sources)
            })
            
        except Exception as e:
            print(f"[WARN] Error processing {row.get('event_id')}: {e}")
            continue
    
    conn.close()
    
    # Save GeoJSON
    os.makedirs(os.path.dirname(GEOJSON_PATH), exist_ok=True)
    with open(GEOJSON_PATH, 'w', encoding='utf-8') as f:
        json.dump({
            "type": "FeatureCollection",
            "features": geojson_features
        }, f, indent=2, ensure_ascii=False)
    
    # Save CSV
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(csv_rows)
    
    print(f"\n[DONE] Export complete!")
    print(f"   GeoJSON: {len(geojson_features)} events -> {GEOJSON_PATH}")
    print(f"   CSV: {len(csv_rows)} rows -> {CSV_PATH}")

    # Export Units
    export_units(unit_stats_acc, orbat_data)


if __name__ == "__main__":
    main()