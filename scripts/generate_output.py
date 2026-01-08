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


def main():
    print("[DB] Connecting to database...")
    
    if not os.path.exists(DB_PATH):
        print(f"[ERR] Database not found: {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
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
    """)
    
    rows = cursor.fetchall()
    print(f"[INFO] Found {len(rows)} completed events")
    
    geojson_features = []
    csv_rows = []
    csv_headers = [
        "ID", "Date", "Title", "Lat", "Lon", "TIE", "K", "T", "E", 
        "Reliability", "Bias", "HasVideo", "Sources"
    ]
    
    for db_row in rows:
        try:
            row = dict(db_row)
            
            # === DIRECT COLUMN READS (No JSON parsing!) ===
            event_id = row['event_id']
            date = row['last_seen_date']
            
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
                    ai_data = json.loads(row['ai_report_json'])
                    tactics = ai_data.get('tactics', {})
                    geo = tactics.get('geo_location', {}).get('explicit', {})
                    lat = geo.get('lat')
                    lon = geo.get('lon')
                    
                    # Also fallback other fields
                    if not title:
                        editorial = ai_data.get('editorial', {})
                        title = editorial.get('title_en', '')
                    if not description:
                        editorial = ai_data.get('editorial', {})
                        description = editorial.get('description_en', '')
                except:
                    pass
            
            # Skip if no coordinates
            if not lat or not lon or float(lat) == 0 or float(lon) == 0:
                continue
            
            # Calculate marker style
            radius, color = get_marker_style(tie_score, e_score)
            
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
                    
                    # AI Content
                    "ai_reasoning": ai_summary,
                    "has_video": has_video,
                    
                    # Sources (JSON serialized for JS)
                    "sources_list": json.dumps(structured_sources),
                    
                    # Marker Style
                    "marker_radius": radius,
                    "marker_color": color
                }
            }
            geojson_features.append(feature)
            
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


if __name__ == "__main__":
    main()