"""
Diagnostic script to analyze why generate_output.py only exports 132 events.
"""
import sqlite3
import json
import os

DB_PATH = r'c:\Users\lucag\.vscode\cli\osint-tracker\war_tracker_v2\data\raw_events.db'

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
c = conn.cursor()

# 1. COMPLETED but NO urls_list
c.execute("""SELECT COUNT(*) FROM unique_events 
    WHERE ai_analysis_status = 'COMPLETED' 
    AND (urls_list IS NULL OR urls_list = '')""")
print(f"COMPLETED but NO urls_list: {c.fetchone()[0]}")

# 2. Table schema
c.execute("PRAGMA table_info(unique_events)")
cols = c.fetchall()
col_names = [col['name'] for col in cols]
print(f"\nALL COLUMNS ({len(cols)}):")
for col in cols:
    print(f"  {col['name']} ({col['type']})")

# 3. Check if lat/lon columns exist
has_lat = 'lat' in col_names or 'latitude' in col_names
print(f"\nDirect lat column exists: {has_lat}")

# 4. Check source_urls in ai_report_json for events WITHOUT urls_list
c.execute("""SELECT event_id, ai_report_json FROM unique_events 
    WHERE ai_analysis_status = 'COMPLETED' 
    AND (urls_list IS NULL OR urls_list = '')
    LIMIT 5""")
print("\nSample COMPLETED events WITHOUT urls_list:")
for row in c.fetchall():
    eid = row['event_id'][:40]
    rj = row['ai_report_json']
    if rj:
        try:
            ai = json.loads(rj)
            keys = list(ai.keys())[:8]
            sources = ai.get('source_urls', ai.get('sources', []))
            tactics = ai.get('tactics', {})
            geo = tactics.get('geo_location', {}).get('explicit', {})
            lat = geo.get('lat')
            lon = geo.get('lon')
            print(f"  {eid}")
            print(f"    top-level keys: {keys}")
            print(f"    sources_in_json: {type(sources).__name__}({len(sources) if isinstance(sources, list) else 'N/A'})")
            print(f"    lat={lat}, lon={lon}")
        except Exception as e:
            print(f"  {eid}: JSON parse error: {e}")
    else:
        print(f"  {eid}: NO ai_report_json")

# 5. Geo analysis for ALL completed events
c.execute("""SELECT ai_report_json FROM unique_events 
    WHERE ai_analysis_status = 'COMPLETED'""")
all_rows = c.fetchall()
has_geo = 0
no_geo = 0
alt_geo_paths = set()
for row in all_rows:
    rj = row['ai_report_json']
    if not rj:
        no_geo += 1
        continue
    try:
        ai = json.loads(rj)
        tactics = ai.get('tactics', {})
        geo = tactics.get('geo_location', {}).get('explicit', {})
        lat = geo.get('lat')
        lon = geo.get('lon')
        if lat and lon and float(lat) != 0 and float(lon) != 0:
            has_geo += 1
        else:
            no_geo += 1
            # Check alt paths
            gl = tactics.get('geo_location', {})
            for k in gl.keys():
                if k != 'explicit':
                    alt_geo_paths.add(f"geo_location.{k}")
            if 'location' in ai:
                alt_geo_paths.add("root.location")
            ea = tactics.get('event_analysis', {})
            if 'location' in ea:
                alt_geo_paths.add("event_analysis.location")
    except:
        no_geo += 1

print(f"\nGEO ANALYSIS (ALL {len(all_rows)} completed):")
print(f"  Has geo: {has_geo}")
print(f"  No geo: {no_geo}")
print(f"  Alt geo paths found: {alt_geo_paths}")

# 6. Check what 'aggregated_urls' and 'source_urls' columns look like
c.execute("""SELECT event_id, urls_list FROM unique_events 
    WHERE ai_analysis_status = 'COMPLETED' 
    AND urls_list IS NOT NULL AND urls_list != ''
    LIMIT 3""")
print("\nSample events WITH urls_list:")
for row in c.fetchall():
    eid = row['event_id'][:40]
    urls = row['urls_list'][:120] if row['urls_list'] else 'NULL'
    print(f"  {eid}: {urls}")

# 7. Check if there's an aggregated_urls or original_urls column
for col_name in ['aggregated_urls', 'original_urls', 'source_urls', 'raw_urls']:
    if col_name in col_names:
        c.execute(f"""SELECT COUNT(*) FROM unique_events 
            WHERE ai_analysis_status = 'COMPLETED' 
            AND {col_name} IS NOT NULL AND {col_name} != '' """)
        count = c.fetchone()[0]
        print(f"\n  Column '{col_name}' has {count} non-empty completed events")

conn.close()
