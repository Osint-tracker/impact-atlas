import json
import os
import sys

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
GEOJSON_PATH = os.path.join(BASE_DIR, '../assets/data/events.geojson')

def verify_geojson():
    if not os.path.exists(GEOJSON_PATH):
        print(f"❌ GeoJSON file not found: {GEOJSON_PATH}")
        return

    print(f"🔍 Inspecting {GEOJSON_PATH}...")
    
    with open(GEOJSON_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)

    features = data.get('features', [])
    total = len(features)
    print(f"Total Events Found: {total}")
    
    missing_sources = 0
    missing_metrics = 0
    zero_metrics = 0
    malformed_sources = 0

    if total == 0:
        print("⚠️ No events to verify.")
        return

    for i, feature in enumerate(features):
        props = feature.get('properties', {})
        id = props.get('id', f"Index_{i}")

        # 1. Check Sources
        sources = props.get('sources_list')
        if not sources or sources == '[]' or sources == '""':
            missing_sources += 1
            # print(f"   [WARN] ID {id} missing sources")
        else:
            try:
                # sources_list should be a JSON string like '[{"name":..., "url":...}]'
                parsed_sources = json.loads(sources)
                if not isinstance(parsed_sources, list) or len(parsed_sources) == 0:
                     malformed_sources += 1
            except:
                malformed_sources += 1

        # 2. Check Metrics
        t = props.get('vec_t')
        k = props.get('vec_k')
        e = props.get('vec_e')
        rel = props.get('reliability')
        bias = props.get('bias_score')

        if t is None or k is None or e is None or rel is None:
            missing_metrics += 1
            print(f"   [ERR] ID {id} missing metrics keys (K:{k} T:{t} E:{e} Rel:{rel})")
        
        # Check for 0-0-0 events (might be valid, but suspicious if ALL are 0)
        if t == 0 and k == 0 and e == 0:
            zero_metrics += 1

    print("\n=== AUDIT RESULTS ===")
    print(f"✅ Total Events Scanned: {total}")
    print(f"--------------------------------")
    print(f"❌ Missing/Empty Sources: {missing_sources}")
    print(f"⚠️ Malformed Source JSON: {malformed_sources}")
    print(f"❌ Missing Metrics Keys:  {missing_metrics}")
    print(f"ℹ️  All-Zero TIE Scores:   {zero_metrics} (Might be valid for low-intensity events)")
    
    if missing_sources == 0 and missing_metrics == 0:
        print("\n🏆 INTEGRITY CHECK PASSED: All events have sources and metrics structure.")
    else:
        print("\n⚠️ INTEGRITY CHECK FAILED: Found missing data.")

if __name__ == "__main__":
    verify_geojson()
