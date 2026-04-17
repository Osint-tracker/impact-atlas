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
from dotenv import load_dotenv

from campaigns_engine import (
    build_campaign_reports,
    build_campaigns_geo,
    ensure_campaign_columns,
    load_campaign_definitions,
)

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
IMPACT_ATLAS_DB_PATH = os.path.join(BASE_DIR, '../impact_atlas.db')
GEOJSON_PATH = os.path.join(BASE_DIR, '../assets/data/events.geojson')
CSV_PATH = os.path.join(BASE_DIR, '../assets/data/events_export.csv')
UNITS_JSON_PATH = os.path.join(BASE_DIR, '../assets/data/units.json')
ORBAT_JSON_PATH = os.path.join(BASE_DIR, '../assets/data/orbat_units.json')
STRATEGIC_TRENDS_PATH = os.path.join(BASE_DIR, '../assets/data/strategic_trends.json')
EXTERNAL_LOSSES_PATH = os.path.join(BASE_DIR, '../assets/data/external_losses.json')
SECTOR_ANOMALIES_PATH = os.path.join(BASE_DIR, '../assets/data/sector_anomalies.json')
ASYMMETRY_INDEX_PATH = os.path.join(BASE_DIR, '../assets/data/asymmetry_index.json')
GLOCS_PATH = os.path.join(BASE_DIR, '../assets/data/glocs.geojson')
CAMPAIGN_DEFINITIONS_CACHE_PATH = os.path.join(BASE_DIR, '../assets/data/campaign_definitions.json')
CAMPAIGN_REPORTS_PATH = os.path.join(BASE_DIR, '../assets/data/campaign_reports.json')
CAMPAIGNS_GEO_PATH = os.path.join(BASE_DIR, '../assets/data/campaigns_geo.json')

import datetime as _dt

load_dotenv()


try:
    from v42_analytics import (
        ensure_sources_reputation_schema,
        apply_reputation_decay,
        domains_from_structured_sources,
        update_event_reputation,
        extract_classification,
        extract_faction,
        parse_event_datetime,
        INSTITUTIONAL_DOMAINS,
        compute_sector_volume_anomalies,
        apply_anomaly_flags,
        compute_asymmetry_index,
        build_glocs_geojson,
        write_json,
    )
except ImportError:
    from scripts2.v42_analytics import (
        ensure_sources_reputation_schema,
        apply_reputation_decay,
        domains_from_structured_sources,
        update_event_reputation,
        extract_classification,
        extract_faction,
        parse_event_datetime,
        INSTITUTIONAL_DOMAINS,
        compute_sector_volume_anomalies,
        apply_anomaly_flags,
        compute_asymmetry_index,
        build_glocs_geojson,
        write_json,
    )

def _date_to_epoch_ms(date_str):
    """Convert a date string to Unix epoch milliseconds.
    Supports: '2026-01-23 12:30:00+00:00', '2026-01-23 12:30:00', '2026-01-23'
    Returns 0 on failure."""
    if not date_str or not isinstance(date_str, str):
        return 0
    date_str = date_str.strip()
    for fmt in ('%Y-%m-%d %H:%M:%S%z', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            dt = _dt.datetime.strptime(date_str[:len(fmt)+5], fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_dt.timezone.utc)
            return int(dt.timestamp() * 1000)
        except (ValueError, OverflowError):
            continue
    return 0


def parse_sources_to_list(sources_str):
    """Parse sources string to structured list of {name, url}.
    
    Handles:
    - JSON arrays of URLs: '["https://t.me/rybar/123", "https://tass.com/..."]'
    - JSON arrays of names: '["Rybar", "GDELT_Network"]'
    - Pipe-separated legacy: 'https://a.com | https://b.com'
    """
    if not sources_str or sources_str == '[]':
        return []
    
    items = []
    # Try JSON parse first
    try:
        parsed = json.loads(sources_str)
        if isinstance(parsed, list):
            items = parsed
    except:
        pass
    
    # Fallback to pipe separators
    if not items:
        if ' ||| ' in str(sources_str):
            items = [u.strip() for u in str(sources_str).split(' ||| ') if u.strip()]
        elif ' | ' in str(sources_str):
            items = [u.strip() for u in str(sources_str).split(' | ') if u.strip()]
        else:
            items = [str(sources_str).strip()] if sources_str else []
    
    result = []
    for item in items:
        item = str(item).strip()
        if len(item) < 3 or item.lower() in ['none', 'null', 'unknown', '[null]']:
            continue
            
        is_url = item.startswith('http') or item.startswith('www.')
        if is_url:
            url = item
            # Special handling for t.me URLs: extract channel name as display
            if 't.me/' in url:
                # https://t.me/rybar/76184 -> channel = "rybar"
                try:
                    parts = url.split('t.me/')[1].split('/')
                    channel_name = parts[0] if parts else 't.me'
                    result.append({"name": channel_name, "url": url})
                except:
                    result.append({"name": "Telegram", "url": url})
            else:
                try:
                    domain = urlparse(url if url.startswith('http') else 'https://'+url).netloc.replace('www.', '')
                    if not domain:
                        domain = "Source"
                except:
                    domain = "Source"
                result.append({"name": domain, "url": url})
        else:
            # Plain name (e.g. channel name "Rybar" or "GDELT_Network" or a domain "tass.com")
            import re as _re
            if item == 'GDELT_Network':
                result.append({"name": "GDELT", "url": "#"})
            elif '.' in item and not item.startswith('@'):
                # Looks like a domain name (has dots, e.g. "dailyadvance.com") — NOT a Telegram handle
                url = f"https://{item}" if not item.startswith('http') else item
                result.append({"name": item, "url": url})
            elif _re.match(r'^[A-Za-z0-9_]+$', item):
                # Looks like a Telegram handle (alphanumeric + underscore only)
                result.append({"name": item, "url": f"https://t.me/{item}"})
            
    # Deduplicate: group by channel/domain, keep first URL per name
    seen_names = {}
    unique_result = []
    for r in result:
        key = r['name'].lower()
        if key not in seen_names:
            seen_names[key] = r
            unique_result.append(r)
        else:
            # If we already have this source but with a '#' url, upgrade it
            if seen_names[key]['url'] == '#' and r['url'] != '#':
                seen_names[key]['url'] = r['url']
            
    return unique_result


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


def classify_sector(lat, lon, target_type):
    """
    Classify event into strategic sector based on geography and target.
    
    Sectors:
    - ENERGY_COERCION: Attacks on power/energy infrastructure
    - DEEP_STRIKES_RU: Strikes into Russian rear areas
    - EASTERN_FRONT: Donbas/Kharkiv axis
    - SOUTHERN_FRONT: Zaporizhzhia/Kherson axis
    """
    target_type_lower = (target_type or '').lower()
    
    # Priority 1: Energy infrastructure (can be anywhere)
    energy_keywords = ['power', 'grid', 'dam', 'plant', 'refinery', 'substation', 'transformer', 'energy']
    if any(kw in target_type_lower for kw in energy_keywords):
        return 'ENERGY_COERCION'
    
    # Priority 2: Deep Strikes into Russia
    # Rough heuristic: lat > 50.0 AND lon > 36.0 (Russian rear/Belgorod/Kursk)
    # Also includes airfield strikes anywhere
    if 'airfield' in target_type_lower or 'airbase' in target_type_lower:
        return 'DEEP_STRIKES_RU'
    if lat and lon and float(lat) > 50.0 and float(lon) > 36.0:
        return 'DEEP_STRIKES_RU'
    
    # Geographic sectors based on coordinates
    try:
        lat_f = float(lat) if lat else 0
        lon_f = float(lon) if lon else 0
    except:
        return 'EASTERN_FRONT'  # Default fallback
    
    # Southern Front: lon <= 36.0 AND lat < 48.0 (Zaporizhzhia/Kherson)
    if lon_f <= 36.0 and lat_f < 48.0:
        return 'SOUTHERN_FRONT'
    
    # Eastern Front: lon > 36.0 AND lat < 50.0 (Donbas/Kharkiv)
    if lon_f > 36.0 and lat_f < 50.0:
        return 'EASTERN_FRONT'
    
    # Default to Eastern Front for unclassified
    return 'EASTERN_FRONT'



def update_unit_stats(stats_acc, unit, event_data):
    """Accumulate statistics for AI Triage + Dossier Card aggregation."""
    import re as _re_local
    key = unit.get('orbat_id')
    if not key:
        key = unit.get('unit_id') or unit.get('unit_name') or 'UNKNOWN'
    
    key = str(key).lower()
    
    if key not in stats_acc:
        stats_acc[key] = {
            "engagement_count": 0,
            "last_active": "2000-01-01",
            "total_tie": 0,
            "tactics_hist": {},
            "roles_hist": {},
            "orbat_id": unit.get('orbat_id'),
            "tie_vectors": [],
            "assets_set": set(),
            "daily_dates": [],
            "recent_events": [],
        }
        
    entry = stats_acc[key]
    entry["engagement_count"] += 1
    
    evt_date = event_data.get('date', '2000-01-01')
    if evt_date and evt_date > entry["last_active"]:
        entry["last_active"] = evt_date
        
    entry["total_tie"] += event_data.get('tie_score', 0)
    
    cls = event_data.get('classification', 'UNKNOWN')
    entry["tactics_hist"][cls] = entry["tactics_hist"].get(cls, 0) + 1

    # --- DOSSIER: T.I.E. Vector Collection ---
    k = event_data.get('kinetic_score', 0)
    t = event_data.get('target_score', 0)
    e = event_data.get('effect_score', 0)
    if k or t or e:
        entry["tie_vectors"].append({"kinetic": float(k), "target": float(t), "effect": float(e)})

    # --- DOSSIER: Asset Detection (Visionary + Regex Fallback) ---
    detected_assets = event_data.get('detected_assets', [])
    if detected_assets:
        for a in detected_assets:
            atype = a.get('type', '') if isinstance(a, dict) else str(a)
            if atype and atype not in ('UNKNOWN_ARMOR', 'UNKNOWN_VEHICLE', 'UNKNOWN_SYSTEM', 'UNKNOWN_AIRCRAFT'):
                entry["assets_set"].add(atype)
    else:
        _ASSET_RE = _re_local.compile(
            r'\b(T-(?:72|80|90|64|55)[A-Z0-9]*|BMP-[123][A-Z]*|BTR-[0-9]+[A-Z]*'
            r'|2S(?:1|3|5|7|19|35)[A-Z\- ]*|HIMARS|GMLRS|M270|M142'
            r'|Grad|Smerch|Uragan|TOS-1[A]?|S-[234]00[A-Z0-9]*|Buk[- ]?[A-Z0-9]*'
            r'|Patriot|NASAMS|IRIS-T|Gepard|Iskander[- ]?[MK]?|Kalibr|Kinzhal'
            r'|Shahed[- ]?1[0-9]{2}|Lancet[- ]?[0-9]*|FPV|Orlan[- ]?10'
            r'|Ka-52|Su-[0-9]+[A-Z]*|Leopard[- ]?[12][A-Z0-9]*|Bradley|CV90'
            r'|CAESAR|PzH[- ]?2000|Krab|M777|Storm Shadow|ATACMS|Javelin|NLAW'
            r'|Stugna[- ]?P?|Kornet)\b', _re_local.IGNORECASE
        )
        text_blob = f"{event_data.get('title', '')} {event_data.get('description', '')}"
        for m in _ASSET_RE.findall(text_blob):
            entry["assets_set"].add(m.strip())

    # --- DOSSIER: Daily Date for Sparkline ---
    if evt_date and evt_date != '2000-01-01':
        entry["daily_dates"].append(evt_date[:10])

    # --- DOSSIER: Recent Events ---
    entry["recent_events"].append({
        "date": evt_date,
        "title": event_data.get('title', ''),
        "location": event_data.get('location', ''),
        "lat": event_data.get('lat'),
        "lon": event_data.get('lon'),
        "url": event_data.get('url', ''),
        "event_id": event_data.get('event_id', ''),
    })


def _build_dossier_fields(stats_entry):
    """Convert accumulated raw stats into final dossier card fields."""
    import datetime as _dt_local
    result = {}

    vecs = stats_entry.get('tie_vectors', [])
    if vecs:
        n = len(vecs)
        result['avg_tie'] = {
            'kinetic': round(sum(v['kinetic'] for v in vecs) / n, 2),
            'target': round(sum(v['target'] for v in vecs) / n, 2),
            'effect': round(sum(v['effect'] for v in vecs) / n, 2),
        }
    else:
        result['avg_tie'] = {'kinetic': 0, 'target': 0, 'effect': 0}

    result['assets_detected'] = sorted(list(stats_entry.get('assets_set', set())))

    raw_dates = stats_entry.get('daily_dates', [])
    if raw_dates:
        valid_dates = sorted([d for d in raw_dates if d and len(d) >= 10])
        anchor = _dt_local.datetime.strptime(valid_dates[-1], '%Y-%m-%d').date() if valid_dates else _dt_local.date.today()
    else:
        anchor = _dt_local.date.today()

    trend = [0] * 30
    date_counter = {}
    for d in raw_dates:
        date_counter[d] = date_counter.get(d, 0) + 1
    for i in range(30):
        day_key = (anchor - _dt_local.timedelta(days=29 - i)).strftime('%Y-%m-%d')
        trend[i] = date_counter.get(day_key, 0)

    result['engagement_trend_30d'] = trend
    result['engagement_trend_anchor'] = anchor.isoformat()

    total_30d = sum(trend)
    if total_30d > 8:
        result['engagement_freq_label'] = 'High'
    elif total_30d >= 3:
        result['engagement_freq_label'] = 'Medium'
    else:
        result['engagement_freq_label'] = 'Low'

    recent = stats_entry.get('recent_events', [])
    recent_sorted = sorted(recent, key=lambda x: x.get('date', ''), reverse=True)
    result['recent_engagements'] = recent_sorted[:5]

    return result


# =============================================================================
# IMPACT ATLAS INTEGRATION: UALosses -> Unit Cards
# =============================================================================
def enrich_units_with_casualties(units_list):
    """Enrich unit objects with verified casualty data from impact_atlas.db.
    
    Uses 'unit_raw' from raw_data as the AUTHORITATIVE source for unit assignment.
    Only matches casualties to UA-faction units (UALosses tracks Ukrainian fallen soldiers).
    """
    if not os.path.exists(IMPACT_ATLAS_DB_PATH):
        print("   [SKIP] impact_atlas.db not found, skipping casualty enrichment.")
        return units_list

    try:
        conn = sqlite3.connect(IMPACT_ATLAS_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Fetch all UALosses events — these are UKRAINIAN casualties
        cursor.execute("""
            SELECT raw_data
            FROM kinetic_events
            WHERE source = 'UALosses'
        """)
        rows = cursor.fetchall()
        conn.close()

        # Build lookup: unit_raw (lowercase) -> list of casualties
        # unit_raw is the AUTHORITATIVE field scraped from ualosses.org
        casualties_by_unit_raw = {}
        no_unit_count = 0
        for row in rows:
            try:
                data = json.loads(row['raw_data'])
                unit_raw = (data.get('unit_raw') or '').strip()
                if not unit_raw:
                    no_unit_count += 1
                    continue
                
                # Normalize: remove rank suffixes that sometimes get appended
                # e.g., "105th Separate Territorial Defense Battalion Master" -> "105th Separate Territorial Defense Battalion"
                unit_key = unit_raw.lower()
                
                if unit_key not in casualties_by_unit_raw:
                    casualties_by_unit_raw[unit_key] = []
                
                casualties_by_unit_raw[unit_key].append({
                    "name": data.get('name', 'Unknown'),
                    "rank": data.get('rank'),
                    "source_url": data.get('source_url', ''),
                    "context": data.get('context', '')
                })
            except Exception:
                pass

        print(f"   [CASUALTIES] Parsed {len(rows)} UALosses records -> {len(casualties_by_unit_raw)} unique unit_raw values ({no_unit_count} without unit)")

        # Match casualties to UA-faction units ONLY
        matched = 0
        total_casualties_assigned = 0
        for unit in units_list:
            faction = (unit.get('faction') or '').upper()
            
            # CRITICAL: UALosses data is ONLY for Ukrainian soldiers
            # Never assign Ukrainian casualties to Russian (RU) units
            if faction != 'UA':
                continue
            
            display_name = (unit.get('display_name') or '').strip().strip('"').strip("'")
            unit_id = unit.get('unit_id', '')
            
            if not display_name and not unit_id:
                continue
            
            # Build candidate match strings from the unit
            candidates = []
            if display_name:
                candidates.append(display_name.lower())
            if unit_id:
                # Convert UA_105TH_SEPARATE_TDB -> "105th separate tdb"
                readable = unit_id.replace('UA_', '').replace('_', ' ').lower()
                candidates.append(readable)
            
            # Try to find a matching unit_raw entry
            best_match = None
            best_count = 0
            
            for unit_key, cas_list in casualties_by_unit_raw.items():
                for candidate in candidates:
                    # Require substantial overlap, not just substring "5" matching everything
                    # Both strings must share significant content
                    if len(candidate) < 5 or len(unit_key) < 5:
                        continue
                    
                    # Strategy 1: One contains the other
                    if candidate in unit_key or unit_key in candidate:
                        if len(cas_list) > best_count:
                            best_match = unit_key
                            best_count = len(cas_list)
                    
                    # Strategy 2: Shared numeric identifier matches
                    # e.g., "47th" in both strings
                    import re
                    cand_nums = set(re.findall(r'\d+', candidate))
                    key_nums = set(re.findall(r'\d+', unit_key))
                    if cand_nums and key_nums and cand_nums == key_nums:
                        # Number match — now check type keyword overlap
                        type_keywords = {'brigade', 'battalion', 'regiment', 'division', 'guard', 'assault', 
                                        'mechanized', 'territorial', 'airborne', 'marine', 'artillery', 'defense'}
                        cand_words = set(candidate.split())
                        key_words = set(unit_key.split())
                        shared_types = (cand_words & key_words) & type_keywords
                        if shared_types and len(cas_list) > best_count:
                            best_match = unit_key
                            best_count = len(cas_list)
            
            if best_match:
                unit['verified_casualties'] = casualties_by_unit_raw[best_match]
                unit['casualty_count'] = len(casualties_by_unit_raw[best_match])
                matched += 1
                total_casualties_assigned += len(casualties_by_unit_raw[best_match])

        print(f"   [CASUALTIES] Enriched {matched} UA units with {total_casualties_assigned} total casualty records.")

    except Exception as e:
        print(f"   [ERR] Failed to enrich casualties: {e}")

    return units_list


# =============================================================================
# IMPACT ATLAS INTEGRATION: Oryx + LostArmour -> Equipment Timeline
# =============================================================================
def export_equipment_losses():
    """Export Oryx and LostArmour data from impact_atlas.db to external_losses.json."""
    if not os.path.exists(IMPACT_ATLAS_DB_PATH):
        print("   [SKIP] impact_atlas.db not found, skipping equipment losses export.")
        return

    try:
        conn = sqlite3.connect(IMPACT_ATLAS_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT event_id, source, date, raw_data
            FROM kinetic_events
            WHERE source IN ('Oryx', 'LostArmour_fpv', 'LostArmour_lancet')
        """)
        rows = cursor.fetchall()
        conn.close()

        losses = []
        for row in rows:
            try:
                data = json.loads(row['raw_data'])
                source = row['source']

                if source == 'Oryx':
                    # Oryx: category (Tanks, IFVs...), entry (model name), status
                    category = data.get('category', 'Vehicle')
                    
                    # Construct per-category Oryx article URL
                    # Oryx has two main articles: Russian losses and Ukrainian losses
                    oryx_ru_base = 'https://www.oryxspioenkop.com/2022/02/attack-on-europe-documenting-equipment.html'
                    oryx_ua_base = 'https://www.oryxspioenkop.com/2022/02/attack-on-europe-documenting-ukrainian.html'
                    
                    # This export tracks Russian losses by default
                    proof_url = oryx_ru_base
                    
                    loss = {
                        "date": row['date'] or 'Unknown',
                        "model": data.get('entry', 'Unknown'),
                        "type": category,
                        "country": "RUS",
                        "status": data.get('status', 'Verified Loss'),
                        "proof_url": proof_url,
                        "source_tag": "Oryx"
                    }
                else:
                    # LostArmour: precision strike data (Lancet, FPV)
                    weapon_type = 'Lancet' if 'lancet' in source.lower() else 'FPV Drone'
                    loss = {
                        "date": row['date'] or 'Unknown',
                        "model": weapon_type,
                        "type": f"Precision Strike ({data.get('tag', weapon_type)})",
                        "country": "UA",  # LostArmour tracks Ukrainian losses
                        "status": "Verified Strike",
                        "proof_url": data.get('source_url', 'https://lostarmour.info'),
                        "source_tag": "LostArmour",
                        "description": data.get('description', '')
                    }

                losses.append(loss)

            except Exception as e:
                continue

        # Sort by date descending
        losses.sort(key=lambda x: x.get('date', ''), reverse=True)

        # Write to file
        with open(EXTERNAL_LOSSES_PATH, 'w', encoding='utf-8') as f:
            json.dump(losses, f, indent=2, ensure_ascii=False)

        print(f"   [EQUIPMENT] Exported {len(losses)} equipment loss records to {EXTERNAL_LOSSES_PATH}")

    except Exception as e:
        print(f"   [ERR] Failed to export equipment losses: {e}")


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
            
            # Fallback for display_name
            if not u.get('display_name'):
                u['display_name'] = u.get('unit_id') or 'Unknown Unit'
            
            # Enrich u with ORBAT metadata for accurate stats matching
            matches = enrich_units([u], orbat_data)
            u = matches[0] if matches else u
            
            # === AI TRIAGE MERGE + DOSSIER CARD FIELDS ===
            if unit_stats:
                # Key Match: Registry matches update_unit_stats key logic
                key = u.get('orbat_id')
                if not key:
                    key = (u.get('unit_id') or u.get('unit_name') or '').lower()
                
                stats = unit_stats.get(str(key).lower())
                
                if stats:
                    u['engagement_count'] = stats['engagement_count']
                    u['last_active'] = stats['last_active']
                    
                    # Top Tactic
                    sorted_tactics = sorted(stats['tactics_hist'].items(), key=lambda x: x[1], reverse=True)
                    u['primary_tactic'] = sorted_tactics[0][0] if sorted_tactics else 'UNKNOWN'

                    # --- DOSSIER CARD FIELDS ---
                    dossier = _build_dossier_fields(stats)
                    u['avg_tie'] = dossier['avg_tie']
                    u['assets_detected'] = dossier['assets_detected']
                    u['engagement_trend_30d'] = dossier['engagement_trend_30d']
                    u['engagement_trend_anchor'] = dossier['engagement_trend_anchor']
                    u['engagement_freq_label'] = dossier['engagement_freq_label']
                    u['recent_engagements'] = dossier['recent_engagements']
                else:
                    u['engagement_count'] = 0
                    u['avg_tie'] = {'kinetic': 0, 'target': 0, 'effect': 0}
                    u['assets_detected'] = []
                    u['engagement_trend_30d'] = [0] * 30
                    u['engagement_freq_label'] = 'Low'
                    u['recent_engagements'] = []
            
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

        # === IMPACT ATLAS: Enrich with UALosses casualties ===
        units = enrich_units_with_casualties(units)

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
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    ensure_campaign_columns(conn)
    ensure_sources_reputation_schema(conn)
    apply_reputation_decay(conn)
    
    # Load ORBAT Data
    orbat_data = load_orbat_data()
    print(f"[INFO] Loaded {len(orbat_data)} ORBAT units for enrichment.")
    sys.stdout.flush()

    campaign_definitions = load_campaign_definitions(
        sheet_url=os.getenv('SHEET_CSV_URL', ''),
        cache_path=CAMPAIGN_DEFINITIONS_CACHE_PATH,
        tab_name='campaign_definitions',
    )
    campaign_index = {c.get('campaign_id'): c for c in campaign_definitions}
    print(f"[INFO] Loaded {len(campaign_definitions)} campaign definitions.")
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
            sources_list,
            -- JSON blob for coordinates fallback
            ai_report_json,
            operational_sector,
            image_phash,
            source_reputation_score,
            ai_analysis_status,
            campaign_id,
            campaign_match_meta,
            campaign_tagged_at
        FROM unique_events 
        WHERE ai_analysis_status = 'COMPLETED'
    """)
    
    rows = cursor.fetchall()
    
    # Pre-build lookup: event_id → actual Telegram deep links from raw_signals
    # This resolves channel-only URLs (e.g. t.me/rybar) to specific messages (t.me/rybar/76184)
    tg_deeplinks = {}  # {event_id: {channel_name: url_with_msg_id}}
    try:
        cursor.execute("""
            SELECT cluster_id, url, source_name 
            FROM raw_signals 
            WHERE url LIKE '%t.me/%/%' AND cluster_id IS NOT NULL
        """)
        for sig in cursor.fetchall():
            cid = sig['cluster_id']
            url = sig['url']
            if not cid or not url:
                continue
            # Extract channel name from URL
            try:
                channel = url.split('t.me/')[1].split('/')[0]
            except:
                continue
            if cid not in tg_deeplinks:
                tg_deeplinks[cid] = {}
            # Keep the first deep link per channel per event
            if channel not in tg_deeplinks[cid]:
                tg_deeplinks[cid][channel] = url
        print(f"[INFO] Built Telegram deep-link lookup: {len(tg_deeplinks)} events with deep links")
    except Exception as e:
        print(f"[WARN] Could not build deep-link lookup: {e}")
    
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
            
            # Sources — MERGE urls_list + sources_list + ai_report_json for best coverage
            all_url_strs = []
            
            # Collect from urls_list (usually has actual deep links)  
            urls_list_str = row.get('urls_list') or ''
            if urls_list_str and urls_list_str not in ('[]', '[""]', '[null]'):
                all_url_strs.append(urls_list_str)
            
            # Collect from sources_list (usually has channel names)
            sources_list_str = row.get('sources_list') or ''
            if sources_list_str and sources_list_str not in ('[]', '[""]', '[null]'):
                all_url_strs.append(sources_list_str)
            
            # Collect from ai_report_json
            if ai_data:
                agg = ai_data.get('Aggregated Sources', [])
                if agg:
                    all_url_strs.append(' | '.join(str(s) for s in agg if s))
            
            # Parse all sources into a combined pool
            combined_sources = []
            for src_str in all_url_strs:
                combined_sources.extend(parse_sources_to_list(src_str))
            
            # Deduplicate by name (keep first occurrence with a real URL)
            seen = {}
            structured_sources = []
            for s in combined_sources:
                key = s['name'].lower()
                if key not in seen:
                    seen[key] = s
                    structured_sources.append(s)
                else:
                    # Upgrade: prefer a URL with a message ID over channel-only
                    existing = seen[key]
                    if (existing['url'] == '#' or not '/' in existing['url'].split('t.me/')[-1] if 't.me/' in existing['url'] else False) and s['url'] != '#':
                        existing['url'] = s['url']
            
            # Resolve channel-only Telegram URLs to deep links using raw_signals lookup
            event_deeplinks = tg_deeplinks.get(event_id, {})
            if event_deeplinks:
                for s in structured_sources:
                    url = s.get('url', '')
                    if 't.me/' in url:
                        channel = url.split('t.me/')[1].split('/')[0]
                        # If current URL has no message ID, upgrade from lookup
                        after_channel = url.split('t.me/' + channel)[-1]
                        if not after_channel or after_channel == '/':
                            if channel in event_deeplinks:
                                s['url'] = event_deeplinks[channel]

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
                    
                    # Fallback: inferred coordinates
                    if not lat or not lon:
                        inferred = tactics.get('geo_location', {}).get('inferred', {})
                        lat = inferred.get('lat')
                        lon = inferred.get('lon')
                    
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
            
            # --- IMINT Evidence Feed: Extract Visionary per-frame analysis ---
            visual_analysis = []
            v_status = ''
            if ai_data:
                tactics_data = ai_data.get('tactics', {})
                visionary_report = tactics_data.get('visionary_report', {}) if isinstance(tactics_data, dict) else {}
                if visionary_report and isinstance(visionary_report, dict):
                    analyzed_frames = visionary_report.get('analyzed_frames', [])
                    v_status = visionary_report.get('visual_confirmation', {}).get('verification_status', '')
                    for af in analyzed_frames:
                        visual_analysis.append({
                            "frame_id": af.get('frame_id', 0),
                            "confidence": af.get('confidence', 0),
                            "selection_reason": af.get('selection_reason', ''),
                            "explanation": af.get('explanation', ''),
                            "base64_data": af.get('base64_data', ''),
                            "verification_status": v_status
                        })
            
            # V4.2 Source Reputation Engine
            event_dt = parse_event_datetime(date)
            source_domains = domains_from_structured_sources(structured_sources)
            classification = extract_classification(ai_data)
            faction = extract_faction(ai_data, f"{title} {description}")
            discrepancy_flag = str(v_status).upper() == 'CONTRADICTED'
            institutional_flag = any(d in INSTITUTIONAL_DOMAINS for d in source_domains)
            hash_duplicate_flag = str(row.get('ai_analysis_status') or '').upper() == 'NULL'
            source_reputation_score = update_event_reputation(
                conn,
                event_id=event_id,
                domains=source_domains,
                event_dt=event_dt,
                discrepancy=discrepancy_flag,
                hash_duplicate=hash_duplicate_flag,
                institutional=institutional_flag
            )

            # Skip if no coordinates, EXCEPT if we have high-value IMINT evidence
            if not lat or not lon or float(lat) == 0 or float(lon) == 0:
                if visual_analysis:
                    # Map to a central "Unmapped Intelligence" zone in Ukraine with slight scatter
                    import hashlib
                    h1 = int(hashlib.md5(event_id.encode('utf-8')).hexdigest()[:8], 16)
                    h2 = int(hashlib.md5(event_id.encode('utf-8')[::-1]).hexdigest()[:8], 16)
                    lat = 48.5 + (h1 % 1000) / 500.0
                    lon = 31.2 + (h2 % 1000) / 500.0
                else:
                    continue

            # Note: events without sources are still valuable for the map
            # (intelligence data, TIE scores, description are still present)
            
            # Calculate marker style
            radius, color = get_marker_style(tie_score, e_score)
            
            # Enrich Units
            raw_units = ai_data.get('tactics', {}).get('military_units_detected', []) if ai_data else []
            enriched_units = enrich_units(raw_units, orbat_data)

            # Strategic Campaign Fields
            campaign_id = (row.get('campaign_id') or '').strip().lower() or None
            if not campaign_id and ai_data:
                campaign_id = (
                    ((ai_data.get('strategy') or {}).get('campaign') or {}).get('campaign_id')
                    or None
                )
                if campaign_id:
                    campaign_id = str(campaign_id).strip().lower()
            campaign_info = campaign_index.get(campaign_id) if campaign_id else None
            campaign_name = campaign_info.get('name') if campaign_info else None
            campaign_color = campaign_info.get('color') if campaign_info else None
            
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
                    "timestamp": _date_to_epoch_ms(date),
                    
                    # TIE Metrics
                    "tie_total": round(tie_score, 1),
                    "vec_k": k_score,
                    "vec_t": t_score,
                    "vec_e": e_score,
                    
                    # Quality Metrics
                    "reliability": reliability,
                    "bias_score": bias_score,

                    # Tactical Data (Extracted for Dashboard)
                    "classification": classification,
                    "target_type": ai_data.get('target_type', ai_data.get('titan_assessment', {}).get('target_type_category', 'UNKNOWN')),
                    "faction": faction,
                    "intensity_score": k_score, # Mapping Kinetic Score to Intensity
                    
                    # AI Content
                    "ai_reasoning": ai_summary,
                    "has_video": has_video,
                    
                    # Sources (JSON serialized for JS)
                    "sources_list": json.dumps(structured_sources),
                    "source_reputation_score": source_reputation_score,
                    "hide_by_default": source_reputation_score < 30,
                    "image_phash": row.get("image_phash") or "",
                    
                    "units": json.dumps(enriched_units),
                    
                    # IMINT Evidence Feed (per-frame Visionary analysis)
                    "visual_analysis": json.dumps(visual_analysis) if visual_analysis else "",
                    
                    # Marker Style
                    "marker_radius": radius,
                    "marker_color": color,
                    
                    # New Tactical Sector Geofencing
                    "operational_sector": row.get('operational_sector', 'UNKNOWN_SECTOR'),

                    # Strategic Campaign Tagging
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_name,
                    "campaign_color": campaign_color,
                    "campaign_match_meta": row.get('campaign_match_meta'),
                    "campaign_tagged_at": row.get('campaign_tagged_at')
                }
            }
            geojson_features.append(feature)
            
            # --- AI TRIAGE + DOSSIER UPDATE ---
            # Extract Visionary detected_assets for dossier card
            detected_assets_raw = []
            if ai_data:
                tactics_d = ai_data.get('tactics', {})
                vis = tactics_d.get('visionary_report', {}) if isinstance(tactics_d, dict) else {}
                if vis and isinstance(vis, dict):
                    detected_assets_raw = vis.get('detected_assets', [])

            # First source URL for the engagement link
            first_url = ''
            if structured_sources:
                first_url = structured_sources[0].get('url', '')

            for u in enriched_units:
                update_unit_stats(unit_stats_acc, u, {
                    "date": date,
                    "event_id": event_id,
                    "tie_score": tie_score,
                    "kinetic_score": k_score,
                    "target_score": t_score,
                    "effect_score": e_score,
                    "classification": classification,
                    "title": title,
                    "description": description,
                    "location": row.get('operational_sector', ''),
                    "lat": lat,
                    "lon": lon,
                    "url": first_url,
                    "detected_assets": detected_assets_raw,
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

    # V4.2 Analytics Post-Processing
    anomalies = compute_sector_volume_anomalies(geojson_features, lookback_days=14)
    geojson_features = apply_anomaly_flags(geojson_features, anomalies)

    asymmetry = compute_asymmetry_index(geojson_features)
    for feat in geojson_features:
        p = feat.get('properties', {})
        sec = p.get('operational_sector') or 'UNKNOWN_SECTOR'
        fac = p.get('faction') or 'UNK'
        p['asymmetry_index'] = (
            asymmetry.get('sectors', {})
            .get(sec, {})
            .get(fac, {})
            .get('asymmetry_index', 0)
        )

    glocs_geojson = build_glocs_geojson(geojson_features)

    # Save GeoJSON
    os.makedirs(os.path.dirname(GEOJSON_PATH), exist_ok=True)
    with open(GEOJSON_PATH, 'w', encoding='utf-8') as f:
        json.dump({
            "type": "FeatureCollection",
            "features": geojson_features
        }, f, indent=2, ensure_ascii=False)
    
    write_json(SECTOR_ANOMALIES_PATH, {
        "generated_at": _dt.datetime.utcnow().isoformat(timespec='seconds'),
        "anomalies": list(anomalies.values())
    })
    write_json(ASYMMETRY_INDEX_PATH, asymmetry)
    write_json(GLOCS_PATH, glocs_geojson)

    campaign_geo_payload = build_campaigns_geo(
        features=geojson_features,
        campaigns=campaign_definitions,
        output_path=CAMPAIGNS_GEO_PATH,
    )
    campaign_reports_payload = build_campaign_reports(
        features=geojson_features,
        campaigns=campaign_definitions,
        output_path=CAMPAIGN_REPORTS_PATH,
        sparkline_days=30,
        weekly_window_days=7,
    )

    # Save CSV
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(csv_rows)
    
    print(f"\n[DONE] Export complete!")
    print(f"   GeoJSON: {len(geojson_features)} events -> {GEOJSON_PATH}")
    print(f"   CSV: {len(csv_rows)} rows -> {CSV_PATH}")
    print(f"   Campaigns Geo: {len(campaign_geo_payload.get('campaigns', []))} campaigns -> {CAMPAIGNS_GEO_PATH}")
    print(f"   Campaign Reports: {len(campaign_reports_payload.get('campaigns', []))} campaigns -> {CAMPAIGN_REPORTS_PATH}")

    # Export Units
    export_units(unit_stats_acc, orbat_data)
    
    # Export Equipment Losses from Impact Atlas
    export_equipment_losses()
    
    # Generate Strategic Trends
    generate_strategic_trends(geojson_features)


def generate_strategic_trends(features):
    """
    Generate strategic_trends.json for sector-based intensity analysis.
    Aggregates TIE scores by date and strategic sector.
    """
    from collections import defaultdict
    
    print("\n[TRENDS] Generating strategic trends...")
    
    # Aggregate: {date: {sector: sum_tie}}
    daily_sectors = defaultdict(lambda: defaultdict(float))
    
    for feature in features:
        props = feature.get('properties', {})
        
        # Extract date (YYYY-MM-DD)
        date_str = props.get('date', '')
        if not date_str or date_str == 'Unknown':
            continue
        # Normalize date format
        if '/' in date_str:
            # DD/MM/YYYY -> YYYY-MM-DD
            parts = date_str.split('/')
            if len(parts) == 3:
                date_str = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        date_str = date_str[:10]  # Just YYYY-MM-DD
        
        # Get coordinates
        coords = feature.get('geometry', {}).get('coordinates', [])
        lon = coords[0] if len(coords) > 0 else None
        lat = coords[1] if len(coords) > 1 else None
        
        # Get target type and TIE score
        target_type = props.get('target_type', '')
        tie_score = props.get('tie_total', 0)
        
        # Fallback calculation if TIE missing
        if not tie_score or tie_score == 0:
            intensity = props.get('intensity_score', 0) or props.get('vec_k', 0)
            reliability = props.get('reliability', 50)
            try:
                tie_score = float(intensity) * float(reliability) / 10
            except:
                tie_score = 0
        
        # Classify sector
        sector = classify_sector(lat, lon, target_type)
        
        # Aggregate
        daily_sectors[date_str][sector] += float(tie_score)
    
    # Sort dates and build output
    sorted_dates = sorted(daily_sectors.keys())
    
    # Initialize datasets
    sectors = ['ENERGY_COERCION', 'DEEP_STRIKES_RU', 'EASTERN_FRONT', 'SOUTHERN_FRONT']
    datasets = {sector: [] for sector in sectors}
    
    for date in sorted_dates:
        for sector in sectors:
            datasets[sector].append(round(daily_sectors[date].get(sector, 0), 1))
    
    output = {
        "dates": sorted_dates,
        "datasets": datasets
    }
    
    # Save
    with open(STRATEGIC_TRENDS_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"   [DONE] Strategic trends: {len(sorted_dates)} days -> {STRATEGIC_TRENDS_PATH}")


if __name__ == "__main__":
    main()
