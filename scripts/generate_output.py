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
import re
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
EVENTS_LATEST_PATH = os.path.join(BASE_DIR, '../assets/data/events_latest.json')
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

OPSEC_CUTOFF_HOURS = 24
LATEST_WINDOW_DAYS = 7
SENSITIVE_MOVEMENT_CLASSES = {
    'MANOEUVRE',
    'MANEUVER',
    'SHAPING_MANOEUVRE',
    'SHAPING_MANEUVER',
}

PII_REDACTION = '[REDACTED]'
PERSON_TITLE_PATTERN = re.compile(
    r'\b(?:Lt\.?\s*Gen\.?|Lieutenant\s+General|Major\s+General|Brigadier\s+General|'
    r'Colonel|Col\.?|Lieutenant|Lt\.?|Captain|Capt\.?|Major|Sgt\.?|Sergeant|'
    r'Commander|President|Minister|Governor|General),?\s+'
    r'[A-Z][A-Za-z\'-]+(?:\s+[A-Z][A-Za-z\'-]+){0,3}\b'
)
CYRILLIC_PERSON_TITLE_PATTERN = re.compile(
    r'\b(?:генерал|полковник|майор|капітан|лейтенант|командир|міністр|губернатор)\s+'
    r'[А-ЯЁІЇЄҐ][а-яёіїєґ\'-]+(?:\s+[А-ЯЁІЇЄҐ][а-яёіїєґ\'-]+){0,3}\b',
    re.IGNORECASE
)
LICENSE_PLATE_PATTERN = re.compile(
    r'\b(?:[A-ZА-ЯІЇЄҐ]{1,3}[-\s]?\d{3,5}[-\s]?[A-ZА-ЯІЇЄҐ]{1,3}|'
    r'\d{2,4}[-\s]?[A-ZА-ЯІЇЄҐ]{2,4}[-\s]?\d{2,4})\b'
)


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


def _parse_event_datetime_utc(date_str):
    if not date_str or not isinstance(date_str, str):
        return None
    clean = date_str.strip()
    if not clean or clean.lower() in {'unknown', 'none', 'nat', 'null'}:
        return None
    try:
        dt = _dt.datetime.fromisoformat(clean.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        return dt.astimezone(_dt.timezone.utc)
    except ValueError:
        pass
    for fmt in ('%Y-%m-%d %H:%M:%S%z', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y', '%d/%m/%y'):
        try:
            dt = _dt.datetime.strptime(clean[:len(fmt) + 5], fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=_dt.timezone.utc)
            return dt.astimezone(_dt.timezone.utc)
        except (ValueError, OverflowError):
            continue
    return None


def _is_sensitive_movement_event(classification, category, title, description):
    text = ' '.join(str(v or '') for v in (classification, category, title, description)).upper()
    return any(token in text for token in SENSITIVE_MOVEMENT_CLASSES)


def _should_publish_event(date_str, classification, category, title, description, export_now):
    if not _is_sensitive_movement_event(classification, category, title, description):
        return True
    event_dt = _parse_event_datetime_utc(date_str)
    if not event_dt:
        return False
    return event_dt <= export_now - _dt.timedelta(hours=OPSEC_CUTOFF_HOURS)


def sanitize_public_text(value):
    if value is None:
        return value
    text = str(value)
    text = PERSON_TITLE_PATTERN.sub(PII_REDACTION, text)
    text = CYRILLIC_PERSON_TITLE_PATTERN.sub(PII_REDACTION, text)
    text = LICENSE_PLATE_PATTERN.sub(PII_REDACTION, text)
    return text


def sanitize_public_object(value):
    if isinstance(value, dict):
        sanitized = {}
        drop_keys = {'name', 'rank', 'commander', 'source_url', 'context', 'person', 'full_name', 'first_name', 'last_name', 'patronymic', 'license_plate'}
        for key, item in value.items():
            if str(key).lower() in drop_keys: continue
            sanitized[key] = sanitize_public_object(item)
        return sanitized
    if isinstance(value, list):
        return [sanitize_public_object(item) for item in value]
    if isinstance(value, str):
        return sanitize_public_text(value)
    return value


def sanitize_public_feature(feature):
    props = feature.get('properties', {})
    for key in ('title', 'description', 'ai_reasoning', 'visual_analysis'):
        if key in props:
            props[key] = sanitize_public_text(props.get(key))
    if props.get('units'):
        try:
            units = json.loads(props['units']) if isinstance(props['units'], str) else props['units']
            props['units'] = json.dumps(sanitize_public_object(units), ensure_ascii=False)
        except: pass
    return feature


def build_public_payload(features, generated_at, opsec_withheld_count):
    latest_dt = None
    for feature in features:
        props = feature.get('properties', {})
        event_dt = _parse_event_datetime_utc(props.get('date'))
        if event_dt and (latest_dt is None or event_dt > latest_dt):
            latest_dt = event_dt
    return {
        "type": "FeatureCollection",
        "metadata": {
            "generated_at": generated_at,
            "latest_event_date": latest_dt.date().isoformat() if latest_dt else None,
            "latest_window_days": LATEST_WINDOW_DAYS,
            "opsec_cutoff_hours": OPSEC_CUTOFF_HOURS,
            "opsec_withheld_count": opsec_withheld_count,
            "pii_sanitized": True
        },
        "features": features
    }


def build_latest_features(features):
    dated = []
    for feature in features:
        event_dt = _parse_event_datetime_utc(feature.get('properties', {}).get('date'))
        if event_dt: dated.append((event_dt, feature))
    if not dated: return []
    latest_dt = max(dt for dt, _ in dated)
    cutoff = latest_dt - _dt.timedelta(days=LATEST_WINDOW_DAYS)
    return [feature for event_dt, feature in dated if event_dt >= cutoff]


def parse_sources_to_list(sources_str):
    if not sources_str or sources_str == '[]': return []
    items = []
    try:
        parsed = json.loads(sources_str)
        if isinstance(parsed, list): items = parsed
    except: pass
    if not items:
        if ' ||| ' in str(sources_str): items = [u.strip() for u in str(sources_str).split(' ||| ') if u.strip()]
        elif ' | ' in str(sources_str): items = [u.strip() for u in str(sources_str).split(' | ') if u.strip()]
        else: items = [str(sources_str).strip()] if sources_str else []
    result = []
    for item in items:
        item = str(item).strip()
        if len(item) < 3 or item.lower() in ['none', 'null', 'unknown', '[null]']: continue
        is_url = item.startswith('http') or item.startswith('www.')
        if is_url:
            url = item
            if 't.me/' in url:
                try:
                    parts = url.split('t.me/')[1].split('/')
                    channel_name = parts[0] if parts else 't.me'
                    result.append({"name": channel_name, "url": url})
                except: result.append({"name": "Telegram", "url": url})
            else:
                try:
                    domain = urlparse(url if url.startswith('http') else 'https://'+url).netloc.replace('www.', '')
                    if not domain: domain = "Source"
                except: domain = "Source"
                result.append({"name": domain, "url": url})
        else:
            import re as _re
            if item == 'GDELT_Network': result.append({"name": "GDELT", "url": "#"})
            elif '.' in item and not item.startswith('@'):
                url = f"https://{item}" if not item.startswith('http') else item
                result.append({"name": item, "url": url})
            elif _re.match(r'^[A-Za-z0-9_]+$', item): result.append({"name": item, "url": f"https://t.me/{item}"})
    seen_names = {}
    unique_result = []
    for r in result:
        key = r['name'].lower()
        if key not in seen_names:
            seen_names[key] = r
            unique_result.append(r)
        elif seen_names[key]['url'] == '#' and r['url'] != '#':
            seen_names[key]['url'] = r['url']
    return unique_result


def load_orbat_data():
    try:
        if os.path.exists(ORBAT_JSON_PATH):
            with open(ORBAT_JSON_PATH, 'r', encoding='utf-8') as f: return json.load(f)
    except Exception as e: print(f"[WARN] Failed to load ORBAT data: {e}")
    return []


def enrich_units(ai_units, orbat_data):
    if not ai_units or not orbat_data: return ai_units
    for u in ai_units:
        best_match, best_score = None, 0
        u_name = (u.get('unit_name') or '').lower()
        u_id = (u.get('unit_id') or '').lower()
        u_faction = (u.get('faction') or 'UNKNOWN').upper()
        for ob in orbat_data:
            if (ob.get('faction') or '').upper() != u_faction: continue
            ob_name = (ob.get('unit_name') or '').lower()
            if not ob_name: continue
            score = 100 if (ob_name == u_name or ob_name == u_id) else (80 if (ob_name in u_name or ob_name in u_id) else 0)
            if score > best_score: best_score, best_match = score, ob
        if best_match and best_score >= 80:
            u['orbat_id'] = best_match.get('orbat_id')
            ob_name = best_match.get('unit_name') or best_match.get('full_name_en')
            if ob_name: u['display_name'] = ob_name
            for k in ['echelon', 'echelon_symbol', 'type', 'branch', 'sub_branch', 'garrison', 'district', 'commander', 'superior']:
                u[k] = best_match.get(k)
            best_match['_used'] = True
    return ai_units


def get_marker_style(tie_score, effect_score):
    try:
        tie_score, effect_score = float(tie_score or 0), float(effect_score or 0)
    except: tie_score, effect_score = 0, 0
    radius = 4 + (tie_score / 10)
    color = "#ef4444" if effect_score >= 8 else ("#f59e0b" if effect_score >= 5 else ("#eab308" if effect_score >= 3 else "#64748b"))
    return radius, color


def classify_sector(lat, lon, target_type):
    target_type_lower = (target_type or '').lower()
    energy_keywords = ['power', 'grid', 'dam', 'plant', 'refinery', 'substation', 'transformer', 'energy']
    if any(kw in target_type_lower for kw in energy_keywords): return 'ENERGY_COERCION'
    if 'airfield' in target_type_lower or 'airbase' in target_type_lower: return 'DEEP_STRIKES_RU'
    if lat and lon and float(lat) > 50.0 and float(lon) > 36.0: return 'DEEP_STRIKES_RU'
    try:
        lat_f, lon_f = float(lat or 0), float(lon or 0)
    except: return 'EASTERN_FRONT'
    if lon_f <= 36.0 and lat_f < 48.0: return 'SOUTHERN_FRONT'
    if lon_f > 36.0 and lat_f < 50.0: return 'EASTERN_FRONT'
    return 'EASTERN_FRONT'


def update_unit_stats(stats_acc, unit, event_data):
    import re as _re_local
    key = str(unit.get('orbat_id') or unit.get('unit_id') or unit.get('unit_name') or 'UNKNOWN').lower()
    if key not in stats_acc:
        stats_acc[key] = {"engagement_count": 0, "last_active": "2000-01-01", "total_tie": 0, "tactics_hist": {}, "roles_hist": {}, "orbat_id": unit.get('orbat_id'), "tie_vectors": [], "assets_set": set(), "daily_dates": [], "recent_events": []}
    entry = stats_acc[key]
    entry["engagement_count"] += 1
    evt_date = event_data.get('date', '2000-01-01')
    if evt_date and evt_date > entry["last_active"]: entry["last_active"] = evt_date
    entry["total_tie"] += event_data.get('tie_score', 0)
    cls = event_data.get('classification', 'UNKNOWN')
    entry["tactics_hist"][cls] = entry["tactics_hist"].get(cls, 0) + 1
    k, t, e = event_data.get('kinetic_score', 0), event_data.get('target_score', 0), event_data.get('effect_score', 0)
    if k or t or e: entry["tie_vectors"].append({"kinetic": float(k), "target": float(t), "effect": float(e)})
    detected_assets = event_data.get('detected_assets', [])
    if detected_assets:
        for a in detected_assets:
            atype = a.get('type', '') if isinstance(a, dict) else str(a)
            if atype and atype not in ('UNKNOWN_ARMOR', 'UNKNOWN_VEHICLE', 'UNKNOWN_SYSTEM', 'UNKNOWN_AIRCRAFT'): entry["assets_set"].add(atype)
    else:
        _ASSET_RE = _re_local.compile(r'\b(T-(?:72|80|90|64|55)[A-Z0-9]*|BMP-[123][A-Z]*|BTR-[0-9]+[A-Z]*|2S(?:1|3|5|7|19|35)[A-Z\- ]*|HIMARS|GMLRS|M270|M142|Grad|Smerch|Uragan|TOS-1[A]?|S-[234]00[A-Z0-9]*|Buk[- ]?[A-Z0-9]*|Patriot|NASAMS|IRIS-T|Gepard|Iskander[- ]?[MK]?|Kalibr|Kinzhal|Shahed[- ]?1[0-9]{2}|Lancet[- ]?[0-9]*|FPV|Orlan[- ]?10|Ka-52|Su-[0-9]+[A-Z]*|Leopard[- ]?[12][A-Z0-9]*|Bradley|CV90|CAESAR|PzH[- ]?2000|Krab|M777|Storm Shadow|ATACMS|Javelin|NLAW|Stugna[- ]?P?|Kornet)\b', _re_local.IGNORECASE)
        text_blob = f"{event_data.get('title', '')} {event_data.get('description', '')}"
        for m in _ASSET_RE.findall(text_blob): entry["assets_set"].add(m.strip())
    if evt_date and evt_date != '2000-01-01': entry["daily_dates"].append(evt_date[:10])
    entry["recent_events"].append({"date": evt_date, "title": event_data.get('title', ''), "location": event_data.get('location', ''), "lat": event_data.get('lat'), "lon": event_data.get('lon'), "url": event_data.get('url', ''), "event_id": event_data.get('event_id', '')})


def _build_dossier_fields(stats_entry):
    import datetime as _dt_local
    result = {}
    vecs = stats_entry.get('tie_vectors', [])
    if vecs:
        n = len(vecs)
        result['avg_tie'] = {'kinetic': round(sum(v['kinetic'] for v in vecs) / n, 2), 'target': round(sum(v['target'] for v in vecs) / n, 2), 'effect': round(sum(v['effect'] for v in vecs) / n, 2)}
    else: result['avg_tie'] = {'kinetic': 0, 'target': 0, 'effect': 0}
    result['assets_detected'] = sorted(list(stats_entry.get('assets_set', set())))
    raw_dates = stats_entry.get('daily_dates', [])
    if raw_dates:
        valid_dates = sorted([d for d in raw_dates if d and len(d) >= 10])
        anchor = _dt_local.datetime.strptime(valid_dates[-1], '%Y-%m-%d').date() if valid_dates else _dt_local.date.today()
    else: anchor = _dt_local.date.today()
    trend = [0] * 30
    date_counter = {}
    for d in raw_dates: date_counter[d] = date_counter.get(d, 0) + 1
    for i in range(30):
        day_key = (anchor - _dt_local.timedelta(days=29 - i)).strftime('%Y-%m-%d')
        trend[i] = date_counter.get(day_key, 0)
    result['engagement_trend_30d'], result['engagement_trend_anchor'] = trend, anchor.isoformat()
    total_30d = sum(trend)
    result['engagement_freq_label'] = 'High' if total_30d > 8 else ('Medium' if total_30d >= 3 else 'Low')
    recent = stats_entry.get('recent_events', [])
    result['recent_engagements'] = sorted(recent, key=lambda x: x.get('date', ''), reverse=True)[:5]
    return result


def enrich_units_with_casualties(units_list):
    if not os.path.exists(IMPACT_ATLAS_DB_PATH): return units_list
    try:
        conn = sqlite3.connect(IMPACT_ATLAS_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT raw_data FROM kinetic_events WHERE source = 'UALosses'")
        rows = cursor.fetchall()
        conn.close()
        casualties_by_unit_raw = {}
        for row in rows:
            try:
                data = json.loads(row['raw_data'])
                unit_raw = (data.get('unit_raw') or '').strip().lower()
                if unit_raw: casualties_by_unit_raw[unit_raw] = casualties_by_unit_raw.get(unit_raw, 0) + 1
            except: pass
        for unit in units_list:
            if (unit.get('faction') or '').upper() != 'UA': continue
            display_name = (unit.get('display_name') or '').strip().lower()
            unit_id = (unit.get('unit_id') or '').replace('UA_', '').replace('_', ' ').lower()
            best_count = 0
            for unit_key, cas_count in casualties_by_unit_raw.items():
                if (display_name and (display_name in unit_key or unit_key in display_name)) or (unit_id and (unit_id in unit_key or unit_key in unit_id)):
                    if cas_count > best_count: best_count = cas_count
            if best_count > 0:
                unit['casualty_count'] = best_count
                unit['missing_count'] = best_count
    except Exception as e: print(f"[ERR] Failed to enrich casualties: {e}")
    return units_list


def export_equipment_losses():
    if not os.path.exists(IMPACT_ATLAS_DB_PATH): return
    try:
        conn = sqlite3.connect(IMPACT_ATLAS_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT event_id, source, date, raw_data FROM kinetic_events WHERE source IN ('Oryx', 'LostArmour_fpv', 'LostArmour_lancet')")
        rows = cursor.fetchall()
        conn.close()
        losses = []
        for row in rows:
            try:
                data = json.loads(row['raw_data'])
                source = row['source']
                if source == 'Oryx':
                    loss = {"date": row['date'] or 'Unknown', "model": data.get('entry', 'Unknown'), "type": data.get('category', 'Vehicle'), "country": "RUS", "status": data.get('status', 'Verified Loss'), "proof_url": 'https://www.oryxspioenkop.com/2022/02/attack-on-europe-documenting-equipment.html', "source_tag": "Oryx"}
                else:
                    weapon_type = 'Lancet' if 'lancet' in source.lower() else 'FPV Drone'
                    loss = {"date": row['date'] or 'Unknown', "model": weapon_type, "type": f"Precision Strike ({data.get('tag', weapon_type)})", "country": "UA", "status": "Verified Strike", "proof_url": data.get('source_url', 'https://lostarmour.info'), "source_tag": "LostArmour", "description": data.get('description', '')}
                losses.append(loss)
            except: continue
        losses.sort(key=lambda x: x.get('date', ''), reverse=True)
        with open(EXTERNAL_LOSSES_PATH, 'w', encoding='utf-8') as f: json.dump(losses, f, indent=2, ensure_ascii=False)
    except: pass


def export_units(unit_stats=None, orbat_data=None):
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='units_registry'")
        if not cursor.fetchone():
            conn.close()
            return
        cursor.execute("SELECT * FROM units_registry ORDER BY last_seen_date DESC")
        rows = cursor.fetchall()
        units = []
        for row in rows:
            u = dict(row)
            if u.get('last_seen_date'): u['last_seen_date'] = str(u['last_seen_date'])
            if not u.get('display_name'): u['display_name'] = u.get('unit_id') or 'Unknown Unit'
            matches = enrich_units([u], orbat_data)
            u = matches[0] if matches else u
            if unit_stats:
                key = str(u.get('orbat_id') or u.get('unit_id') or u.get('unit_name') or '').lower()
                stats = unit_stats.get(key)
                if stats:
                    u['engagement_count'], u['last_active'] = stats['engagement_count'], stats['last_active']
                    sorted_tactics = sorted(stats['tactics_hist'].items(), key=lambda x: x[1], reverse=True)
                    u['primary_tactic'] = sorted_tactics[0][0] if sorted_tactics else 'UNKNOWN'
                    dossier = _build_dossier_fields(stats)
                    for k in ['avg_tie', 'assets_detected', 'engagement_trend_30d', 'engagement_trend_anchor', 'engagement_freq_label', 'recent_engagements']: u[k] = dossier[k]
                else:
                    u['engagement_count'], u['avg_tie'], u['assets_detected'], u['engagement_trend_30d'], u['engagement_freq_label'], u['recent_engagements'] = 0, {'kinetic': 0, 'target': 0, 'effect': 0}, [], [0] * 30, 'Low', []
            units.append(u)
        if orbat_data:
            for ob in orbat_data:
                if not ob.get('_used'):
                    new_u = {"unit_id": ob.get('orbat_id') or ob.get('unit_name'), "display_name": ob.get('unit_name'), "faction": ob.get('faction'), "type": ob.get('type') or 'UNKNOWN', "echelon": ob.get('echelon'), "branch": ob.get('branch'), "sub_branch": ob.get('sub_branch'), "garrison": ob.get('garrison'), "district": ob.get('district'), "commander": ob.get('commander'), "superior": ob.get('superior'), "last_seen_lat": ob.get('lat'), "last_seen_lon": ob.get('lon'), "last_seen_date": ob.get('updated_at'), "status": "ACTIVE", "source": "PARABELLUM", "engagement_count": 0, "avg_tie": 0}
                    if new_u['last_seen_lat'] and new_u['last_seen_lon']: units.append(new_u)
        units = enrich_units_with_casualties(units)
        units = sanitize_public_object(units)
        with open(UNITS_JSON_PATH, 'w', encoding='utf-8') as f: json.dump(units, f, indent=2, ensure_ascii=False)
        conn.close()
    except Exception as e: print(f"[ERR] Failed to export units: {e}")


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
    tg_deeplinks = {}
    try:
        cursor.execute("""
            SELECT cluster_id, url 
            FROM raw_signals 
            WHERE url LIKE '%t.me/%/%' AND cluster_id IS NOT NULL
        """)
        for sig in cursor.fetchall():
            cid, url = sig['cluster_id'], sig['url']
            if not cid or not url: continue
            try:
                channel = url.split('t.me/')[1].split('/')[0]
                if cid not in tg_deeplinks: tg_deeplinks[cid] = {}
                tg_deeplinks[cid][channel] = url
            except: continue
        print(f"[INFO] Built Telegram deep-link lookup: {len(tg_deeplinks)} events")
    except Exception as e:
        print(f"[WARN] Could not build deep-link lookup: {e}")
    
    print(f"[INFO] Found {len(rows)} completed events")
    
    # 3. AI Triage Accumulator
    unit_stats_acc = {}
    
    geojson_features = []
    csv_rows = []
    export_now = _dt.datetime.now(_dt.timezone.utc)
    generated_at = export_now.isoformat(timespec='seconds').replace('+00:00', 'Z')
    opsec_withheld_count = 0
    csv_headers = ["ID", "Date", "Title", "Lat", "Lon", "TIE", "K", "T", "E", "Reliability", "Bias", "HasVideo", "Sources"]
    
    for db_row in rows:
        try:
            row = dict(db_row)
            ai_data = {}

            event_id = row['event_id']
            date = row['last_seen_date']
            if not date or str(date).lower() in ['none', 'nat', 'null', '']:
                if row.get('ai_report_json'):
                    try:
                        ai_data = json.loads(row['ai_report_json'])
                        date = (ai_data.get('timestamp_generated') or '')[:10]
                    except: pass
                if not date: date = 'Unknown'
            
            title = row.get('title') or ''
            description = row.get('description') or ''
            tie_score = float(row.get('tie_score') or 0)
            k_score = float(row.get('kinetic_score') or 0)
            t_score = float(row.get('target_score') or 0)
            e_score = float(row.get('effect_score') or 0)
            reliability = int(row.get('reliability') or 0)
            bias_score = float(row.get('bias_score') or 0)
            ai_summary = row.get('ai_summary') or ''
            has_video = bool(row.get('has_video'))
            
            # Source aggregation
            all_url_strs = []
            if row.get('urls_list'): all_url_strs.append(row['urls_list'])
            if row.get('sources_list'): all_url_strs.append(row['sources_list'])
            
            combined_sources = []
            for src_str in all_url_strs:
                combined_sources.extend(parse_sources_to_list(src_str))
            
            # Deduplicate sources
            seen = {}
            structured_sources = []
            for s in combined_sources:
                key = s['name'].lower()
                if key not in seen:
                    seen[key] = s
                    structured_sources.append(s)
                else:
                    existing = seen[key]
                    if (existing['url'] == '#' or 't.me/' in existing['url'] and not '/' in existing['url'].split('t.me/')[-1]) and s['url'] != '#':
                        existing['url'] = s['url']
            
            # Inject deep links
            event_deeplinks = tg_deeplinks.get(event_id, {})
            if event_deeplinks:
                for s in structured_sources:
                    url = s.get('url', '')
                    if 't.me/' in url:
                        channel = url.split('t.me/')[1].split('/')[0]
                        if channel in event_deeplinks and (not '/' in url.split('t.me/' + channel)[-1]):
                            s['url'] = event_deeplinks[channel]

            # Coordinate & Metadata Recovery
            lat, lon = None, None
            if row.get('ai_report_json'):
                try:
                    if not ai_data: ai_data = json.loads(row['ai_report_json'])
                    tactics = ai_data.get('tactics') or {}
                    geo = (tactics.get('geo_location') or {}).get('explicit') or {}
                    lat, lon = geo.get('lat'), geo.get('lon')
                    if not lat or not lon:
                        inferred = (tactics.get('geo_location') or {}).get('inferred') or {}
                        lat, lon = inferred.get('lat'), inferred.get('lon')
                    
                    # Robust Title/Description Fallbacks
                    if not title: title = (ai_data.get('editorial') or {}).get('title_en', '')
                    if not description: description = (ai_data.get('editorial') or {}).get('description_en', '')
                    if not description: description = (tactics.get('event_analysis') or {}).get('summary_en', '')
                    if not description: description = (ai_data.get('strategy') or {}).get('strategic_value_assessment', '')
                    if not description and ai_summary:
                        description = ai_summary.split('[IT]')[0].replace('[EN]', '').strip()[:300]
                except: pass
            
            # IMINT Analysis Recovery
            visual_analysis = []
            v_status = ''
            if ai_data:
                visionary_report = (ai_data.get('tactics') or {}).get('visionary_report') or {}
                if isinstance(visionary_report, dict):
                    analyzed_frames = visionary_report.get('analyzed_frames') or visionary_report.get('per_frame_analysis') or []
                    v_status = (visionary_report.get('visual_confirmation') or {}).get('verification_status', '')
                    for af in analyzed_frames:
                        if not isinstance(af, dict): continue
                        visual_analysis.append({
                            "frame_id": af.get('frame_id', 0),
                            "confidence": af.get('confidence', 0),
                            "selection_reason": af.get('selection_reason', ''),
                            "explanation": af.get('explanation', ''),
                            "base64_data": af.get('base64_data', ''),
                            "verification_status": v_status
                        })
            
            # Reputation & Styles
            source_domains = domains_from_structured_sources(structured_sources)
            classification = extract_classification(ai_data)
            faction = extract_faction(ai_data, f"{title} {description}")
            category_hint = ai_data.get('classification') or ''
            
            if not _should_publish_event(date, classification, category_hint, title, description, export_now):
                opsec_withheld_count += 1
                continue

            # Geo Jitter Fallback for IMINT-only events
            if not lat or not lon or float(lat) == 0 or float(lon) == 0:
                if visual_analysis:
                    import hashlib
                    h1 = int(hashlib.md5(event_id.encode('utf-8')).hexdigest()[:8], 16)
                    h2 = int(hashlib.md5(event_id.encode('utf-8')[::-1]).hexdigest()[:8], 16)
                    lat = 48.3 + (h1 % 1000) / 400.0  # Jitter around Central Ukraine
                    lon = 31.1 + (h2 % 1000) / 400.0
                else: continue

            # Final marker styles
            radius, color = get_marker_style(tie_score, e_score)
            raw_units = (ai_data.get('tactics') or {}).get('military_units_detected', []) if ai_data else []
            enriched_units = enrich_units(raw_units, orbat_data)
            
            campaign_id = (row.get('campaign_id') or '').strip().lower() or None
            campaign_info = campaign_index.get(campaign_id) if campaign_id else None
            
            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
                "properties": {
                    "id": event_id, "title": title, "description": description, "date": date, "timestamp": _date_to_epoch_ms(date),
                    "tie_total": round(tie_score, 1), "vec_k": k_score, "vec_t": t_score, "vec_e": e_score,
                    "reliability": reliability, "bias_score": bias_score, "classification": classification,
                    "target_type": ai_data.get('target_type', 'UNKNOWN'), "faction": faction,
                    "ai_reasoning": ai_summary, "has_video": has_video, "sources_list": json.dumps(structured_sources),
                    "source_reputation_score": row.get('source_reputation_score', 50), 
                    "image_phash": row.get("image_phash") or "", "units": json.dumps(enriched_units),
                    "visual_analysis": json.dumps(visual_analysis) if visual_analysis else "",
                    "marker_radius": radius, "marker_color": color, 
                    "operational_sector": row.get('operational_sector', 'UNKNOWN_SECTOR'),
                    "campaign_id": campaign_id, 
                    "campaign_name": campaign_info.get('name') if campaign_info else None,
                    "campaign_color": campaign_info.get('color') if campaign_info else None
                }
            }
            geojson_features.append(sanitize_public_feature(feature))
            
            # Unit stats update
            first_url = structured_sources[0].get('url', '') if structured_sources else ''
            detected_assets_raw = (ai_data.get('tactics') or {}).get('visionary_report', {}).get('detected_assets', []) if ai_data else []
            for u in enriched_units:
                update_unit_stats(unit_stats_acc, u, {
                    "date": date, "event_id": event_id, "tie_score": tie_score, "kinetic_score": k_score, "target_score": t_score, "effect_score": e_score,
                    "classification": classification, "title": title, "description": description, "location": row.get('operational_sector', ''),
                    "lat": lat, "lon": lon, "url": first_url, "detected_assets": detected_assets_raw
                })
            
            csv_rows.append({"ID": event_id, "Date": date, "Title": title[:50], "Lat": lat, "Lon": lon, "TIE": round(tie_score, 1), "K": k_score, "T": t_score, "E": e_score, "Reliability": reliability, "Bias": bias_score, "HasVideo": 1 if has_video else 0, "Sources": len(structured_sources)})
        except Exception as e:
            print(f"Error processing {row.get('event_id', 'UNKNOWN')}: {e}")
            continue
    
    conn.close()
    
    # Save outputs
    os.makedirs(os.path.dirname(GEOJSON_PATH), exist_ok=True)
    with open(GEOJSON_PATH, 'w', encoding='utf-8') as f:
        json.dump(build_public_payload(geojson_features, generated_at, opsec_withheld_count), f, indent=2, ensure_ascii=False)
    
    build_campaigns_geo(geojson_features, campaign_definitions, CAMPAIGNS_GEO_PATH)
    build_campaign_reports(geojson_features, campaign_definitions, CAMPAIGN_REPORTS_PATH, 30, 7)
    generate_strategic_trends(geojson_features)
    
    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(csv_rows)
    
    export_units(unit_stats_acc, orbat_data)
    export_equipment_losses()
    print('Export complete.')


def generate_strategic_trends(features):
    from collections import defaultdict
    daily_sectors = defaultdict(lambda: defaultdict(float))
    for feature in features:
        props = feature.get('properties', {})
        date_str = props.get('date', '')
        if not date_str or date_str == 'Unknown': continue
        if '/' in date_str:
            parts = date_str.split('/')
            if len(parts) == 3: date_str = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        date_str = date_str[:10]
        coords = feature.get('geometry', {}).get('coordinates', [])
        lon, lat = (coords[0] if len(coords) > 0 else None), (coords[1] if len(coords) > 1 else None)
        target_type, tie_score = props.get('target_type', ''), props.get('tie_total', 0)
        if not tie_score or tie_score == 0:
            intensity, reliability = props.get('intensity_score', 0) or props.get('vec_k', 0), props.get('reliability', 50)
            try: tie_score = float(intensity) * float(reliability) / 10
            except: tie_score = 0
        sector = classify_sector(lat, lon, target_type)
        daily_sectors[date_str][sector] += float(tie_score)
    sorted_dates = sorted(daily_sectors.keys())
    sectors = ['ENERGY_COERCION', 'DEEP_STRIKES_RU', 'EASTERN_FRONT', 'SOUTHERN_FRONT']
    datasets = {sector: [] for sector in sectors}
    for date in sorted_dates:
        for sector in sectors: datasets[sector].append(round(daily_sectors[date].get(sector, 0), 1))
    output = {"dates": sorted_dates, "datasets": datasets}
    with open(STRATEGIC_TRENDS_PATH, 'w', encoding='utf-8') as f: json.dump(output, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()

