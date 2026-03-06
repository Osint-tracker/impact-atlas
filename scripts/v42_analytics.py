import json
import math
import datetime as dt
from collections import defaultdict
from urllib.parse import urlparse

INSTITUTIONAL_DOMAINS = {
    'isw.pub', 'mod.gov.ua', 'mil.gov.ua', 'defence-ua.com',
    'mod.mil.ru', 'government.ru', 'nato.int', 'europa.eu',
    'osce.org', 'un.org'
}


def parse_event_datetime(date_str):
    if not date_str:
        return dt.datetime.utcnow()
    s = str(date_str).strip()
    for fmt in ('%Y-%m-%d %H:%M:%S%z', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            parsed = dt.datetime.strptime(s[:len(fmt) + 5], fmt)
            if parsed.tzinfo:
                parsed = parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
            return parsed
        except Exception:
            continue
    try:
        return dt.datetime.fromisoformat(s.replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return dt.datetime.utcnow()


def normalize_domain(value):
    if not value:
        return ''
    item = str(value).strip()
    if not item:
        return ''
    try:
        if item.startswith('http://') or item.startswith('https://'):
            netloc = urlparse(item).netloc
        else:
            netloc = urlparse('https://' + item).netloc
    except Exception:
        netloc = item
    netloc = netloc.lower().replace('www.', '').split('/')[0].split('?')[0]
    return netloc


def domains_from_structured_sources(structured_sources):
    domains = set()
    for src in structured_sources or []:
        if isinstance(src, dict):
            d = normalize_domain(src.get('url') or src.get('name'))
        else:
            d = normalize_domain(src)
        if d:
            domains.add(d)
    return sorted(domains)


def ensure_sources_reputation_schema(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sources_reputation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT UNIQUE,
            score INTEGER DEFAULT 50,
            last_verified TEXT
        )
    """)
    try:
        conn.execute("ALTER TABLE unique_events ADD COLUMN source_reputation_score REAL")
    except Exception:
        pass
    conn.commit()


def _decay_to_center(score, last_verified, current_dt):
    base = int(score or 50)
    if not last_verified:
        return base
    try:
        prev = parse_event_datetime(last_verified)
        steps = max(0, (current_dt.date() - prev.date()).days // 15)
        if steps == 0:
            return base
        if base > 50:
            return max(50, base - steps)
        if base < 50:
            return min(50, base + steps)
        return base
    except Exception:
        return base


def apply_reputation_decay(conn, current_dt=None):
    now_dt = current_dt or dt.datetime.utcnow()
    cur = conn.cursor()
    cur.execute("SELECT id, score, last_verified FROM sources_reputation")
    rows = cur.fetchall()
    updates = []
    for rid, score, last_verified in rows:
        new_score = _decay_to_center(score, last_verified, now_dt)
        if int(score or 50) != int(new_score):
            updates.append((int(new_score), now_dt.isoformat(timespec='seconds'), rid))
    if updates:
        cur.executemany(
            "UPDATE sources_reputation SET score = ?, last_verified = ? WHERE id = ?",
            updates
        )
        conn.commit()


def update_event_reputation(conn, event_id, domains, event_dt, discrepancy=False, hash_duplicate=False, institutional=False):
    cur = conn.cursor()
    event_dt = event_dt or dt.datetime.utcnow()

    # Base delta from event signals
    delta = 0
    if discrepancy or hash_duplicate:
        delta -= 10
    if institutional:
        delta += 2

    scores = []
    for domain in domains:
        if not domain:
            continue
        cur.execute("SELECT score, last_verified FROM sources_reputation WHERE domain = ?", (domain,))
        row = cur.fetchone()
        if row:
            decayed = _decay_to_center(row[0], row[1], event_dt)
            new_score = max(0, min(100, int(decayed + delta)))
            cur.execute(
                "UPDATE sources_reputation SET score = ?, last_verified = ? WHERE domain = ?",
                (new_score, event_dt.isoformat(timespec='seconds'), domain)
            )
            scores.append(new_score)
        else:
            new_score = max(0, min(100, int(50 + delta)))
            cur.execute(
                "INSERT INTO sources_reputation(domain, score, last_verified) VALUES (?, ?, ?)",
                (domain, new_score, event_dt.isoformat(timespec='seconds'))
            )
            scores.append(new_score)

    event_score = min(scores) if scores else 50
    cur.execute(
        "UPDATE unique_events SET source_reputation_score = ? WHERE event_id = ?",
        (event_score, event_id)
    )
    conn.commit()
    return float(event_score)


def extract_classification(ai_data):
    if not isinstance(ai_data, dict):
        return 'UNKNOWN'
    candidates = []
    candidates.append(ai_data.get('classification'))
    t = ai_data.get('tactics', {}) if isinstance(ai_data.get('tactics'), dict) else {}
    s = ai_data.get('strategy', {}) if isinstance(ai_data.get('strategy'), dict) else {}
    ea = t.get('event_analysis', {}) if isinstance(t.get('event_analysis'), dict) else {}
    candidates.append(ea.get('classification'))
    candidates.append(s.get('event_category'))
    candidates.append(t.get('event_category'))

    for c in candidates:
        if c and isinstance(c, str):
            return c.strip().upper()
    return 'UNKNOWN'


def extract_faction(ai_data, fallback_text=''):
    if isinstance(ai_data, dict):
        tactics = ai_data.get('tactics', {}) if isinstance(ai_data.get('tactics'), dict) else {}
        strategy = ai_data.get('strategy', {}) if isinstance(ai_data.get('strategy'), dict) else {}
        actors = tactics.get('actors', strategy.get('actors', {}))
        if isinstance(actors, dict):
            agg = actors.get('aggressor', {}) if isinstance(actors.get('aggressor'), dict) else {}
            side = str(agg.get('side') or '').upper()
            if 'RUS' in side or 'RU' in side:
                return 'RU'
            if 'UKR' in side or 'UA' in side:
                return 'UA'

    txt = (fallback_text or '').upper()
    ru_hits = sum(k in txt for k in ['RUSSIA', 'RUSSIAN', 'MOSCOW', 'KREMLIN'])
    ua_hits = sum(k in txt for k in ['UKRAINE', 'UKRAINIAN', 'KYIV', 'AFU', 'ZSU'])
    if ru_hits > ua_hits:
        return 'RU'
    if ua_hits > ru_hits:
        return 'UA'
    return 'UNK'


def compute_sector_volume_anomalies(features, lookback_days=14):
    per_sector_per_day = defaultdict(lambda: defaultdict(int))
    all_dates = set()

    for f in features:
        props = f.get('properties', {})
        sector = props.get('operational_sector') or 'UNKNOWN_SECTOR'
        date_str = str(props.get('date') or '')[:10]
        if len(date_str) != 10:
            continue
        per_sector_per_day[sector][date_str] += 1
        all_dates.add(date_str)

    if not all_dates:
        return {}

    latest_date = max(all_dates)
    latest_dt = dt.datetime.strptime(latest_date, '%Y-%m-%d').date()

    anomalies = {}
    for sector, by_day in per_sector_per_day.items():
        current_count = by_day.get(latest_date, 0)
        history = []
        for i in range(1, lookback_days + 1):
            d = (latest_dt - dt.timedelta(days=i)).strftime('%Y-%m-%d')
            history.append(by_day.get(d, 0))

        if not history:
            continue

        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std = math.sqrt(variance)
        threshold = mean + (2 * std)

        if current_count > threshold and current_count > 0:
            anomalies[sector] = {
                'sector': sector,
                'date': latest_date,
                'current_volume': current_count,
                'mean_14d': round(mean, 2),
                'std_14d': round(std, 2),
                'threshold': round(threshold, 2)
            }

    return anomalies


def apply_anomaly_flags(features, anomalies):
    anomaly_set = set(anomalies.keys())
    for f in features:
        props = f.get('properties', {})
        sector = props.get('operational_sector') or 'UNKNOWN_SECTOR'
        props['is_anomaly_sector'] = sector in anomaly_set
    return features


def compute_asymmetry_index(features):
    """
    Asymmetry Index = sum(K_faction * E_faction) / sum(T_target_destroyed)
    aggregated by sector and faction.
    """
    accum = defaultdict(lambda: defaultdict(lambda: {'num': 0.0, 'den': 0.0, 'events': 0}))

    for f in features:
        p = f.get('properties', {})
        sector = p.get('operational_sector') or 'UNKNOWN_SECTOR'
        faction = p.get('faction') or 'UNK'
        try:
            k = float(p.get('vec_k') or 0)
            e = float(p.get('vec_e') or 0)
            t = float(p.get('vec_t') or 0)
        except Exception:
            continue

        accum[sector][faction]['num'] += (k * e)
        accum[sector][faction]['den'] += max(t, 0)
        accum[sector][faction]['events'] += 1

    out = {'sectors': {}, 'global': {}}
    global_num = defaultdict(float)
    global_den = defaultdict(float)

    for sector, by_faction in accum.items():
        out['sectors'][sector] = {}
        for faction, vals in by_faction.items():
            den = vals['den']
            idx = (vals['num'] / den) if den > 0 else 0.0
            out['sectors'][sector][faction] = {
                'asymmetry_index': round(idx, 4),
                'events': vals['events'],
                'numerator': round(vals['num'], 4),
                'denominator': round(den, 4)
            }
            global_num[faction] += vals['num']
            global_den[faction] += den

    for faction in sorted(global_num.keys()):
        den = global_den[faction]
        out['global'][faction] = round((global_num[faction] / den), 4) if den > 0 else 0.0

    return out


def _haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def build_glocs_geojson(features, max_km=40.0, max_hours=24.0):
    logistics = []
    for f in features:
        p = f.get('properties', {})
        cls = str(p.get('classification') or '').upper()
        if cls != 'LOGISTICS':
            continue
        coords = f.get('geometry', {}).get('coordinates', [])
        if len(coords) < 2:
            continue
        t = p.get('timestamp') or 0
        if not t:
            continue
        logistics.append({
            'id': p.get('id'),
            'sector': p.get('operational_sector') or 'UNKNOWN_SECTOR',
            'lon': float(coords[0]),
            'lat': float(coords[1]),
            'timestamp': int(t)
        })

    logistics.sort(key=lambda x: x['timestamp'])

    clusters = []
    for e in logistics:
        placed = False
        for c in clusters:
            last = c[-1]
            hours = abs(e['timestamp'] - last['timestamp']) / 3600000.0
            dist = _haversine_km(e['lat'], e['lon'], last['lat'], last['lon'])
            if hours <= max_hours and dist <= max_km:
                c.append(e)
                placed = True
                break
        if not placed:
            clusters.append([e])

    lines = []
    for idx, c in enumerate(clusters, start=1):
        if len(c) < 2:
            continue
        c_sorted = sorted(c, key=lambda x: x['timestamp'])
        coords = [[x['lon'], x['lat']] for x in c_sorted]
        sectors = sorted({x['sector'] for x in c_sorted})
        lines.append({
            'type': 'Feature',
            'geometry': {
                'type': 'LineString',
                'coordinates': coords
            },
            'properties': {
                'id': f'gloc_{idx}',
                'event_count': len(c_sorted),
                'start_ts': c_sorted[0]['timestamp'],
                'end_ts': c_sorted[-1]['timestamp'],
                'sectors': sectors
            }
        })

    return {
        'type': 'FeatureCollection',
        'features': lines
    }


def write_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
