import argparse
import json
import os
import sqlite3
from datetime import datetime

import numpy as np
from dotenv import load_dotenv
from geopy.distance import geodesic
from openai import OpenAI

# =============================================================================
# SMART FUSION ENGINE V4.3
# - Vector similarity + Judge AI
# - Perceptual hash anti-propaganda gate (pre-fusion)
# - Incremental mode with fusion_checked_at
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
load_dotenv()

client_judge = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
    default_headers={"X-Title": "OSINT Tracker"}
)

WINDOW_SIZE = 3000
WINDOW_OVERLAP = 200
VECTOR_THRESHOLD = 0.45
MAX_TIME_DIFF_HOURS = 48
PHASH_SIMILARITY_THRESHOLD = 95.0
PHASH_HISTORY_MIN_DAYS = 60


def ask_the_judge(evt_a, evt_b):
    """Ask AI judge only for strong candidates."""
    prompt = f"""
    Are these the SAME physical event?

    A: "{evt_a['title']}" ({evt_a['date']})
    Details A: {evt_a['text'][:4000]}...

    B: "{evt_b['title']}" ({evt_b['date']})
    Details B: {evt_b['text'][:4000]}...

    RULES:
    - Same location + similar time + same action = TRUE.
    - Updates/Follow-ups to the same event = TRUE.
    - Distinct attacks in different places = FALSE.

    OUTPUT JSON ONLY: {{ "is_same_event": boolean, "confidence": float }}
    """
    try:
        res = client_judge.chat.completions.create(
            model="minimax/minimax-m2.5:free",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        return json.loads(res.choices[0].message.content)
    except Exception:
        return None


def parse_iso_datetime(value):
    if not value:
        return None
    v = str(value).strip()
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
            try:
                return datetime.strptime(v[:len(fmt)], fmt)
            except Exception:
                continue
    return None


def phash_similarity_percent(hash_a, hash_b):
    """Compute pHash similarity in percentage from two hex strings."""
    if not hash_a or not hash_b:
        return 0.0
    a = str(hash_a).strip().lower()
    b = str(hash_b).strip().lower()
    if len(a) != len(b):
        return 0.0
    try:
        ia = int(a, 16)
        ib = int(b, 16)
    except ValueError:
        return 0.0

    bits = len(a) * 4
    hamming = (ia ^ ib).bit_count()
    return max(0.0, ((bits - hamming) / bits) * 100.0)


def ensure_reputation_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sources_reputation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT UNIQUE,
            score INTEGER DEFAULT 50,
            last_verified TEXT
        )
    """)


def ensure_incremental_columns(cursor):
    for ddl in [
        "ALTER TABLE unique_events ADD COLUMN fusion_checked_at TEXT"
    ]:
        try:
            cursor.execute(ddl)
        except sqlite3.OperationalError:
            pass


def normalize_domain(value):
    if not value:
        return ""
    d = str(value).strip().lower()
    d = d.replace('https://', '').replace('http://', '').replace('www.', '')
    d = d.split('/')[0]
    d = d.split('?')[0]
    return d


def parse_source_domains(sources_blob):
    """Parse unique_events.sources_list and extract domains/handles."""
    if not sources_blob:
        return set()

    domains = set()
    raw = str(sources_blob)

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            for item in parsed:
                if not item:
                    continue
                d = normalize_domain(item)
                if d:
                    domains.add(d)
            return domains
    except Exception:
        pass

    parts = [p.strip() for p in raw.replace('|||', '|').split('|') if p.strip()]
    for p in parts:
        d = normalize_domain(p)
        if d:
            domains.add(d)
    return domains


def apply_reputation_delta(cursor, domains, delta, now_dt):
    if not domains or delta == 0:
        return

    now_iso = now_dt.isoformat(timespec='seconds')
    for domain in domains:
        cursor.execute("SELECT score FROM sources_reputation WHERE domain = ?", (domain,))
        row = cursor.fetchone()
        if row:
            score = int(row[0] or 50)
            new_score = max(0, min(100, score + delta))
            cursor.execute(
                "UPDATE sources_reputation SET score = ?, last_verified = ? WHERE domain = ?",
                (new_score, now_iso, domain)
            )
        else:
            base = 50
            new_score = max(0, min(100, base + delta))
            cursor.execute(
                "INSERT INTO sources_reputation(domain, score, last_verified) VALUES (?, ?, ?)",
                (domain, new_score, now_iso)
            )


def find_historical_phash_match(event_id, event_dt, event_hash, historical_rows):
    """
    Return a matching historical event if pHash similarity is >95 and older by >= 60 days.
    """
    if not event_hash or not event_dt:
        return None

    for old in historical_rows:
        if old['event_id'] == event_id:
            continue
        old_dt = old['date']
        if not old_dt:
            continue
        age_days = (event_dt - old_dt).days
        if age_days < PHASH_HISTORY_MIN_DAYS:
            continue
        sim = phash_similarity_percent(event_hash, old['image_phash'])
        if sim > PHASH_SIMILARITY_THRESHOLD:
            return {
                'old_event_id': old['event_id'],
                'similarity': sim,
                'age_days': age_days
            }
    return None


def load_historical_rows(cursor):
    cursor.execute("""
        SELECT event_id, last_seen_date, image_phash
        FROM unique_events
        WHERE image_phash IS NOT NULL AND image_phash != ''
    """)
    historical_rows = []
    for row in cursor.fetchall():
        historical_rows.append({
            'event_id': row['event_id'],
            'date': parse_iso_datetime(row['last_seen_date']),
            'image_phash': row['image_phash']
        })
    return historical_rows


def load_completed_rows(cursor):
    cursor.execute("""
        SELECT event_id, ai_report_json, last_seen_date, full_text_dossier,
               embedding_vector, image_phash, sources_list, lat, lon,
               fusion_checked_at, ai_analysis_status
        FROM unique_events
        WHERE embedding_vector IS NOT NULL
          AND ai_analysis_status = 'COMPLETED'
        ORDER BY last_seen_date DESC
    """)
    return cursor.fetchall()


def _decode_vector(vector_blob):
    try:
        vec = json.loads(vector_blob)
        if not vec:
            return None, None
        arr = np.array(vec, dtype=float)
        if arr.ndim != 1 or arr.size == 0:
            return None, None
        norm = np.linalg.norm(arr)
        normed = arr / (norm + 1e-10)
        return arr, normed
    except Exception:
        return None, None


def _row_to_event(row):
    event_dt = parse_iso_datetime(row['last_seen_date'])
    if event_dt is None:
        return None

    vec_raw, vec_norm = _decode_vector(row['embedding_vector'])
    if vec_raw is None:
        return None

    title = (row['full_text_dossier'] or '')[:50]
    if row['ai_report_json']:
        try:
            j = json.loads(row['ai_report_json'])
            title = j.get('editorial', {}).get('title_en', title)
        except Exception:
            pass

    return {
        "id": row['event_id'],
        "title": title,
        "text": row['full_text_dossier'] or '',
        "date": event_dt,
        "lat": row['lat'],
        "lon": row['lon'],
        "vector": vec_raw,
        "vector_norm": vec_norm,
        "fusion_checked_at": parse_iso_datetime(row['fusion_checked_at']) if row['fusion_checked_at'] else None
    }


def _should_run_incremental_check(event):
    checked_at = event.get('fusion_checked_at')
    last_seen = event.get('date')
    if last_seen is None:
        return False
    if checked_at is None:
        return True
    return checked_at < last_seen


def _evaluate_pair(evt_a, evt_b, score):
    delta = abs((evt_a['date'] - evt_b['date']).total_seconds()) / 3600
    if delta > MAX_TIME_DIFF_HOURS:
        return False

    print(f"  🔗 Checking: {evt_a['title'][:30]} vs {evt_b['title'][:30]} (Sim: {score:.2f})")

    lat_i, lon_i = evt_a['lat'], evt_a['lon']
    lat_j, lon_j = evt_b['lat'], evt_b['lon']

    distance_known = False
    if lat_i is not None and lon_i is not None and lat_j is not None and lon_j is not None:
        try:
            distance_km = geodesic((float(lat_i), float(lon_i)), (float(lat_j), float(lon_j))).kilometers
            distance_known = True
        except (ValueError, TypeError):
            distance_km = float('inf')
    else:
        distance_km = float('inf')

    print(f"      Dist: {distance_km if distance_km != float('inf') else 'N/A'}km | Time: {delta:.1f}h")

    is_match = False

    if score >= 0.85 and distance_known and distance_km <= 10.0 and delta <= 12:
        is_match = True
        print("      🚀 FAST-TRACK AUTO-MERGE (No LLM needed)")
    else:
        if not distance_known:
            if score >= 0.95:
                is_match = True
                print("      🚀 AUTO-MERGE HIGH SIM NO-GEO (>=0.95, Judge skipped)")
            else:
                print("      ⚖️ JUDGE NO-GEO: Distance unavailable, asking The Judge...")
                verdict = ask_the_judge(evt_a, evt_b)
                if verdict and verdict.get('is_same_event'):
                    is_match = True
                    print(f"      ✅ AI CONFIRMED (Conf: {verdict.get('confidence')})")
                else:
                    print("      ❌ AI REJECTED")
        else:
            if distance_km > 150.0 and score <= 0.93:
                is_match = False
                print("      🛑 REJECTED TOO-FAR: (>150km) and similarity not extreme.")
            else:
                print("      ⚖️ INCONCLUSIVE: Asking The Judge...")
                verdict = ask_the_judge(evt_a, evt_b)
                if verdict and verdict.get('is_same_event'):
                    is_match = True
                    print(f"      ✅ AI CONFIRMED (Conf: {verdict.get('confidence')})")
                else:
                    print("      ❌ AI REJECTED")

    return is_match


def _pick_master_victim(evt_a, evt_b):
    if evt_a['date'] < evt_b['date']:
        return evt_a, evt_b
    return evt_b, evt_a


def _apply_merges(cursor, merges):
    if not merges:
        return 0

    print(f"💾 Scrittura {len(merges)} fusioni nel DB...")
    for m, v in merges:
        new_text = f"{m['text']} ||| [MERGED]: {v['text']}"
        cursor.execute(
            "UPDATE unique_events SET ai_analysis_status='MERGED' WHERE event_id=?",
            (v['id'],)
        )
        cursor.execute("""
            UPDATE unique_events
            SET full_text_dossier=?,
                ai_analysis_status='PENDING',
                ai_report_json=NULL,
                embedding_vector=NULL,
                fusion_checked_at=NULL
            WHERE event_id=?
        """, (new_text, m['id']))

    return len(merges)


def _mark_targets_checked(cursor, checked_event_ids, checked_at_iso):
    if not checked_event_ids:
        return

    placeholders = ','.join(['?'] * len(checked_event_ids))
    params = [checked_at_iso] + list(checked_event_ids)
    cursor.execute(f"""
        UPDATE unique_events
        SET fusion_checked_at = ?
        WHERE event_id IN ({placeholders})
          AND ai_analysis_status = 'COMPLETED'
          AND embedding_vector IS NOT NULL
    """, params)


def _prepare_active_events(rows, cursor, historical_rows):
    events = []
    vectors = []
    already_completed = set()
    total_tagged_null = 0
    expected_dim = None  # inferred from first valid vector

    for r in rows:
        try:
            event_dt = parse_iso_datetime(r['last_seen_date'])
            phash = r['image_phash']

            match = find_historical_phash_match(r['event_id'], event_dt, phash, historical_rows)
            if match:
                cursor.execute(
                    """
                    UPDATE unique_events
                    SET ai_analysis_status='NULL',
                        ai_summary='Propaganda suspected: recycled visual asset detected by pHash (>95%).'
                    WHERE event_id=?
                    """,
                    (r['event_id'],)
                )
                domains = parse_source_domains(r['sources_list'])
                apply_reputation_delta(cursor, domains, -10, datetime.utcnow())
                total_tagged_null += 1
                continue

            raw_vec = json.loads(r['embedding_vector'])
            if not raw_vec or not isinstance(raw_vec, list):
                continue

            # Enforce dimension consistency — discard corrupt/legacy vectors
            if expected_dim is None:
                expected_dim = len(raw_vec)
            elif len(raw_vec) != expected_dim:
                continue

            title = r['full_text_dossier'][:50]
            if r['ai_report_json']:
                j = json.loads(r['ai_report_json'])
                title = j.get('editorial', {}).get('title_en', title)

            if event_dt is None:
                continue

            events.append({
                "id": r['event_id'],
                "title": title,
                "text": r['full_text_dossier'] or '',
                "date": event_dt,
                "lat": r['lat'],
                "lon": r['lon'],
                "status": r['ai_analysis_status'],
            })
            vectors.append(raw_vec)
            # Track events already processed by the FUSION engine (not just AI-analyzed).
            # fusion_checked_at is set AFTER a successful fusion run — NULL means never fused yet.
            if r['fusion_checked_at'] is not None:
                already_completed.add(r['event_id'])
        except Exception:
            continue

    return events, vectors, already_completed, total_tagged_null


def _run_full_scan(cursor, active_events, vectors, already_completed):
    """Full rolling-window scan. Examines ALL pairs above VECTOR_THRESHOLD.
    already_completed is passed for compatibility but NOT used to skip pairs here.
    """
    total_fused = 0
    total_events = len(active_events)
    start_idx = 0

    while start_idx < total_events:
        end_idx = min(start_idx + WINDOW_SIZE, total_events)
        print(f"\n\U0001f504 Processando Finestra: {start_idx} -> {end_idx} (di {total_events})...")

        window_events = active_events[start_idx:end_idx]
        window_vectors = np.array(vectors[start_idx:end_idx], dtype=float)

        norm = np.linalg.norm(window_vectors, axis=1, keepdims=True)
        normed = window_vectors / (norm + 1e-10)
        sim_matrix = np.dot(normed, normed.T)

        np.fill_diagonal(sim_matrix, 0)
        sim_matrix = np.triu(sim_matrix)

        candidate_idx = np.argwhere(sim_matrix > VECTOR_THRESHOLD)
        if len(candidate_idx) > 0:
            scores_for_sort = sim_matrix[candidate_idx[:, 0], candidate_idx[:, 1]]
            sort_order = np.argsort(scores_for_sort)[::-1]
            candidates = candidate_idx[sort_order]
        else:
            candidates = candidate_idx
        print(f"\U0001f9d0 Candidati vettoriali trovati: {len(candidates)} (ordinati per similarity desc)")

        merges_in_window = []
        processed_ids = set()
        evaluated = 0

        for i, j in candidates:
            evt_i = window_events[i]
            evt_j = window_events[j]

            if evt_i['id'] in processed_ids or evt_j['id'] in processed_ids:
                continue

            score = float(sim_matrix[i, j])
            evaluated += 1

            if _evaluate_pair(evt_i, evt_j, score):
                master, victim = _pick_master_victim(evt_i, evt_j)
                merges_in_window.append((master, victim))
                processed_ids.add(master['id'])
                processed_ids.add(victim['id'])

        print(f"   Coppie valutate: {evaluated}")
        total_fused += _apply_merges(cursor, merges_in_window)
        start_idx += (WINDOW_SIZE - WINDOW_OVERLAP)

    return total_fused


def _run_incremental(cursor, active_events, vectors, targets, already_completed):
    """Incremental mode: only examines target events not yet fusion-checked.
    Cross-pairs with already-checked events are examined only if sim >= 0.85.
    """
    total_fused = 0
    processed_ids = set()
    checked_target_ids = set()
    HIGH_SIM_THRESHOLD = 0.85

    # Build normed matrix for dot-product lookups
    mat = np.array(vectors, dtype=float)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    normed = mat / (norms + 1e-10)

    id_to_idx = {e['id']: i for i, e in enumerate(active_events)}

    print(f"   \u23f3 Smart Fusion Scope: Incremental mode ({len(targets)} target events)")

    for idx, target in enumerate(targets, start=1):
        target_id = target['id']
        if target_id in processed_ids:
            continue

        t_idx = id_to_idx.get(target_id)
        if t_idx is None:
            continue

        print(f"\n\U0001f504 Incremental target {idx}/{len(targets)}: {target['title'][:60]}")

        # Compute similarity of this target against all other events
        sims = normed.dot(normed[t_idx])

        candidate_pool = []
        for other in active_events:
            oth_id = other['id']
            if oth_id == target_id or oth_id in processed_ids:
                continue
            o_idx = id_to_idx.get(oth_id)
            if o_idx is None:
                continue
            score = float(sims[o_idx])
            if score <= VECTOR_THRESHOLD:
                continue
            # Skip already fusion-checked counterparts unless high similarity
            if oth_id in already_completed and score < HIGH_SIM_THRESHOLD:
                continue
            candidate_pool.append((score, other))

        candidate_pool.sort(key=lambda x: x[0], reverse=True)
        print(f"\U0001f9d0 Candidati trovati: {len(candidate_pool)}")

        merged = False
        for score, other in candidate_pool:
            if _evaluate_pair(target, other, score):
                master, victim = _pick_master_victim(target, other)
                total_fused += _apply_merges(cursor, [(master, victim)])
                processed_ids.add(master['id'])
                processed_ids.add(victim['id'])
                merged = True
                break

        if not merged:
            checked_target_ids.add(target_id)

    return total_fused, checked_target_ids


def main():
    parser = argparse.ArgumentParser(description="Smart Fusion Engine")
    parser.add_argument(
        "--full-scan",
        action="store_true",
        help="Analyze all completed events with rolling-window mode (legacy behavior)."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Incremental mode only: max target events to check in this run."
    )
    args = parser.parse_args()

    mode = "FULL-SCAN" if args.full_scan else "INCREMENTAL"
    print(f"🚀 AVVIO SMART FUSION ({mode}) + PHASH ANTI-PROPAGANDA")
    if args.full_scan:
        print(f"   Window Size: {WINDOW_SIZE} | Overlap: {WINDOW_OVERLAP}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()

    ensure_reputation_table(cursor)
    ensure_incremental_columns(cursor)
    conn.commit()

    historical_rows = load_historical_rows(cursor)
    all_rows = load_completed_rows(cursor)
    total_events = len(all_rows)
    print(f"✅ Indice caricato: {total_events} eventi pronti.")

    if total_events == 0:
        print("⚠️ Nessun evento con vettori trovato.")
        conn.close()
        return

    active_events, vectors, already_completed, total_tagged_null = _prepare_active_events(all_rows, cursor, historical_rows)
    conn.commit()

    if not active_events:
        conn.close()
        print("⚠️ Nessun evento attivo dopo filtro propaganda/validazione.")
        return

    checked_ids = []

    if args.full_scan:
        print("   ⏳ Smart Fusion Scope: Analyzing ALL processed events")
        total_fused = _run_full_scan(cursor, active_events, vectors, already_completed)
        checked_ids = [e['id'] for e in active_events]
    else:
        targets = [e for e in active_events if _should_run_incremental_check(e)]
        if args.limit and args.limit > 0:
            targets = targets[:args.limit]

        if not targets:
            conn.close()
            print("✅ Nessun target incrementale da processare. Tutto aggiornato.")
            print(f"🛡️ Eventi taggati NULL (propaganda pHash): {total_tagged_null}")
            return

        # If the target set is too large, full scan is more efficient.
        if len(targets) >= max(1000, int(len(active_events) * 0.6)):
            print("   ⏳ Troppi target incrementali: fallback automatico a FULL-SCAN per questa run.")
            total_fused = _run_full_scan(cursor, active_events, vectors, already_completed)
            checked_ids = [e['id'] for e in active_events]
        else:
            total_fused, checked_ids = _run_incremental(cursor, active_events, vectors, targets, already_completed)

    checked_iso = datetime.utcnow().isoformat(timespec='seconds')
    _mark_targets_checked(cursor, checked_ids, checked_iso)

    conn.commit()
    conn.close()

    print(f"\n🏁 CLUSTERING COMPLETATO. Totale fusioni: {total_fused}")
    print(f"🛡️ Eventi taggati NULL (propaganda pHash): {total_tagged_null}")


if __name__ == "__main__":
    main()
