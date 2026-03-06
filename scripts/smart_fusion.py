import sqlite3
import json
import os
import re
import numpy as np
from datetime import datetime, timedelta
from openai import OpenAI
from dotenv import load_dotenv
import time

# =============================================================================
# SMART FUSION ENGINE V4.2
# - Vector similarity + Judge AI
# - Perceptual hash anti-propaganda gate (pre-fusion)
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
    Details A: {evt_a['text'][:400]}...

    B: "{evt_b['title']}" ({evt_b['date']})
    Details B: {evt_b['text'][:400]}...

    RULES:
    - Same location + similar time + same action = TRUE.
    - Updates/Follow-ups to the same event = TRUE.
    - Distinct attacks in different places = FALSE.

    OUTPUT JSON ONLY: {{ "is_same_event": boolean, "confidence": float }}
    """
    try:
        res = client_judge.chat.completions.create(
            model="meta-llama/llama-3.3-70b-instruct",
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


def main():
    print("🚀 AVVIO SMART FUSION: ROLLING WINDOW MODE + PHASH ANTI-PROPAGANDA")
    print(f"   Window Size: {WINDOW_SIZE} | Overlap: {WINDOW_OVERLAP}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    cursor = conn.cursor()

    ensure_reputation_table(cursor)
    conn.commit()

    cutoff_date = (datetime.now() - timedelta(hours=96)).isoformat()
    print(f"   ⏳ Smart Fusion Scope: Analyzing events newer than {cutoff_date}")

    # Full history for anti-propaganda hash check
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

    cursor.execute("""
        SELECT event_id, last_seen_date
        FROM unique_events
        WHERE embedding_vector IS NOT NULL
          AND ai_analysis_status != 'MERGED'
          AND ai_analysis_status != 'NULL'
          AND last_seen_date >= ?
        ORDER BY last_seen_date DESC
    """, (cutoff_date,))
    all_index = cursor.fetchall()
    total_events = len(all_index)
    print(f"✅ Indice caricato: {total_events} eventi pronti.")

    if total_events == 0:
        print("⚠️ Nessun evento con vettori trovato.")
        conn.close()
        return

    start_idx = 0
    total_fused = 0
    total_tagged_null = 0

    while start_idx < total_events:
        end_idx = min(start_idx + WINDOW_SIZE, total_events)
        print(f"\n🔄 Processando Finestra: {start_idx} -> {end_idx} (di {total_events})...")

        current_ids = [row['event_id'] for row in all_index[start_idx:end_idx]]
        placeholders = ','.join(['?'] * len(current_ids))

        cursor.execute(f"""
            SELECT event_id, ai_report_json, last_seen_date, full_text_dossier,
                   embedding_vector, image_phash, sources_list
            FROM unique_events WHERE event_id IN ({placeholders})
        """, current_ids)
        rows = cursor.fetchall()

        events = []
        vectors = []

        for r in rows:
            try:
                event_dt = parse_iso_datetime(r['last_seen_date'])
                phash = r['image_phash']

                # PRE-CHECK: if media hash is a near-duplicate of old months-old material,
                # classify as propaganda/noise before semantic fusion.
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

                vec = json.loads(r['embedding_vector'])
                if not vec:
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
                    "date": event_dt
                })
                vectors.append(vec)
            except Exception:
                continue

        conn.commit()

        if not vectors:
            start_idx += (WINDOW_SIZE - WINDOW_OVERLAP)
            continue

        matrix = np.array(vectors)
        norm = np.linalg.norm(matrix, axis=1, keepdims=True)
        matrix = matrix / (norm + 1e-10)

        print(f"⚡ Calcolo similarità matriciale ({len(matrix)}x{len(matrix)})...")
        sim_matrix = np.dot(matrix, matrix.T)

        np.fill_diagonal(sim_matrix, 0)
        sim_matrix = np.triu(sim_matrix)

        candidates = np.argwhere(sim_matrix > VECTOR_THRESHOLD)
        print(f"🧐 Candidati vettoriali trovati: {len(candidates)}")

        merges_in_window = []
        processed_ids = set()

        for i, j in candidates:
            if events[i]['id'] in processed_ids or events[j]['id'] in processed_ids:
                continue

            delta = abs((events[i]['date'] - events[j]['date']).total_seconds()) / 3600
            if delta > MAX_TIME_DIFF_HOURS:
                continue

            score = sim_matrix[i, j]
            print(f"   🔗 Checking: {events[i]['title'][:30]} vs {events[j]['title'][:30]} (Sim: {score:.2f})")

            is_match = False
            if score > 0.96 and delta < 12:
                is_match = True
                print("      🚀 AUTO-MERGE (Super High Confidence)")
            else:
                verdict = ask_the_judge(events[i], events[j])
                if verdict and verdict.get('is_same_event'):
                    is_match = True
                    print(f"      ✅ AI CONFIRMED (Conf: {verdict.get('confidence')})")
                else:
                    print("      ❌ AI REJECTED")

            if is_match:
                if events[i]['date'] < events[j]['date']:
                    master, victim = events[i], events[j]
                else:
                    master, victim = events[j], events[i]

                merges_in_window.append((master, victim))
                processed_ids.add(master['id'])
                processed_ids.add(victim['id'])

        if merges_in_window:
            print(f"💾 Scrittura {len(merges_in_window)} fusioni nel DB...")
            for m, v in merges_in_window:
                new_text = f"{m['text']} ||| [MERGED]: {v['text']}"
                cursor.execute(
                    "UPDATE unique_events SET ai_analysis_status='MERGED' WHERE event_id=?",
                    (v['id'],)
                )
                cursor.execute("""
                    UPDATE unique_events
                    SET full_text_dossier=?, ai_analysis_status='PENDING', ai_report_json=NULL, embedding_vector=NULL
                    WHERE event_id=?
                """, (new_text, m['id']))

            conn.commit()
            total_fused += len(merges_in_window)

        start_idx += (WINDOW_SIZE - WINDOW_OVERLAP)
        del matrix
        del sim_matrix
        del events
        del vectors

    conn.close()
    print(f"\n🏁 CLUSTERING COMPLETATO. Totale fusioni: {total_fused}")
    print(f"🛡️ Eventi taggati NULL (propaganda pHash): {total_tagged_null}")


if __name__ == "__main__":
    main()
