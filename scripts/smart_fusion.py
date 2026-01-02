import sqlite3
import json
import os
import ast
import re
import numpy as np
from datetime import datetime
from difflib import SequenceMatcher  # <--- FIX: Mancava questo import
from geopy.distance import geodesic
from openai import OpenAI
from dotenv import load_dotenv

# =============================================================================
# 🛠️ CONFIGURAZIONE E SETUP
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
load_dotenv()

# 1. SETUP CLIENT OPENAI (Usa la tua chiave esistente per embeddings)
client_openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# 2. SETUP CLIENT JUDGE (DeepSeek via OpenRouter)
client_judge = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
    default_headers={"X-Title": "OSINT Tracker"}
)

# --- PARAMETRI ---
MAX_DISTANCE_KM = 150.0
MAX_TIME_DIFF_HOURS = 96

# SOGLIE VETTORIALI
VECTOR_SIM_THRESHOLD = 0.70

# SOGLIE TESTUALI (LEGACY/FALLBACK)
TEXT_SIM_THRESHOLD_GEO = 0.05
TEXT_SIM_THRESHOLD_BLIND = 0.05

# =============================================================================
# 🧮 FUNZIONI HELPER
# =============================================================================


def get_embedding(text):
    """Trasforma il testo in un vettore di numeri usando OpenAI."""
    try:
        # Taglia a 8k caratteri per sicurezza
        safe_text = text.replace("\n", " ")[:8000]
        response = client_openai.embeddings.create(
            input=[safe_text],
            model="text-embedding-3-small"
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"      ⚠️ Embedding Error: {e}")
        return None


def cosine_similarity(v1, v2):
    """Calcola quanto sono simili due vettori (0 = diversi, 1 = identici)."""
    if v1 is None or v2 is None:
        return 0.0

    if isinstance(v1, list):
        v1 = np.array(v1)
    if isinstance(v2, list):
        v2 = np.array(v2)

    dot_product = np.dot(v1, v2)
    norm_a = np.linalg.norm(v1)
    norm_b = np.linalg.norm(v2)

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return dot_product / (norm_a * norm_b)


def parse_list_field(field_value):
    """Helper per leggere le liste dal DB"""
    if not field_value:
        return []
    try:
        if isinstance(field_value, list):
            return field_value
        text = str(field_value).strip()
        if text.startswith('[') and text.endswith(']'):
            return ast.literal_eval(text)
        if ' ||| ' in text:
            return text.split(' ||| ')
        return [text]
    except:
        return []


def init_history_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comparison_history (
            event_a TEXT,
            event_b TEXT,
            verdict TEXT,
            timestamp TEXT,
            PRIMARY KEY (event_a, event_b)
        )
    """)
    conn.commit()


def check_history(cursor, id_a, id_b):
    first, second = sorted([id_a, id_b])
    cursor.execute(
        "SELECT verdict FROM comparison_history WHERE event_a = ? AND event_b = ?", (first, second))
    result = cursor.fetchone()
    return result[0] if result else None


def save_history(cursor, id_a, id_b, verdict):
    first, second = sorted([id_a, id_b])
    cursor.execute("INSERT OR REPLACE INTO comparison_history VALUES (?, ?, ?, ?)",
                   (first, second, verdict, datetime.now().isoformat()))


def tokenize(text):
    words = re.findall(r'\w+', text.lower())
    return set(w for w in words if len(w) > 3)


def get_combined_similarity(text_a, text_b):
    """Calcola similarità testuale classica (Jaccard + SequenceMatcher)"""
    seq_score = SequenceMatcher(None, text_a, text_b).ratio()
    tokens_a = tokenize(text_a)
    tokens_b = tokenize(text_b)
    if not tokens_a or not tokens_b:
        jaccard_score = 0.0
    else:
        intersection = tokens_a.intersection(tokens_b)
        union = tokens_a.union(tokens_b)
        jaccard_score = len(intersection) / len(union)
    return max(seq_score, jaccard_score)


def ask_the_judge(event_a, event_b):
    """DeepSeek V3 Judge"""
    prompt = f"""
    OBJECTIVE: Entity Resolution. Do these reports refer to the EXACT SAME PHYSICAL EVENT?

    --- REPORT A ({event_a['date']}) ---
    Title: "{event_a['title']}"
    Details: "{event_a['text_compare'][:1000]}"

    --- REPORT B ({event_b['date']}) ---
    Title: "{event_b['title']}"
    Details: "{event_b['text_compare'][:1000]}"

    RULES:
    - Same location + same time (<48h) + compatible details = TRUE.
    - One specific, one generic about same strike = TRUE.
    - Different locations or distinct strikes >4h apart = FALSE.

    OUTPUT JSON ONLY: {{ "is_same_event": boolean, "confidence": float, "reason": "string" }}
    """
    try:
        # FIX: Usa client_judge, non client
        response = client_judge.chat.completions.create(
            model="meta-llama/llama-3.3-70b-instruct",
            messages=[
                {"role": "system", "content": "Output JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        content = response.choices[0].message.content.strip()
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0].strip()
        elif "```" in content:
            content = content.split("```")[1].split("```")[0].strip()
        return json.loads(content)
    except Exception as e:
        print(f"      ❌ Errore API Judge: {e}")
        return None


def find_connected_components(nodes, edges):
    """Graph Theory: Trova gruppi connessi"""
    adj = {node: [] for node in nodes}
    for u, v in edges:
        adj[u].append(v)
        adj[v].append(u)

    visited = set()
    components = []

    for node in nodes:
        if node not in visited:
            component = []
            stack = [node]
            visited.add(node)
            while stack:
                curr = stack.pop()
                component.append(curr)
                for neighbor in adj[curr]:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        stack.append(neighbor)
            if len(component) > 1:
                components.append(component)
    return components

# =============================================================================
# 🚀 MAIN LOOP
# =============================================================================


def main():
    print("🕸️  AVVIO CLUSTERING ENGINE (VECTOR + DYNAMIC DISTANCE)...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_history_table(conn)
    cursor = conn.cursor()

    # 1. CARICAMENTO DATI
    cursor.execute("""
        SELECT event_id, ai_report_json, last_seen_date, full_text_dossier, 
               urls_list, sources_list, embedding_vector
        FROM unique_events 
        WHERE ai_analysis_status IN ('COMPLETED', 'VERIFIED', 'PENDING', 'MERGED') 
        -- AND ai_analysis_status != 'MERGED' 
        AND ai_report_json IS NOT NULL
    """)
    rows = cursor.fetchall()

    events_map = {}
    event_ids = []

    print(f"   📂 Caricati {len(rows)} eventi. Verifica Embeddings...")

    embeddings_generated = 0

    for r in rows:
        try:
            data = json.loads(r['ai_report_json'])
            geo = data.get('tactics', {}).get(
                'geo_location', {}).get('explicit', {}) or {}
            editorial = data.get('editorial', {})

            dt = None
            if r['last_seen_date']:
                try:
                    dt = datetime.fromisoformat(r['last_seen_date'])
                    # FIX: Rimuovi info timezone per uniformare tutto a "naive"
                    if dt and dt.tzinfo is not None:
                        dt = dt.replace(tzinfo=None)
                except:
                    pass

            # --- GESTIONE VETTORE ---
            vec = None
            if 'embedding_vector' in r.keys() and r['embedding_vector']:
                try:
                    vec = np.array(json.loads(r['embedding_vector']))
                except:
                    vec = None

            if vec is None:
                text_for_embed = f"{editorial.get('title_it', '')} {editorial.get('description_it', '')} {r['full_text_dossier'][:500]}"
                vec_list = get_embedding(text_for_embed)
                if vec_list:
                    vec = np.array(vec_list)
                    try:
                        cursor.execute("UPDATE unique_events SET embedding_vector = ? WHERE event_id = ?",
                                       (json.dumps(vec_list), r['event_id']))
                        embeddings_generated += 1
                    except:
                        pass

            evt = {
                "id": r['event_id'],
                "date": dt,
                "lat": float(geo.get('lat')) if geo.get('lat') else None,
                "lon": float(geo.get('lon')) if geo.get('lon') else None,
                "title": editorial.get('title_it', '') or "",
                "text_compare": (editorial.get('description_it', '') + " " + (r['full_text_dossier'][:500] or "")).lower(),
                "vector": vec,
                "full_text": r['full_text_dossier'] or "",
                "urls": parse_list_field(r['urls_list']),
                "sources": parse_list_field(r['sources_list'])
            }
            events_map[r['event_id']] = evt
            event_ids.append(r['event_id'])
        except Exception as e:
            continue

    if embeddings_generated > 0:
        conn.commit()
        print(
            f"   🧠 Generati {embeddings_generated} nuovi embeddings vettoriali.")

    # 2. RICERCA CONNESSIONI
    confirmed_links = []
    sorted_ids = sorted(
        event_ids, key=lambda eid: events_map[eid]['date'] if events_map[eid]['date'] else datetime.min)

    api_calls = 0
    cache_hits = 0

    print("   🔍 Analisi incrociata (Hybrid: Vector + Geo + Text)...")

    for i in range(len(sorted_ids)):
        id_a = sorted_ids[i]
        evt_a = events_map[id_a]

        for j in range(i + 1, len(sorted_ids)):
            id_b = sorted_ids[j]
            evt_b = events_map[id_b]

            # A. FILTRO TEMPORALE (Rigido)
            if evt_a['date'] and evt_b['date']:
                delta = abs((evt_a['date'] - evt_b['date']
                             ).total_seconds()) / 3600
                if delta > MAX_TIME_DIFF_HOURS:
                    continue

            # B. CALCOLO VETTORIALE & DISTANZA DINAMICA
            sim_score = 0.0
            if evt_a['vector'] is not None and evt_b['vector'] is not None:
                sim_score = cosine_similarity(evt_a['vector'], evt_b['vector'])

            # Soglia dinamica basata sulla similarità
            current_max_dist = MAX_DISTANCE_KM  # Default 25km
            if sim_score > 0.95:
                current_max_dist = 2000.0  # Stesso evento, location vaghe
            elif sim_score > 0.90:
                current_max_dist = 100.0  # Tolleranza regionale

            # C. FILTRO GEOGRAFICO (Con soglia dinamica)
            is_near = False
            if evt_a['lat'] and evt_b['lat']:
                try:
                    dist = geodesic(
                        (evt_a['lat'], evt_a['lon']), (evt_b['lat'], evt_b['lon'])).km
                    if dist <= current_max_dist:
                        is_near = True
                except:
                    pass

            # Se coordinate presenti ma lontani -> SCARTA
            if (evt_a['lat'] and evt_b['lat']) and not is_near:
                continue

            # D. FILTRO VETTORIALE (Hard Threshold)
            if sim_score < VECTOR_SIM_THRESHOLD:
                continue

            # E. FILTRO TESTUALE (Sanity Check Finale)
            threshold = TEXT_SIM_THRESHOLD_GEO if is_near else TEXT_SIM_THRESHOLD_BLIND
            text_sim = get_combined_similarity(
                evt_a['text_compare'], evt_b['text_compare'])

            # Se passa anche il filtro testuale (o ha score vettoriale altissimo)
            if text_sim > threshold or sim_score > 0.92:
                # CHECK CACHE
                history = check_history(cursor, id_a, id_b)
                if history == 'MATCH':
                    confirmed_links.append((id_a, id_b))
                    cache_hits += 1
                    continue
                elif history == 'DIFF':
                    cache_hits += 1
                    continue

                # CHIAMATA AI (DeepSeek)
                print(
                    f"\n   🔗 Sospetto Link (Vec: {sim_score:.2f} | Txt: {text_sim:.2f}): {evt_a['title'][:30]}... <--> {evt_b['title'][:30]}...")
                verdict = ask_the_judge(evt_a, evt_b)
                api_calls += 1

                if verdict and verdict.get('is_same_event'):
                    print(f"      ✅ LINK CONFERMATO!")
                    save_history(cursor, id_a, id_b, 'MATCH')
                    confirmed_links.append((id_a, id_b))
                else:
                    print(f"      ❌ Falso Positivo.")
                    save_history(cursor, id_a, id_b, 'DIFF')

            if api_calls % 10 == 0:
                conn.commit()

    # 3. CREAZIONE CLUSTER
    print(
        f"\n   🧩 Analisi Grafo (Calls: {api_calls}, Cache Hits: {cache_hits})...")
    clusters = find_connected_components(event_ids, confirmed_links)
    print(f"   📊 Trovati {len(clusters)} Super-Eventi (Cluster).")

    # 4. FUSIONE
    fusions_count = 0
    for cluster in clusters:
        cluster_sorted = sorted(
            cluster, key=lambda eid: events_map[eid]['date'] if events_map[eid]['date'] else datetime.min)
        master_id = cluster_sorted[0]
        master_evt = events_map[master_id]

        merged_full_text = master_evt['full_text']
        merged_urls = set(master_evt['urls'])
        merged_sources = set(master_evt['sources'])

        print(
            f"   🔥 FUSIONE CLUSTER ({len(cluster)} eventi) -> MASTER: {master_id[:8]}...")

        for victim_id in cluster_sorted[1:]:
            victim_evt = events_map[victim_id]
            if victim_evt['full_text'] not in merged_full_text:
                merged_full_text += f" ||| [MERGED {victim_id[:6]}]: {victim_evt['full_text']}"
            merged_urls.update(victim_evt['urls'])
            merged_sources.update(victim_evt['sources'])
            cursor.execute(
                "UPDATE unique_events SET ai_analysis_status = 'MERGED' WHERE event_id = ?", (victim_id,))

        cursor.execute("""
            UPDATE unique_events 
            SET full_text_dossier = ?, urls_list = ?, sources_list = ?,
                ai_analysis_status = 'PENDING', ai_report_json = NULL, embedding_vector = NULL
            WHERE event_id = ?
        """, (merged_full_text, str(list(merged_urls)), str(list(merged_sources)), master_id))

        fusions_count += 1

    conn.commit()
    conn.close()
    print(f"\n✅ CLUSTERING COMPLETATO. {fusions_count} fusioni.")


if __name__ == "__main__":
    main()
