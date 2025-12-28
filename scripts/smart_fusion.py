import sqlite3
import json
import os
import ast
import re
from datetime import datetime
from difflib import SequenceMatcher
from geopy.distance import geodesic
from openai import OpenAI
from dotenv import load_dotenv

# =============================================================================
# 🛠️ CONFIGURAZIONE CLUSTERING ENGINE (SMART CACHE V2)
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
load_dotenv()

# Client configurato per OpenRouter
client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
    default_headers={
        "HTTP-Referer": "https://github.com/lucagimbo12-star/osint-tracker",
        "X-Title": "OSINT Tracker"
    }
)

# --- PARAMETRI ---
MAX_DISTANCE_KM = 30.0
MAX_TIME_DIFF_HOURS = 48
TEXT_SIM_THRESHOLD_GEO = 0.15
TEXT_SIM_THRESHOLD_BLIND = 0.25


def init_history_table(conn):
    """Crea la tabella per ricordare i confronti già fatti ed evitare costi inutili"""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS comparison_history (
            event_a TEXT,
            event_b TEXT,
            verdict TEXT, -- 'MATCH' o 'DIFF'
            timestamp TEXT,
            PRIMARY KEY (event_a, event_b)
        )
    """)
    conn.commit()


def check_history(cursor, id_a, id_b):
    """Controlla se abbiamo già confrontato questi due eventi (in qualsiasi ordine)"""
    # Ordiniamo gli ID per garantire unicità (A-B è uguale a B-A)
    first, second = sorted([id_a, id_b])
    cursor.execute(
        "SELECT verdict FROM comparison_history WHERE event_a = ? AND event_b = ?", (first, second))
    result = cursor.fetchone()
    return result[0] if result else None


def save_history(cursor, id_a, id_b, verdict):
    """Salva il risultato del confronto"""
    first, second = sorted([id_a, id_b])
    cursor.execute("""
        INSERT OR REPLACE INTO comparison_history (event_a, event_b, verdict, timestamp)
        VALUES (?, ?, ?, ?)
    """, (first, second, verdict, datetime.now().isoformat()))


def tokenize(text):
    words = re.findall(r'\w+', text.lower())
    return set(w for w in words if len(w) > 3)


def get_combined_similarity(text_a, text_b):
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


def parse_list_field(field_value):
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


def ask_the_judge(event_a, event_b):
    """DeepSeek V3.2 Speciale Judge"""
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
        response = client.chat.completions.create(
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
        print(f"      ❌ Errore API: {e}")
        return None  # Ritorna None per NON salvare in cache (riproveremo)

# --- LOGICA DI CLUSTERING (GRAPH THEORY) ---


def find_connected_components(nodes, edges):
    """Trova i gruppi di eventi collegati tra loro"""
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


def main():
    print("🕸️  AVVIO CLUSTERING ENGINE (SMART CACHE)...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_history_table(conn)  # Inizializza tabella cache
    cursor = conn.cursor()

    # 1. CARICAMENTO EVENTI (Escludiamo i MERGED!)
    cursor.execute("""
        SELECT event_id, ai_report_json, last_seen_date, full_text_dossier, urls_list, sources_list
        FROM unique_events 
        WHERE ai_analysis_status IN ('COMPLETED', 'VERIFIED', 'PENDING') 
        AND ai_analysis_status != 'MERGED' 
        AND ai_report_json IS NOT NULL
    """)
    rows = cursor.fetchall()

    events_map = {}
    event_ids = []

    print(
        f"   📂 Indicizzazione di {len(rows)} eventi attivi (ignorati i MERGED)...")
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
                except:
                    pass

            evt = {
                "id": r['event_id'],
                "date": dt,
                "lat": float(geo.get('lat')) if geo.get('lat') else None,
                "lon": float(geo.get('lon')) if geo.get('lon') else None,
                "title": editorial.get('title_it', '') or "",
                "text_compare": (editorial.get('description_it', '') + " " + (r['full_text_dossier'][:500] or "")).lower(),
                "full_text": r['full_text_dossier'] or "",
                "urls": parse_list_field(r['urls_list']),
                "sources": parse_list_field(r['sources_list'])
            }
            events_map[r['event_id']] = evt
            event_ids.append(r['event_id'])
        except:
            continue

    # 2. RICERCA CONNESSIONI (PHASE 1)
    confirmed_links = []
    sorted_ids = sorted(
        event_ids, key=lambda eid: events_map[eid]['date'] if events_map[eid]['date'] else datetime.min)

    api_calls = 0
    cache_hits = 0

    for i in range(len(sorted_ids)):
        id_a = sorted_ids[i]
        evt_a = events_map[id_a]

        for j in range(i + 1, len(sorted_ids)):
            id_b = sorted_ids[j]
            evt_b = events_map[id_b]

            # PRE-FILTRI VELOCI (Gratis)
            if evt_a['date'] and evt_b['date']:
                delta = abs((evt_a['date'] - evt_b['date']
                             ).total_seconds()) / 3600
                if delta > MAX_TIME_DIFF_HOURS:
                    continue  # Troppo distanti nel tempo

            is_near = False
            if evt_a['lat'] and evt_b['lat']:
                try:
                    dist = geodesic(
                        (evt_a['lat'], evt_a['lon']), (evt_b['lat'], evt_b['lon'])).km
                    if dist <= MAX_DISTANCE_KM:
                        is_near = True
                except:
                    pass

            if (evt_a['lat'] and evt_b['lat']) and not is_near:
                continue  # Troppo distanti nello spazio

            # FILTRO TESTO (Gratis)
            threshold = TEXT_SIM_THRESHOLD_GEO if is_near else TEXT_SIM_THRESHOLD_BLIND
            sim = get_combined_similarity(
                evt_a['text_compare'], evt_b['text_compare'])

            if sim > threshold:
                # 🛑 CHECK CACHE PRIMA DI CHIAMARE L'AI
                history = check_history(cursor, id_a, id_b)

                if history == 'MATCH':
                    print(
                        f"      ⚡ CACHE: Link già confermato ({evt_a['title'][:15]}... <-> {evt_b['title'][:15]}...)")
                    confirmed_links.append((id_a, id_b))
                    cache_hits += 1
                    continue
                elif history == 'DIFF':
                    # Già controllato e scartato
                    cache_hits += 1
                    continue

                # SE NON IN CACHE -> CHIAMA AI
                print(
                    f"\n   🔗 Sospetto Link (Sim: {sim:.2f}): {evt_a['title'][:30]}... <--> {evt_b['title'][:30]}...")
                verdict = ask_the_judge(evt_a, evt_b)
                api_calls += 1

                if verdict is None:
                    continue  # Errore API, riprova la prossima volta

                if verdict.get('is_same_event'):
                    print(f"      ✅ LINK CONFERMATO!")
                    save_history(cursor, id_a, id_b, 'MATCH')  # Salva Match
                    confirmed_links.append((id_a, id_b))
                else:
                    print(f"      ❌ Nessun link.")
                    # Salva Differenza
                    save_history(cursor, id_a, id_b, 'DIFF')

            # Commit intermedio ogni 10 chiamate per sicurezza
            if api_calls % 10 == 0:
                conn.commit()

    # 3. CREAZIONE CLUSTER (PHASE 2)
    print(
        f"\n   🧩 Analisi Grafo (Calls: {api_calls}, Cache Hits: {cache_hits})...")
    clusters = find_connected_components(event_ids, confirmed_links)

    print(f"   📊 Trovati {len(clusters)} Super-Eventi (Cluster).")

    # 4. ESECUZIONE FUSIONE (PHASE 3)
    fusions_count = 0

    for cluster in clusters:
        # Ordiniamo per data
        cluster_sorted = sorted(
            cluster, key=lambda eid: events_map[eid]['date'] if events_map[eid]['date'] else datetime.min)

        master_id = cluster_sorted[0]
        master_evt = events_map[master_id]

        # Se il master è già stato arricchito (contiene [MERGED]), partiamo da quello
        merged_full_text = master_evt['full_text']
        merged_urls = set(master_evt['urls'])
        merged_sources = set(master_evt['sources'])

        print(
            f"\n   🔥 FUSIONE CLUSTER ({len(cluster)} eventi) -> MASTER: {master_id[:8]}...")

        for victim_id in cluster_sorted[1:]:
            victim_evt = events_map[victim_id]

            # Unione Testo solo se non già presente (evita duplicati in merge ripetuti)
            if victim_evt['full_text'] not in merged_full_text:
                merged_full_text += f" ||| [MERGED {victim_id[:6]}]: {victim_evt['full_text']}"

            merged_urls.update(victim_evt['urls'])
            merged_sources.update(victim_evt['sources'])

            # Segna VICTIM come MERGED
            cursor.execute(
                "UPDATE unique_events SET ai_analysis_status = 'MERGED' WHERE event_id = ?", (victim_id,))

        # Aggiorna MASTER -> PENDING (per ri-analisi AI Agent)
        cursor.execute("""
            UPDATE unique_events 
            SET full_text_dossier = ?, urls_list = ?, sources_list = ?,
                ai_analysis_status = 'PENDING', ai_report_json = NULL
            WHERE event_id = ?
        """, (merged_full_text, str(list(merged_urls)), str(list(merged_sources)), master_id))

        fusions_count += 1

    conn.commit()
    conn.close()

    print(f"\n✅ CLUSTERING COMPLETATO.")
    print(
        f"   🔄 Creati {fusions_count} Super-Eventi. Ora lancia 'ai_agent.py'.")


if __name__ == "__main__":
    main()
