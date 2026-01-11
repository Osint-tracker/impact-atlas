import sqlite3
import json
import os
import numpy as np
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv
import time

# =============================================================================
# ⚙️ CONFIGURAZIONE PER ALTE PRESTAZIONI
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
load_dotenv()

# Client per il Giudice (DeepSeek/Llama)
client_judge = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv("OPENROUTER_API_KEY"),
    default_headers={"X-Title": "OSINT Tracker"}
)

# PARAMETRI DI SCALABILITÀ
WINDOW_SIZE = 3000        # Quanti eventi caricare in RAM per volta
WINDOW_OVERLAP = 200      # Sovrapposizione per non perdere bordi
VECTOR_THRESHOLD = 0.45   # Soglia minima per disturbare l'AI
MAX_TIME_DIFF_HOURS = 48  # Se distano più di 48h, non sono lo stesso evento tattico


def ask_the_judge(evt_a, evt_b):
    """Chiede all'AI se sono lo stesso evento (Solo per i candidati forti)"""
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
            model="meta-llama/llama-3.3-70b-instruct",  # O deepseek-v3
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0, response_format={"type": "json_object"}
        )
        return json.loads(res.choices[0].message.content)
    except:
        return None


def main():
    print(f"🚀 AVVIO SMART FUSION: ROLLING WINDOW MODE")
    print(f"   Window Size: {WINDOW_SIZE} | Overlap: {WINDOW_OVERLAP}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Indicizzazione Leggera (Carichiamo solo ID e DATE)
    # [MOD 96H] Filter scope to recent events
    from datetime import timedelta
    cutoff_date = (datetime.now() - timedelta(hours=96)).isoformat()
    print(f"   ⏳ Smart Fusion Scope: Analyzing events newer than {cutoff_date}")

    cursor.execute("""
        SELECT event_id, last_seen_date 
        FROM unique_events 
        WHERE embedding_vector IS NOT NULL 
        AND ai_analysis_status != 'MERGED'
        AND last_seen_date >= ?
        ORDER BY last_seen_date DESC
    """, (cutoff_date,))
    all_index = cursor.fetchall()
    total_events = len(all_index)
    print(f"✅ Indice caricato: {total_events} eventi pronti.")

    if total_events == 0:
        print("⚠️ Nessun evento con vettori trovato. Esegui prima 'migrate_vectors.py'!")
        return

    # Variabili di stato
    start_idx = 0
    total_fused = 0

    # 2. Loop della Finestra Mobile
    while start_idx < total_events:
        end_idx = min(start_idx + WINDOW_SIZE, total_events)
        print(
            f"\n🔄 Processando Finestra: {start_idx} -> {end_idx} (di {total_events})...")

        # A. Caricamento Dati Completi (Solo per la finestra corrente)
        current_ids = [row['event_id'] for row in all_index[start_idx:end_idx]]
        placeholders = ','.join(['?'] * len(current_ids))

        cursor.execute(f"""
            SELECT event_id, ai_report_json, last_seen_date, full_text_dossier, embedding_vector
            FROM unique_events WHERE event_id IN ({placeholders})
        """, current_ids)
        rows = cursor.fetchall()

        # B. Preparazione Matrici NumPy (Velocità Pura)
        events = []
        vectors = []

        for r in rows:
            try:
                vec = json.loads(r['embedding_vector'])
                if not vec:
                    continue

                # Titolo rapido
                title = r['full_text_dossier'][:50]
                if r['ai_report_json']:
                    j = json.loads(r['ai_report_json'])
                    title = j.get('editorial', {}).get('title_it', title)

                dt = datetime.fromisoformat(
                    r['last_seen_date']).replace(tzinfo=None)

                events.append({
                    "id": r['event_id'],
                    "title": title,
                    "text": r['full_text_dossier'],
                    "date": dt
                })
                vectors.append(vec)
            except:
                continue

        if not vectors:
            start_idx += (WINDOW_SIZE - WINDOW_OVERLAP)
            continue

        # Converti in matrice NumPy
        matrix = np.array(vectors)  # Shape: (N, 1536)

        # Normalizzazione (per sicurezza coseno)
        norm = np.linalg.norm(matrix, axis=1, keepdims=True)
        matrix = matrix / (norm + 1e-10)  # Evita divisione per zero

        print(
            f"⚡ Calcolo similarità matriciale ({len(matrix)}x{len(matrix)})...")

        # C. Moltiplicazione Matriciale (Il trucco magico)
        # Calcola TUTTE le similarità in un colpo solo
        sim_matrix = np.dot(matrix, matrix.T)

        # Azzera la diagonale e il triangolo inferiore (evita auto-confronto e doppi)
        np.fill_diagonal(sim_matrix, 0)
        sim_matrix = np.triu(sim_matrix)

        # D. Estrazione Candidati (Solo quelli sopra soglia)
        # Restituisce gli indici (i, j) dove score > soglia
        candidates = np.argwhere(sim_matrix > VECTOR_THRESHOLD)

        print(f"🧐 Candidati vettoriali trovati: {len(candidates)}")

        # E. Verifica Raffinata (Tempo + AI)
        merges_in_window = []
        processed_ids = set()

        for i, j in candidates:
            # Se uno dei due è già stato fuso in questo giro, salta
            if events[i]['id'] in processed_ids or events[j]['id'] in processed_ids:
                continue

            # Check Temporale
            delta = abs((events[i]['date'] - events[j]
                        ['date']).total_seconds()) / 3600
            if delta > MAX_TIME_DIFF_HOURS:
                continue

            score = sim_matrix[i, j]

            # --- ZONA AI ---
            print(
                f"   🔗 Checking: {events[i]['title'][:30]} vs {events[j]['title'][:30]} (Sim: {score:.2f})")

            # Se è praticamente identico, unisci senza chiedere (risparmia API)
            is_match = False
            if score > 0.96 and delta < 12:
                is_match = True
                print("      🚀 AUTO-MERGE (Super High Confidence)")
            else:
                # Chiedi al Giudice
                verdict = ask_the_judge(events[i], events[j])
                if verdict and verdict.get('is_same_event'):
                    is_match = True
                    print(
                        f"      ✅ AI CONFIRMED (Conf: {verdict.get('confidence')})")
                else:
                    print("      ❌ AI REJECTED")

            if is_match:
                # Logica Master/Victim (Il più vecchio è il Master)
                if events[i]['date'] < events[j]['date']:
                    master, victim = events[i], events[j]
                else:
                    master, victim = events[j], events[i]

                merges_in_window.append((master, victim))
                processed_ids.add(master['id'])
                processed_ids.add(victim['id'])

        # F. Scrittura nel DB (Batch)
        if merges_in_window:
            print(f"💾 Scrittura {len(merges_in_window)} fusioni nel DB...")
            for m, v in merges_in_window:
                new_text = f"{m['text']} ||| [MERGED]: {v['text']}"
                # Segna vittima come MERGED
                cursor.execute(
                    "UPDATE unique_events SET ai_analysis_status='MERGED' WHERE event_id=?", (v['id'],))
                # Aggiorna Master e rimetti in PENDING per nuova analisi
                cursor.execute("""
                    UPDATE unique_events 
                    SET full_text_dossier=?, ai_analysis_status='PENDING', ai_report_json=NULL, embedding_vector=NULL
                    WHERE event_id=?
                """, (new_text, m['id']))

            conn.commit()
            total_fused += len(merges_in_window)

        # G. Avanzamento Finestra
        start_idx += (WINDOW_SIZE - WINDOW_OVERLAP)

        # Pulizia RAM aggressiva
        del matrix
        del sim_matrix
        del events
        del vectors

    conn.close()
    print(f"\n🏁 CLUSTERING COMPLETATO. Totale fusioni: {total_fused}")


if __name__ == "__main__":
    main()
