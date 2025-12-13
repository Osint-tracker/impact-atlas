import os
import json
import time
import math
import gspread
from google.oauth2.service_account import Credentials
from tavily import TavilyClient
from openai import OpenAI
from dotenv import load_dotenv

# Carica variabili d'ambiente
load_dotenv()

# --- CONFIGURAZIONE ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1NEyNXzCSprGOw6gCmVVbtwvFmz8160Oag-WqG93ouoQ/edit"

# Percorsi Assoluti per i Database JSON
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCES_DB_PATH = os.path.join(BASE_DIR, '../assets/data/sources_db.json')
KEYWORDS_DB_PATH = os.path.join(BASE_DIR, '../assets/data/keywords_db.json')

# --- FUNZIONE HELPER PER SCRITTURA SICURA ---


def safe_update(worksheet, row, col, value):
    """
    Gestisce il limite di velocità di Google (Errore 429).
    Se bloccati, aspetta e riprova.
    """
    if value is None:
        return

    max_retries = 5
    for attempt in range(max_retries):
        try:
            worksheet.update_cell(row, col, value)
            time.sleep(0.8)  # Pausa preventiva
            return
        except Exception as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                print(
                    f"   ⏳ Google ci sta bloccando (Quota 429). Pausa caffè di 60 secondi...")
                time.sleep(65)
                print("   ▶️ Riprendo...")
            else:
                print(
                    f"   ⚠️ Errore scrittura cella {row},{col} (ignoro): {e}")
                return


class OSINTAgent:
    def __init__(self):
        # 1. Setup API
        self.tavily_api_key = os.getenv("TAVILY_API_KEY")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")

        if not self.tavily_api_key or not self.openai_api_key:
            raise ValueError("❌ ERRORE: Chiavi API mancanti nel file .env")

        self.tavily = TavilyClient(api_key=self.tavily_api_key)
        self.client = OpenAI(api_key=self.openai_api_key)

        # 2. Caricamento Knowledge Base
        self.sources_db = self._load_json_db(SOURCES_DB_PATH, "sources")
        self.keywords_db = self._load_json_db(KEYWORDS_DB_PATH, "keywords")

        print("✅ Agente Inizializzato (HBC Logic + 18 Columns Layout).")

    def _load_json_db(self, path, key_name):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get(key_name, {})
        except Exception as e:
            print(f"❌ Errore caricamento DB {path}: {e}")
            return {}

    # =========================================================================
    # 🧠 CORE LOGIC: FORMULA HBC (Hybrid Bias Calculation)
    # =========================================================================
    def calculate_hybrid_bias(self, source_name, text, ai_score):
        """
        Applica la formula 'Reliability-Gated Weighting'.
        Score = (B_base * 2 * 0.4) + (S_ai * 0.4) + (S_sem * M_rel)
        """
        # 1. Recupera metriche Fonte
        source_data = self.sources_db.get(source_name.lower(), {})

        # Fuzzy match
        if not source_data:
            for db_key, db_val in self.sources_db.items():
                if db_key in source_name.lower():
                    source_data = db_val
                    break

        R = source_data.get('reliability', 50)      # R
        B_base = source_data.get('bias', 0)         # B_base

        # 2. Calcolo Score Semantico (S_sem)
        S_sem = 0
        text_lower = text.lower()

        for keyword, score in self.keywords_db.items():
            if keyword.startswith("__"):
                continue
            if keyword in text_lower:
                S_sem += score

        # 3. Calcolo Moltiplicatore di Affidabilità (M_rel)
        M_rel = max(0.2, 1.2 - (R / 100.0))

        # 4. Applicazione Formula Matematica
        W_base = 0.4
        W_ai = 0.4

        raw_score = (B_base * 2 * W_base) + (ai_score * W_ai) + (S_sem * M_rel)

        # 5. Clamping (-10 a +10)
        final_score = max(-10, min(10, raw_score))

        # 6. Safety Check
        if abs(ai_score - S_sem) > 5:
            final_score = final_score * 0.8
            R = int(R * 0.8)

        return round(final_score, 1), R

    def get_bias_label(self, score):
        """Converte lo score HBC in etichetta (Stile Intelligence - Opzione A)"""
        # Range Estremi (Propaganda di Stato / Narrazione imposta)
        if score <= -7:
            return "RUS STATE NARRATIVE"
        if score >= 7:
            return "UKR STATE NARRATIVE"

        # Range Intermedi (Bias editoriale / Opinione forte)
        if score <= -3:
            return "PRO-RUSSIA BIAS"
        if score >= 3:
            return "PRO-UKRAINE BIAS"

        # Zona Centrale (Fatti puri o bilanciati)
        return "NEUTRAL / FACTUAL"

    # =========================================================================
    # 🕵️ AGENT ACTIONS (DOUBLE HAT)
    # =========================================================================

    def perform_search(self, query):
        try:
            print(f"   🔎 Ricerca: '{query}'...")
            response = self.tavily.search(
                query=query, search_depth="basic", max_results=5)

            context_text = "\n\n".join(
                [f"SRC: {r['url']}\nTXT: {r['content']}" for r in response['results']])
            urls = [r['url'] for r in response['results']]

            primary_source = "unknown"
            if urls:
                try:
                    from urllib.parse import urlparse
                    primary_source = urlparse(
                        urls[0]).netloc.replace('www.', '')
                except:
                    pass

            return context_text, primary_source, urls
        except Exception as e:
            print(f"   ❌ Errore Tavily: {e}")
            return "", "unknown", []

    def run_analyst_hat(self, raw_text, current_row):
        """
        Agente Intelligence v3: Coordinate, Bias, Fonti, Impatto Strategico e Affidabilità.
        Incorpora la logica 'analyze_event_pro'.
        """
        if not raw_text:
            return None

        # Recupera dati esistenti per il contesto dell'AI
        evt_title = current_row.get('Title', 'N/D')
        evt_loc = current_row.get('Location', 'N/D')
        evt_date = current_row.get('Date', 'N/D')
        evt_lat = current_row.get('Latitude', '0')
        evt_lon = current_row.get('Longitude', '0')

        prompt = f"""
        Sei un analista di intelligence militare (conflitto UKR-RUS).
        
        DATI ESISTENTI:
        - Titolo: {evt_title}
        - Luogo: {evt_loc}
        - Data: {evt_date}
        - Coord attuali: {evt_lat}, {evt_lon}

        NEWS CONTEXT:
        {raw_text[:4000]}

        --- TASK ---
        1. GEOLOCALIZZAZIONE E TIPO TARGET (CRITICO):
           - Cerca coordinate nel testo. Se mancano, stima le coordinate dell'edificio specifico se identificabile.
           - Assegna "location_precision" scegliendo OBBLIGATORIAMENTE una di queste categorie:
             * "REFINERY": Depositi carburante, raffinerie.
             * "ELECTRICAL SUBSTATION": Sottostazioni, trasformatori (no grandi centrali).
             * "INFRASTRUCTURE": Ponti, ferrovie, porti, logistica.
             * "MILITARY BASE": Aeroporti, caserme, depositi.
             * "CIVILIAN FACILITY": Edifici civili specifici (scuole, hotel, centri comm.).
             * "CITY": Target generico sulla città.
             * "REGION": Area vasta/indefinita.
           - Restituisci lat/lon 0.0 se non trovi nulla di meglio di quanto già presente.

        2. INTELLIGENCE:
           - "dominant_bias_label": PRO_RUSSIA, PRO_UKRAINE, o NEUTRAL (basato sul tono).
           - "ai_score": Punteggio da -10 (Max RUS) a +10 (Max UKR).
           - "actor": Chi ha attaccato? (RUS, UKR, UNK).

        3. INTENSITÀ (SCALA RIGIDA 0.0-1.0):
           - 0.1-0.3 (TACTICAL): Danni lievi, schermaglie, droni abbattuti.
           - 0.4-0.6 (OPERATIONAL): Danni infrastrutture, conquiste villaggi, blackout locali.
           - 0.7-0.8 (STRATEGIC): Colpi strategici, città prese, stragi (>10 civili).
           - 0.9-1.0 (CRITICAL): Evento storico/nucleare/catastrofico (es. Diga Kakhovka).

        4. FATTORI DI AFFIDABILITÀ (PER ALGORITMO INTERNO):
           - "has_visual": true SE c'è menzione esplicita di VIDEO, FOTO o GEOLOCALIZZAZIONE confermata.
           - "is_uncertain": true SE il testo usa parole come "rumors", "unconfirmed", "allegedly", "possible".
           - "num_sources": Stima intera del numero di fonti diverse citate nel contesto.

        OUTPUT JSON:
        {{
            "new_title": "Titolo Tecnico Militare in Italiano",
            "description_it": "Riassunto dettagliato e asettico in italiano",
            "intensity": 0.5,
            "ai_score": 0, 
            "actor": "UNK",
            "location_precision": "CITY",
            "lat": 0.0,
            "lon": 0.0,
            "sources_list": ["url1", "url2"],
            "has_visual": false,
            "is_uncertain": false,
            "num_sources": 1
        }}
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "system", "content": "Sei un analista intelligence esperto."},
                          {"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.2
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            print(f"   ❌ Errore Analyst Hat: {e}")
            return None

    # =========================================================================
    # 🔄 PROCESS FLOW
    # =========================================================================
    def process_row(self, row):
        query = row.get('Title') or row.get('Event')
        if not query:
            return None

        # 1. Ricerca
        raw_text, prim_src, urls = self.perform_search(query)
        if not raw_text:
            return None

        # 2. Analisi Avanzata (Unico passaggio Intelligence)
        # Passiamo 'row' per permettere all'AI di vedere i dati esistenti
        ai_data = self.run_analyst_hat(raw_text, row)
        if not ai_data:
            return None

        # 3. Estrazione Dati AI
        ai_score = int(ai_data.get('ai_score', 0))
        intensity = float(ai_data.get('intensity', 0.5))
        precision = ai_data.get('location_precision', 'REGION')
        actor = ai_data.get('actor', 'UNK').upper()
        desc = ai_data.get('description_it', '')
        new_title = ai_data.get('new_title', query)

        # Gestione Coordinate (Logica "Tranne se personalizzata")
        # Se l'AI restituisce 0.0 o valori nulli, manteniamo quelli del foglio
        new_lat = ai_data.get('lat', 0)
        new_lon = ai_data.get('lon', 0)

        final_lat = row.get('Latitude')
        final_lon = row.get('Longitude')
        final_loc_prec = row.get('Location Precision') or precision

        # Se l'AI ha trovato coordinate specifiche diverse da 0, usiamo quelle
        if new_lat != 0 and new_lon != 0:
            final_lat = new_lat
            final_lon = new_lon
            final_loc_prec = precision  # Aggiorniamo la precisione se cambiamo le coord

        # 4. Formula HBC (Hybrid Bias Calculation)
        final_score, base_reliability = self.calculate_hybrid_bias(
            prim_src, raw_text, ai_score)

        # Raffinamento Reliability basato su fattori interni (Visual / Incertezza)
        final_reliability = base_reliability
        if ai_data.get('has_visual'):
            # Boost se c'è video/foto
            final_reliability = min(100, int(final_reliability * 1.15))
        if ai_data.get('is_uncertain'):
            final_reliability = int(
                final_reliability * 0.75)  # Penalità se rumors

        bias_label = self.get_bias_label(final_score)

        # 5. Output Completo
        return {
            "Title": new_title,
            "Source": urls[0] if urls else "",
            "Description": desc,
            "Intensity": intensity,
            "Actor": actor,
            "Bias dominante": bias_label,
            "Location Precision": final_loc_prec,
            "Latitude": final_lat,  # Scriviamo la lat (vecchia o nuova)
            "Longitude": final_lon,  # Scriviamo la lon (vecchia o nuova)
            "Aggregated Sources": " | ".join(urls[:5]),
            "Reliability": final_reliability,
            "Verification": "Verified",
            # Debug info
            "Bias Score": final_score
        }

# =============================================================================
# 🚀 MAIN LOOP (18 COLONNE)
# =============================================================================


# =============================================================================
# 🚀 MAIN LOOP (LOGICA DI SELEZIONE E AGGIORNAMENTO)
# =============================================================================
def main():
    print("🤖 Avvio AI LDO MORO...")

    # 1. Connessione Google Sheets
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']
    try:
        creds_path = os.path.join(BASE_DIR, 'service_account.json')
        if not os.path.exists(creds_path):
            creds_path = os.path.join(BASE_DIR, '../service_account.json')

        client = gspread.authorize(
            Credentials.from_service_account_file(creds_path, scopes=scope))
        sheet = client.open_by_url(SHEET_URL).get_worksheet(0)
    except Exception as e:
        print(f"❌ Errore GSheet: {e}")
        return

    agent = OSINTAgent()

    # 2. Lettura Headers e Mappatura Dinamica
    headers = sheet.row_values(1)

    def get_col_index(name):
        try:
            return headers.index(name) + 1
        except ValueError:
            return None

    col_map = {
        'Title': get_col_index('Title'),
        'Source': get_col_index('Source'),
        'Verification': get_col_index('Verification'),             # Col 9
        'Description': get_col_index('Description'),
        'Intensity': get_col_index('Intensity'),
        'Actor': get_col_index('Actor'),
        'Bias dominante': get_col_index('Bias dominante'),
        'Location Precision': get_col_index('Location Precision'),
        'Aggregated Sources': get_col_index('Aggregated Sources'),
        'Reliability': get_col_index('Reliability'),
        'Bias Score': get_col_index('Bias Score'),                 # Col 19
        'Latitude': get_col_index('Latitude'),
        'Longitude': get_col_index('Longitude')
    }

    if not col_map['Verification']:
        print("⚠️ ERRORE: Colonna 'Verification' non trovata! Impossibile tracciare il progresso.")
        return

    # 3. Selezione Righe da Processare
    data = sheet.get_all_records()
    rows_to_process = []

    print("🔍 Cerco le righe non ancora verificate...")

    for i, row in enumerate(data):
        # Normalizziamo il valore di Verification (toglie spazi e mette minuscolo)
        verification_status = str(row.get('Verification', '')).strip().lower()

        # LOGICA DI SALTO:
        # Se c'è scritto "verified", SALTALA.
        # Se è vuota o c'è scritto altro (es. "pending"), PROCESSALA.
        if verification_status == 'verified':
            continue

        # Se manca il titolo, saltiamo a prescindere (riga vuota)
        if not row.get('Title') and not row.get('Event'):
            continue

        # Aggiungiamo alla lista delle cose da fare
        rows_to_process.append((i + 2, row))

    BATCH_SIZE = 1400
    print(f"📋 Trovate {len(rows_to_process)} righe da fare. Inizio...")

    # 4. Elaborazione Batch
    for row_idx, row_data in rows_to_process[:BATCH_SIZE]:
        print(
            f"\n⚙️ Riga #{row_idx}: {row_data.get('Title') or 'No Title'}...")

        try:
            result = agent.process_row(row_data)

            if result:
                print(f"   💾 Scrivo dati e timbro 'Verified'...")

                # Scrittura Cella per Cella (inclusa Verification)
                for key, value in result.items():
                    if key in col_map and col_map[key] is not None:
                        col_idx = col_map[key]
                        if isinstance(value, list):
                            value = str(value)

                        safe_update(sheet, row_idx, col_idx, value)

                print(f"   ✅ Riga {row_idx} completata e verificata.")

        except Exception as e:
            print(f"   ⚠️ Errore riga {row_idx}: {e}")

    print("\n🏁 Batch completato.")


if __name__ == "__main__":
    main()
