import os
import json
import gspread
from google.oauth2.service_account import Credentials
from tavily import TavilyClient
from openai import OpenAI
import pandas as pd
import time
from dotenv import load_dotenv  # <--- 1. AGGIUNTO QUESTO

# Carica le chiavi dal file .env
load_dotenv()  # <--- 2. AGGIUNTO QUESTO

# --- CONFIGURAZIONE ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1NEyNXzCSprGOw6gCmVVbtwvFmz8160Oag-WqG93ouoQ/edit"
CONFIDENCE_THRESHOLD = 85
BATCH_SIZE = 100


def setup_clients():
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']

    # --- MODIFICA PER LOCALE (WINDOWS) ---
    # Invece di cercare la variabile d'ambiente, leggiamo il file fisico
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(script_dir, 'service_account.json')

    if not os.path.exists(json_path):
        # Prova a cercarlo nella cartella superiore se non è in scripts
        json_path = os.path.join(script_dir, '..', 'service_account.json')

    if not os.path.exists(json_path):
        raise FileNotFoundError(
            "❌ ERRORE: Non trovo 'service_account.json'. Assicurati che sia nella cartella scripts!")

    creds = Credentials.from_service_account_file(json_path, scopes=scope)
    # -------------------------------------

    gc = gspread.authorize(creds)

    # Recupera le chiavi dal .env
    tavily_key = os.environ.get('TAVILY_API_KEY')
    openai_key = os.environ.get('OPENAI_API_KEY')

    if not tavily_key:
        raise ValueError("❌ Manca TAVILY_API_KEY nel file .env")

    tavily = TavilyClient(api_key=tavily_key)
    openai = OpenAI(api_key=openai_key)

    return gc, tavily, openai


def analyze_event_pro(openai, event, news_context):
    """
    Super-Agente: Verifica, Rinomina e Calcola Intensità Dinamica.
    """
    prompt = f"""
    Sei un analista di intelligence militare senior specializzato nel conflitto Ucraina-Russia.
    Analizza i dati e le news fornite per compilare un report strutturato.

    DATI INPUT:
    - Titolo: {event.get('Title', '')}
    - Luogo: {event.get('Location', '')}
    - Data: {event.get('Date', '')}

    NEWS CONTESTUALI (SOURCES):
    {news_context}

    --- ISTRUZIONI OPERATIVE ---

    1. VERIFICA DELLA FONTE:
       Confronta data e luogo. Se le news parlano di un altro giorno o luogo, match = false.

    2. RE-TITOLAZIONE (Stile Militare):
       Usa italiano tecnico. Format: "TIPO ATTACCO + BERSAGLIO + CITTÀ".
       Esempio: "Attacco missilistico Iskander contro infrastrutture energetiche a Kharkiv".

    3. CLASSIFICAZIONE (Scegli UNA):
       [
         "Drone Strike", "Missile Strike", "Artillery", "Airstrike", "Sabotage", "Naval Strike",
         "Energy Infrastructure", "Cultural Heritage", "Eco-Impact", "Cyber Attack", "Unknown"
       ]

    4. CALCOLO INTENSITÀ (0.1 - 1.0) - USA QUESTA RUBRICA RIGIDA:
       - 0.0 - 0.1 (SUPERFICIALE) Attacco intercettato/fallito, caduta detriti, nessun ferito, danni estetici (finestre rotte).
       - 0.1 - 0.3 (BASSA): Colpo a segno su edificio non strategico, feriti lievi, incendio locale, danni infrastrutturali riparabili.
       - 0.4 - 0.6 (MEDIA): Morti (1-9), infrastruttura energetica/industriale danneggiata, blackout locale.
       - 0.7 - 0.8 (ALTA): Morti (10-20), infrastruttura energetica/industriale distrutta, blackout regionale, ospedali/scuole colpiti.
       - 0.9 - 1.0 (CRITICA): Strage (>20 morti), distruzione diga/centrale elettrica maggiore, rischio nucleare/chimico, distruzione totale quartiere.

    5. DESCRIZIONE:
       Riassunto in italiano (max 300 caratteri). Includi dettagli su armi usate (es. Shahed, S-300) e bilancio vittime se noto.

    RISPONDI ESCLUSIVAMENTE IN JSON:
    {{
        "match": true,
        "confidence": 90,
        "new_title": "...",
        "new_type": "...",
        "description_it": "...",
        "video_url": "URL o null",
        "intensity": 0.5,
        "best_link": "URL fonte migliore"
    }}
    """

    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.3  # Bassa temperatura per essere più analitico e meno creativo
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"Errore AI: {e}")
        return {"match": False, "confidence": 0}


def main():
    print("🤖 Avvio Agente OSINT Editor...")
    try:
        gc, tavily, openai = setup_clients()
        sh = gc.open_by_url(SHEET_URL)
        worksheet = sh.get_worksheet(0)

        headers = worksheet.row_values(1)
        data = worksheet.get_all_records()

        # Mappatura Colonne (Gestione errori se mancano intestazioni)
        def get_col_index(name):
            try:
                return headers.index(name) + 1
            except:
                return None

        col_map = {
            'title': get_col_index('Title'),
            'type': get_col_index('Type'),
            'ver': get_col_index('Verification'),
            'src': get_col_index('Source'),
            'desc': get_col_index('Description'),
            'vid': get_col_index('Video'),
            'int': get_col_index('Intensity')
        }

        # Se mancano colonne fondamentali, stop.
        if not col_map['title']:
            print("❌ Errore: Intestazioni non trovate nello Sheet.")
            return

        # Coda di lavoro
        rows_to_process = []
        for i, row in enumerate(data):
            # Logica: Processa se NON verificato OPPURE se l'intensità è vuota/0 (Retro-analisi)
            is_verified = str(row.get('Verification', '')
                              ).lower() == 'verified'
            has_intensity = str(row.get('Intensity', '')) != ''

            # Processiamo se non è verificato, o se è verificato ma manca l'intensità (per aggiornare i vecchi)
            if not is_verified or (is_verified and not has_intensity):
                # +2 perché spreadsheet è 1-based e ha header
                rows_to_process.append((i + 2, row))

        print(
            f"📋 Eventi da analizzare: {len(rows_to_process)}. Eseguo batch di {BATCH_SIZE}...")

        for row_idx, event in rows_to_process[:BATCH_SIZE]:
            title_orig = event.get('Title', 'Evento')
            print(f"\n🔍 Analisi: {title_orig}...")

            # Query ottimizzata per OSINT
            query = f"{title_orig} {event.get('Location')} {event.get('Date')} war conflict ukraine russia details casualties damages"

            try:
                search = tavily.search(
                    query, search_depth="advanced", include_images=False, max_results=4)
                context = "\n".join(
                    [f"- {r['content']} (Fonte: {r['url']})" for r in search['results']])
            except Exception as e:
                print(f"⚠️ Tavily Error: {e}")
                context = "Nessuna informazione aggiuntiva trovata."

            # Chiamata AI
            res = analyze_event_pro(openai, event, context)

            if res.get('match') and res.get('confidence') >= CONFIDENCE_THRESHOLD:
                print(
                    f"   ✅ VERIFICATO | Int: {res.get('intensity')} | Tipo: {res.get('new_type')}")

                # Aggiornamento Cella per Cella (per sicurezza)
                updates = []

                # 1. Verifica e Fonte
                worksheet.update_cell(row_idx, col_map['ver'], "verified")
                if res.get('best_link') and not event.get('Source'):
                    worksheet.update_cell(
                        row_idx, col_map['src'], res['best_link'])

                # 2. Titolo e Tipo (Cleaning)
                if res.get('new_title'):
                    worksheet.update_cell(
                        row_idx, col_map['title'], res['new_title'])
                if res.get('new_type'):
                    worksheet.update_cell(
                        row_idx, col_map['type'], res['new_type'])

                # 3. Arricchimento (Descrizione e Intensità)
                if col_map['desc']:
                    worksheet.update_cell(
                        row_idx, col_map['desc'], res.get('description_it', ''))

                if col_map['int']:
                    worksheet.update_cell(
                        row_idx, col_map['int'], res.get('intensity', 0.2))

                if col_map['vid'] and res.get('video_url'):
                    worksheet.update_cell(
                        row_idx, col_map['vid'], res.get('video_url'))

                time.sleep(1)  # Rate limit gentile
            else:
                print(
                    f"   ⚠️ Bassa confidenza ({res.get('confidence')}%) o nessun match.")
                # Opzionale: marcare come 'check_manual' se fallisce spesso
                time.sleep(1)

    except Exception as e:
        print(f"❌ ERRORE CRITICO SCRIPT: {e}")
        raise e


if __name__ == "__main__":
    main()
