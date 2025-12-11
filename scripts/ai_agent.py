import os
import json
import gspread
from google.oauth2.service_account import Credentials
from tavily import TavilyClient
from openai import OpenAI
import time
from dotenv import load_dotenv

# Carica le variabili dal file .env
load_dotenv()

# --- CONFIGURAZIONE ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1NEyNXzCSprGOw6gCmVVbtwvFmz8160Oag-WqG93ouoQ/edit"
BATCH_SIZE = 1400  # Numero di righe da processare in un run
CONFIDENCE_THRESHOLD = 80  # Soglia minima di confidenza per accettazione


def setup_clients():
    """Configura i client per Google Sheets, OpenAI e Tavily."""
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']

    # Gestione percorso file credenziali (Windows locale)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_key_path = os.path.join(script_dir, 'service_account.json')
    if not os.path.exists(json_key_path):
        json_key_path = os.path.join(script_dir, '..', 'service_account.json')

    if not os.path.exists(json_key_path):
        raise FileNotFoundError("❌ ERRORE: Non trovo 'service_account.json'.")

    creds = Credentials.from_service_account_file(json_key_path, scopes=scope)
    gc = gspread.authorize(creds)

    tavily = TavilyClient(api_key=os.environ.get('TAVILY_API_KEY'))
    openai = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))

    return gc, tavily, openai


def safe_update(worksheet, row, col, value):
    """
    Funzione magica che gestisce il limite di velocità di Google (Errore 429).
    Se veniamo bloccati, aspetta 60 secondi e riprova.
    """
    if value is None:
        return  # Non scrivere nulla se vuoto

    max_retries = 5
    for attempt in range(max_retries):
        try:
            worksheet.update_cell(row, col, value)
            time.sleep(0.8)  # Piccola pausa preventiva tra ogni cella
            return
        except Exception as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                print(
                    f"   ⏳ Google ci sta bloccando (Quota 429). Pausa caffè di 60 secondi...")
                time.sleep(65)  # Aspetta che il blocco passi
                print("   ▶️ Riprendo...")
            else:
                print(f"   ⚠️ Errore scrittura cella (ignoro): {e}")
                return


def analyze_event_pro(openai, event, news_context):
    """
    Agente Intelligence v2: Coordinate, Bias, Fonti e Impatto Strategico.
    """
    prompt = f"""
    Sei un analista di intelligence militare (conflitto UKR-RUS).
    
    DATI:
    - Titolo: {event.get('Title')}
    - Luogo: {event.get('Location')}
    - Data: {event.get('Date')}
    - Coord attuali: {event.get('Latitude', '0')}, {event.get('Longitude', '0')}

    NEWS:
    {news_context}

    --- TASK ---
    1. GEOLOCALIZZAZIONE E TIPO TARGET (CRITICO):
       - Cerca coordinate nel testo. Se mancano, stima le coordinate dell'edificio specifico.
       - Assegna "location_precision" scegliendo OBBLIGATORIAMENTE una di queste categorie:
         * "REFINERY": Depositi di carburante, raffinerie petrolifere, stoccaggio oil & gas.
         * "ELECTRICAL SUBSTATION": Sottostazioni elettriche, trasformatori, nodi della rete elettrica (distinti dalle grandi centrali).
         * "INFRASTRUCTURE": Dighe, ponti, ferrovie, strade, porti e altre infrastrutture logistiche/trasporti.
         * "MILITARY BASE": Aeroporti, caserme, depositi munizioni, centri di comando.
         * "CIVILIAN FACILITY": Edifici civili specifici (hotel, centri commerciali, scuole, palazzi amministrativi) o punti precisi non strategici.
         * "CITY": Target generico sulla città (es. "Colpita Kharkiv").
         * "REGION": Area vasta/indefinita.

       - Restituisci lat/lon 0.0 se non trovi nulla di meglio di quanto già presente.

    2. INTELLIGENCE:
       - "dominant_bias": PRO_RUSSIA, PRO_UKRAINE, o NEUTRAL.
       - "sources": Lista URL delle fonti più rilevanti.

    3. INTENSITÀ (SCALA RIGIDA 0.0-1.0):
       - 0.1-0.3 (TACTICAL): Danni lievi, intercettazioni, nessun morto, schermaglie, bombardamenti routine, droni abbattuti, morti civili (1-5).
       - 0.4-0.6 (OPERATIONAL): Danni infrastrutture, morti civili (6-10), prese minori, Conquista villaggio, colpo a caserma/deposito, blackout locale.
       - 0.7-0.8 (STRATEGIC): Colpi strategici, conquista città chiave, distruzione centrale elettrica, nave affondata, stragi (>10), città prese.
       - 0.9-1.0 (CRITICAL): Evento storico/nucleare/catastrofico, evento che cambia la guerra (es. Diga Kakhovka, colpo nucleare tattico, caduta Kiev).

    4. FONTI AGGREGATE:
       Estrai i LINK delle fonti pertinenti trovate nel contesto.

    OUTPUT JSON:
    {{
        "match": true,
        "confidence": 90,
        "new_title": "Titolo Tecnico Militare in Italiano",
        "new_type": "Missile/Drone/Ground...",
        "description_it": "Riassunto dettagliato...",
        "video_url": "URL o null",
        "intensity": 0.5,
        "dominant_bias": "NEUTRAL",
        "location_precision": "CITY", // o qualsiasi altra categoria
        "lat": 0.0,  // Metti 0 se non trovi di meglio di quanto già presente
        "lon": 0.0,
        "sources": ["url1", "url2"]
    }}
    """

    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"Errore AI: {e}")
        return {"match": False, "confidence": 0}


def main():
    print("🤖 Avvio Agente OSINT v2 (Full Intel)...")
    try:
        gc, tavily, openai = setup_clients()
        sh = gc.open_by_url(SHEET_URL)
        worksheet = sh.get_worksheet(0)

        headers = worksheet.row_values(1)
        data = worksheet.get_all_records()

       # --- BLOCCO DA SOSTITUIRE IN MAIN ---

        # Mappatura Colonne Dinamica

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
            'int': get_col_index('Intensity'),
            # Nuove Colonne
            'lat': get_col_index('Latitude'),
            'lon': get_col_index('Longitude'),
            'bias': get_col_index('Dominant Bias'),
            'prec': get_col_index('Location Precision'),
            'agg_src': get_col_index('Aggregated Sources')
        }

        if not col_map['bias']:
            print("⚠️ ERRORE: Aggiungi le colonne 'Dominant Bias', 'Location Precision', 'Aggregated Sources' al Sheet!")
            return

        # Selezione righe da processare
        rows_to_process = []
        for i, row in enumerate(data):
            # Logica: Processa se non verificato O se manca il Bias (per aggiornare i vecchi)
            is_verified = str(row.get('Verification', '')
                              ).lower() == 'verified'
            has_bias = str(row.get('Dominant Bias', '')) != ''

            if not is_verified or (is_verified and not has_bias):
                rows_to_process.append((i + 2, row))  # +2 per header e index

        print(
            f"📋 Eventi in coda: {len(rows_to_process)}. Analizzo batch di {BATCH_SIZE}...")

        for row_idx, event in rows_to_process[:BATCH_SIZE]:
            print(f"\n🔍 #{row_idx}: {event.get('Title')}...")

            # 1. Ricerca Tavily con URL
            query = f"{event.get('Title')} {event.get('Location')} {event.get('Date')} war ukraine russia coordinates details"
            try:
                search = tavily.search(
                    query, search_depth="advanced", max_results=4)
                context = "\n".join(
                    [f"- {r['content']} (Link: {r['url']})" for r in search['results']])
            except:
                context = "Nessuna info."

            # 2. Analisi AI
            res = analyze_event_pro(openai, event, context)

            if res.get('match') and res.get('confidence') >= CONFIDENCE_THRESHOLD:
                print(
                    f"   ✅ AGGIORNATO | Int: {res.get('intensity')} | Bias: {res.get('dominant_bias')}")

                # Lista aggiornamenti
                cells = []
                cells.append(gspread.Cell(row_idx, col_map['ver'], "verified"))

                # Campi Base
                if res.get('new_title'):
                    cells.append(gspread.Cell(
                        row_idx, col_map['title'], res['new_title']))
                if res.get('description_it'):
                    cells.append(gspread.Cell(
                        row_idx, col_map['desc'], res['description_it']))
                if res.get('intensity'):
                    cells.append(gspread.Cell(
                        row_idx, col_map['int'], res['intensity']))

                # Campi Avanzati
                if res.get('dominant_bias'):
                    cells.append(gspread.Cell(
                        row_idx, col_map['bias'], res['dominant_bias']))
                if res.get('location_precision'):
                    cells.append(gspread.Cell(
                        row_idx, col_map['prec'], res['location_precision']))

                # Fonti
                if res.get('sources'):
                    src_str = " | ".join(res['sources'])[:4000]
                    cells.append(gspread.Cell(
                        row_idx, col_map['agg_src'], src_str))

                # Coordinate (Solo se trovate nuove)
                new_lat = res.get('lat', 0)
                new_lon = res.get('lon', 0)
                if new_lat != 0 and new_lon != 0:
                    cells.append(gspread.Cell(
                        row_idx, col_map['lat'], new_lat))
                    cells.append(gspread.Cell(
                        row_idx, col_map['lon'], new_lon))
                    print(f"      📍 Nuove coord: {new_lat}, {new_lon}")

                # Scrittura batch
                try:
                    worksheet.update_cells(cells)
                    time.sleep(1.2)
                except Exception as e:
                    print(f"   ❌ Errore scrittura cella: {e}")

            else:
                print("   ⚠️ Analisi incerta, salto.")
                time.sleep(1)

    except Exception as e:
        print(f"❌ ERRORE CRITICO: {e}")


if __name__ == "__main__":
    main()
