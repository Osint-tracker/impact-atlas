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

# ==============================================================================
# 📚 DATABASE FONTI & AFFIDABILITÀ (Tier List v2.1 - Calibrated)
# ==============================================================================
# Lo script cerca queste stringhe (lowercase) nell'URL o nel nome della fonte.
# Punteggio Base: 0-100.
# ==============================================================================

SOURCE_TIERS = {
    # --- TIER A: GOLD STANDARD (90-100%) ---
    # Prove visive, geolocalizzazioni confermate, agenzie globali primarie.
    'geoconfirmed': 90,      'bellingcat': 95,       'oryx': 95,
    'deepstatemap': 90,      'osinttechnical': 88,   'brady africk': 90,
    'reuters': 90,           'ap news': 90,          'associated press': 90,
    'afp': 90,               'nasa firms': 95,       'csis': 88,
    'militarnyi': 92,        'defencehq': 95,        'balldontliedude': 90,

    # --- TIER B: ALTA AFFIDABILITÀ (75-89%) ---
    # Media internazionali autorevoli, Think Tanks, Stampa nazionale di qualità.
    # USA/UK/INT
    'nytimes': 85,           'new york times': 85,   'washington post': 85,
    'wsj': 85,               'bbc': 85,              'guardian': 82,
    'financial times': 85,   'economist': 85,        'politico': 80,
    'isw': 85,               'defense express': 85,  'the war zone': 82,
    # UCRAINA & OSINT SPECIFICI
    'cinca_afu': 80,         'tatarigami_ua': 80,    'dronebomber': 80,
    'karymat': 80,           'stanislav_osman': 80,  'officer_33': 80,
    'warmonitors': 85,       'majakovsk73': 85,      'chriso_wiki': 80,
    'kyiv independent': 80,  'ukrainska pravda': 78, 'maks23_nafo': 75,
    'noel_reports': 75,      'liveuamap': 78,        'playfraosint': 80,
    # ITALIA (Top Quality)
    'il sole 24 ore': 80,    'il post': 80,          'rainews': 80,
    'ansa': 75,              'adnkronos': 72,        'avvenire': 70,

    # --- TIER C: MEDIA AFFIDABILITÀ / PARTISAN (50-74%) ---
    # Fonti generaliste, canali Telegram di parte ma informati.
    # TELEGRAM RU/UA (Bias noto)
    'rybar': 70,             'britishmi6': 70,       'two_majors': 55,
    'dariodangelo': 80,      'deepstateua': 75,      'ukrliberation': 65,
    'fighter_bomber': 65,    'strelkovii': 60,       'lost_armour': 60,
    'grey_zone': 55,         'wargonzo': 55,          'ukrainewarcrimes': 60,
    # STAMPA ITALIANA GENERALISTA
    'corriere': 70,          'repubblica': 70,       'la stampa': 70,
    'sky tg24': 70,          'tgcom24': 60,          'fanpage': 65,
    'open': 60,
    # INTERNAZIONALI
    'cnn': 70,               'fox news': 60,         'le monde': 74,
    'nexta': 65,             'visegrad24': 60,       'spectator': 65,

    # --- TIER D: BASSA AFFIDABILITÀ / PROPAGANDA / TABLOID (10-49%) ---
    # Tabloid, media di stato russi, clickbait estremo.
    # TABLOID ITALIANI
    'il fatto quotidiano': 40, 'libero': 40,          'la verità': 40,
    'il giornale': 40,         'dagospia': 40,
    # TABLOID INTERNAZIONALI
    'daily mail': 45,          'the sun': 40,         'nypost': 45,
    # PROPAGANDA RU
    'tass': 40,                'ria novosti': 40,     'rt': 30,
    'sputnik': 30,             'intel slava': 30,     'azgeopolitics': 30,
    # GENERICI
    'twitter': 45,             'x.com': 45,           'telegram': 45,
    'facebook': 30,            'tiktok': 20,          'unknown': 30
}


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
    Agente Intelligence v3: Coordinate, Bias, Fonti, Impatto Strategico e Affidabilità Algoritmica.
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

    5. FATTORI DI AFFIDABILITÀ (PER ALGORITMO INTERNO):
       - "has_visual": true SE c'è menzione esplicita di VIDEO, FOTO, IMMAGINI SATELLITARI o GEOLOCALIZZAZIONE confermata.
       - "is_uncertain": true SE il testo usa parole come "rumors", "unconfirmed", "allegedly", "possible", "claims".
       - "num_sources": Stima intera del numero di fonti diverse citate o trovate nel contesto.

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
        "location_precision": "CITY", # Una delle categorie sopra
        "lat": 0.0,
        "lon": 0.0,
        "sources": ["url1", "url2"],
        "has_visual": false,
        "is_uncertain": false,
        "num_sources": 1
    }}
    """

    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.1
        )
        data = json.loads(response.choices[0].message.content)

        # --- ALGORITMO DI CALCOLO AFFIDABILITÀ (Hybrid Score) ---

        # 1. Base Score (Gerarchia Fonti)
        # Recupera lo score dal dizionario globale SOURCE_TIERS (definito a inizio script)
        source_name = str(event.get('Source', '')).lower()
        base_score = 40  # Default Tier D

        # Cerca nel dizionario globale
        if 'SOURCE_TIERS' in globals():
            for key, score in SOURCE_TIERS.items():
                if key in source_name:
                    base_score = score
                    break

        # 2. Moltiplicatori (Evidence & Corroboration)
        visual_bonus = 20 if data.get('has_visual') else 0

        # Bonus fonti: +10 per ogni fonte extra (max 30 punti bonus)
        src_count = int(data.get('num_sources', 1))
        corroboration_bonus = min((src_count - 1) * 10, 30)
        if corroboration_bonus < 0:
            corroboration_bonus = 0

        # 3. Malus (Incertezza Semantica)
        uncertainty_malus = -25 if data.get('is_uncertain') else 0

        # 4. Calcolo Finale (Limitato tra 10 e 100)
        final_score = base_score + visual_bonus + \
            corroboration_bonus + uncertainty_malus
        data['reliability_score'] = max(10, min(100, final_score))

        return data

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
            'lat': get_col_index('Latitude'),
            'lon': get_col_index('Longitude'),
            'bias': get_col_index('Dominant Bias'),
            'prec': get_col_index('Location Precision'),
            'agg_src': get_col_index('Aggregated Sources'),
            'rel': get_col_index('Reliability')
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

                    # Aggiornamento Affidabilità Calcolata
                if col_map.get('rel') and res.get('reliability_score'):
                    cells.append(gspread.Cell(
                        row_idx, col_map['rel'], res['reliability_score']))

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
