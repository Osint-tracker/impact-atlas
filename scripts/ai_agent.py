import os
import json
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI
import time
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURAZIONE ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1NEyNXzCSprGOw6gCmVVbtwvFmz8160Oag-WqG93ouoQ/edit"
# Elabora eventi non verificati O quelli con coordinate mancanti (0,0)
REPROCESS_ALL_BAD_COORDS = True


def setup_clients():
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']
    json_key_path = os.path.join(
        os.path.dirname(__file__), 'service_account.json')

    if not os.path.exists(json_key_path):
        # Fallback percorso
        json_key_path = os.path.join(os.path.dirname(
            __file__), '..', 'service_account.json')

    creds = Credentials.from_service_account_file(json_key_path, scopes=scope)
    gc = gspread.authorize(creds)
    openai = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
    return gc, openai


def analyze_event_smart(openai, event):
    """
    Agente correttivo: inferisce coordinate, rimuove emoji e valida.
    """
    prompt = f"""
    Sei un analista OSINT militare. Il tuo compito è pulire e strutturare i dati grezzi.

    INPUT DATI:
    - Testo Originale: {event.get('Title', '')} {event.get('Description', '')}
    - Luogo Attuale: {event.get('Location', '')}
    - Coordinate Attuali: {event.get('Latitude')}, {event.get('Longitude')}

    ISTRUZIONI RIGIDE:
    1. **GEOCODING (CRUCIALE):** Se le coordinate mancano o sono 0,0, DEVI stimarle basandoti sul nome della città/villaggio menzionato nel testo. Usa la tua conoscenza geografica interna. Es: "Attacco a Kiev" -> Lat: 50.45, Lon: 30.52.
    2. **NO EMOJI:** Rimuovi TUTTE le emoji dal titolo e dalla descrizione. Solo testo professionale.
    3. **TITOLO:** Stile militare conciso (es. "Attacco missilistico su Kharkiv"). Max 10 parole.
    4. **DESCRIZIONE:** Riassunto in Italiano. Includi dettagli armamenti se noti.
    5. **FONTE:** Se nel testo c'è un link o menzione (es. @Rybar), estrailo nel campo source.

    RISPONDI SOLO JSON:
    {{
        "verified": true,
        "title_clean": "...",
        "description_clean": "...",
        "lat": 0.0000,
        "lon": 0.0000,
        "type": "ground/air/missile/artillery",
        "intensity": 0.5,
        "source_extracted": "..." 
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
        return None


def main():
    print("🧠 Avvio AI Smart Fixer (Geocoding & Cleaning)...")
    gc, openai = setup_clients()
    sh = gc.open_by_url(SHEET_URL)
    worksheet = sh.get_worksheet(0)

    data = worksheet.get_all_records()
    headers = worksheet.row_values(1)

    # Mappatura indici colonne (1-based per gspread)
    try:
        idx_ver = headers.index('Verification') + 1
        idx_lat = headers.index('Latitude') + 1
        idx_lon = headers.index('Longitude') + 1
        idx_title = headers.index('Title') + 1
        idx_desc = headers.index('Description') + 1
        idx_src = headers.index('Source') + 1
        idx_type = headers.index('Type') + 1
        idx_int = headers.index('Intensity') + 1
    except ValueError as e:
        print(f"❌ Errore colonne: {e}")
        return

    updates_count = 0

    for i, row in enumerate(data):
        row_num = i + 2  # Header + 0-index

        # Condizione: Se non è verificato O se le coordinate sembrano sbagliate (0, vuote, o None)
        lat_val = str(row.get('Latitude', '')).replace(',', '.')
        is_bad_coords = not lat_val or lat_val == '0' or lat_val == '0.0'

        # Se vuoi forzare il fix su tutto, togli l'if e lascia correre
        if str(row.get('Verification', '')).lower() != 'verified' or is_bad_coords:

            print(f"🔧 Fixing riga {row_num}: {row.get('Title')[:30]}...")

            res = analyze_event_smart(openai, row)

            if res:
                # Aggiornamento Cella per Cella
                worksheet.update_cell(row_num, idx_ver, "verified")
                worksheet.update_cell(row_num, idx_title,
                                      res.get('title_clean'))
                worksheet.update_cell(
                    row_num, idx_desc, res.get('description_clean'))
                worksheet.update_cell(row_num, idx_lat, res.get('lat'))
                worksheet.update_cell(row_num, idx_lon, res.get('lon'))
                worksheet.update_cell(row_num, idx_type, res.get('type'))
                worksheet.update_cell(row_num, idx_int, res.get('intensity'))

                # Aggiorna la fonte solo se vuota o se l'AI ne ha trovata una migliore
                if res.get('source_extracted') and not row.get('Source'):
                    worksheet.update_cell(
                        row_num, idx_src, res.get('source_extracted'))

                print(
                    f"   ✅ Fixed: {res.get('lat')}, {res.get('lon')} | No Emojis")
                updates_count += 1
                time.sleep(1)  # Rispetto rate limits
            else:
                print("   ⚠️ AI fallita su questa riga.")

    print(f"🏁 Finito. Aggiornate {updates_count} righe.")


if __name__ == "__main__":
    main()
