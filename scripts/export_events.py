import json
import gspread
import os
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURAZIONE ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1NEyNXzCSprGOw6gCmVVbtwvFmz8160Oag-WqG93ouoQ/edit"
OUTPUT_FILE = "../assets/data/events.geojson"


def get_google_client():
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']

    # Cerca il file delle credenziali in modo flessibile
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_key_path = os.path.join(script_dir, 'service_account.json')

    if not os.path.exists(json_key_path):
        # Fallback: prova nella cartella superiore
        json_key_path = os.path.join(script_dir, '..', 'service_account.json')

    if not os.path.exists(json_key_path):
        raise FileNotFoundError("❌ ERRORE: Non trovo 'service_account.json'.")

    creds = Credentials.from_service_account_file(json_key_path, scopes=scope)
    return gspread.authorize(creds)


def safe_float(value, default=0.0):
    """
    Converte una stringa in float gestendo errori, virgole e stringhe vuote.
    """
    if not value:
        return default
    try:
        # Gestisce sia "45.5" che "45,5"
        clean_val = str(value).replace(',', '.').strip()
        if clean_val == "":
            return default
        return float(clean_val)
    except (ValueError, TypeError):
        return default


def main():
    print("🌍 Exporting Events from Google Sheet...")

    try:
        gc = get_google_client()
        sh = gc.open_by_url(SHEET_URL)
        worksheet = sh.get_worksheet(0)
        records = worksheet.get_all_records()

        print(f"📊 Trovate {len(records)} righe totali.")

        features = []
        skipped = 0

        for row in records:
            # 1. Pulizia e Conversione Coordinate
            lat = safe_float(row.get('Latitude'), 0)
            lon = safe_float(row.get('Longitude'), 0)

            # Se le coordinate sono 0 (o fallite), saltiamo la riga
            if lat == 0 or lon == 0:
                skipped += 1
                continue

            # 2. Gestione Media e Fonti
            media_url = row.get('Video') or row.get(
                'Image') or row.get('Media') or ""
            source_url = row.get('Source') or "Unknown Source"

            # Rende il link cliccabile se è un username telegram
            if "t.me" in source_url and not source_url.startswith("http"):
                source_url = "https://" + source_url

            # 3. Costruzione Feature GeoJSON
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]  # GeoJSON vuole [Lon, Lat]
                },
                "properties": {
                    "title": row.get('Title', 'Evento senza titolo'),
                    "type": str(row.get('Type', 'unknown')).lower(),
                    "description": row.get('Description', ''),
                    "date": str(row.get('Date', '')),
                    "source": source_url,
                    "image": media_url,
                    "intensity": safe_float(row.get('Intensity'), 0.5),
                    "verified": True
                }
            }
            features.append(feature)

        # 4. Salvataggio su file
        geojson = {
            "type": "FeatureCollection",
            "metadata": {"count": len(features)},
            "features": features
        }

        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, OUTPUT_FILE)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2, ensure_ascii=False)

        print(f"✅ Export completato!")
        print(f"   - Eventi validi esportati: {len(features)}")
        print(f"   - Eventi saltati (no coords): {skipped}")
        print(f"   - File salvato in: {output_path}")

    except Exception as e:
        print(f"❌ Errore critico durante l'export: {e}")


if __name__ == "__main__":
    main()
