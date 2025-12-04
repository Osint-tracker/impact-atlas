import json
import gspread
import os
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()
SHEET_URL = "https://docs.google.com/spreadsheets/d/1NEyNXzCSprGOw6gCmVVbtwvFmz8160Oag-WqG93ouoQ/edit"
OUTPUT_FILE = "../assets/data/events.geojson"


def get_google_client():
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']
    script_dir = os.path.dirname(os.path.abspath(__file__))
    paths = [os.path.join(script_dir, 'service_account.json'), os.path.join(
        script_dir, '..', 'service_account.json')]
    json_path = next((p for p in paths if os.path.exists(p)), None)
    if not json_path:
        raise FileNotFoundError("Service account json non trovato")
    return gspread.authorize(Credentials.from_service_account_file(json_path, scopes=scope))


def main():
    print("🌍 Exporting Events...")
    try:
        gc = get_google_client()
        sh = gc.open_by_url(SHEET_URL)
        records = sh.get_worksheet(0).get_all_records()

        features = []
        for row in records:
            # Esportiamo SOLO se abbiamo coordinate valide
            try:
                lat = float(str(row.get('Latitude', 0)).replace(',', '.'))
                lon = float(str(row.get('Longitude', 0)).replace(',', '.'))
                if lat == 0 or lon == 0:
                    continue
            except:
                continue

            # Gestione Immagini e Fonti
            # Cerchiamo colonne come 'Image', 'Video' o 'Media'
            media_url = row.get('Video') or row.get(
                'Image') or row.get('Media') or ""
            source_url = row.get('Source') or "Unknown Source"

            # Pulizia Source (rende cliccabile se è un URL grezzo)
            if "t.me" in source_url and not source_url.startswith("http"):
                source_url = "https://" + source_url

            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "title": row.get('Title'),
                    "type": row.get('Type', 'unknown').lower(),
                    "description": row.get('Description'),
                    "date": row.get('Date'),
                    "source": source_url,  # <--- ECCO LA FONTE
                    "image": media_url,    # <--- ECCO L'IMMAGINE
                    "intensity": float(str(row.get('Intensity', 0.5)).replace(',', '.')),
                    "verified": True
                }
            }
            features.append(feature)

        # Salvataggio
        geojson = {"type": "FeatureCollection", "features": features}
        script_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(script_dir, OUTPUT_FILE), 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2, ensure_ascii=False)

        print(f"✅ Export completato: {len(features)} eventi validi.")

    except Exception as e:
        print(f"❌ Errore: {e}")


if __name__ == "__main__":
    main()
