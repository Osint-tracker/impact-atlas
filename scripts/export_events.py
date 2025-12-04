import json
import gspread
import os
from google.oauth2.service_account import Credentials
from pathlib import Path
from dotenv import load_dotenv

# Carica variabili d'ambiente (.env)
load_dotenv()

# --- CONFIGURAZIONE ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1NEyNXzCSprGOw6gCmVVbtwvFmz8160Oag-WqG93ouoQ/edit?usp=sharing"
# Percorso relativo alla cartella scripts
OUTPUT_FILE = "../assets/data/events.geojson"


def get_google_client():
    """Connette a Google Sheets cercando il file json in varie posizioni"""
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']

    # Cartella dove si trova questo script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Possibili posizioni del file
    paths_to_check = [
        # Dentro /scripts
        os.path.join(script_dir, 'service_account.json'),
        # Nella cartella principale
        os.path.join(script_dir, '..', 'service_account.json')
    ]

    json_key_path = None
    for path in paths_to_check:
        if os.path.exists(path):
            json_key_path = path
            break

    if not json_key_path:
        raise FileNotFoundError(
            f"Errore: Non trovo 'service_account.json'. Controlla di averlo messo nella cartella principale o in 'scripts'.")

    print(f"🔑 Trovate credenziali in: {json_key_path}")  # Debug info
    creds = Credentials.from_service_account_file(json_key_path, scopes=scope)
    return gspread.authorize(creds)


def map_icon_by_type(event_type):
    """Assegna un'icona basata sul tipo di evento"""
    event_type = str(event_type).lower()
    if "missile" in event_type or "strike" in event_type:
        return "💥"
    if "drone" in event_type:
        return "🚁"
    if "fire" in event_type or "incendio" in event_type:
        return "🔥"
    if "artillery" in event_type:
        return "💣"
    if "clash" in event_type or "combat" in event_type:
        return "⚔️"
    return "⚠️"  # Default


def main():
    print("🌍 Avvio esportazione eventi da Google Sheet a GeoJSON...")

    try:
        # 1. Connessione
        gc = get_google_client()
        sh = gc.open_by_url(SHEET_URL)
        worksheet = sh.get_worksheet(0)

        # 2. Scarica tutti i dati
        records = worksheet.get_all_records()
        print(f"📊 Trovate {len(records)} righe nel foglio.")

        features = []
        skipped = 0

        for row in records:
            # Filtra: Solo eventi VERIFICATI
            if str(row.get('Verification', '')).lower() != 'verified':
                continue

            # Controllo Coordinate
            try:
                lat = row.get('Latitude')
                lon = row.get('Longitude')

                # Se le coordinate sono vuote o stringhe strane, salta
                if not lat or not lon:
                    skipped += 1
                    continue

                lat = float(str(lat).replace(',', '.'))
                lon = float(str(lon).replace(',', '.'))
            except ValueError:
                skipped += 1
                continue

            # Costruisci la "Feature" GeoJSON
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    # GeoJSON vuole PRIMA Longitude, POI Latitude
                    "coordinates": [lon, lat]
                },
                "properties": {
                    "id": str(row.get('ID', '')),
                    "title": row.get('Title', 'Evento'),
                    "type": row.get('Type', 'Unknown'),
                    "description": row.get('Description', ''),
                    "date": row.get('Date', ''),
                    "source": row.get('Source', ''),
                    "intensity": row.get('Intensity', 0.5),
                    "icon": map_icon_by_type(row.get('Type', ''))
                }
            }
            features.append(feature)

        # 3. Creazione oggetto GeoJSON finale
        geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "generated_at": "Today",
                "count": len(features)
            },
            "features": features
        }

        # 4. Salvataggio su file
        # Calcola percorso assoluto per evitare errori
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, OUTPUT_FILE)

        # Assicurati che la cartella esista
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2, ensure_ascii=False)

        print(f"✅ Esportazione completata!")
        print(f"   - Eventi salvati: {len(features)}")
        print(f"   - Eventi saltati (no coords/non verificati): {skipped}")
        print(f"   - File salvato in: {output_path}")

    except Exception as e:
        print(f"❌ Errore critico: {e}")


if __name__ == "__main__":
    main()
