import json
import gspread
import os
import hashlib
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURAZIONE ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1NEyNXzCSprGOw6gCmVVbtwvFmz8160Oag-WqG93ouoQ/edit"
OUTPUT_FILE = "../assets/data/events.geojson"


def get_google_client():
    """Autenticazione Google Sheet sicura"""
    scope = ['https://www.googleapis.com/auth/spreadsheets',
             'https://www.googleapis.com/auth/drive']
    script_dir = os.path.dirname(os.path.abspath(__file__))
    json_key_path = os.path.join(script_dir, 'service_account.json')
    if not os.path.exists(json_key_path):
        json_key_path = os.path.join(script_dir, '..', 'service_account.json')
    if not os.path.exists(json_key_path):
        raise FileNotFoundError("❌ ERRORE: Non trovo 'service_account.json'.")
    creds = Credentials.from_service_account_file(json_key_path, scopes=scope)
    return gspread.authorize(creds)


def safe_float(value, default=0.0):
    """Pulisce coordinate e numeri"""
    if not value:
        return default
    try:
        clean_val = str(value).replace(',', '.').strip()
        return float(clean_val) if clean_val else default
    except:
        return default


def safe_int(value, default=0):
    """Pulisce interi (utile per reliability/bias score)"""
    if not value:
        return default
    try:
        return int(float(str(value).replace(',', '.')))
    except:
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
            # 1. Coordinate
            lat = safe_float(row.get('Latitude'), 0)
            lon = safe_float(row.get('Longitude'), 0)

            if lat == 0 or lon == 0:
                skipped += 1
                continue

            # 2. Generazione ID Univoco
            unique_string = f"{row.get('Date')}{lat}{lon}{row.get('Title')}"
            event_id = hashlib.md5(unique_string.encode()).hexdigest()[:12]

            # 3. Gestione Media
            media_url = row.get('Video') or row.get(
                'Image') or row.get('Media') or ""

            # 4. Gestione Fonti (Parsing Aggregato)
            source_url = row.get('Source') or "Unknown Source"
            # Fix link Telegram
            if "t.me" in source_url and not source_url.startswith("http"):
                source_url = "https://" + source_url

            raw_agg = str(row.get('Aggregated Sources', ''))
            references = [s.strip() for s in raw_agg.split('|') if s.strip()]

            # Se la lista è vuota, aggiungi la fonte principale
            if not references and source_url != "Unknown Source":
                references.append(source_url)

            # 5. Costruzione GeoJSON
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat]
                },
                "properties": {
                    # Identificatori
                    "event_id": event_id,
                    "title": row.get('Title', 'Evento senza titolo'),
                    "date": str(row.get('Date', '')),

                    # Dati Descrittivi
                    "description": row.get('Description', ''),
                    "type": str(row.get('Type', 'unknown')).lower(),
                    # Per retrocompatibilità
                    "category": str(row.get('Type', '')).upper(),

                    # Intelligence & Metadati
                    "actor": str(row.get('Actor', 'UNK')).upper().strip(),
                    "intensity": safe_float(row.get('Intensity'), 0.5),
                    "reliability": safe_int(row.get('Reliability'), 50),

                    # BIAS (Label + Numero)
                    # Colonna 15
                    "dominant_bias": str(row.get('Bias dominante', 'NEUTRAL')),
                    # Colonna 19 (NUOVA)
                    "bias_score": safe_float(row.get('Bias Score'), 0.0),

                    # Location
                    "location_precision": str(row.get('Location Precision', 'CITY')),

                    # Media & Fonti
                    "source": source_url,
                    "references": references,  # Lista JSON per il frontend
                    "video": media_url,
                    "image": media_url,

                    "verified": True  # Default
                }
            }
            features.append(feature)

        # 6. Salvataggio
        geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "count": len(features),
                "generated": "Impact Atlas Exporter"
            },
            "features": features
        }

        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, OUTPUT_FILE)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2, ensure_ascii=False)

        print(f"✅ Export completato!")
        print(f"   - Eventi esportati: {len(features)}")
        print(f"   - Saltati (no coord): {skipped}")
        print(f"   - File: {output_path}")

    except Exception as e:
        print(f"❌ Errore critico: {e}")


if __name__ == "__main__":
    main()
