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

    # Fallback per percorsi diversi
    if not os.path.exists(json_key_path):
        json_key_path = os.path.join(script_dir, '..', 'service_account.json')

    if not os.path.exists(json_key_path):
        raise FileNotFoundError("❌ ERRORE: Non trovo 'service_account.json'.")

    creds = Credentials.from_service_account_file(json_key_path, scopes=scope)
    return gspread.authorize(creds)


def safe_float(value, default=0.0):
    """Pulisce coordinate e numeri (gestisce virgole e spazi)"""
    if not value:
        return default
    try:
        clean_val = str(value).replace(',', '.').strip()
        return float(clean_val) if clean_val else default
    except:
        return default


def main():
    print("🌍 Exporting Events from Google Sheet...")

    try:
        gc = get_google_client()
        sh = gc.open_by_url(SHEET_URL)
        worksheet = sh.get_worksheet(0)

        # Scarica tutti i dati in una volta sola
        records = worksheet.get_all_records()
        print(f"📊 Trovate {len(records)} righe totali.")

        features = []
        skipped = 0

        for row in records:
            # 1. Coordinate (Fondamentali)
            lat = safe_float(row.get('Latitude'), 0)
            lon = safe_float(row.get('Longitude'), 0)

            if lat == 0 or lon == 0:
                skipped += 1
                continue

            # 2. Gestione Media e Fonte Principale
            media_url = row.get('Video') or row.get(
                'Image') or row.get('Media') or ""
            source_url = row.get('Source') or "Unknown Source"

            # Fix link Telegram
            if "t.me" in source_url and not source_url.startswith("http"):
                source_url = "https://" + source_url

                # Generazione ID Univoco (Fondamentale per il bottone Dossier)
            unique_string = f"{row.get('Date')}{lat}{lon}{row.get('Title')}"
            event_id = hashlib.md5(unique_string.encode()).hexdigest()[:12]

            # Parsing Fonti Aggregate (da stringa "a | b" a lista ["a", "b"])
            raw_sources = str(row.get('Aggregated Sources', ''))
            references = [s.strip()
                          for s in raw_sources.split('|') if s.strip()]

            # Fallback: se non ci sono fonti aggregate, usa la fonte principale
            if not references and source_url != "Unknown Source":
                references.append(source_url)

            # 3. Gestione Fonti Aggregate (Importante per il nuovo modale)
            # L'AI le salva come "url1 | url2", qui le trasformiamo in lista
            raw_sources = str(row.get('Aggregated Sources', ''))
            references = [s.strip()
                          for s in raw_sources.split('|') if s.strip()]

            # Se non ci sono fonti aggregate, usiamo la fonte principale come fallback
            if not references and source_url != "Unknown Source":
                references.append(source_url)

            # 4. Costruzione Feature GeoJSON
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    # GeoJSON vuole prima Longitude, poi Latitude
                    "coordinates": [lon, lat]
                },
                "properties": {
                    "event_id": event_id,  # <--- NUOVO
                    "title": row.get('Title', 'Evento senza titolo'),
                    "type": str(row.get('Type', 'unknown')).lower(),
                    "description": row.get('Description', ''),
                    "date": str(row.get('Date', '')),

                    # Media
                    "source": source_url,
                    "image": media_url,
                    "video": media_url,  # Per compatibilità

                    # Dati Tecnici
                    "intensity": safe_float(row.get('Intensity'), 0.5),
                    "verified": True,

                    # --- NUOVI DATI INTELLIGENCE ---
                    "dominant_bias": str(row.get('Dominant Bias', 'NEUTRAL')),
                    "location_precision": str(row.get('Location Precision', 'CITY')),
                    "references": references,  # La lista creata sopra
                    "category": str(row.get('Type', '')).upper()
                }
            }
            features.append(feature)

        # 5. Salvataggio su file
        geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "count": len(features),
                "generated": "Impact Atlas Exporter"
            },
            "features": features
        }

        # Calcola percorso assoluto per evitare errori
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, OUTPUT_FILE)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2, ensure_ascii=False)

        print(f"✅ Export completato con successo!")
        print(f"   - Eventi validi esportati: {len(features)}")
        print(f"   - Eventi saltati (no coordinate): {skipped}")
        print(f"   - File salvato in: {output_path}")

    except Exception as e:
        print(f"❌ Errore critico durante l'export: {e}")


if __name__ == "__main__":
    main()
