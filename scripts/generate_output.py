import sqlite3
import json
import os
import csv
import ast
from datetime import datetime

# =============================================================================
# 🛠️ CONFIGURAZIONE PERCORSI
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Percorso del DB (Adattato alla tua struttura)
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

# Percorsi Output
GEOJSON_PATH = os.path.join(BASE_DIR, '../assets/data/events.geojson')
CSV_PATH = os.path.join(BASE_DIR, '../assets/data/events_export.csv')


def parse_list_field(field_value):
    """
    Tenta di convertire una stringa sporca dal DB in una lista Python pulita.
    Gestisce formati come: "['a', 'b']", "a, b", "a ||| b"
    """
    if not field_value:
        return []

    # Se è già una lista (raro in sqlite, ma possibile)
    if isinstance(field_value, list):
        return field_value

    text = str(field_value).strip()

    # Caso 1: È una rappresentazione letterale di lista Python "['url1', 'url2']"
    if text.startswith('[') and text.endswith(']'):
        try:
            return ast.literal_eval(text)
        except:
            pass  # Fallback se il parsing fallisce

    # Caso 2: Separatore custom usato spesso nei crawler OSINT
    if ' ||| ' in text:
        return [x.strip() for x in text.split(' ||| ') if x.strip()]

    # Caso 3: Virgola semplice
    if ',' in text:
        return [x.strip() for x in text.split(',') if x.strip()]

    # Caso 4: Singolo valore
    return [text]


def main():
    print("🚀 AVVIO EXPORT INTELLIGENCE (GeoJSON + CSV) con LINK...")
    print(f"   📂 Database target: {os.path.normpath(DB_PATH)}")

    if not os.path.exists(DB_PATH):
        print(
            f"❌ ERRORE CRITICO: Il database non esiste al percorso: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Estrazione Dati: Ora prendiamo anche urls_list e sources_list
    try:
        cursor.execute("""
            SELECT event_id, ai_report_json, urls_list, sources_list, last_seen_date
            FROM unique_events 
            WHERE ai_report_json IS NOT NULL 
            AND (ai_analysis_status = 'COMPLETED' OR ai_analysis_status = 'VERIFIED')
        """)
        rows = cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(f"❌ ERRORE SQL: {e}")
        return

    if not rows:
        print("⚠️ NESSUN DATO TROVATO: Nessun evento con status COMPLETED o VERIFIED.")
        return

    print(
        f"   🔍 Trovati {len(rows)} report. Unione AI + Link Originali in corso...")

    geojson_features = []
    csv_rows = []

    csv_headers = [
        "Cluster_ID", "Date",
        "Title", "Title_EN",
        "Description", "Description_EN",
        "Type", "Location_Name", "Lat", "Lon",
        "Actor", "Reliability", "Verification_Status",
        "Source_Names", "Source_Links"  # Nuove colonne per il CSV
    ]

    count_errors = 0

    for row in rows:
        try:
            # --- A. Dati dall'AI ---
            json_content = row['ai_report_json']
            if not json_content:
                continue

            ai_data = json.loads(json_content)

            # --- B. Dati dal DB (Link Originali) ---
            raw_urls = parse_list_field(row['urls_list'])
            raw_sources = parse_list_field(row['sources_list'])

            # Creiamo una lista di oggetti strutturati per il GeoJSON
            # Esempio: [{"name": "DeepState", "url": "http..."}, ...]
            structured_sources = []

            # Se le liste hanno lunghezze diverse, usiamo la più lunga o tronchiamo in sicurezza
            max_len = max(len(raw_urls), len(raw_sources))
            for i in range(max_len):
                s_name = raw_sources[i] if i < len(
                    raw_sources) else "Unknown Source"
                s_url = raw_urls[i] if i < len(raw_urls) else ""

                # Aggiungiamo solo se c'è almeno l'URL o il nome
                if s_url or s_name != "Unknown Source":
                    structured_sources.append({"name": s_name, "url": s_url})

            # --- C. Estrazione Campi Standard ---
            editorial = ai_data.get('editorial', {})
            strategy = ai_data.get('strategy', {})
            tactics = ai_data.get('tactics', {})
            scores = ai_data.get('scores', {})

            title_it = editorial.get(
                'title_it') or editorial.get('title', 'Evento')
            desc_it = editorial.get(
                'description_it') or editorial.get('description', '')
            title_en = editorial.get('title_en', title_it)
            desc_en = editorial.get('description_en', desc_it)

            event_type = strategy.get('implicit_signal') or tactics.get(
                'target_category') or 'Unknown'
            actor = strategy.get('actor', 'UNK')

            geo = tactics.get('geo_location') or {}
            explicit_geo = geo.get('explicit') or {}
            lat = explicit_geo.get('lat')
            lon = explicit_geo.get('lon')

            # Timestamp: preferiamo quello del DB (last_seen), fallback sull'AI
            timestamp = row['last_seen_date'] or ai_data.get(
                'timestamp_generated', datetime.now().isoformat())

            cluster_id = row['event_id']

            # --- D. Costruzione GeoJSON ---
            has_coords = lat is not None and lon is not None and lat != 0 and lon != 0

            if has_coords:
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(lon), float(lat)]
                    },
                    "properties": {
                        "cluster_id": cluster_id,
                        "title": title_it,
                        "description": desc_it,
                        "title_en": title_en,
                        "description_en": desc_en,
                        "date": timestamp,
                        "type": event_type,
                        "actor": actor,
                        "reliability": scores.get('reliability', 0),
                        "intensity": scores.get('intensity', 0),
                        "sources": structured_sources  # <--- ECCO I LINK PER LA DASHBOARD!
                    }
                }
                geojson_features.append(feature)

            # --- E. Costruzione CSV ---
            csv_rows.append({
                "Cluster_ID": cluster_id,
                "Date": timestamp,
                "Title": title_it,
                "Title_EN": title_en,
                "Description": desc_it,
                "Description_EN": desc_en,
                "Type": event_type,
                "Location_Name": geo.get('inferred', {}).get('toponym_raw', ''),
                "Lat": lat if has_coords else "",
                "Lon": lon if has_coords else "",
                "Actor": actor,
                "Reliability": scores.get('reliability', 0),
                "Verification_Status": ai_data.get('status', 'Unverified'),
                "Source_Names": " | ".join(raw_sources),
                "Source_Links": " | ".join(raw_urls)
            })

        except Exception as e:
            count_errors += 1
            # print(f"Errore: {e}")
            continue

    # Scrittura Files
    os.makedirs(os.path.dirname(GEOJSON_PATH), exist_ok=True)

    with open(GEOJSON_PATH, 'w', encoding='utf-8') as f:
        json.dump({"type": "FeatureCollection",
                  "features": geojson_features}, f, indent=2, ensure_ascii=False)

    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(csv_rows)

    print("\n✅ ESPORTAZIONE COMPLETATA")
    print(f"   🗺️  GeoJSON: {len(geojson_features)} eventi (con Link attivi)")
    print(f"   📊 CSV:     {len(csv_rows)} righe")


if __name__ == "__main__":
    main()
