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
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
GEOJSON_PATH = os.path.join(BASE_DIR, '../assets/data/events.geojson')
CSV_PATH = os.path.join(BASE_DIR, '../assets/data/events_export.csv')


def parse_list_field(field_value):
    """Converte stringhe sporche in liste Python pulite."""
    if not field_value:
        return []
    if isinstance(field_value, list):
        return field_value
    text = str(field_value).strip()
    if text.startswith('[') and text.endswith(']'):
        try:
            return ast.literal_eval(text)
        except:
            pass
    if ' ||| ' in text:
        return [x.strip() for x in text.split(' ||| ') if x.strip()]
    if ',' in text:
        return [x.strip() for x in text.split(',') if x.strip()]
    return [text]


def get_marker_style(tie_score, effect_score):
    """Calcola stile marker basato su T.I.E."""
    # Radius (Dimensione): Base 4 + bonus TIE
    radius = 4 + (tie_score / 10)

    # Color (Effetto): Rosso=Distruzione, Arancio=Danni, Grigio=Ignoto
    if effect_score >= 8:
        color = "#ef4444"  # Critical Red
    elif effect_score >= 5:
        color = "#f97316"  # High Orange
    elif effect_score >= 2:
        color = "#eab308"  # Medium Yellow
    else:
        color = "#64748b"  # Low Slate

    return radius, color


def main():
    print("🚀 AVVIO EXPORT INTELLIGENCE (GeoJSON + CSV + TIE)...")

    if not os.path.exists(DB_PATH):
        print(f"❌ ERRORE: Database non trovato: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Query completa
    try:
        cursor.execute("""
            SELECT event_id, ai_report_json, urls_list, sources_list, last_seen_date,
                   tie_score, tie_status, titan_metrics
            FROM unique_events 
            WHERE ai_report_json IS NOT NULL 
            AND ai_analysis_status IN ('COMPLETED', 'VERIFIED')
            AND ai_analysis_status != 'MERGED'
        """)
        rows = cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(f"❌ ERRORE SQL: {e}")
        return

    if not rows:
        print("⚠️ NESSUN DATO TROVATO.")
        return

    geojson_features = []
    csv_rows = []

    csv_headers = [
        "Cluster_ID", "Date", "Title", "Type", "Lat", "Lon", "TIE_Score",
        "Kinetic", "Target", "Effect", "Reliability", "Sources"
    ]

    for row in rows:
        try:
            # 1. Parsing JSON Report
            json_content = row['ai_report_json']
            if not json_content:
                continue
            ai_data = json.loads(json_content)

            # 2. Parsing Liste Link/Fonti
            raw_urls = parse_list_field(row['urls_list'])
            raw_sources = parse_list_field(row['sources_list'])
            structured_sources = []

            # Unione intelligente nomi + link
            max_len = max(len(raw_urls), len(raw_sources))
            for i in range(max_len):
                s_name = raw_sources[i] if i < len(raw_sources) else "Source"
                s_url = raw_urls[i] if i < len(raw_urls) else ""
                if s_url or s_name != "Source":
                    structured_sources.append({"name": s_name, "url": s_url})

            # 3. Estrazione Campi
            editorial = ai_data.get('editorial', {})
            strategy = ai_data.get('strategy', {})
            tactics = ai_data.get('tactics', {})
            scores = ai_data.get('scores', {})

            # --- ESTRAZIONE DATI T.I.E. ---
            # Cerchiamo prima nella colonna dedicata, poi nel JSON
            tie_val = row['tie_score'] or 0

            # Recupero vettori K, T, E (Gestione fallback robusta)
            # A volte è salvato come stringa nel DB, a volte dict
            metrics = row['titan_metrics']
            if isinstance(metrics, str):
                try:
                    metrics = json.loads(metrics)
                except:
                    metrics = {}
            elif not isinstance(metrics, dict):
                metrics = {}

            # Se vuoto, cerca nel JSON report
            if not metrics:
                metrics = ai_data.get('titan_metrics', {})

            k_score = float(metrics.get('kinetic_score') or 0)
            t_score = float(metrics.get('target_score') or 0)
            e_score = float(metrics.get('effect_score') or 0)

            # Calcolo Stile Marker
            radius, color = get_marker_style(tie_val, e_score)

            # 4. Geometria
            geo = tactics.get('geo_location', {}).get('explicit', {})
            lat = geo.get('lat')
            lon = geo.get('lon')
            has_coords = lat and lon and lat != 0 and lon != 0

            # 5. Costruzione GeoJSON
            if has_coords:
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(lon), float(lat)]
                    },
                    "properties": {
                        "id": row['event_id'],
                        "title": editorial.get('title_it') or editorial.get('title', 'Evento'),
                        "desc": editorial.get('description_it') or editorial.get('description', ''),
                        "date": row['last_seen_date'] or ai_data.get('timestamp_generated'),

                        # Categorie
                        "category": strategy.get('event_category', 'UNKNOWN'),
                        "actor": strategy.get('actor', 'UNK'),

                        # Metriche T.I.E. (Fondamentali per la Intel Card)
                        "tie_total": tie_val,
                        "vec_k": k_score,
                        "vec_t": t_score,
                        "vec_e": e_score,

                        # Metriche KPI
                        "reliability": scores.get('reliability', 0),
                        "bias_score": scores.get('bias_score', 0),
                        
                        # AI Strategist Comment
                        "ai_reasoning": ai_data.get('ai_summary', ''),

                        # Link
                        # Serializzato per sicurezza JS
                        "sources_list": json.dumps(structured_sources),

                        # Stile pre-calcolato (Opzionale ma utile)
                        "marker_radius": radius,
                        "marker_color": color
                    }
                }
                geojson_features.append(feature)

            # 6. Costruzione CSV (Semplificato)
            csv_rows.append({
                "Cluster_ID": row['event_id'],
                "Date": row['last_seen_date'],
                "Title": editorial.get('title', ''),
                "Type": strategy.get('event_category', ''),
                "Lat": lat if has_coords else "",
                "Lon": lon if has_coords else "",
                "TIE_Score": tie_val,
                "Kinetic": k_score,
                "Target": t_score,
                "Effect": e_score,
                "Reliability": scores.get('reliability', 0),
                "Sources": " | ".join([s['url'] for s in structured_sources])
            })

        except Exception as e:
            # print(f"Skipping row: {e}")
            continue

    # Salvataggio
    os.makedirs(os.path.dirname(GEOJSON_PATH), exist_ok=True)

    with open(GEOJSON_PATH, 'w', encoding='utf-8') as f:
        json.dump({"type": "FeatureCollection",
                  "features": geojson_features}, f, indent=2, ensure_ascii=False)

    with open(CSV_PATH, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(csv_rows)

    print("\n✅ ESPORTAZIONE COMPLETATA")
    print(f"   🗺️  GeoJSON: {len(geojson_features)} eventi (con dati T.I.E.)")


if __name__ == "__main__":
    main()
