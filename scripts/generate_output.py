import sqlite3
import json
import os
import csv
import ast
from datetime import datetime
from urllib.parse import urlparse

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
    
    # Caso JSON stringa
    if text.startswith('[') and text.endswith(']'):
        try:
            return ast.literal_eval(text)
        except:
            pass
            
    # Separatori comuni
    if ' ||| ' in text:
        return [x.strip() for x in text.split(' ||| ') if x.strip()]
    if ' | ' in text:
        return [x.strip() for x in text.split(' | ') if x.strip()]
    if ',' in text:
        return [x.strip() for x in text.split(',') if x.strip()]
        
    return [text]


def get_marker_style(tie_score, effect_score):
    """Calcola stile marker basato su T.I.E."""
    try:
        tie_score = float(tie_score)
        effect_score = float(effect_score)
    except:
        tie_score = 0
        effect_score = 0

    radius = 4 + (tie_score / 10)

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

    try:
        # FIX: Rimosse Latitude e Longitude dalla SELECT perché non esistono come colonne
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
        "Kinetic", "Target", "Effect", "Reliability", "Bias", "Sources"
    ]

    count = 0
    for db_row in rows:
        try:
            # Convertiamo sqlite3.Row in dizionario per usare .get() senza errori
            row = dict(db_row) 
            
            # 1. Parsing JSON Report
            json_content = row['ai_report_json']
            if not json_content:
                continue
            
            try:
                ai_data = json.loads(json_content)
            except json.JSONDecodeError:
                print(f"⚠️ JSON Corrotto per ID {row['event_id']}")
                continue

            # 2. Estrazione Fonti
            final_urls = []
            agg_src_str = ai_data.get("Aggregated Sources", "")
            if agg_src_str:
                final_urls = parse_list_field(agg_src_str)
            
            if not final_urls:
                final_urls = parse_list_field(row.get('urls_list'))

            structured_sources = []
            for url in final_urls:
                url = str(url).strip()
                if len(url) < 5 or url.lower() in ['none', 'null', 'unknown']:
                    continue
                try:
                    domain = urlparse(url).netloc.replace('www.', '')
                    if not domain: domain = "Source"
                except:
                    domain = "Source"
                structured_sources.append({"name": domain, "url": url})

            # 3. Score
            scores = ai_data.get('scores', {})
            rel_score = scores.get('reliability') or ai_data.get('Reliability', 0)
            bias_score = scores.get('bias_score') or ai_data.get('Bias Score', 0)

            try:
                tie_val = float(row.get('tie_score') or ai_data.get('TIE_Score') or 0)
            except:
                tie_val = 0.0

            # 4. Metriche Titan
            metrics = row.get('titan_metrics')
            if isinstance(metrics, str):
                try: metrics = json.loads(metrics)
                except: metrics = {}
            if not metrics:
                metrics = ai_data.get('titan_metrics', {})
                if not metrics:
                    metrics = ai_data.get('scores', {})

            if isinstance(metrics, dict):
                k_score = float(metrics.get('kinetic_score') or metrics.get('vec_k') or 0)
                t_score = float(metrics.get('target_score') or metrics.get('vec_t') or 0)
                e_score = float(metrics.get('effect_score') or metrics.get('vec_e') or 0)
            else:
                k_score, t_score, e_score = 0, 0, 0

            # Recalc TIE visivo se necessario
            if tie_val == 0 and (k_score + t_score + e_score) > 0:
                tie_val = (t_score * 1.5 + k_score + e_score) * 2 
                if tie_val > 100: tie_val = 100

            radius, color = get_marker_style(tie_val, e_score)

            # 5. Geometria (Solo dal JSON ora)
            tactics = ai_data.get('tactics', {})
            geo = tactics.get('geo_location', {}).get('explicit', {})
            lat = geo.get('lat')
            lon = geo.get('lon')
            
            # Nota: Non cerchiamo più row['Latitude'] perché non esiste.
            # Ci fidiamo solo del JSON processato dall'AI.

            has_coords = lat and lon and float(lat) != 0 and float(lon) != 0

            editorial = ai_data.get('editorial', {})
            strategy = ai_data.get('strategy', {})

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
                        "description": editorial.get('description_it') or editorial.get('description_en') or editorial.get('description', ''),
                        "date": row['last_seen_date'] or ai_data.get('timestamp_generated'),
                        "category": strategy.get('event_category', ai_data.get('Type', 'UNKNOWN')),
                        "actor": strategy.get('actor', ai_data.get('Actor', 'UNK')),
                        "tie_total": round(tie_val, 1),
                        "vec_k": k_score,
                        "vec_t": t_score,
                        "vec_e": e_score,
                        "reliability": rel_score,
                        "bias_score": bias_score,
                        "sources_list": json.dumps(structured_sources),
                        "ai_reasoning": ai_data.get('ai_summary', ''),
                        "marker_radius": radius,
                        "marker_color": color
                    }
                }
                geojson_features.append(feature)
                count += 1

            csv_rows.append({
                "Cluster_ID": row['event_id'],
                "Date": row['last_seen_date'],
                "Title": editorial.get('title', ''),
                "Type": strategy.get('event_category', ''),
                "Lat": lat if has_coords else "",
                "Lon": lon if has_coords else "",
                "TIE_Score": round(tie_val, 1),
                "Kinetic": k_score,
                "Target": t_score,
                "Effect": e_score,
                "Reliability": rel_score,
                "Bias": bias_score,
                "Sources": " | ".join([s['url'] for s in structured_sources])
            })

        except Exception as e:
            # print(f"⚠️ Errore processamento riga {row.get('event_id')}: {e}")
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
    print(f"   🗺️  GeoJSON: {count} eventi esportati (con metadati completi)")


if __name__ == "__main__":
    main()