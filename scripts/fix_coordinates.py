import sqlite3
import os
import json

# CONFIGURAZIONE BOUNDING BOX (Il "Recinto" della Guerra)
# Tutto ciò che è fuori da questo rettangolo verrà cancellato.
VALID_LAT_MIN = 44.0
VALID_LAT_MAX = 60.0
VALID_LON_MIN = 22.0  # <--- Questo esclude l'Italia (che è a Longitudine 7-18)
VALID_LON_MAX = 55.0

DB_PATH = os.path.join('war_tracker_v2', 'data', 'raw_events.db')


def sanitize_database():
    if not os.path.exists(DB_PATH):
        print("❌ Database non trovato.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("🧹 Inizio pulizia coordinate errate (Torino, Nantes, etc.)...")

    # 1. Recupera gli eventi. NOTA: Uso 'ai_report_json' come da tuo codice
    try:
        cursor.execute(
            "SELECT event_id, ai_report_json FROM unique_events WHERE ai_report_json IS NOT NULL")
    except sqlite3.OperationalError:
        print("❌ Errore: Colonna 'ai_report_json' non trovata. Controlla il nome della colonna nel DB.")
        return

    rows = cursor.fetchall()
    print(f"🔍 Analisi di {len(rows)} eventi...")

    fixed_count = 0

    for event_id, json_str in rows:
        try:
            data = json.loads(json_str)

            # Naviga nel JSON per trovare le coordinate
            geo = data.get('tactics', {}).get(
                'geo_location', {}).get('explicit', {})

            # Se non ci sono coordinate, passa oltre
            if not geo:
                continue

            lat = geo.get('lat')
            lon = geo.get('lon')

            # Se lat/lon sono None o 0, passa oltre
            if not lat or not lon:
                continue

            # IL CONTROLLO
            is_valid = (VALID_LAT_MIN <= float(lat) <= VALID_LAT_MAX) and \
                       (VALID_LON_MIN <= float(lon) <= VALID_LON_MAX)

            if not is_valid:
                print(
                    f"   🚫 Trovato intruso: {lat}, {lon} (Event ID: {event_id[:8]}) -> PULIZIA")

                # Imposta a NULL nel JSON
                data['tactics']['geo_location']['explicit'] = None

                # Se c'è anche corrected_coordinates, puliamo anche quello
                if 'strategy' in data and 'corrected_coordinates' in data['strategy']:
                    data['strategy']['corrected_coordinates'] = None

                # Aggiorna il DB
                new_json = json.dumps(data, ensure_ascii=False)
                cursor.execute(
                    "UPDATE unique_events SET ai_report_json = ? WHERE event_id = ?", (new_json, event_id))
                fixed_count += 1

        except Exception as e:
            # print(f"Errore parsing JSON ID {event_id}: {e}") # Decommenta per debug aggressivo
            pass

    conn.commit()
    conn.close()
    print(f"✅ Finito. Coordinate corrette/cancellate: {fixed_count}")


if __name__ == "__main__":
    sanitize_database()
