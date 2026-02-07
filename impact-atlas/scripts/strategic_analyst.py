import sqlite3
import json
import numpy as np
import os
from datetime import datetime, timedelta

# =============================================================================
# ⚙️ CONFIGURAZIONE
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

# Configurazione Sensibilità Statistica
WINDOW_DAYS = 30        # Giorni di storia da analizzare
MIN_EVENTS_HISTORY = 5  # Minimo eventi per avere una baseline credibile
DRIFT_WINDOW = 7        # Giorni per calcolare il trend (Recente vs Passato)


class StrategicAnalyst:
    def __init__(self):
        print("🕵️  INIT: Strategic Analyst (Layer 2: Statistical Context)")
        if not os.path.exists(DB_PATH):
            raise FileNotFoundError(f"❌ DB not found: {DB_PATH}")

    def get_db_connection(self):
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn

    def get_location_history(self, cursor, location_name, ref_date_str):
        """
        Estrae i punteggi T.I.E. degli ultimi 30 giorni per una specifica località.
        """
        # Calcoliamo la data limite (es. 30 giorni prima dell'evento)
        try:
            ref_dt = datetime.fromisoformat(ref_date_str)
        except:
            ref_dt = datetime.now()

        start_date = (ref_dt - timedelta(days=WINDOW_DAYS)).isoformat()

        # Query veloce su JSON
        # Nota: Filtriamo solo eventi COMPLETED e con TIE valido
        query = """
            SELECT 
                json_extract(ai_report_json, '$.scores.tie_score') as score,
                last_seen_date
            FROM unique_events
            WHERE json_extract(ai_report_json, '$.tactics.geo_location.inferred.toponym_raw') = ?
            AND last_seen_date BETWEEN ? AND ?
            AND ai_analysis_status = 'COMPLETED'
        """
        cursor.execute(query, (location_name, start_date, ref_date_str))
        rows = cursor.fetchall()

        # Restituisce lista di (score, date) ignorando i null
        return [(r['score'], r['last_seen_date']) for r in rows if r['score'] is not None]

    def calculate_statistics(self, current_score, history_data):
        """
        Il cuore matematico del Layer 2.
        Calcola Z-Score (Picco) e Drift (Tendenza).
        """
        if len(history_data) < MIN_EVENTS_HISTORY:
            return {
                "status": "INSUFFICIENT_DATA",
                "z_score": 0.0,
                "baseline_mean": 0.0,
                "drift_pct": 0.0,
                "description": "Not enough history for this location."
            }

        scores = [h[0] for h in history_data]

        # 1. Calcolo Statistiche Base
        mean = np.mean(scores)
        std_dev = np.std(scores)

        # Evitiamo divisione per zero se la varianza è nulla (tutti eventi uguali)
        if std_dev < 1:
            std_dev = 1

        # 2. Z-SCORE (Quanto è anomalo l'evento di OGGI?)
        #
        z_score = (current_score - mean) / std_dev

        # 3. DRIFT ANALYSIS (La situazione sta peggiorando?)
        # Dividiamo la storia in due metà: Vecchia (15-30gg fa) vs Recente (0-14gg fa)
        mid_point = len(scores) // 2
        old_slice = scores[:mid_point]
        recent_slice = scores[mid_point:]

        avg_old = np.mean(old_slice) if old_slice else 0
        avg_recent = np.mean(recent_slice) if recent_slice else 0

        drift_pct = 0
        if avg_old > 0:
            drift_pct = ((avg_recent - avg_old) / avg_old) * 100

        # 4. Assegnazione Etichetta (Status)
        status = "ROUTINE"
        if z_score >= 3.0:
            status = "CRITICAL_OUTLIER"  # Evento eccezionale (es. 99%)
        elif z_score >= 2.0:
            status = "SIGNIFICANT_SPIKE"  # Evento molto forte (es. 95%)
        elif z_score >= 1.0:
            status = "ELEVATED"          # Sopra la media

        return {
            "status": status,
            "z_score": round(float(z_score), 2),
            "baseline_mean": round(float(mean), 2),
            "drift_pct": round(float(drift_pct), 1),
            "history_count": len(scores),
            "description": f"Event is {z_score} deviations from norm. Trend: {drift_pct:+.1f}%"
        }

    def run_cycle(self):
        conn = self.get_db_connection()
        cursor = conn.cursor()

        # 1. Troviamo eventi processati dal Layer 1 ma SENZA analisi statistica
        print("🔍 Scanning for un-analyzed events...")

        # Cerchiamo eventi che hanno 'tie_score' ma NON hanno ancora 'statistical_context'
        cursor.execute("""
            SELECT event_id, ai_report_json, last_seen_date 
            FROM unique_events 
            WHERE ai_analysis_status = 'COMPLETED'
            AND json_extract(ai_report_json, '$.context_analysis') IS NULL
            ORDER BY last_seen_date DESC
            LIMIT 50
        """)

        rows = cursor.fetchall()
        print(f"📊 Found {len(rows)} events needing Contextual Analysis.")

        for row in rows:
            try:
                event_id = row['event_id']
                data = json.loads(row['ai_report_json'])

                # Estrazione Dati Layer 1
                tie_score = data.get('scores', {}).get('tie_score', 0)
                # Fallback location: Inference -> Explicit -> Unknown
                location = data.get('tactics', {}).get(
                    'geo_location', {}).get('inferred', {}).get('toponym_raw')

                if not location:
                    print(f"   ⚠️ Skipping {event_id}: No location found.")
                    continue

                # 2. Recupero Storia
                history = self.get_location_history(
                    cursor, location, row['last_seen_date'])

                # 3. Calcolo Statistiche
                stats = self.calculate_statistics(tie_score, history)

                # 4. Aggiornamento JSON (Aggiungiamo il blocco Layer 2)
                data['context_analysis'] = stats

                # Salvataggio nel DB
                cursor.execute("""
                    UPDATE unique_events 
                    SET ai_report_json = ? 
                    WHERE event_id = ?
                """, (json.dumps(data, ensure_ascii=False), event_id))

                conn.commit()

                # Log Console Carino
                icon = "🟢"
                if stats['status'] == "ELEVATED":
                    icon = "🟡"
                if stats['status'] == "SIGNIFICANT_SPIKE":
                    icon = "🟠"
                if stats['status'] == "CRITICAL_OUTLIER":
                    icon = "🔴"

                print(
                    f"   {icon} {location.ljust(15)} | TIE: {tie_score} | Z: {stats['z_score']} ({stats['status']})")

            except Exception as e:
                print(f"   ❌ Error analyzing {event_id}: {e}")

        conn.close()
        print("✅ Layer 2 Cycle Complete.")


if __name__ == "__main__":
    analyst = StrategicAnalyst()
    analyst.run_cycle()
