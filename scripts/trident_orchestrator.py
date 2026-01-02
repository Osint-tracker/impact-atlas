import sqlite3
import json
import sys
import os

# Setup Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from layer3_analyst import BehaviorAnalyst
except ImportError:
    print("❌ ERRORE: 'layer3_analyst.py' mancante.")
    sys.exit(1)

# DATABASE REALE
DB_PATH = os.path.join('war_tracker_v2', 'data', 'raw_events.db')


class TridentOrchestrator:
    def __init__(self):
        print("🕵️  INIT: TRIDENT SYSTEM (Live Data Mode)")
        # Usa il DB piccolo per la memoria previsionale
        self.analyst = BehaviorAnalyst('osint_tracker.db')

    def get_real_events(self, limit=1000):
        """Recupera gli eventi più recenti e rilevanti dal DB"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Prendiamo gli eventi più recenti che hanno un minimo di rilevanza (TIE > 10)
        # Assumiamo che la tabella abbia una colonna data, altrimenti ordiniamo per inserimento
        query = """
        SELECT event_hash, text_content, tie_score, k_metric, t_metric, e_metric 
        FROM raw_signals 
        WHERE tie_score > 10
        ORDER BY rowid DESC LIMIT ?
        """
        cursor.execute(query, (limit,))
        events = cursor.fetchall()
        conn.close()
        return events

    def extract_targets(self, text):
        """
        Micro-NLP per identificare i target nel testo reale.
        Serve al Layer 3 per distinguere Shaping OFFENSIVO vs COERCITIVO.
        """
        text = text.lower()
        targets = []

        # Target Offensivi (Militari/Logistici)
        if any(x in text for x in ['depot', 'ammo', 'fuel', 'bridge', 'command', 'barracks', 'troop']):
            targets.append('ammo_depot')

        # Target Coercitivi (Civili/Politici/Energia)
        if any(x in text for x in ['civilian', 'school', 'hospital', 'energy', 'grid', 'power', 'dam', 'gov']):
            targets.append('civilian_grid')

        return targets

    def determine_context_status(self, tie_score):
        """
        Simula il Layer 2 (Context) basandosi su una soglia.
        In futuro qui collegherai lo Z-Score reale.
        """
        if tie_score > 60:
            return "DRIFT"     # Situazione fuori controllo
        if tie_score > 40:
            return "SPIKE"     # Picco di attività
        return "ROUTINE"

    def run_pipeline(self):
        if not os.path.exists(DB_PATH):
            print(f"❌ Errore: Database {DB_PATH} non trovato.")
            return

        events = self.get_real_events(limit=1000)
        print(f"🔄 Analisi TRIDENT su {len(events)} eventi recenti...\n")

        for ev in events:
            evt_hash, text, tie, k, t, e = ev

            # Taglio del testo per visualizzazione
            short_text = (text[:60] + '...') if len(text) > 60 else text

            print(f"📍 EVENTO: {short_text}")

            # 1. INPUT DAL DB (Già calcolato dal Layer 1)
            # Mappiamo le metriche TITAN ai concetti astratti dell'Analista
            # K (Violenza) -> Focus (Intensità)
            # T (Tempo)    -> Tempo (Frequenza/Urgenza)
            # E (Effetto)  -> Depth (Profondità/Impatto)

            # 2. STATUS CONTESTUALE (Simulato/Calcolato)
            context_status = self.determine_context_status(tie)
            print(
                f"   📊 [METRICS] TIE: {tie:.1f} | Status: {context_status} (K:{k} T:{t} E:{e})")

            # 3. LAYER 3: Rilevamento Segnali
            signals = self.analyst.detect_signals(
                focus_metric=k, tempo_metric=t, depth_metric=e)
            if signals:
                print(f"   ⚠️  [SIGNALS]: {signals}")

            # 4. Estrazione Target (per logica O vs C)
            targets = self.extract_targets(text)

            # 5. LAYER 3: Classificazione Finale
            result = self.analyst.classify_phase(context_status, tie, targets)

            print(f"   🧠 [STRATEGY]: {result['phase']}")
            print(f"      Confidence: {int(result['confidence']*100)}%")

            if result['falsification_notes']:
                print(
                    f"      Falsification Warning: {result['falsification_notes']}")

            print("-" * 50)

            # 6. SAVE TO MEMORY
            full_context = {
                "signals": signals,
                "targets": targets,
                "metrics": {"k": k, "t": t, "e": e},
                "text_snippet": text[:200]
            }

            self.analyst.save_prediction(result, tie, full_context)

            print("-" * 50)


if __name__ == "__main__":
    system = TridentOrchestrator()
    system.run_pipeline()
