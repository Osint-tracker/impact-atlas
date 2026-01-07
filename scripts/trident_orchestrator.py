from layer1_sensor import TitanSensor                # La Calcolatrice
from ai_inference_node import TitanIntelligenceNode  # Il Cervello
import sqlite3
import json
import sys
import os
import time

# Setup Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# --- NUOVI IMPORT ---

# Configurazione
# --- CONFIGURAZIONE PATH ROBUSTA ---
# 1. Trova la cartella dove si trova QUESTO script (cioè /scripts)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Risale di un livello per trovare la root del progetto (osint-tracker)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)

# 3. Costruisce il percorso assoluto verso il DB
# os.path.join gestisce automaticamente i backslash/slash per Windows/Linux
DB_PATH = os.path.join(PROJECT_ROOT, 'war_tracker_v2', 'data', 'raw_events.db')

# Debug: Stampa il percorso per essere sicuri
print(f"📂 DATABASE PATH: {DB_PATH}")

# Verifica che il DB esista
if not os.path.exists(DB_PATH):
    print("❌ ATTENZIONE: Il file del database non esiste nel percorso calcolato!")
    print("   Controlla di aver creato la cartella 'war_tracker_v2/data'")

OPENAI_FT_MODEL_ID = "ft:gpt-4o-mini-2024-07-18:personal:osint-analyst-v4-clean:Cv5yHxTJ"
MODEL_PATH = OPENAI_FT_MODEL_ID


class NewTridentOrchestrator:
    def __init__(self):
        print("🚀 INIT: TRIDENT 2.0 (OpenAI Cloud Engine)")

        # 1. Inizializza il client OpenAI con l'ID del modello
        self.titan = TitanIntelligenceNode(OPENAI_FT_MODEL_ID)

        # 2. Carica la Calcolatrice (Locale)
        self.sensor = TitanSensor()

    def get_pending_events(self, limit=50):
        """Recupera eventi non ancora processati dall'AI"""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        try:
            query = "SELECT rowid, text_content FROM raw_signals WHERE ai_processed = 0 ORDER BY rowid DESC LIMIT ?"
            cursor.execute(query, (limit,))
        except sqlite3.OperationalError:
            # Fallback se la migrazione non è stata fatta
            print("⚠️ Colonna 'ai_processed' non trovata. Analizzo gli ultimi arrivi.")
            query = "SELECT rowid, text_content FROM raw_signals ORDER BY rowid DESC LIMIT ?"
            cursor.execute(query, (limit,))

        events = cursor.fetchall()
        conn.close()
        return events

    def save_intelligence(self, row_id, ai_data, metrics, layer):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        query = """
            UPDATE raw_signals SET 
                classification = ?,
                confidence = ?,
                reasoning = ?,
                tie_score = ?,
                visibility_layer = ?,
                ai_processed = 1
            WHERE rowid = ?
        """
        cursor.execute(query, (
            ai_data.get('classification', 'NULL'),
            ai_data.get('confidence', 0.0),
            ai_data.get('reasoning', ''),
            metrics.get('tie_score', 0),
            layer,
            row_id
        ))
        conn.commit()
        conn.close()

    def run_pipeline(self):
        # Facciamo 20 alla volta per non bruciare crediti API
        events = self.get_pending_events(limit=2000)

        for row_id, text in events:
            if not text:
                continue

            print(f"\n📍 Event ID {row_id}: {text[:50]}...")

            # STEP 1: AI (OpenAI Cloud)
            ai_result = self.titan.analyze(text)

            classification = ai_result.get('classification', 'NULL')
            confidence = ai_result.get('confidence', 0.0)

            # STEP 2: METRICS (Locale)
            metrics = self.sensor.analyze_text(text)

            # STEP 3: STRATEGIST (Logica)
            layer = 3
            if confidence >= 0.85 and classification in ['MANOUVRE', 'SHAPING_OFFENSIVE', 'SHAPING_COERCIVE']:
                layer = 1
            elif confidence >= 0.65 and classification in ['ATTRITION', 'INCOHERENT_DISARRAY', 'HYBRID_PRESSURE']:
                layer = 2

            print(f"   🤖 Titan: {classification} ({confidence})")
            print(f"   👁️  Layer: {layer}")

            # STEP 4: SAVE
            self.save_intelligence(row_id, ai_result, metrics, layer)

            # Importante: piccolo sleep per evitare Rate Limit errors di OpenAI
            time.sleep(0.5)


if __name__ == "__main__":
    orchestrator = NewTridentOrchestrator()
    orchestrator.run_pipeline()
