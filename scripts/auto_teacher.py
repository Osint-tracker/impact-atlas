import sqlite3
import json
import os
import time
from typing import Dict, Any, Optional
from openai import OpenAI
from dotenv import load_dotenv

# Carica variabili d'ambiente
load_dotenv()

# CONFIGURAZIONE
DB_PATH = 'osint_tracker.db'
# Salviamo i dati nuovi in un file separato per non mischiarli con quelli vecchi
LOG_FILE = os.path.join(os.path.dirname(__file__), 'training_dataset.jsonl')
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY")


class AutoTeacherAgent:
    def __init__(self):
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=OPENROUTER_KEY,
        )
        self.teacher_model = "deepseek/deepseek-r1"

    def _get_teacher_reasoning(self, text: str, predicted_phase: str, targets: list, signals_context: dict) -> str:
        """
        DeepSeek R1 agisce come generatore di dati sintetici.
        Output atteso: Una stringa JSON valida.
        """
        # Formattiamo i segnali
        signals_text = ", ".join(
            [k for k, v in signals_context.items() if v]) if signals_context else "None"
        targets_text = ", ".join(targets) if targets else "None"

        prompt = f"""
ROLE: You are a Senior Intelligence Analyst acting as a Data Generator for a specialized AI.
TASK: Analyze the provided Event Text and metadata to generate a "Ground Truth" classification. You must validate the input data, ignore hallucinations, and output a strictly formatted training example.

--- INPUT DATA PROVIDED TO YOU ---
EVENT TEXT: "{text}"
DETECTED TARGETS: {targets_text}
CURRENT CLASSIFICATION: {predicted_phase}

--- SYSTEM DEFINITIONS (STRICT) ---
1. ATTRITION: Routine exchanges, static shelling, no strategic targets involved.
2. SHAPING_OFFENSIVE: Strikes on offensive logistics (ammo, fuel, command, bridges). Preparing for maneuver.
3. SHAPING_COERCIVE: Strikes on civilian infrastructure (grid, food, gov buildings). Psychological pressure.
4. HYBRID_PRESSURE: Simultaneous strikes on BOTH military and civilian targets.
5. INCOHERENT_DISARRAY: Contradictory signals, failure sequences, panic, or non-kinetic/irrelevant confusion.
6. NULL: Spam, ads, crypto, or text unrelated to the war/conflict.

--- REASONING PROTOCOL ---
1. **Verification:** Check if `DETECTED TARGETS` actually exist in `EVENT TEXT`. If the text says "Bridge" but targets say "Ammo Depot", ignore the targets. Trust the Text.
2. **Logic Application:** Map the *verified* target/action to the Definitions above.
3. **Distillation:** Compress your logic into a single, dry sentence.

--- OUTPUT SCHEMA (JSON ONLY) ---
Return ONLY a valid JSON object. Do not use Markdown blocks.

{{
  "classification": "ENUM (One of the 6 classes above)",
  "target_type": "STRING (e.g. 'ammo_depot', 'bridge', 'civilian_grid', 'none')",
  "reasoning": "STRING (Strict format: 'Target: [X] | Action: [Y] -> Rule: [Z] matches [CLASS]')",
  "confidence": FLOAT (0.0 - 1.0)
}}
"""

        try:
            response = self.client.chat.completions.create(
                model=self.teacher_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,  # Temperatura bassa per rigore JSON
                extra_headers={
                    "HTTP-Referer": "https://osint-tracker.local", "X-Title": "OsintTracker"}
            )
            # Pulizia brutale per estrarre solo il JSON se il modello chiacchiera
            content = response.choices[0].message.content
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()

            return content
        except Exception as e:
            print(f"❌ [TEACHER ERROR] {e}")
            return ""

    def save_gold_data(self, event_id, text, verdict_json):
        """
        Salva direttamente nel formato training per Qwen/Llama (JSONL Chat Format).
        """
        # Creiamo l'entry per il fine-tuning
        training_entry = {
            "messages": [
                {
                    "role": "system",
                    "content": "You are a military intelligence analyst. Output strict JSON."
                },
                {
                    "role": "user",
                    "content": text
                },
                {
                    "role": "assistant",
                    # Salviamo il JSON come stringa nell'output
                    "content": json.dumps(verdict_json)
                }
            ]
        }

        # Salviamo su file
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(training_entry, ensure_ascii=False) + "\n")

    def run_batch(self, limit=20):
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
        except sqlite3.Error as e:
            print(f"❌ DB Error: {e}")
            return

        cursor.execute("""
            SELECT id, predicted_phase, confidence_score, signals_context 
            FROM prediction_history 
            WHERE verification_status = 'PENDING'
            ORDER BY created_at DESC LIMIT ?
        """, (limit,))
        rows = cursor.fetchall()

        print(
            f"🚀 STARTING AUTO-TEACHER V2 (Direct Generation) ON {len(rows)} EVENTS\n" + "="*50)

        processed = 0

        for row in rows:
            pred_id, current_phase, conf, context_raw = row
            try:
                full_context = json.loads(context_raw)
                text_snippet = full_context.get('text_snippet', '')
                targets = full_context.get('targets', [])
                signals = full_context.get('signals', {})
            except:
                text_snippet = ""
                targets = []
                signals = {}

            if not text_snippet:
                continue

            print(f"\n🔹 ID: {pred_id}")

            # 1. GENERAZIONE DIRETTA (DeepSeek fa tutto)
            print("   🧠 Teacher Thinking...", end="\r")
            json_response_str = self._get_teacher_reasoning(
                text_snippet, current_phase, targets, signals)

            if not json_response_str:
                print("   ❌ Empty Response.")
                continue

            try:
                # Parsiamo la stringa per assicurarci che sia JSON valido
                verdict_data = json.loads(json_response_str)

                # Validazione campi minimi
                if 'classification' not in verdict_data or 'reasoning' not in verdict_data:
                    print("   ⚠️ Invalid JSON structure. Skip.")
                    continue

                # Normalizzazione Classe (Maiuscolo)
                final_class = verdict_data['classification'].upper()
                verdict_data['classification'] = final_class

                print(
                    f"   ✅ Generated: {final_class} | Conf: {verdict_data.get('confidence', 0.0)}")

                # 2. SALVATAGGIO & AGGIORNAMENTO DB
                # Salviamo nel file per il training
                self.save_gold_data(pred_id, text_snippet, verdict_data)

                # Aggiorniamo il DB (segniamo come verificato)
                # Nota: Qui assumiamo che il Teacher abbia sempre ragione (Ground Truth)
                new_status = 'CONFIRMED'
                cursor.execute(
                    "UPDATE prediction_history SET verification_status = ? WHERE id = ?", (new_status, pred_id))

                processed += 1
                time.sleep(0.5)  # Piccolo delay per non spammare API

            except json.JSONDecodeError:
                print(f"   ❌ JSON Error: {json_response_str[:50]}...")
                continue

        conn.commit()
        conn.close()
        print(f"\n🎓 Completed. Processed: {processed}")


if __name__ == "__main__":
    if not OPENROUTER_KEY:
        print("❌ Error: OPENROUTER_API_KEY environment variable is missing.")
    else:
        bot = AutoTeacherAgent()
        bot.run_batch(limit=100)
