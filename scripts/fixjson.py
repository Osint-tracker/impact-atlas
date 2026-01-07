import json
import os
import time
from typing import Dict, Any, Optional
from openai import OpenAI
from dotenv import load_dotenv

# Carica variabili d'ambiente
load_dotenv()

# CONFIGURAZIONE FILE
# Assicurati che il percorso sia corretto e usa r'' per Windows
INPUT_FILE = r'scripts/old_dataset.jsonl'
OUTPUT_FILE = 'training_dataset_final.jsonl'
OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY")


class UpgradeDatasetAgent:
    def __init__(self):
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=OPENROUTER_KEY,
        )
        # Usiamo DeepSeek R1 per standardizzare il ragionamento
        self.teacher_model = "deepseek/deepseek-r1"

    def _get_teacher_reasoning(self, text: str) -> str:
        """
        Prompt che forza la creazione del JSON standardizzato e sintetico.
        """
        prompt = f"""
ROLE: Senior Intelligence Analyst.
TASK: Analyze the provided EVENT TEXT and generate a strict JSON classification.

--- INPUT DATA ---
EVENT TEXT: "{text}"

--- SYSTEM DEFINITIONS (STRICT) ---
1. ATTRITION: Routine exchanges, static shelling, no strategic targets involved.
2. SHAPING_OFFENSIVE: Strikes on offensive logistics (ammo, fuel, command, bridges). Preparing for maneuver.
3. SHAPING_COERCIVE: Strikes on civilian infrastructure (grid, food, malls). Psychological pressure.
4. MANOUVRE: Significant troop movement, territorial gains, encirclements.
5. INCOHERENT_DISARRAY: Contradictory signals, technical failures, panic, mutiny.
6. NULL: Spam, ads, crypto, politics, history, or text unrelated to active conflict.

--- OUTPUT SCHEMA (JSON ONLY) ---
Return ONLY a valid JSON object.
{{
  "classification": "ENUM (ATTRITION, SHAPING_OFFENSIVE, SHAPING_COERCIVE, MANOUVRE, INCOHERENT_DISARRAY, NULL)",
  "target_type": "STRING (e.g. 'ammo_depot', 'bridge', 'civilian_grid', 'none')",
  "reasoning": "STRING (Format: 'Target: [X] | Action: [Y] -> Rule: [Z] matches [CLASS]')",
  "confidence": FLOAT (0.0 - 1.0)
}}
"""
        try:
            response = self.client.chat.completions.create(
                model=self.teacher_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                extra_headers={
                    "HTTP-Referer": "https://osint-tracker.local", "X-Title": "DatasetStandardizer"}
            )

            content = response.choices[0].message.content
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()

            return content
        except Exception as e:
            print(f"❌ [API ERROR] {e}")
            return ""

    def save_upgraded_entry(self, original_text, verdict_json):
        # Salviamo nel formato 'messages' pronto per Llama 3 / Qwen
        training_entry = {
            "messages": [
                {"role": "system", "content": "You are a military intelligence analyst. Output strict JSON."},
                {"role": "user", "content": original_text},
                {"role": "assistant", "content": json.dumps(verdict_json)}
            ]
        }

        with open(OUTPUT_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(training_entry, ensure_ascii=False) + "\n")

    def run_upgrade(self):
        if not os.path.exists(INPUT_FILE):
            print(f"❌ File {INPUT_FILE} non trovato.")
            return

        print(
            f"🚀 STARTING STANDARDIZATION\nReading: {INPUT_FILE}\nWriting: {OUTPUT_FILE}\n" + "="*50)

        processed = 0
        skipped = 0

        with open(INPUT_FILE, 'r', encoding='utf-8') as infile:
            lines = infile.readlines()
            total = len(lines)

            for i, line in enumerate(lines):
                if not line.strip():
                    continue

                try:
                    data = json.loads(line)

                    # LOGICA DI ESTRAZIONE MODIFICATA PER IL TUO FORMATO
                    # Cerca la chiave "text" (come nel campione che hai incollato)
                    user_text = data.get("text", "")

                    # Fallback se il formato fosse diverso in alcune righe
                    if not user_text and "messages" in data:
                        for msg in data["messages"]:
                            if msg["role"] == "user":
                                user_text = msg["content"]
                                break

                    if not user_text:
                        print(
                            f"⚠️ Line {i+1}: No text found. Keys: {list(data.keys())}")
                        skipped += 1
                        continue

                    print(
                        f"\n🔹 [{i+1}/{total}] Processing: {user_text[:40]}...")

                    # 1. Generazione
                    print("   🧠 Standardizing...", end="\r")
                    json_response_str = self._get_teacher_reasoning(user_text)

                    if not json_response_str:
                        continue

                    # 2. Parsing
                    verdict_data = json.loads(json_response_str)

                    # Normalizza maiuscolo
                    if 'classification' in verdict_data:
                        verdict_data['classification'] = verdict_data['classification'].upper(
                        )
                        print(
                            f"   ✅ Verdict: {verdict_data['classification']}")

                    # 3. Salvataggio
                    self.save_upgraded_entry(user_text, verdict_data)
                    processed += 1

                    # Pausa anti-spam
                    time.sleep(0.3)

                except json.JSONDecodeError:
                    print(f"   ❌ JSON Error on line {i+1}")
                except Exception as e:
                    print(f"   ❌ Error: {e}")

        print(f"\n🎓 DONE. Processed: {processed}, Skipped: {skipped}")


if __name__ == "__main__":
    if not OPENROUTER_KEY:
        print("❌ Error: OPENROUTER_API_KEY environment variable is missing.")
    else:
        agent = UpgradeDatasetAgent()
        agent.run_upgrade()
