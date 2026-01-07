import json
import re
import os
from openai import OpenAI
from dotenv import load_dotenv

# Carica le variabili d'ambiente (.env)
load_dotenv()


class TitanIntelligenceNode:
    def __init__(self, model_id):
        """
        model_id: L'ID del tuo modello fine-tuned su OpenAI (inizia con 'ft:...')
        """
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("❌ OPENAI_API_KEY mancante nel file .env")

        print(f"☁️ [TITAN] Connessione a OpenAI. Modello: {model_id}...")
        self.client = OpenAI(api_key=api_key)
        self.model_id = model_id

        # --- MODIFICA QUI: IL NUOVO SYSTEM PROMPT ---
        # Questo prompt risolve i problemi di ATTRITION vs MANOUVRE e filtra i digest.
        self.system_prompt = """You are a military intelligence analyst. Output strict JSON.

CRITICAL CLASSIFICATION RULES:
1. NOISE FILTER: If the text is a summary (e.g., "Daily Digest", "Weekly Map"), historical analysis, political opinion, or describes static maps without specific new kinetic events, classify as NULL.
2. MANOUVRE PRIORITY: If the text mentions ANY territorial change (e.g., "captured", "liberated", "advanced", "retreated", "abandoned", "flag raised", "entered the village"), classify as MANOUVRE. This takes precedence over ATTRITION.
3. SHAPING PRIORITY: If the text describes strikes on deep rear targets, capital cities, critical infrastructure (energy, ports, bridges), or logistics (ammo, fuel), classify as SHAPING (OFFENSIVE or COERCIVE), not ATTRITION.
4. ATTRITION: Use this ONLY for static fighting, shelling, or tactical clashes where no territory changes hands and no strategic target is hit."""

    def analyze(self, raw_text):
        """
        Invia il testo a OpenAI e riceve il JSON classificato.
        """
        # 1. Prompt (Usa la variabile self.system_prompt definita sopra)
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": raw_text}
        ]

        try:
            # 2. Chiamata API
            response = self.client.chat.completions.create(
                model=self.model_id,
                messages=messages,
                temperature=0.0,  # Determinismo massimo
                max_tokens=512,
                # Forza il JSON mode (fondamentale per evitare errori di sintassi)
                response_format={"type": "json_object"}
            )

            generated_text = response.choices[0].message.content.strip()

            # 3. Parsing e Validazione
            try:
                # Pulizia opzionale nel caso il modello aggiunga markdown
                if "```json" in generated_text:
                    generated_text = generated_text.split(
                        "```json")[1].split("```")[0].strip()
                elif "```" in generated_text:
                    generated_text = generated_text.split(
                        "```")[1].split("```")[0].strip()

                data = json.loads(generated_text)

                # Normalizzazione Maiuscola
                if "classification" in data:
                    data["classification"] = data["classification"].upper()

                return data

            except json.JSONDecodeError:
                # Tentativo disperato di riparazione (se manca la graffa finale)
                if not generated_text.endswith("}"):
                    generated_text += "}"
                    return json.loads(generated_text)
                raise

        except Exception as e:
            print(f"❌ [TITAN API ERROR] {e}")
            print(
                f"   Raw Output: {locals().get('generated_text', 'No output')}")

            return {
                "classification": "NULL",
                "target_type": "none",
                "reasoning": f"API Error: {str(e)}",
                "confidence": 0.0
            }
