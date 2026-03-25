import sqlite3
import json
import os
import time
import requests
from dotenv import load_dotenv
from tqdm import tqdm 

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../data/raw_events.db')
load_dotenv()

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

def get_embedding(text):
    if not OPENROUTER_API_KEY:
        print("ERROR: OPENROUTER_API_KEY missing in .env")
        return None
    try:
        # Qwen3-8B has a larger context window, we can just replace newlines
        safe_text = text.replace("\n", " ")
        headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/Osint-tracker/impact-atlas",
            "X-Title": "Impact Atlas Refiner"
        }
        payload = {
            "model": "qwen/qwen3-embedding-8b",
            "input": [safe_text]
        }
        
        resp = requests.post("https://openrouter.ai/api/v1/embeddings", headers=headers, json=payload, timeout=60)
        
        if resp.status_code == 200:
            data = resp.json()
            return data["data"][0]["embedding"]
        else:
            print(f"API Error {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"Request failed: {e}")
        return None


def main():
    print(f"ANALISI VETTORI MANCANTI...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Conta quanti mancano
    cursor.execute(
        "SELECT count(*) FROM unique_events WHERE embedding_vector IS NULL AND full_text_dossier IS NOT NULL AND ai_analysis_status = 'COMPLETED'")
    missing_count = cursor.fetchone()[0]

    if missing_count == 0:
        print("Tutti gli eventi hanno già i vettori! Puoi passare alla fase 2.")
        return

    print(
        f"Trovati {missing_count} eventi senza vettori. Generazione in corso...")

    # Carica ID e Testo di quelli mancanti
    cursor.execute(
        "SELECT event_id, full_text_dossier FROM unique_events WHERE embedding_vector IS NULL AND full_text_dossier IS NOT NULL AND ai_analysis_status = 'COMPLETED'")
    rows = cursor.fetchall()

    updated = 0
    # Usa TQDM per la barra di caricamento
    for row in tqdm(rows, desc="Generating Embeddings", unit="evt"):
        evt_id = row[0]
        text = row[1]

        vec = get_embedding(text)
        if vec:
            cursor.execute("UPDATE unique_events SET embedding_vector = ? WHERE event_id = ?",
                           (json.dumps(vec), evt_id))
            updated += 1

            # Commit ogni 50 per sicurezza
            if updated % 50 == 0:
                conn.commit()

    conn.commit()
    conn.close()
    print(
        f"\nFINITO. Generati {updated} vettori. Ora lo Smart Fusion volerà.")


if __name__ == "__main__":
    main()
