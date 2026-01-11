import sqlite3
import json
import os
import time
from openai import OpenAI
from dotenv import load_dotenv
from tqdm import tqdm  # Devi installarlo: pip install tqdm

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_embedding(text):
    try:
        # Pulizia e taglio per risparmiare token
        safe_text = text.replace("\n", " ")[:4000]
        response = client.embeddings.create(
            input=[safe_text], model="text-embedding-3-small"
        )
        return response.data[0].embedding
    except Exception as e:
        return None


def main():
    print(f"ANALISI VETTORI MANCANTI...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Conta quanti mancano
    cursor.execute(
        "SELECT count(*) FROM unique_events WHERE embedding_vector IS NULL AND full_text_dossier IS NOT NULL")
    missing_count = cursor.fetchone()[0]

    if missing_count == 0:
        print("Tutti gli eventi hanno già i vettori! Puoi passare alla fase 2.")
        return

    print(
        f"Trovati {missing_count} eventi senza vettori. Generazione in corso...")

    # Carica ID e Testo di quelli mancanti
    cursor.execute(
        "SELECT event_id, full_text_dossier FROM unique_events WHERE embedding_vector IS NULL AND full_text_dossier IS NOT NULL")
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
