import sqlite3
import json
import os
import time
from openai import OpenAI
from dotenv import load_dotenv
from tqdm import tqdm

# Path Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Assumes structure: osint-tracker/scripts/generate_vectors.py
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

load_dotenv()

# Initialize OpenAI
# Note: Ensure OPENAI_API_KEY is in .env
try:
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
except Exception as e:
    print(f"Error initializing OpenAI client: {e}")
    client = None

def get_embedding(text):
    if not client:
        return None
    try:
        # Truncate to avoid token limits (8191 tokens max for text-embedding-3-small)
        # 4000 chars is safe (~1000 tokens)
        safe_text = text.replace("\n", " ")[:6000]
        response = client.embeddings.create(
            input=[safe_text], 
            model="text-embedding-3-small"
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"Embedding API Error: {e}")
        return None

def main():
    print("ANALYZING MISSING VECTORS...")
    
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return

    # Set timeout to 60s to wait for locks to clear
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    # Enable WAL mode for better concurrency
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    # 1. Count Missing
    try:
        cursor.execute(
            "SELECT count(*) FROM unique_events WHERE embedding_vector IS NULL AND full_text_dossier IS NOT NULL"
        )
        missing_count = cursor.fetchone()[0]
    except sqlite3.OperationalError as e:
        print(f"Error accessing DB: {e}")
        return

    if missing_count == 0:
        print("All events have vectors! Ready for Smart Fusion.")
        return

    print(f"Found {missing_count} events without vectors. Starting Generation...")

    # 2. Fetch Candidates
    cursor.execute(
        "SELECT event_id, full_text_dossier FROM unique_events WHERE embedding_vector IS NULL AND full_text_dossier IS NOT NULL"
    )
    rows = cursor.fetchall()

    updated = 0
    errors = 0
    
    print("Starting processing loop (Ctrl+C to stop)...")
    
    for row in tqdm(rows, desc="Generating Embeddings", unit="evt"):
        evt_id = row[0]
        text = row[1]
        
        if not text:
            continue

        vec = get_embedding(text)
        if vec:
            # Retry loop for DB lock
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    cursor.execute(
                        "UPDATE unique_events SET embedding_vector = ? WHERE event_id = ?",
                        (json.dumps(vec), evt_id)
                    )
                    # Commit every 50 to keep transactions short
                    if updated > 0 and updated % 50 == 0:
                        conn.commit()
                    break # Success
                except sqlite3.OperationalError as e:
                    if "locked" in str(e) and attempt < max_retries - 1:
                        time.sleep(0.5 * (attempt + 1)) # Backoff
                        continue
                    else:
                        print(f"Failed to update {evt_id}: {e}")
                        errors += 1
                        break
            updated += 1
        else:
            errors += 1

    try:
        conn.commit()
    except Exception as e:
        print(f"Final commit failed: {e}")
        
    conn.close()
    print(f"\nFINISHED. Generated {updated} vectors. Errors: {errors}.")

if __name__ == "__main__":
    main()
