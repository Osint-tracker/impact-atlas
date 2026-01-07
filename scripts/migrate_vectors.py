import sqlite3
import json
import os
import chromadb
import numpy as np  # Assicurati di avere numpy importato
from tqdm import tqdm

# CONFIGURAZIONE
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Percorsi basati sulla tua struttura
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
CHROMA_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/chroma_store')


def to_list(obj):
    """Converte array NumPy in liste Python standard per JSON serialization"""
    if hasattr(obj, 'tolist'):
        return obj.tolist()
    return obj


def main():
    print(f"🚛 AVVIO MIGRAZIONE VETTORI (ChromaDB -> SQLite) - FIX NUMPY...")

    if not os.path.exists(CHROMA_PATH):
        print(f"❌ Errore: Cartella Chroma non trovata in {CHROMA_PATH}")
        return

    # 1. Connessione ai DB
    print("🔌 Connessione ai database...")
    chroma_client = chromadb.PersistentClient(path=CHROMA_PATH)
    try:
        # Usa il nome della collezione che hai nel refiner.py ("war_events_v2")
        collection = chroma_client.get_collection(name="war_events_v2")
        count = collection.count()
        print(f"📦 Trovati {count} vettori in ChromaDB.")
    except Exception as e:
        print(f"❌ Errore apertura collezione Chroma: {e}")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 2. Carichiamo gli eventi SQLite che hanno bisogno di vettori
    # Creiamo una mappa URL -> ID per fare il match veloce se l'ID diretto fallisce
    print("🗺️  Mappatura eventi SQLite (URL Indexing)...")
    cursor.execute(
        "SELECT event_id, urls_list FROM unique_events WHERE embedding_vector IS NULL")
    sqlite_rows = cursor.fetchall()

    # Mappa veloce: URL -> event_id
    url_map = {}
    ids_needing_vectors = set()

    for row in sqlite_rows:
        evt_id = row[0]
        ids_needing_vectors.add(evt_id)
        raw_urls = row[1]

        # Parsing URLs
        try:
            if raw_urls:
                import ast
                urls = ast.literal_eval(
                    raw_urls) if raw_urls.startswith('[') else [raw_urls]
                for u in urls:
                    if u and len(u) > 10:
                        url_map[u.strip().lower()] = evt_id
        except:
            pass

    print(f"🎯 Target: {len(sqlite_rows)} eventi SQLite senza vettore.")
    print(f"🔗 Indice URL creato: {len(url_map)} link mappati.")

    if len(sqlite_rows) == 0:
        print("✅ Nessun evento ha bisogno di migrazione. Tutto pronto!")
        return

    # 3. Estrazione Batch da Chroma e Aggiornamento
    BATCH_SIZE = 2000
    migrated_count = 0

    # Calcolo totale batch
    total_batches = (count // BATCH_SIZE) + 1

    for i in tqdm(range(total_batches), desc="Migrating", unit="batch"):
        # Fetch dati da Chroma
        results = collection.get(
            include=['embeddings', 'metadatas'],
            limit=BATCH_SIZE,
            offset=i * BATCH_SIZE
        )

        if not results['ids']:
            break

        updates = []  # Lista di tuple (vector_json, event_id)

        for idx, chroma_id in enumerate(results['ids']):
            embedding = results['embeddings'][idx]
            metadata = results['metadatas'][idx] or {}

            target_id = None

            # TENTATIVO 1: Match diretto per ID
            if chroma_id in ids_needing_vectors:
                target_id = chroma_id

            # TENTATIVO 2: Match per URL (se ID fallisce)
            if not target_id:
                chroma_url = metadata.get('url', '').strip().lower()
                if chroma_url in url_map:
                    target_id = url_map[chroma_url]

            # Se abbiamo trovato una corrispondenza, prepariamo l'update
            if target_id:
                # --- FIX QUI: Convertiamo NumPy Array in Lista ---
                vector_list = to_list(embedding)
                updates.append((json.dumps(vector_list), target_id))

        # Scrittura su SQLite
        if updates:
            cursor.executemany(
                "UPDATE unique_events SET embedding_vector = ? WHERE event_id = ?", updates)
            conn.commit()
            migrated_count += len(updates)

    conn.close()
    print(f"\n✅ MIGRAZIONE COMPLETATA.")
    print(f"💰 Vettori recuperati e salvati: {migrated_count}")

    if migrated_count > 0:
        print("👉 ORA puoi lanciare 'smart_fusion.py' (la versione Rolling Window)!")
    else:
        print("⚠️ Nessun match trovato. Gli ID o gli URL tra Chroma e SQLite sono completamente diversi.")


if __name__ == "__main__":
    main()
