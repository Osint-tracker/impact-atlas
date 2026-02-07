"""
refiner_fast.py - High-Speed Embedding with GDELT Filter Bypass
================================================================
Based on refiner.py but with:
1. GDELT sources bypass the strict MANDATORY_SET filter
2. Batched API calls (50 texts per request)
3. Async concurrency (10 parallel requests)
4. Dry-run mode to compare old vs new filter logic
5. Resets rejected GDELT for reprocessing

The "Sanfilippo Method": Observe the delta before committing.
"""

import os
import sqlite3
import json
import asyncio
import aiohttp
import time
from datetime import datetime
from dotenv import load_dotenv
from tqdm import tqdm
import uuid
import argparse

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))  # Go up 2 levels to osint-tracker
DB_PATH = os.path.join(PROJECT_ROOT, "war_tracker_v2", "data", "raw_events.db")
ENV_PATH = os.path.join(PROJECT_ROOT, "war_tracker_v2", ".env")

# Debug
print(f"DB_PATH: {DB_PATH}")
print(f"DB exists: {os.path.exists(DB_PATH)}")

load_dotenv(ENV_PATH)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Processing parameters
EMBEDDING_MODEL = "text-embedding-3-small"
TEXTS_PER_API_CALL = 50      # Batch 50 texts per API request
CONCURRENT_REQUESTS = 10     # 10 parallel requests
DB_BATCH_SIZE = 500          # Fetch 500 from DB at a time
MIN_TEXT_LENGTH = 50         # Lowered to include shorter GDELT (was 150)

# Original strict filter (from refiner.py) - for comparison
MANDATORY_ANCHORS_STRICT = [
    "ukrain", "russia", "moscow", "kremlin", "kyiv", "kiev", "donetsk", "luhansk",
    "kharkiv", "kherson", "zaporizhzhia", "crimea", "mariupol", "bakhmut"
]

# New relaxed filter - accepts any war-related keyword
MANDATORY_ANCHORS_LOOSE = [
    "ukrain", "russia", "moscow", "kremlin", "kyiv", "kiev", "donetsk", "luhansk",
    "kharkiv", "kherson", "zaporizhzhia", "crimea", "mariupol", "bakhmut", "drone",
    "missile", "artillery", "tank", "military", "troops", "soldiers", "war", "attack",
    "strike", "offensive", "defense", "front", "battle", "rocket", "shell", "bomb",
    "infantry", "brigade", "regiment", "wagner", "azov", "himars", "patriot"
]


def is_relevant_strict(text, url):
    """Original strict filter logic."""
    content = ""
    if isinstance(text, str):
        content += text.lower()
    if isinstance(url, str):
        content += " " + url.lower()
    
    if not content.strip() or len(content) < MIN_TEXT_LENGTH:
        return False
    
    for anchor in MANDATORY_ANCHORS_STRICT:
        if anchor in content:
            return True
    return False


def is_relevant_loose(text, url, source_type):
    """New relaxed filter with GDELT bypass."""
    content = ""
    if isinstance(text, str):
        content += text.lower()
    if isinstance(url, str):
        content += " " + url.lower()
    
    if not content.strip() or len(content) < MIN_TEXT_LENGTH:
        return False
    
    # GDELT/WEB_NEWS: Use looser filter
    if source_type in ['GDELT', 'WEB_NEWS']:
        for anchor in MANDATORY_ANCHORS_LOOSE:
            if anchor in content:
                return True
        return False
    else:
        # Telegram: Keep original strict filter
        for anchor in MANDATORY_ANCHORS_STRICT:
            if anchor in content:
                return True
        return False


def dry_run_comparison(conn):
    """Compare old vs new filter on GDELT data. Shows delta."""
    print("\n" + "=" * 60)
    print("DRY RUN: Filter Comparison (Sanfilippo Self-Check)")
    print("=" * 60)
    
    cursor = conn.cursor()
    
    # Get all GDELT with content > MIN_TEXT_LENGTH
    cursor.execute("""
        SELECT event_hash, text_content, url, source_type, is_embedded
        FROM raw_signals 
        WHERE source_type = 'GDELT' 
        AND LENGTH(text_content) >= ?
        LIMIT 10000
    """, (MIN_TEXT_LENGTH,))
    
    rows = cursor.fetchall()
    print(f"Analyzing {len(rows)} GDELT records with content >= {MIN_TEXT_LENGTH} chars...")
    
    old_accept = 0
    old_reject = 0
    new_accept = 0
    new_reject = 0
    delta_rescued = 0  # Previously rejected, now accepted
    
    for event_hash, text, url, source_type, is_embedded in rows:
        old_result = is_relevant_strict(text, url)
        new_result = is_relevant_loose(text, url, source_type)
        
        if old_result:
            old_accept += 1
        else:
            old_reject += 1
        
        if new_result:
            new_accept += 1
        else:
            new_reject += 1
        
        if not old_result and new_result:
            delta_rescued += 1
    
    print(f"\n  OLD Filter (Strict):")
    print(f"    Accept: {old_accept} ({old_accept/len(rows)*100:.1f}%)")
    print(f"    Reject: {old_reject} ({old_reject/len(rows)*100:.1f}%)")
    
    print(f"\n  NEW Filter (Loose for GDELT):")
    print(f"    Accept: {new_accept} ({new_accept/len(rows)*100:.1f}%)")
    print(f"    Reject: {new_reject} ({new_reject/len(rows)*100:.1f}%)")
    
    print(f"\n  DELTA (Rescued by new filter): {delta_rescued}")
    
    if delta_rescued == 0:
        print("\n  ⚠️ WARNING: Delta is 0. Either:")
        print("     - All GDELT already passed old filter (unlikely)")
        print("     - Your GDELT content is still too short (check scraper)")
        print("     - Logic error in filter comparison")
    else:
        print(f"\n  ✅ New filter would rescue {delta_rescued} additional GDELT records.")
    
    return delta_rescued


async def generate_embeddings_async(session, texts, semaphore):
    """Generate embeddings for a batch of texts."""
    async with semaphore:
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # Truncate texts
        safe_texts = [str(t).replace("\n", " ")[:6000] for t in texts]
        
        payload = {
            "model": EMBEDDING_MODEL,
            "input": safe_texts
        }
        
        try:
            async with session.post(
                "https://api.openai.com/v1/embeddings",
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return [item["embedding"] for item in data["data"]]
                elif response.status == 429:
                    # Rate limited - wait and retry
                    await asyncio.sleep(5)
                    return await generate_embeddings_async(session, texts, semaphore)
                else:
                    error = await response.text()
                    print(f"API Error {response.status}: {error[:200]}")
                    return None
        except Exception as e:
            print(f"Request failed: {e}")
            return None


async def main_async(dry_run=False, skip_reset=False, limit=None):
    print("=" * 60)
    print("REFINER FAST - High-Speed Embedding with GDELT Fix")
    print("=" * 60)
    
    if not OPENAI_API_KEY:
        print("ERROR: OPENAI_API_KEY not found in .env")
        return
    
    # Connect to DB
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()
    
    # DRY RUN: Show filter comparison
    if dry_run:
        delta = dry_run_comparison(conn)
        conn.close()
        return
    
    # Step 1: Reset rejected GDELT for reprocessing
    if not skip_reset:
        print("\n[1/4] Resetting rejected GDELT sources...")
        cursor.execute("""
            UPDATE raw_signals 
            SET is_embedded = 0 
            WHERE source_type = 'GDELT' AND is_embedded = 2
        """)
        reset_count = cursor.rowcount
        conn.commit()
        print(f"      Reset {reset_count} GDELT records for reprocessing.")
    else:
        print("\n[1/4] Skipping reset (--skip-reset flag)")
    
    # Step 2: Count unprocessed with sufficient content
    cursor.execute("""
        SELECT COUNT(*) FROM raw_signals 
        WHERE is_embedded = 0 AND LENGTH(text_content) >= ?
    """, (MIN_TEXT_LENGTH,))
    total_unprocessed = cursor.fetchone()[0]
    print(f"\n[2/4] Found {total_unprocessed} unprocessed records with content >= {MIN_TEXT_LENGTH} chars.")
    
    if total_unprocessed == 0:
        print("Nothing to process!")
        conn.close()
        return
    
    if limit:
        total_unprocessed = min(total_unprocessed, limit)
        print(f"      Limiting to {total_unprocessed} for this run.")
    
    # Step 3: Process in batches with async
    print(f"\n[3/4] Processing with {CONCURRENT_REQUESTS} concurrent requests...")
    print(f"      Batch size: {TEXTS_PER_API_CALL} texts per API call")
    
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    total_embedded = 0
    total_skipped = 0
    
    async with aiohttp.ClientSession() as session:
        pbar = tqdm(total=total_unprocessed, desc="Embedding", unit="evt")
        
        processed_so_far = 0
        
        while processed_so_far < total_unprocessed:
            # Fetch batch from DB
            cursor.execute("""
                SELECT event_hash, source_type, text_content, url
                FROM raw_signals 
                WHERE is_embedded = 0 AND LENGTH(text_content) >= ?
                LIMIT ?
            """, (MIN_TEXT_LENGTH, DB_BATCH_SIZE))
            rows = cursor.fetchall()
            
            if not rows:
                break
            
            # Filter relevant rows (using new logic)
            relevant = []
            skipped_hashes = []
            
            for event_hash, source_type, text, url in rows:
                if is_relevant_loose(text, url, source_type):
                    relevant.append((event_hash, text))
                else:
                    skipped_hashes.append(event_hash)
            
            # Mark skipped as rejected (is_embedded=2)
            for h in skipped_hashes:
                cursor.execute("UPDATE raw_signals SET is_embedded = 2 WHERE event_hash = ?", (h,))
            total_skipped += len(skipped_hashes)
            
            # Process relevant in API batches
            for i in range(0, len(relevant), TEXTS_PER_API_CALL):
                chunk = relevant[i:i + TEXTS_PER_API_CALL]
                chunk_hashes = [r[0] for r in chunk]
                chunk_texts = [r[1] for r in chunk]
                
                embeddings = await generate_embeddings_async(session, chunk_texts, semaphore)
                
                if embeddings:
                    for j, emb in enumerate(embeddings):
                        cursor.execute("""
                            UPDATE raw_signals 
                            SET embedding_vector = ?, is_embedded = 1, cluster_id = ?
                            WHERE event_hash = ?
                        """, (json.dumps(emb), str(uuid.uuid4()), chunk_hashes[j]))
                    total_embedded += len(embeddings)
            
            conn.commit()
            processed_so_far += len(rows)
            pbar.update(len(rows))
        
        pbar.close()
    
    # Step 4: Summary
    print(f"\n[4/4] Complete!")
    print(f"      Embedded: {total_embedded}")
    print(f"      Skipped:  {total_skipped}")
    
    # Verify
    cursor.execute("""
        SELECT source_type, COUNT(*) FROM raw_signals 
        WHERE is_embedded = 1 
        GROUP BY source_type
    """)
    print("\n      Embeddings by source:")
    for row in cursor.fetchall():
        print(f"        {row[0]}: {row[1]}")
    
    conn.close()
    print("\nNext step: Run event_builder.py to create unique_events.")


def main():
    parser = argparse.ArgumentParser(description="Fast Refiner with GDELT Fix")
    parser.add_argument("--dry-run", action="store_true", 
                        help="Compare old vs new filter logic without making changes")
    parser.add_argument("--skip-reset", action="store_true",
                        help="Skip resetting rejected GDELT records")
    parser.add_argument("--limit", type=int,
                        help="Limit number of records to process")
    args = parser.parse_args()
    
    asyncio.run(main_async(dry_run=args.dry_run, skip_reset=args.skip_reset, limit=args.limit))


if __name__ == "__main__":
    main()
