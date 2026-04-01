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
import sys
import io

# Force UTF-8 encoding for stdout/stderr to handle emojis on Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# --- CONFIGURATION ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))  # Go up 2 levels to osint-tracker
DB_PATH = os.path.join(PROJECT_ROOT, "war_tracker_v2", "data", "raw_events.db")
ENV_PATH = os.path.join(PROJECT_ROOT, "war_tracker_v2", ".env")

# Debug
print(f"DB_PATH: {DB_PATH}")
print(f"DB exists: {os.path.exists(DB_PATH)}")

load_dotenv(ENV_PATH)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# Processing parameters
EMBEDDING_MODEL = "qwen/qwen3-embedding-8b"
TEXTS_PER_API_CALL = 50      # Batch 50 texts per API request
CONCURRENT_REQUESTS = 10     # 10 parallel requests
DB_BATCH_SIZE = 500          # Fetch 500 from DB at a time
MIN_TEXT_LENGTH = 0         # User requested "fetch everything" (was 50)
LOOKBACK_DAYS = 20          # Only process records from the last N days (performance guard)

# Original strict filter (from refiner.py) - for comparison
MANDATORY_ANCHORS_STRICT = [
    "ukrain", "russia", "moscow", "kremlin", "kyiv", "kiev", "donetsk", "luhansk",
    "kharkiv", "kherson", "zaporizhzhia", "crimea", "mariupol", "bakhmut",
    # Cyrillic equivalents (using stems to match cases)
    "украї", "украи", "росі", "росси", "москв", "кремл", "киї", "киев",
    "донець", "донец", "луган", "харк", "харьк", "херсон",
    "запорі", "запоро", "крим", "крым", "маріу", "мариу", "бахмут",
    "рф", "всу", "зсу", "сбу", "гур", "дрон", "бпла", "ракет", "артилери", 
    "танк", "військ", "воен", "атак", "удар", "вибух", "взрыв", "нпз"
]

# New relaxed filter - accepts any war-related keyword
MANDATORY_ANCHORS_LOOSE = [
    "ukrain", "russia", "moscow", "kremlin", "kyiv", "kiev", "donetsk", "luhansk",
    "kharkiv", "kherson", "zaporizhzhia", "crimea", "mariupol", "bakhmut", "drone",
    "missile", "artillery", "tank", "military", "troops", "soldiers", "war", "attack",
    "strike", "offensive", "defense", "front", "battle", "rocket", "shell", "bomb",
    "infantry", "brigade", "regiment", "wagner", "azov", "himars", "patriot",
    # Cyrillic equivalents (using stems to match cases)
    "украї", "украи", "росі", "росси", "москв", "кремл", "киї", "киев",
    "донець", "донец", "луган", "харк", "харьк", "херсон",
    "запорі", "запоро", "крим", "крым", "маріу", "мариу", "бахмут",
    "дрон", "бпла", "ракет", "артилери", "артиллери", "танк", "військ", "воен",
    "солдат", "війн", "войн", "атак", "удар", "насту", "оборон",
    "фронт", "бій", "бой", "град", "химарс", "хаймерс", "патріо", "патрио",
    "рф", "всу", "зсу", "сбу", "гур", "вибух", "взрыв", "збит", "сбит", "нпз"
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
    
    # Get records with content >= MIN_TEXT_LENGTH
    cursor.execute("""
        SELECT event_hash, text_content, url, source_type, is_embedded
        FROM raw_signals 
        WHERE LENGTH(text_content) >= ?
        LIMIT 20000
    """, (MIN_TEXT_LENGTH,))
    
    rows = cursor.fetchall()
    print(f"Analyzing {len(rows)} records with content >= {MIN_TEXT_LENGTH} chars...")
    
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
    
    print(f"\n  NEW Filter (Loose/Cyrillic):")
    print(f"    Accept: {new_accept} ({new_accept/len(rows)*100:.1f}%)")
    print(f"    Reject: {new_reject} ({new_reject/len(rows)*100:.1f}%)")
    
    print(f"\n  DELTA (Rescued by new filter): {delta_rescued}")
    
    if delta_rescued == 0:
        print("\n  ⚠️ WARNING: Delta is 0. Either:")
        print("     - All GDELT already passed old filter (unlikely)")
        print("     - Your GDELT content is still too short (check scraper)")
        print("     - Logic error in filter comparison")
    else:
        print(f"\n  ✅ New filter would rescue {delta_rescued} additional records.")
    
    return delta_rescued


async def generate_embeddings_async(session, texts, semaphore):
    """Generate embeddings for a batch of texts."""
    async with semaphore:
        headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/Osint-tracker/impact-atlas",
            "X-Title": "Impact Atlas Refiner"
        }
        
        # Puoi alzare o rimuovere il limite dei 6000 caratteri dato che Qwen3 8B 
        # ha una context window molto più ampia di text-embedding-3-small
        safe_texts = [str(t).replace("\n", " ") for t in texts]
        
        payload = {
            "model": EMBEDDING_MODEL,
            "input": safe_texts
        }
        
        try:
            async with session.post(
                "https://openrouter.ai/api/v1/embeddings",
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


async def main_async(dry_run=False, skip_reset=False, limit=None, lookback_days=None):
    print("=" * 60)
    print("REFINER FAST - High-Speed Embedding with GDELT Fix")
    print("=" * 60)

    effective_lookback = lookback_days if lookback_days is not None else LOOKBACK_DAYS
    cutoff_date = (datetime.utcnow() - __import__('datetime').timedelta(days=effective_lookback)).isoformat()
    print(f"🗓️  Temporal filter: last {effective_lookback} days (cutoff: {cutoff_date[:10]})")
    
    if not OPENROUTER_API_KEY:
        print("ERROR: OPENROUTER_API_KEY not found in .env")
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
    
    # Step 1: Reset rejected sources for reprocessing (temporal window only)
    if not skip_reset:
        print(f"\n[1/4] Resetting rejected records within the last {effective_lookback} days...")
        cursor.execute("""
            UPDATE raw_signals 
            SET is_embedded = 0 
            WHERE is_embedded = 2
              AND date_published >= ?
        """, (cutoff_date,))
        reset_count = cursor.rowcount
        conn.commit()
        print(f"      Reset {reset_count} records for reprocessing.")
    else:
        print("\n[1/4] Skipping reset (--skip-reset flag)")
    
    # Step 2: Count unprocessed with sufficient content (within temporal window)
    cursor.execute("""
        SELECT COUNT(*) FROM raw_signals 
        WHERE is_embedded = 0
          AND LENGTH(text_content) >= ?
          AND date_published >= ?
    """, (MIN_TEXT_LENGTH, cutoff_date))
    total_unprocessed = cursor.fetchone()[0]
    print(f"\n[2/4] Found {total_unprocessed} unprocessed records (last {effective_lookback} days, content >= {MIN_TEXT_LENGTH} chars).")
    
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
            # Fetch batch from DB (within temporal window)
            cursor.execute("""
                SELECT event_hash, source_type, text_content, url
                FROM raw_signals 
                WHERE is_embedded = 0
                  AND LENGTH(text_content) >= ?
                  AND date_published >= ?
                LIMIT ?
            """, (MIN_TEXT_LENGTH, cutoff_date, DB_BATCH_SIZE))
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
    parser.add_argument("--lookback-days", type=int, default=None,
                        help=f"Override temporal window (default: {LOOKBACK_DAYS} days)")
    args = parser.parse_args()
    
    asyncio.run(main_async(
        dry_run=args.dry_run,
        skip_reset=args.skip_reset,
        limit=args.limit,
        lookback_days=args.lookback_days
    ))


if __name__ == "__main__":
    main()
