"""
gdelt_scraper.py - Fetch full article content for GDELT entries
================================================================
Scrapes the actual article content from GDELT URLs stored in raw_signals.
Uses async + concurrent requests for speed.

INSTRUMENTATION:
- Tracks scrape_attempt_count to avoid infinite retries on dead links
- Max 3 attempts per URL, then marked as permanently failed
- Updates text_content only on successful scrape

WARNING: This will take HOURS for 300k+ URLs. Run overnight.
"""

import sqlite3
import asyncio
import aiohttp
import os
import time
import re
from bs4 import BeautifulSoup
from tqdm import tqdm
from datetime import datetime
import argparse

# Configuration
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, '../war_tracker_v2/data/raw_events.db')

# Scraping parameters
CONCURRENT_REQUESTS = 20      # Parallel requests
TIMEOUT_SECONDS = 10          # Per-request timeout
BATCH_SIZE = 100              # Commit to DB every N updates
MIN_CONTENT_LENGTH = 200      # Minimum chars to consider success
MAX_SCRAPE_ATTEMPTS = 3       # Max retries per URL
DEFAULT_RECENT_DAYS = 7       # Default days for --recent flag

# User agent rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]


def ensure_scrape_columns(conn):
    """Add instrumentation columns if they don't exist."""
    cursor = conn.cursor()
    
    # Check if columns exist
    cursor.execute("PRAGMA table_info(raw_signals)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if 'scrape_attempt_count' not in columns:
        print("Adding scrape_attempt_count column...")
        cursor.execute("ALTER TABLE raw_signals ADD COLUMN scrape_attempt_count INTEGER DEFAULT 0")
    
    if 'scrape_last_error' not in columns:
        print("Adding scrape_last_error column...")
        cursor.execute("ALTER TABLE raw_signals ADD COLUMN scrape_last_error TEXT")
    
    if 'scrape_last_attempt' not in columns:
        print("Adding scrape_last_attempt column...")
        cursor.execute("ALTER TABLE raw_signals ADD COLUMN scrape_last_attempt TEXT")
    
    conn.commit()


def extract_article_text(html):
    """Extract main article text from HTML."""
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove script, style, nav, header, footer
        for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'form', 'iframe']):
            tag.decompose()
        
        # Try common article containers
        article = None
        for selector in ['article', 'main', '.article-body', '.story-body', '.post-content', 
                         '.entry-content', '#article-body', '.article-content', '.content']:
            article = soup.select_one(selector)
            if article:
                break
        
        if article:
            text = article.get_text(separator=' ', strip=True)
        else:
            # Fallback: get all paragraphs
            paragraphs = soup.find_all('p')
            text = ' '.join([p.get_text(strip=True) for p in paragraphs])
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text if len(text) >= MIN_CONTENT_LENGTH else None
        
    except Exception as e:
        return None


async def fetch_url(session, event_hash, url, semaphore, user_agent_idx):
    """Fetch a single URL and extract article text."""
    async with semaphore:
        headers = {"User-Agent": USER_AGENTS[user_agent_idx % len(USER_AGENTS)]}
        error_msg = None
        
        try:
            async with session.get(
                url, 
                headers=headers, 
                timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
                ssl=False  # Skip SSL verification for speed
            ) as response:
                if response.status == 200:
                    html = await response.text()
                    text = extract_article_text(html)
                    if text:
                        return event_hash, text, None
                    else:
                        return event_hash, None, "EXTRACTION_FAILED: Could not extract meaningful text"
                elif response.status == 403:
                    return event_hash, None, f"HTTP_403: Access forbidden (paywall?)"
                elif response.status == 404:
                    return event_hash, None, f"HTTP_404: Page not found"
                else:
                    return event_hash, None, f"HTTP_{response.status}: Server error"
        except asyncio.TimeoutError:
            return event_hash, None, "TIMEOUT: Request took too long"
        except aiohttp.ClientError as e:
            return event_hash, None, f"CLIENT_ERROR: {str(e)[:100]}"
        except Exception as e:
            return event_hash, None, f"UNKNOWN_ERROR: {str(e)[:100]}"


async def process_batch(rows, session, semaphore):
    """Process a batch of (event_hash, url) tuples concurrently."""
    tasks = []
    
    for i, (event_hash, url) in enumerate(rows):
        if url and url.startswith('http'):
            task = fetch_url(session, event_hash, url, semaphore, i)
            tasks.append(task)
    
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if not isinstance(r, Exception)]
    return []


async def main_async(limit=None, dry_run=False, recent_days=None):
    print("=" * 60)
    print("GDELT CONTENT SCRAPER (Instrumented)")
    print("=" * 60)
    
    # Connect to DB
    conn = sqlite3.connect(DB_PATH, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL")
    ensure_scrape_columns(conn)
    cursor = conn.cursor()
    
    # Build date filter if --recent is specified
    date_filter = ""
    date_params = []
    if recent_days:
        from datetime import timedelta
        # Support both YYYYMMDDHHMMSS and ISO date formats
        cutoff_date = (datetime.now() - timedelta(days=recent_days)).strftime('%Y%m%d')
        date_filter = " AND date_published >= ?"
        date_params = [cutoff_date]
        print(f"📅 RECENT MODE: Only scraping events from {cutoff_date} onwards ({recent_days} days)")
    
    # Count GDELT entries needing content (exclude those with 3+ failed attempts)
    query = f"""
        SELECT COUNT(*) FROM raw_signals 
        WHERE source_type IN ('GDELT', 'WEB_NEWS') 
        AND LENGTH(text_content) < 100
        AND (scrape_attempt_count IS NULL OR scrape_attempt_count < ?)
        {date_filter}
    """
    cursor.execute(query, [MAX_SCRAPE_ATTEMPTS] + date_params)
    total = cursor.fetchone()[0]
    print(f"Found {total} GDELT entries eligible for scraping.")
    
    # Count permanently failed
    cursor.execute("""
        SELECT COUNT(*) FROM raw_signals 
        WHERE source_type = 'GDELT' 
        AND scrape_attempt_count >= ?
    """, (MAX_SCRAPE_ATTEMPTS,))
    perm_failed = cursor.fetchone()[0]
    print(f"Permanently failed (>= {MAX_SCRAPE_ATTEMPTS} attempts): {perm_failed}")
    
    if total == 0:
        print("Nothing to scrape!")
        conn.close()
        return
    
    if dry_run:
        print(f"\n[DRY RUN MODE] Would scrape {total} URLs. Exiting.")
        conn.close()
        return
    
    # Apply limit if specified
    if limit:
        total = min(total, limit)
        print(f"Limiting to {total} URLs for this run.")
    
    # Fetch eligible GDELT/WEB_NEWS URLs
    fetch_query = f"""
        SELECT event_hash, url FROM raw_signals 
        WHERE source_type IN ('GDELT', 'WEB_NEWS') 
        AND LENGTH(text_content) < 100
        AND (scrape_attempt_count IS NULL OR scrape_attempt_count < ?)
        {date_filter}
        LIMIT ?
    """
    cursor.execute(fetch_query, [MAX_SCRAPE_ATTEMPTS] + date_params + [total])
    all_rows = cursor.fetchall()
    
    print(f"Starting scrape with {CONCURRENT_REQUESTS} concurrent requests...")
    print("Press Ctrl+C to stop gracefully.")
    
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    success_count = 0
    fail_count = 0
    now = datetime.now().isoformat()
    
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS, limit_per_host=5)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        pbar = tqdm(total=len(all_rows), desc="Scraping", unit="url")
        
        # Process in batches
        for i in range(0, len(all_rows), BATCH_SIZE):
            batch = all_rows[i:i + BATCH_SIZE]
            
            try:
                results = await process_batch(batch, session, semaphore)
                
                # Update database
                for event_hash, text, error in results:
                    if text:
                        # SUCCESS: Update content, reset is_embedded for re-processing
                        cursor.execute("""
                            UPDATE raw_signals 
                            SET text_content = ?, 
                                is_embedded = 0,
                                scrape_attempt_count = COALESCE(scrape_attempt_count, 0) + 1,
                                scrape_last_attempt = ?,
                                scrape_last_error = NULL
                            WHERE event_hash = ?
                        """, (text, now, event_hash))
                        success_count += 1
                    else:
                        # FAILURE: Increment attempt count, log error
                        cursor.execute("""
                            UPDATE raw_signals 
                            SET scrape_attempt_count = COALESCE(scrape_attempt_count, 0) + 1,
                                scrape_last_attempt = ?,
                                scrape_last_error = ?
                            WHERE event_hash = ?
                        """, (now, error, event_hash))
                        fail_count += 1
                
                pbar.update(len(batch))
                conn.commit()
                
            except KeyboardInterrupt:
                print("\nGraceful shutdown...")
                break
            except Exception as e:
                print(f"\nBatch error: {e}")
                continue
        
        pbar.close()
    
    conn.close()
    
    # Summary
    print(f"\n{'='*60}")
    print("SCRAPE COMPLETE")
    print(f"{'='*60}")
    print(f"  Successfully scraped: {success_count}")
    print(f"  Failed this run:      {fail_count}")
    print(f"  Success rate:         {success_count/(success_count+fail_count)*100:.1f}%")
    print(f"\nNext step: Run refiner_fast.py to embed the new content.")


def main():
    parser = argparse.ArgumentParser(description="GDELT Content Scraper")
    parser.add_argument("--limit", type=int, help="Limit number of URLs to scrape")
    parser.add_argument("--dry-run", action="store_true", help="Count eligible URLs without scraping")
    parser.add_argument("--recent", type=int, nargs='?', const=DEFAULT_RECENT_DAYS, 
                        help=f"Only scrape events from last N days (default: {DEFAULT_RECENT_DAYS})")
    args = parser.parse_args()
    
    asyncio.run(main_async(limit=args.limit, dry_run=args.dry_run, recent_days=args.recent))


if __name__ == "__main__":
    main()
