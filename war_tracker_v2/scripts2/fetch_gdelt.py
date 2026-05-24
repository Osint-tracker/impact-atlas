"""
Optimized GDELT Fetcher for backfill ingestion.
- Async HTTP with aiohttp for concurrent window fetching
- Semaphore(4) for bounded concurrency
- Reduced inter-window sleep (1s vs 4s)
- Exponential backoff on 429/503 preserved
"""

import sys
import os
import asyncio
import datetime
from datetime import timedelta

import aiohttp

# --- SETUP IMPORT ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
if grandparent_dir not in sys.path:
    sys.path.insert(0, grandparent_dir)

from war_tracker_v2.scripts2.db_manager import save_raw_events

# Concurrency settings
WINDOW_CONCURRENCY = 4
INTER_WINDOW_SLEEP = 1.0  # Reduced from 4s — backoff handles rate limits

# Header for requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


async def fetch_gdelt_window(session, start_str, end_str):
    """
    Fetch a single GDELT time window asynchronously.
    Retry with exponential backoff on 429/503.
    """
    url = "http://api.gdeltproject.org/api/v2/doc/doc"

    query = "(Ukraine OR Russia OR Putin OR Zelensky OR Kyiv OR Moscow)"

    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": "250",
        "format": "json",
        "startdatetime": start_str,
        "enddatetime": end_str,
        "sort": "DateDesc"
    }

    max_retries = 5
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    return data.get('articles', [])

                elif resp.status in (429, 503):
                    wait_time = min((2 ** attempt) * 5, 120)
                    print(f"   [WARNING] HTTP {resp.status}. Backoff {wait_time}s (attempt {attempt+1}/{max_retries})...")
                    await asyncio.sleep(wait_time)
                    continue

                else:
                    print(f"   [ERROR] HTTP {resp.status}")
                    return []

        except asyncio.TimeoutError:
            print(f"   [ERROR] GDELT timeout. Skipping window {start_str}.")
            return []
        except aiohttp.ClientError as e:
            print(f"   [ERROR] GDELT connection failed: {e}")
            return []
        except Exception as e:
            print(f"   [ERROR] GDELT unexpected: {type(e).__name__}: {e}")
            return []

    print(f"   [ERROR] GDELT rate-limit exhausted after {max_retries} retries. Skipping window.")
    return []


async def _process_window(session, sem, s_str, e_str, results_accumulator):
    """Fetch a single window with semaphore-bounded concurrency."""
    async with sem:
        print(f"   [FETCH] Scarico finestra: {s_str} -> {e_str} ... ", end="")
        articles = await fetch_gdelt_window(session, s_str, e_str)

        if articles:
            clean_events = []
            for art in articles:
                clean_events.append({
                    'text': f"{art.get('title')} - {art.get('url')}",
                    'source': art.get('domain', 'GDELT_Network'),
                    'type': 'WEB_NEWS',
                    'date': art.get('seendate')
                })

            saved = save_raw_events(clean_events)
            results_accumulator.append(saved)
            print(f"Trovati {len(articles)}, Nuovi Salvati: {saved}")
        else:
            print("Nessun dato.")

        # Courtesy pause between windows
        await asyncio.sleep(INTER_WINDOW_SLEEP)


async def fetch_gdelt_news_async(start_date, end_date):
    """
    Async GDELT fetcher: iterates day-by-day windows with bounded concurrency.
    Accepts YYYYMMDDHHMMSS strings.
    """
    print(f"[INFO] GDELT: Avvio scraping massivo da {start_date} a {end_date}...")

    try:
        dt_start = datetime.datetime.strptime(start_date, "%Y%m%d%H%M%S")
        dt_end = datetime.datetime.strptime(end_date, "%Y%m%d%H%M%S")
    except ValueError:
        print("[ERROR] Errore formato data GDELT. Usa YYYYMMDDHHMMSS")
        return

    # Build list of windows
    windows = []
    current_cursor = dt_start
    while current_cursor < dt_end:
        next_cursor = current_cursor + timedelta(days=1)
        if next_cursor > dt_end:
            next_cursor = dt_end
        s_str = current_cursor.strftime("%Y%m%d%H%M%S")
        e_str = next_cursor.strftime("%Y%m%d%H%M%S")
        windows.append((s_str, e_str))
        current_cursor = next_cursor

    print(f"[INFO] GDELT: {len(windows)} finestre giornaliere, concorrenza={WINDOW_CONCURRENCY}")

    results = []
    sem = asyncio.Semaphore(WINDOW_CONCURRENCY)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _process_window(session, sem, s, e, results)
            for s, e in windows
        ]
        await asyncio.gather(*tasks)

    total_saved = sum(results)
    print(f"[SUCCESS] GDELT COMPLETATO: {total_saved} articoli totali salvati nel DB.")


def fetch_gdelt_news(start_date, end_date):
    """
    Synchronous wrapper — maintains backward compatibility with run_backfill.py.
    Internally runs async.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(fetch_gdelt_news_async(start_date, end_date))
        else:
            loop.run_until_complete(fetch_gdelt_news_async(start_date, end_date))
    except RuntimeError:
        asyncio.run(fetch_gdelt_news_async(start_date, end_date))


if __name__ == "__main__":
    fetch_gdelt_news("20260514000000", "20260518000000")
