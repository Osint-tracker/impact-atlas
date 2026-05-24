"""
Daily Scraper — fetches last 24h of Telegram + GDELT data.
Updated to use optimized local modules + parallel execution.
"""

import sys
import os
import io
import asyncio
from datetime import datetime, timedelta, timezone

# Add project root to path BEFORE importing local modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from war_tracker_v2.scripts2.fetch_telegram import _run_scraper_async
from war_tracker_v2.scripts2.fetch_gdelt import fetch_gdelt_news_async


async def main():
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)

    print(f"[DAILY] DAILY SCRAPE: {yesterday.date()} -> {now.date()}")

    # GDELT date strings
    g_start = yesterday.strftime("%Y%m%d%H%M%S")
    g_end = now.strftime("%Y%m%d%H%M%S")

    # Parallel execution
    results = await asyncio.gather(
        _run_scraper_async(start_date=yesterday, end_date=now),
        fetch_gdelt_news_async(g_start, g_end),
        return_exceptions=True
    )

    for i, result in enumerate(results):
        source = ["TELEGRAM", "GDELT"][i]
        if isinstance(result, Exception):
            print(f"[ERROR] Errore {source}: {result}")

    print("[DONE] DATI GIORNALIERI ACQUISITI.")


if __name__ == "__main__":
    asyncio.run(main())
