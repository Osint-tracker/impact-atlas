import sys
import os
from datetime import datetime, timedelta, timezone

# Add project root to path BEFORE importing local modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import io
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

from ingestion.fetch_telegram import run_telegram_scraper
from ingestion.fetch_gdelt import fetch_gdelt_news


def main():
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)

    print(f"📅 DAILY SCRAPE: {yesterday.date()} -> {now.date()}")

    # 1. Telegram (ultime 24h)
    run_telegram_scraper(start_date=yesterday, end_date=now)

    # 2. GDELT (ultime 24h)
    g_start = yesterday.strftime("%Y%m%d%H%M%S")
    g_end = now.strftime("%Y%m%d%H%M%S")
    fetch_gdelt_news(g_start, g_end)

    print("✅ DATI GIORNALIERI ACQUISITI.")


if __name__ == "__main__":
    main()
