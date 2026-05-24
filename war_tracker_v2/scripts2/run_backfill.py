"""
Optimized Backfill Runner.
- Telegram and GDELT run in PARALLEL via asyncio.gather()
- Local imports from scripts2/ (no more ingestion/ dependency)
"""

import sys
import os
import io
import argparse
import subprocess
import asyncio

# Force UTF-8 encoding for stdout/stderr to handle emojis on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
from datetime import datetime, timezone

# 1. Calculate paths
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
grandparent_dir = os.path.abspath(os.path.join(parent_dir, '..'))

# 2. Add to sys.path
if grandparent_dir not in sys.path:
    sys.path.insert(0, grandparent_dir)

# 3. Import from optimized local modules
try:
    from war_tracker_v2.scripts2.fetch_gdelt import fetch_gdelt_news_async
    from war_tracker_v2.scripts2.fetch_telegram import _run_scraper_async
except ImportError as e:
    print(f"[ERROR] ERRORE CRITICO DI IMPORT: {e}")
    print(f"   Python sta cercando in: {sys.path}")
    sys.exit(1)


async def main():
    parser = argparse.ArgumentParser(description="Backfill ingestion + optional strategic campaign admission")
    parser.add_argument(
        "--with-campaign-admission",
        action="store_true",
        help="Run scripts/run_backfill.py after Telegram+GDELT backfill",
    )
    parser.add_argument("--admission-limit", type=int, default=0, help="Limit for admission run")
    parser.add_argument("--admission-dry-run", action="store_true", help="Dry-run admission")
    args = parser.parse_args()

    print("[START] AVVIO BACKFILL OTTIMIZZATO")

    # Timezone Aware (UTC)
    start_dt = datetime(2026, 5, 13, 0, 0, 0, tzinfo=timezone.utc)
    end_dt = datetime(2026, 5, 30, 0, 0, 0, tzinfo=timezone.utc)

    # GDELT date strings
    gdelt_start = start_dt.strftime("%Y%m%d%H%M%S")
    gdelt_end = end_dt.strftime("%Y%m%d%H%M%S")

    # --- PARALLEL EXECUTION: Telegram + GDELT ---
    print("\n--- AVVIO PARALLELO: TELEGRAM + GDELT ---")
    telegram_task = _run_scraper_async(start_date=start_dt, end_date=end_dt)
    gdelt_task = fetch_gdelt_news_async(start_date=gdelt_start, end_date=gdelt_end)

    # Run both concurrently — they are independent data sources
    results = await asyncio.gather(
        telegram_task,
        gdelt_task,
        return_exceptions=True
    )

    # Report any exceptions
    for i, result in enumerate(results):
        source = ["TELEGRAM", "GDELT"][i]
        if isinstance(result, Exception):
            print(f"[ERROR] Errore {source}: {result}")

    if args.with_campaign_admission:
        print("\n--- AVVIO STRATEGIC CAMPAIGN ADMISSION ---")
        admission_script = os.path.join(grandparent_dir, "scripts", "run_backfill.py")
        cmd = [sys.executable, admission_script]
        if args.admission_limit and args.admission_limit > 0:
            cmd.extend(["--limit", str(args.admission_limit)])
        if args.admission_dry_run:
            cmd.append("--dry-run")

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as exc:
            print(f"[ERROR] Errore Campaign Admission: {exc}")

    print("\n[DONE] BACKFILL COMPLETATO.")


if __name__ == "__main__":
    asyncio.run(main())
