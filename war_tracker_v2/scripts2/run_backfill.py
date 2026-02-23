import sys
import os
import io

# Force UTF-8 encoding for stdout/stderr to handle emojis on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
from datetime import datetime, timezone

# 1. PRIMA calcoliamo il percorso della cartella madre (osint-tracker)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
grandparent_dir = os.path.abspath(os.path.join(parent_dir, '..'))

# 2. POI lo aggiungiamo al sistema
if grandparent_dir not in sys.path:
    sys.path.append(grandparent_dir)

# 3. ADESSO (e solo adesso) possiamo importare i moduli dalla cartella ingestion
try:
    from ingestion.fetch_gdelt import fetch_gdelt_news
    from ingestion.fetch_telegram import run_telegram_scraper
except ImportError as e:
    print(f"[ERROR] ERRORE CRITICO DI IMPORT: {e}")
    print(f"   Python sta cercando in: {sys.path}")
    sys.exit(1)


def main():
    print("[START] AVVIO BACKFILL: 22 Gennaio 2026 -> 24 Gennaio 2026")

    # Timezone Aware (UTC)
    start_dt = datetime(2026, 2, 22, 0, 0, 0, tzinfo=timezone.utc)
    # Impostiamo la fine a "domani" per essere sicuri di prendere tutto oggi
    end_dt = datetime(2026, 2, 24, 0, 0, 0, tzinfo=timezone.utc)

    # 1. Esegui Telegram
    print("\n--- AVVIO TELEGRAM ---")
    try:
        run_telegram_scraper(start_date=start_dt, end_date=end_dt)
    except Exception as e:
        print(f"[ERROR] Errore Telegram: {e}")

    # 2. Esegui GDELT
    print("\n--- AVVIO GDELT ---")
    gdelt_start = start_dt.strftime("%Y%m%d%H%M%S")
    gdelt_end = end_dt.strftime("%Y%m%d%H%M%S")

    fetch_gdelt_news(start_date=gdelt_start, end_date=gdelt_end)

    print("\n[DONE] BACKFILL COMPLETATO.")


if __name__ == "__main__":
    main()
