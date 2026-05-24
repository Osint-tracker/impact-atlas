"""
Optimized Telegram Fetcher for backfill ingestion.
- Concurrent channel scraping with asyncio.Semaphore(3)
- offset_date to skip messages outside target range
- Larger DB_BATCH_SIZE (200) for fewer DB round-trips
- ALL anti-ban measures preserved:
  * FloodWaitError handling + sleep(e.seconds + 5)
  * SLEEP_EVERY_N_MSGS = 200 with random.uniform(2, 5)
  * wait_time=1 in iter_messages
  * Sleep between channels
  * Single Telethon client
"""

import os
import sys
import asyncio
import json
import random
import emoji
from datetime import datetime, timezone

# Telethon
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    ChannelPrivateError,
    UsernameInvalidError
)
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

# --- SETUP IMPORT ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
if grandparent_dir not in sys.path:
    sys.path.insert(0, grandparent_dir)

from war_tracker_v2.scripts2.db_manager import save_raw_events
from dotenv import load_dotenv

load_dotenv(os.path.join(parent_dir, '.env'))
# Fallback to root .env
if not os.getenv('TELEGRAM_API_ID'):
    load_dotenv(os.path.join(grandparent_dir, '.env'))

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# --- CONFIGURAZIONE ANTI-BAN (IMMUTABLE — DO NOT CHANGE) ---
DB_BATCH_SIZE = 200           # Batch size for DB writes (DB-side only, no API impact)
SLEEP_EVERY_N_MSGS = 200      # Anti-ban: sleep every N messages
MIN_SLEEP = 2                 # Anti-ban: minimum random sleep
MAX_SLEEP = 5                 # Anti-ban: maximum random sleep
CHANNEL_CONCURRENCY = 3       # Max concurrent channel scrapes
INTER_CHANNEL_SLEEP = 2       # Anti-ban: sleep between channel starts

# Lista Canali
CHANNELS_METADATA = {

  "deepstatemap": {"bias": "PRO_UA", "reliability": 0.90, "type": "OSINT"},
  "DeepStateUA": {"bias": "PRO_UA", "reliability": 0.90, "type": "OSINT"},
  "MAKS23_NAFO": {"bias": "PRO_UA", "reliability": 0.55, "type": "UA_ACTIVIST"},
  "Tatarigami_UA": {"bias": "PRO_UA", "reliability": 0.85, "type": "UA_ANALYST"},
  "ukrliberation": {"bias": "PRO_UA", "reliability": 0.60, "type": "UA_NEWS"},
  "DroneBomber": {"bias": "PRO_UA", "reliability": 0.80, "type": "UA_NEWS"},
  "karymat": {"bias": "PRO_UA", "reliability": 0.85, "type": "OSINT"},
  "stanislav_osman": {"bias": "PRO_UA", "reliability": 0.75, "type": "UA_MILITARY"},
  "officer_33": {"bias": "PRO_UA", "reliability": 0.75, "type": "UA_MILITARY"},
  "magyarbirds414": {"bias": "PRO_UA", "reliability": 0.85, "type": "UA_MILITARY"},
  "ssternenko": {"bias": "PRO_UA", "reliability": 0.70, "type": "UA_ACTIVIST"},
  "insiderUKR": {"bias": "PRO_UA", "reliability": 0.60, "type": "AGGREGATOR"},
  "serhii_flash": {"bias": "PRO_UA", "reliability": 0.90, "type": "UA_ANALYST"},
  "AMK_Mapping": {"bias": "PRO_RU", "reliability": 0.70, "type": "OSINT"},
  "bahshiddemon": {"bias": "PRO_UA", "reliability": 0.70, "type": "UA_MILITARY"},
  "rybar": {"bias": "PRO_RU", "reliability": 0.65, "type": "MILBLOGGER"},
  "fighter_bomber": {"bias": "PRO_RU", "reliability": 0.80, "type": "MILBLOGGER"},
  "strelkovii": {"bias": "PRO_RU", "reliability": 0.80, "type": "RU_ANALYST"},
  "lost_armour": {"bias": "PRO_RU", "reliability": 0.80, "type": "OSINT"},
  "grey_zone": {"bias": "PRO_RU", "reliability": 0.65, "type": "WAGNER"},
  "voenkorKotenok": {"bias": "PRO_RU", "reliability": 0.55, "type": "REPORTER"},
  "belarusian_silovik": {"bias": "PRO_RU", "reliability": 0.55, "type": "PROPAGANDA"},
  "GeoConfirmed": {"bias": "NEUTRAL", "reliability": 0.95, "type": "OSINT"},
  "Osinttechnical": {"bias": "PRO_UA", "reliability": 0.90, "type": "OSINT"},
  "WarMonitors": {"bias": "NEUTRAL", "reliability": 0.50, "type": "AGGREGATOR"},
  "noel_reports": {"bias": "PRO_UA", "reliability": 0.80, "type": "AGGREGATOR"},
  "ChrisO_wiki": {"bias": "WESTERN_MEDIA", "reliability": 0.85, "type": "OSINT"},
  "Majakovsk73": {"bias": "NEUTRAL", "reliability": 0.90, "type": "OSINT"},
  "parabellumcommunity": {"bias": "NEUTRAL", "reliability": 0.90, "type": "ANALYST"},
  "UkraineWarReports": {"bias": "PRO_UA", "reliability": 0.70, "type": "UA_NEWS"},
  "myro_shnykov": {"bias": "PRO_UA", "reliability": 0.75, "type": "UA_NEWS"},
  "spravdi": {"bias": "PRO_UA", "reliability": 0.60, "type": "STATE_MEDIA"},
  "clement_molin": {"bias": "NEUTRAL", "reliability": 0.95, "type": "OSINT"},
  "war_mapper": {"bias": "NEUTRAL", "reliability": 0.85, "type": "OSINT"},
  "supernova_plus": {"bias": "PRO_UA", "reliability": 0.75, "type": "AGGREGATOR"},
  "dva_majors": {"bias": "PRO_RU", "reliability": 0.60, "type": "MILBLOGGER"},
  "astrapress": {"bias": "NEUTRAL", "reliability": 0.85, "type": "JOURNALIST"},
  "sashakots": {"bias": "PRO_RU", "reliability": 0.30, "type": "PROPAGANDA"},
  "ab3army": {"bias": "PRO_UA", "reliability": 0.85, "type": "MILITARY_OFFICIAL"},
  "wargonzo": {"bias": "PRO_RU", "reliability": 0.30, "type": "PROPAGANDA"},
  "Sladkov_plus": {"bias": "PRO_RU", "reliability": 0.40, "type": "PROPAGANDA"},
  "lachentyt": {"bias": "PRO_UA", "reliability": 0.65, "type": "UA_ACTIVIST"},
  "ButusovPlus": {"bias": "PRO_UA", "reliability": 0.80, "type": "JOURNALIST"},
  "moscowcalling": {"bias": "NEUTRAL", "reliability": 0.75, "type": "OSINT"},
  "brygada47": {"bias": "PRO_UA", "reliability": 0.85, "type": "MILITARY_OFFICIAL"},
  "rusich_army": {"bias": "PRO_RU", "reliability": 0.60, "type": "PARAMILITARY"},
  "kherson_non_fake": {"bias": "PRO_UA", "reliability": 0.75, "type": "OSINT"},
  "vysokygovorit": {"bias": "PRO_RU", "reliability": 0.60, "type": "MILBLOGGER"},
  "exilenova_plus": {"bias": "PRO_UA", "reliability": 0.75, "type": "OSINT"},
  "CITeam": {"bias": "NEUTRAL", "reliability": 0.90, "type": "OSINT"},
  "batalyon_monaco": {"bias": "PRO_UA", "reliability": 0.40, "type": "MEME_POLITICAL"},
  "z_komitet": {"bias": "PRO_RU", "reliability": 0.40, "type": "MILBLOGGER"}
  }


def clean_text_content(text):
    """Remove emoji and extra whitespace."""
    if not text:
        return ""
    text_no_emoji = emoji.replace_emoji(text, replace='')
    return " ".join(text_no_emoji.split())


async def fetch_channel_history(client, channel_name, start_date, end_date=None):
    """
    Fetch channel history respecting date limits.
    Anti-ban measures: wait_time=1, sleep every 200 msgs, FloodWait handling.
    """
    print(f"\n[CONNECT] Analisi canale: {channel_name}...")

    signals_batch = []
    total_channel_saved = 0
    msgs_since_sleep = 0

    meta = CHANNELS_METADATA.get(channel_name, {})
    bias = meta.get('bias', 'UNKNOWN')

    try:
        entity = await client.get_entity(channel_name)

        # OPTIMIZATION: offset_date starts iteration from end_date instead of "now"
        # This is a server-side parameter — DECREASES API calls, zero ban risk
        iter_kwargs = {'wait_time': 1}
        if end_date:
            iter_kwargs['offset_date'] = end_date

        async for message in client.iter_messages(entity, **iter_kwargs):

            # --- ANTI-BAN: PROTEZIONE FLOOD (PRESERVED) ---
            msgs_since_sleep += 1
            if msgs_since_sleep >= SLEEP_EVERY_N_MSGS:
                sleep_time = random.uniform(MIN_SLEEP, MAX_SLEEP)
                await asyncio.sleep(sleep_time)
                msgs_since_sleep = 0

            # 1. Controllo Validità Messaggio
            if not message.date:
                continue

            msg_date = message.date

            # STOP if we go past the start_date
            if msg_date < start_date:
                print(
                    f"   [STOP] Raggiunta data limite ({start_date.date()}). Stop canale.")
                break

            # SKIP if message is newer than end_date
            if end_date and msg_date > end_date:
                continue

            # 2. PULIZIA TESTO
            raw_text = message.text or ""
            cleaned_text = clean_text_content(raw_text)

            if len(cleaned_text) < 20:
                continue

            # 3. Preparazione Dati per DB Manager
            media_urls = []
            if message.media:
                media_url = f"https://t.me/{channel_name}/{message.id}"
                media_urls.append(media_url)

            date_str = msg_date.strftime("%Y-%m-%d %H:%M:%S")

            event_obj = {
                'text': cleaned_text,
                'source': channel_name,
                'type': 'TELEGRAM',
                'date': date_str,
                'url': f"https://t.me/{channel_name}/{message.id}",
                'media_urls': json.dumps(media_urls)
            }

            signals_batch.append(event_obj)

            # 4. Salvataggio Batch (DB-side, no API impact)
            if len(signals_batch) >= DB_BATCH_SIZE:
                saved = save_raw_events(signals_batch)
                total_channel_saved += saved
                signals_batch = []
                sys.stdout.write(
                    f"\r   [SAVE] {channel_name} ({bias}): {total_channel_saved} msg...")
                sys.stdout.flush()

        # Flush remaining
        if signals_batch:
            saved = save_raw_events(signals_batch)
            total_channel_saved += saved
            print(
                f"\r   [SAVE] {channel_name} ({bias}): {total_channel_saved} msg...")

    except FloodWaitError as e:
        # ANTI-BAN: PRESERVED — respect Telegram's wait request
        print(
            f"\n   [FLOOD] FLOOD WAIT: Aspetto {e.seconds} secondi.")
        print(f"   [SLEEP] Dormo per {e.seconds + 5} secondi per sicurezza...")
        await asyncio.sleep(e.seconds + 5)

    except (ValueError, ChannelPrivateError, UsernameInvalidError) as e:
        print(
            f"\n   [ERROR] Canale '{channel_name}': Non trovato o Privato. Salto.")
    except Exception as e:
        print(f"\n   [ERROR] Errore generico su {channel_name}: {e}")

    print(f"   [DONE] Finito {channel_name}. Totale salvati: {total_channel_saved}")


# --- ENTRY POINT ---

async def _run_scraper_async(start_date, end_date):
    """
    Async entry point: scrapes all channels with bounded concurrency.
    Uses Semaphore(3) — Telethon serializes MTProto requests on a single
    connection anyway, so concurrency only interleaves I/O waits.
    """
    if not API_ID or not API_HASH:
        print("[ERROR] TELEGRAM: API_ID o API_HASH mancanti nel file .env")
        return

    session_path = os.path.join(parent_dir, 'data', 'telegram_session')

    async with TelegramClient(session_path, API_ID, API_HASH) as client:
        if not await client.is_user_authorized():
            print("[AUTH] RICHIESTA AUTENTICAZIONE UTENTE (Primo avvio)")
            await client.start(phone=PHONE)

        channels = list(CHANNELS_METADATA.keys())
        print(
            f"[START] TELEGRAM SCRAPER: {start_date.date()} -> {end_date.date() if end_date else 'Oggi'}")
        print(f"[INFO] Canali: {len(channels)} | Concorrenza: {CHANNEL_CONCURRENCY}")

        sem = asyncio.Semaphore(CHANNEL_CONCURRENCY)

        async def _scrape_with_semaphore(ch):
            async with sem:
                await fetch_channel_history(client, ch, start_date, end_date)
                # ANTI-BAN: PRESERVED — sleep between channel starts
                await asyncio.sleep(INTER_CHANNEL_SLEEP)

        # Launch all channel tasks with bounded concurrency
        tasks = [_scrape_with_semaphore(ch) for ch in channels]
        await asyncio.gather(*tasks)

        print("\n[DONE] Ingestione Telegram Completata.")


def run_telegram_scraper(start_date, end_date=None):
    """Synchronous wrapper called by run_backfill.py"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(_run_scraper_async(start_date, end_date))
        else:
            loop.run_until_complete(_run_scraper_async(start_date, end_date))
    except RuntimeError:
        asyncio.run(_run_scraper_async(start_date, end_date))


if __name__ == "__main__":
    test_start = datetime(2025, 12, 25, tzinfo=timezone.utc)
    run_telegram_scraper(test_start)
