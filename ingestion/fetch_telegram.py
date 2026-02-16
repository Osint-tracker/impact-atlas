import os
import sys
import asyncio
import hashlib
import random
import emoji
from datetime import datetime, timezone

# Librerie Telethon
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    ChannelPrivateError,
    UsernameInvalidError
)
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument

# IMPORT DATABASE MANAGER
# Usiamo la funzione standard del progetto
from ingestion.db_manager import save_raw_events

# --- CONFIGURAZIONE VARIABILI AMBIENTE ---
# Carichiamo le variabili d'ambiente se non sono già caricate
from dotenv import load_dotenv
load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# Configurazione Anti-Ban
DB_BATCH_SIZE = 50
SLEEP_EVERY_N_MSGS = 200
MIN_SLEEP = 2
MAX_SLEEP = 5

# Lista Canali
CHANNELS_METADATA = {
    # UA Sources
    'deepstatemap': {'bias': 'PRO_UA', 'reliability': 0.85, 'type': 'UA_MILITARY'},
    'DeepStateUA': {'bias': 'PRO_UA', 'reliability': 0.85, 'type': 'UA_MILITARY'},
    'CinCA_AFU': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_MILITARY'},
    'MAKS23_NAFO': {'bias': 'PRO_UA', 'reliability': 0.75, 'type': 'UA_ACTIVIST'},
    'Tatarigami_UA': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_ANALYST'},
    'ukrliberation': {'bias': 'PRO_UA', 'reliability': 0.65, 'type': 'UA_NEWS'},
    'DroneBomber': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_NEWS'},
    'karymat': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_NEWS'},
    'stanislav_osman': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_MILITARY'},
    'officer_33': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'UA_MILITARY'},

    # RU Sources
    'rybar': {'bias': 'PRO_RU', 'reliability': 0.70, 'type': 'RU_MILITARY'},
    'fighter_bomber': {'bias': 'PRO_RU', 'reliability': 0.65, 'type': 'RU_MILITARY'},
    'strelkovii': {'bias': 'PRO_RU', 'reliability': 0.60, 'type': 'RU_ANALYST'},
    'lost_armour': {'bias': 'PRO_RU', 'reliability': 0.60, 'type': 'RU_ANALYST'},
    'grey_zone': {'bias': 'PRO_RU', 'reliability': 0.60, 'type': 'WAGNER'},
    'voenkorKotenok': {'bias': 'PRO_RU', 'reliability': 0.65, 'type': 'REPORTER'},

    # Neutral/Western/OSINT
    'GeoConfirmed': {'bias': 'NEUTRAL', 'reliability': 0.90, 'type': 'OSINT'},
    'Osinttechnical': {'bias': 'NEUTRAL', 'reliability': 0.88, 'type': 'OSINT'},
    'WarMonitors': {'bias': 'NEUTRAL', 'reliability': 0.85, 'type': 'AGGREGATOR'},
    'DefenceHQ': {'bias': 'WESTERN_MEDIA', 'reliability': 0.95, 'type': 'OFFICIAL'},
    'noel_reports': {'bias': 'WESTERN_MEDIA', 'reliability': 0.75, 'type': 'JOURNALIST'},
    'ChrisO_wiki': {'bias': 'WESTERN_MEDIA', 'reliability': 0.80, 'type': 'ANALYST'},
    'Majakovsk73': {'bias': 'ANALYST', 'reliability': 0.85, 'type': 'BLOGGER'},
    'parabellumcommunity': {'bias': 'ANALYST', 'reliability': 0.80, 'type': 'BLOGGER'},
    'UkraineWarReports': {'bias': 'PRO_UA', 'reliability': 0.70, 'type': 'UA_NEWS'},
    'myro_shnykov': {'bias': 'PRO_UA', 'reliability': 0.75, 'type': 'UA_NEWS'},

    # NEW_CHANNELS
    'supernova_plus': {'bias': 'PRO_UA', 'reliability': 0.85, 'type': 'AGGREGATOR'},
    'dva_majors': {'bias': 'PRO_RU', 'reliability': 0.45, 'type': 'MILBLOGGER'},
    'sternenko': {'bias': 'PRO_UA', 'reliability': 0.70, 'type': 'MILBLOGGER'},
    'astrapress': {'bias': 'NEUTRAL', 'reliability': 0.90, 'type': 'OSINT'},
    'sashakots': {'bias': 'PRO_RU', 'reliability': 0.30, 'type': 'PROPAGANDA'},
    'ab3army': {'bias': 'PRO_UA', 'reliability': 0.95, 'type': 'MILITARY_OFFICIAL'},
    'wargonzo': {'bias': 'PRO_RU', 'reliability': 0.35, 'type': 'MILBLOGGER'},
    'insiderUKR': {'bias': 'PRO_UA', 'reliability': 0.60, 'type': 'AGGREGATOR'},
    'Sladkov_plus': {'bias': 'PRO_RU', 'reliability': 0.40, 'type': 'MILBLOGGER'},
    'lachentyt': {'bias': 'PRO_UA', 'reliability': 0.75, 'type': 'MILBLOGGER'},
    'ButusovPlus': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'MILBLOGGER'},
    'moscowcalling': {'bias': 'NEUTRAL', 'reliability': 0.75, 'type': 'OSINT'},
    'brygada47': {'bias': 'PRO_UA', 'reliability': 0.95, 'type': 'MILITARY_OFFICIAL'},
    'rusich_army': {'bias': 'PRO_RU', 'reliability': 0.50, 'type': 'PARTISAN'},
    'kherson_non_fake': {'bias': 'PRO_UA', 'reliability': 0.85, 'type': 'OSINT'},
    'vysokygovorit': {'bias': 'PRO_RU', 'reliability': 0.55, 'type': 'MILBLOGGER'},
    'exilenova_plus': {'bias': 'PRO_UA', 'reliability': 0.80, 'type': 'OSINT'},
    'CITeam': {'bias': 'NEUTRAL', 'reliability': 0.92, 'type': 'OSINT'},
    'batalyon_monaco': {'bias': 'PRO_UA', 'reliability': 0.50, 'type': 'HUMANITARIAN'},
    'z_komitet': {'bias': 'PRO_RU', 'reliability': 0.40, 'type': 'MILBLOGGER'}
}


def clean_text_content(text):
    if not text:
        return ""
    text_no_emoji = emoji.replace_emoji(text, replace='')
    return " ".join(text_no_emoji.split())


async def fetch_channel_history(client, channel_name, start_date, end_date=None):
    """
    Scarica lo storico di un canale rispettando i limiti di data.
    """
    print(f"\n📡 [CONNECT] Analisi canale: {channel_name}...")

    signals_batch = []
    total_channel_saved = 0
    msgs_since_sleep = 0

    meta = CHANNELS_METADATA.get(channel_name, {})
    bias = meta.get('bias', 'UNKNOWN')

    try:
        entity = await client.get_entity(channel_name)

        # Iteriamo i messaggi
        async for message in client.iter_messages(entity, wait_time=1):

            # --- PROTEZIONE FLOOD ---
            msgs_since_sleep += 1
            if msgs_since_sleep >= SLEEP_EVERY_N_MSGS:
                sleep_time = random.uniform(MIN_SLEEP, MAX_SLEEP)
                await asyncio.sleep(sleep_time)
                msgs_since_sleep = 0

            # 1. Controllo Validità Messaggio
            if not message.date:
                continue

            # Data del messaggio (Aware)
            msg_date = message.date

            # STOP se andiamo troppo indietro nel passato
            if msg_date < start_date:
                print(
                    f"   🛑 Raggiunta data limite ({start_date.date()}). Stop canale.")
                break

            # SKIP se il messaggio è troppo recente (oltre la end_date richiesta)
            if end_date and msg_date > end_date:
                continue

            # 2. PULIZIA TESTO
            raw_text = message.text or ""
            cleaned_text = clean_text_content(raw_text)

            if len(cleaned_text) < 20:
                continue

            # 3. Preparazione Dati per DB Manager
            # Adattiamo il formato al 'save_raw_events' standard
            # Nota: save_raw_events si aspetta {'text', 'source', 'type', 'date'}

            # Formattiamo la data come stringa per il DB
            date_str = msg_date.strftime("%Y-%m-%d %H:%M:%S")

            event_obj = {
                'text': cleaned_text,       # Testo pulito
                'source': channel_name,     # Nome canale
                'type': 'TELEGRAM',         # Tipo fonte
                'date': date_str,           # Data stringa
                # I metadati extra li mettiamo nel testo o li ignoriamo per ora
                # (Il DB raw_events.db attuale è semplice)
            }

            signals_batch.append(event_obj)

            # 4. Salvataggio Batch
            if len(signals_batch) >= DB_BATCH_SIZE:
                # Usiamo la funzione importata
                saved = save_raw_events(signals_batch)
                total_channel_saved += saved
                signals_batch = []
                sys.stdout.write(
                    f"\r   📥 {channel_name}: Salvati {total_channel_saved} msg...")
                sys.stdout.flush()

        # Salvataggio residui
        if signals_batch:
            saved = save_raw_events(signals_batch)
            total_channel_saved += saved
            print(
                f"\r   📥 {channel_name}: Salvati {total_channel_saved} msg...")

    except FloodWaitError as e:
        print(f"\n   ⚠️ FLOOD WAIT: Aspetto {e.seconds} secondi.")
        await asyncio.sleep(e.seconds + 5)
    except Exception as e:
        print(f"\n   ❌ Errore su {channel_name}: {e}")

    print(f"   ✅ Finito {channel_name}. Totale salvati: {total_channel_saved}")


# --- ENTRY POINT PER GLI SCRIPT ESTERNI ---
async def _run_scraper_async(start_date, end_date):
    if not API_ID or not API_HASH:
        print("❌ ERRORE TELEGRAM: API_ID o API_HASH mancanti nel file .env")
        return

    # Percorso sessione
    session_path = 'osint_session'

    async with TelegramClient(session_path, API_ID, API_HASH) as client:
        # Autenticazione (se serve)
        if not await client.is_user_authorized():
            # Nota: Questo funziona male in script automatici se richiede input.
            # La prima volta va lanciato a mano per creare il file .session
            print("⚠️ RICHIESTA AUTENTICAZIONE UTENTE (Primo avvio)")
            await client.start(phone=PHONE)

        print(
            f"🚀 TELEGRAM SCRAPER: {start_date.date()} -> {end_date.date() if end_date else 'Oggi'}")

        for channel in CHANNELS_METADATA.keys():
            await fetch_channel_history(client, channel, start_date, end_date)
            await asyncio.sleep(2)  # Pausa tra canali


def run_telegram_scraper(start_date, end_date=None):
    """
    Funzione wrapper sincrona chiamata da run_backfill.py
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Siamo già in un loop (raro qui, ma possibile)
            asyncio.create_task(_run_scraper_async(start_date, end_date))
        else:
            loop.run_until_complete(_run_scraper_async(start_date, end_date))
    except RuntimeError:
        # Fallback per nuovi event loop
        asyncio.run(_run_scraper_async(start_date, end_date))


if __name__ == "__main__":
    # Test manuale se lanci il file direttamente
    # Usa una data fittizia per test
    test_start = datetime(2025, 12, 25, tzinfo=timezone.utc)
    run_telegram_scraper(test_start)
