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

# --- SETUP IMPORT ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    from war_tracker_v2.ingestion.db_manager import save_batch_signals
    from dotenv import load_dotenv
except ImportError:
    sys.path.append(os.path.join(parent_dir, 'scripts'))
    from war_tracker_v2.ingestion.db_manager import save_batch_signals
    from dotenv import load_dotenv

load_dotenv(os.path.join(parent_dir, '.env'))

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# --- CONFIGURAZIONE AVANZATA ---

# Data limite (Qui puoi impostare 2024 se vuoi scaricare meno roba)
CUTOFF_DATE = datetime(2025, 12, 23, tzinfo=timezone.utc)

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
    'myro_shnykov': {'bias': 'PRO_UA', 'reliability': 0.75, 'type': 'UA_NEWS'}
}


def clean_text_content(text):
    """
    Rimuove emoji e spazi extra.
    Esempio: "Attack on Kyiv! 🚀💥" -> "Attack on Kyiv!"
    """
    if not text:
        return ""
    # Rimuove le emoji
    text_no_emoji = emoji.replace_emoji(text, replace='')
    # Rimuove spazi multipli e trimma
    return " ".join(text_no_emoji.split())


def generate_hash(date_str, text, source):
    """Crea hash univoco per il messaggio basandosi sul testo PULITO."""
    # Nota: text qui è già pulito dalle emoji
    clean_text = str(text).strip()[:100]
    raw_str = f"{date_str}|{source}|{clean_text}"
    return hashlib.md5(raw_str.encode('utf-8')).hexdigest()


async def fetch_channel_history(client, channel_name):
    print(f"\n📡 [CONNECT] Analisi canale: {channel_name}...")

    signals_batch = []
    total_channel_saved = 0
    msgs_since_sleep = 0

    meta = CHANNELS_METADATA.get(channel_name, {})
    bias = meta.get('bias', 'UNKNOWN')

    try:
        entity = await client.get_entity(channel_name)

        async for message in client.iter_messages(entity, wait_time=1):

            # --- PROTEZIONE FLOOD ---
            msgs_since_sleep += 1
            if msgs_since_sleep >= SLEEP_EVERY_N_MSGS:
                sleep_time = random.uniform(MIN_SLEEP, MAX_SLEEP)
                await asyncio.sleep(sleep_time)
                msgs_since_sleep = 0

            # 1. Controllo Data
            if not message.date:
                continue

            if message.date < CUTOFF_DATE:
                print(
                    f"   🛑 Data limite ({CUTOFF_DATE.strftime('%Y-%m-%d')}) raggiunta. Stop canale.")
                break

            # 2. PULIZIA TESTO (Rimuoviamo emoji PRIMA di controllare la lunghezza)
            raw_text = message.text or ""
            cleaned_text = clean_text_content(raw_text)

            # 3. Controllo Contenuto (Sul testo pulito!)
            # Se un messaggio era solo emoji, cleaned_text sarà vuoto e verrà scartato.
            if len(cleaned_text) < 20:
                continue

            # 4. Preparazione Dati
            date_str = message.date.strftime("%Y%m%d%H%M%S")

            # Generiamo l'hash sul testo pulito per consistenza futura
            h = generate_hash(date_str, cleaned_text, channel_name)

            has_media = 0
            if isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument)):
                has_media = 1

            signal = {
                'hash': h,
                'type': 'TELEGRAM',
                'source': channel_name,
                'date': date_str,
                'text': cleaned_text,  # Salviamo il testo SENZA emoji
                'url': f"https://t.me/{channel_name}/{message.id}",
                'lat': None,
                'lon': None,
                'media_has_video': has_media
            }

            signals_batch.append(signal)

            # 5. Salvataggio Batch
            if len(signals_batch) >= DB_BATCH_SIZE:
                saved = save_batch_signals(signals_batch)
                total_channel_saved += saved
                signals_batch = []
                sys.stdout.write(
                    f"\r   📥 {channel_name} ({bias}): Salvati {total_channel_saved} messaggi (No Emoji)...")
                sys.stdout.flush()

        if signals_batch:
            saved = save_batch_signals(signals_batch)
            total_channel_saved += saved
            print(
                f"\r   📥 {channel_name} ({bias}): Salvati {total_channel_saved} messaggi (No Emoji)...")

    except FloodWaitError as e:
        print(
            f"\n   ⚠️ FLOOD WAIT RILEVATO: Telegram chiede di aspettare {e.seconds} secondi.")
        print(f"   💤 Dormo per {e.seconds + 5} secondi per sicurezza...")
        await asyncio.sleep(e.seconds + 5)

    except (ValueError, ChannelPrivateError, UsernameInvalidError) as e:
        print(
            f"\n   ❌ Errore Canale '{channel_name}': Non trovato o Privato. Salto.")
    except Exception as e:
        print(f"\n   ❌ Errore generico su {channel_name}: {e}")

    print(f"   ✅ Finito {channel_name}. Totale nel DB: {total_channel_saved}")


async def main():
    if not API_ID or not API_HASH:
        print("❌ ERRORE: Credenziali mancanti nel .env")
        return

    session_path = os.path.join(parent_dir, 'data', 'telegram_session')

    async with TelegramClient(session_path, API_ID, API_HASH) as client:
        print("🔐 Autenticazione Telegram...")
        if not await client.is_user_authorized():
            await client.send_code_request(PHONE)
            code = input('Inserisci codice Telegram: ')
            try:
                await client.sign_in(PHONE, code)
            except SessionPasswordNeededError:
                pw = input('Inserisci password 2FA: ')
                await client.sign_in(password=pw)

        print(f"🚀 Inizio Ingestione da {len(CHANNELS_METADATA)} canali...")
        print(f"🚫 Filtro Emoji: ATTIVO")
        print(f"📅 Data Inizio: {CUTOFF_DATE.strftime('%Y-%m-%d')}")

        for channel_name in CHANNELS_METADATA.keys():
            await fetch_channel_history(client, channel_name)
            await asyncio.sleep(3)

        print("\n🏁 Ingestione Telegram Completata.")

if __name__ == "__main__":
    asyncio.run(main())
