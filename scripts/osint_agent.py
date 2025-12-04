from datetime import datetime, timedelta
from dotenv import load_dotenv
import nest_asyncio
from openai import OpenAI
from telethon import TelegramClient
from datetime import datetime
import shutil
import gzip
import requests
import re
import feedparser
import random
import os
import json
import asyncio
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError
print("🔴 SONO IL FILE CORRETTO v3.0 🔴")

# ==========================================
# ⚙️ CONFIGURAZIONE PERCORSI
# ==========================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_FILE_PATH = os.path.join(SCRIPT_DIR, 'osint_session')

# --- CONFIGURAZIONE VS CODE LOCALE ---
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
env_path = os.path.join(ROOT_DIR, '.env')
print(f"🔍 Caricamento .env: {env_path}")
load_dotenv(env_path)

# Gestione Percorsi Dati
possible_paths = [
    os.path.join(SCRIPT_DIR, '..', 'assets', 'data', 'events.geojson'),
    os.path.join(SCRIPT_DIR, '..', 'data', 'events.geojson'),
    os.path.join(SCRIPT_DIR, '..', 'events.geojson')
]
DATA_FILE = None
for path in possible_paths:
    if os.path.exists(path):
        DATA_FILE = os.path.abspath(path)
        break
if not DATA_FILE:
    DATA_FILE = os.path.abspath(os.path.join(
        SCRIPT_DIR, '..', 'assets', 'data', 'events.geojson'))
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)

# Cartella base per salvare le mappe
DATA_DIR = os.path.dirname(DATA_FILE)

# ==========================================
# ⚙️ CREDENZIALI
# ==========================================
try:
    TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID'))
except:
    TELEGRAM_API_ID = 0
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
ALERT_CHAT_ID = os.getenv('ALERT_CHAT_ID')

# ------------------------------------------
# 🎯 LISTE TARGET (INALTERATE)
# ------------------------------------------

TELEGRAM_CHANNELS = [
    'deepstatemap',
    'rybar',
    'WarMonitors',
    'CinCA_AFU',
    'britishmi6',
    'noel_reports',
    'MAKS23_NAFO',
    'Majakovsk73',
    'fighter_bomber',
    'lost_armour',
    'GeoConfirmed',
    'PlayfraOSINT',
    'karymat',
    'DeepStateUA',
    'parabellumcommunity',
    'DroneBomber',
    'bpo_com'
]

TWITTER_ACCOUNTS = [
    'DefenceHQ',
    'ISW',
    'Osinttechnical',
    'ChrisO_wiki',
    'Tatarigami_UA',
    'clement_molin',
    'Mylovanov',
    '414magyarbirds',
    'wartranslated',
    'Maks_NAFO_FELLA',
    'Playfra0',
    'Rebel44CZ'
]

NITTER_INSTANCES = [
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.lucabased.xyz"
]

# ==========================================
# 🗺️ MAP DOWNLOADER SYSTEM (SAFE VERSION)
# ==========================================


def save_geojson(path, data):
    """Salva i dati su file in modo sicuro."""
    try:
        with open(path, 'wb') as f:
            f.write(data)
        print(f"   ✅ Salvato: {os.path.basename(path)}")
        return True
    except Exception as e:
        print(f"   ❌ Errore salvataggio {os.path.basename(path)}: {e}")
        return False


def download_file(url, filename, headers=None):
    target_path = os.path.join(DATA_DIR, filename)
    print(f"   ⬇️  Scaricamento {filename}...")

    try:
        # Timeout aumentato a 30s per connessioni lente
        r = requests.get(url, headers=headers, timeout=30)

        # 1. Controllo HTTP Status
        if r.status_code != 200:
            print(f"      ⚠️ Errore HTTP {r.status_code} per {url}")
            return False

        # 2. Controllo Validità JSON (Per evitare di salvare pagine di errore HTML come mappa)
        try:
            r.json()  # Prova a decodificare
        except ValueError:
            print(f"      ⚠️ Il file scaricato non è un JSON valido.")
            return False

        # 3. Salvataggio
        return save_geojson(target_path, r.content)

    except Exception as e:
        print(f"      ⚠️ Fallito download: {e}")
        return False


def create_dummy_map(filename):
    """Crea una mappa vuota per non far crashare il sito se i download falliscono."""
    target_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(target_path):
        print(f"   ⚠️ Creazione mappa vuota di emergenza per {filename}...")
        dummy_data = {
            "type": "FeatureCollection",
            "features": []
        }
        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(dummy_data, f)


# ==========================================
# 🗺️ ROBUST MAP DOWNLOADER v3.1 - OPTIMIZED
# ==========================================
# Based on real-world testing: DeepState archive works, ISW needs alternatives


def download_with_retry(url, headers=None, max_retries=2, timeout=30):
    """
    Downloads a file with retry logic and proper error handling.
    Returns: (success: bool, content: bytes or None)
    """
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers,
                             timeout=timeout, allow_redirects=True)

            if r.status_code == 200:
                return True, r.content
            else:
                if attempt == max_retries - 1:  # Only print on last attempt
                    print(f"      ⚠️ HTTP {r.status_code}")

        except requests.exceptions.Timeout:
            if attempt == max_retries - 1:
                print(f"      ⚠️ Timeout")
        except requests.exceptions.ConnectionError:
            if attempt == max_retries - 1:
                print(f"      ⚠️ Connection Error")
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"      ⚠️ Error: {e}")

        if attempt < max_retries - 1:
            time.sleep(1)

    return False, None


def validate_geojson(content):
    """
    Validates that content is valid GeoJSON.
    Returns: (valid: bool, data: dict or None)
    """
    try:
        if isinstance(content, bytes):
            data = json.loads(content.decode('utf-8'))
        else:
            data = json.loads(content)

        if data.get('type') not in ['FeatureCollection', 'Feature']:
            return False, None

        if data.get('type') == 'FeatureCollection' and 'features' not in data:
            return False, None

        return True, data

    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return False, None


def save_geojson_safe(path, content, validate=True):
    """
    Safely saves GeoJSON with validation and backup.
    """
    try:
        if validate:
            valid, data = validate_geojson(content)
            if not valid:
                print(f"      ⚠️ Invalid GeoJSON format")
                return False

        # Create backup of existing file
        if os.path.exists(path):
            backup_path = path + '.backup'
            shutil.copy2(path, backup_path)

        # Write new file
        with open(path, 'wb') as f:
            if isinstance(content, bytes):
                f.write(content)
            else:
                f.write(json.dumps(content, ensure_ascii=False,
                        indent=2).encode('utf-8'))

        print(f"      ✅ Saved: {os.path.basename(path)}")

        # Remove backup if successful
        if os.path.exists(path + '.backup'):
            os.remove(path + '.backup')

        return True

    except Exception as e:
        print(f"      ❌ Save failed: {e}")

        # Restore backup if it exists
        backup_path = path + '.backup'
        if os.path.exists(backup_path):
            shutil.copy2(backup_path, path)
            os.remove(backup_path)
            print(f"      🔄 Restored from backup")

        return False


def create_emergency_geojson(filename):
    """
    Creates a valid but empty GeoJSON file as fallback.
    """
    target_path = os.path.join(DATA_DIR, filename)

    emergency_data = {
        "type": "FeatureCollection",
        "features": [],
        "properties": {
            "source": "emergency_fallback",
            "created": datetime.now().isoformat(),
            "note": "Empty fallback file - original source unavailable"
        }
    }

    try:
        with open(target_path, 'w', encoding='utf-8') as f:
            json.dump(emergency_data, f, indent=2, ensure_ascii=False)
        print(f"      ⚠️ Created emergency fallback: {filename}")
        return True
    except Exception as e:
        print(f"      ❌ Failed to create emergency file: {e}")
        return False


def extract_latest_from_geojson(data):
    """
    Extracts the most recent feature from a GeoJSON with historical data.
    Returns a clean FeatureCollection with just the latest state.
    """
    try:
        if not data.get('features'):
            return None

        # If features have a 'date' property, sort by it
        features = data['features']

        # Try to find the latest date
        dated_features = []
        for f in features:
            props = f.get('properties', {})
            date_str = props.get('date') or props.get(
                'Date') or props.get('update_date')

            if date_str:
                try:
                    # Parse various date formats
                    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']:
                        try:
                            date_obj = datetime.strptime(
                                str(date_str).split()[0], fmt)
                            dated_features.append((date_obj, f))
                            break
                        except:
                            continue
                except:
                    pass

        if dated_features:
            # Sort by date and take the latest
            dated_features.sort(key=lambda x: x[0], reverse=True)
            latest_feature = dated_features[0][1]
        else:
            # No dates found, just take the last feature
            latest_feature = features[-1]

        return {
            "type": "FeatureCollection",
            "features": [latest_feature],
            "properties": {
                "source": data.get('properties', {}).get('source', 'Unknown'),
                "extracted": datetime.now().isoformat(),
                "note": "Latest entry extracted from historical dataset"
            }
        }

    except Exception as e:
        print(f"      ⚠️ Extraction error: {e}")
        return None


def download_deepstate_map():
    """
    Downloads DeepState frontline data from cyterat's GitHub compressed archive.
    This is the ONLY reliable DeepState source (daily files get 404).
    """
    print("\n   📡 DeepState Download (cyterat/compressed archive)")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, application/octet-stream, */*"
    }

    # Strategy: Use compressed archive (ONLY working method)
    archive_url = "https://github.com/cyterat/deepstate-map-data/raw/main/deepstate-map-data.geojson.gz"

    print(f"      Downloading compressed archive...")
    success, content = download_with_retry(archive_url, headers, max_retries=3)

    if success:
        try:
            # Decompress gzip
            print(f"      Decompressing...")
            decompressed = gzip.decompress(content)

            # Validate
            valid, data = validate_geojson(decompressed)

            if valid and data.get('features'):
                print(
                    f"      Found {len(data['features'])} historical entries")

                # Extract latest entry
                frontline_data = extract_latest_from_geojson(data)

                if frontline_data:
                    frontline_data['properties']['source'] = 'DeepState (cyterat archive)'
                    frontline_data['properties']['url'] = archive_url

                    target_path = os.path.join(DATA_DIR, 'frontline.geojson')
                    if save_geojson_safe(target_path, json.dumps(frontline_data, indent=2).encode('utf-8')):
                        return True

        except Exception as e:
            print(f"      ⚠️ Processing failed: {e}")

    return False


def download_isw_map():
    """
    Downloads ISW frontline data using multiple fallback sources.
    Priority order based on reliability and update frequency.
    """
    print("\n   📡 ISW Download (Multi-source fallback)")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, application/geo+json, */*",
        "Referer": "https://storymaps.arcgis.com/"
    }

    # Priority sources for ISW data
    sources = [
        {
            "name": "ISW ArcGIS FeatureServer",
            "url": "https://services3.arcgis.com/6JC1jVGYHYnfDhvk/arcgis/rest/services/Ukraine_Control_All/FeatureServer/0/query?where=1%3D1&outFields=*&f=geojson",
            "needs_processing": False
        },
        {
            "name": "War Mapper (Aggregated ISW)",
            "url": "https://raw.githubusercontent.com/War-Mapper/War-Mapper-Data/main/ukraine-control.geojson",
            "needs_processing": False
        },
        {
            "name": "UaWarData (Henry Schlottman)",
            "url": "https://raw.githubusercontent.com/simonhuwiler/uawardata/main/data/units_current.geojson",
            "needs_processing": True,  # This is unit positions, not frontlines
            "fallback": True
        }
    ]

    for source in sources:
        if source.get('fallback'):
            print(f"      Trying FALLBACK: {source['name']}")
        else:
            print(f"      Trying: {source['name']}")

        success, content = download_with_retry(
            source['url'], headers, max_retries=2)

        if success:
            valid, data = validate_geojson(content)

            if valid:
                # Add metadata
                if 'properties' not in data:
                    data['properties'] = {}

                data['properties']['source'] = source['name']
                data['properties']['download_time'] = datetime.now().isoformat()

                target_path = os.path.join(DATA_DIR, 'frontline_isw.geojson')
                if save_geojson_safe(target_path, json.dumps(data, indent=2).encode('utf-8')):
                    return True

    return False


def check_file_age(filepath, max_age_days=7):
    """
    Checks if a file exists and is recent enough to use.
    Returns: (exists, age_days, is_recent)
    """
    if not os.path.exists(filepath):
        return False, None, False

    age_seconds = datetime.now().timestamp() - os.path.getmtime(filepath)
    age_days = age_seconds / 86400
    is_recent = age_days <= max_age_days

    return True, age_days, is_recent


def update_maps():
    """
    Main update function with comprehensive error handling and smart fallback logic.
    """
    print(f"\n{'='*60}")
    print(
        f"🗺️  MAP DOWNLOADER v3.1 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    results = {
        'deepstate': False,
        'isw': False,
        'deepstate_cached': False,
        'isw_cached': False
    }

    ds_path = os.path.join(DATA_DIR, 'frontline.geojson')
    isw_path = os.path.join(DATA_DIR, 'frontline_isw.geojson')

    # Check existing files
    ds_exists, ds_age, ds_recent = check_file_age(ds_path, max_age_days=7)
    isw_exists, isw_age, isw_recent = check_file_age(isw_path, max_age_days=7)

    # 1. Download DeepState
    try:
        results['deepstate'] = download_deepstate_map()
    except Exception as e:
        print(f"   ❌ DeepState download failed: {e}")

    # 2. Download ISW
    try:
        results['isw'] = download_isw_map()
    except Exception as e:
        print(f"   ❌ ISW download failed: {e}")

    # 3. Smart Fallback Logic
    print(f"\n   📊 Results Summary:")

    # Handle DeepState
    if results['deepstate']:
        print(f"      ✅ DeepState: Downloaded successfully")
    elif ds_exists:
        if ds_recent:
            print(
                f"      ⚠️ DeepState: Using cached file (age: {ds_age:.1f} days)")
            results['deepstate_cached'] = True
        else:
            print(
                f"      ⚠️ DeepState: Cached file is OLD (age: {ds_age:.1f} days)")
            results['deepstate_cached'] = True
    else:
        print(f"      ❌ DeepState: Creating emergency file")
        create_emergency_geojson('frontline.geojson')

    # Handle ISW
    if results['isw']:
        print(f"      ✅ ISW: Downloaded successfully")
    elif isw_exists:
        if isw_recent:
            print(f"      ⚠️ ISW: Using cached file (age: {isw_age:.1f} days)")
            results['isw_cached'] = True
        else:
            print(
                f"      ⚠️ ISW: Cached file is OLD (age: {isw_age:.1f} days)")
            results['isw_cached'] = True
    elif results['deepstate'] or ds_exists:
        # Copy DeepState to ISW
        print(f"      🔄 ISW: Copying DeepState as fallback")
        shutil.copy2(ds_path, isw_path)
    else:
        print(f"      ❌ ISW: Creating emergency file")
        create_emergency_geojson('frontline_isw.geojson')

    print(f"{'='*60}\n")

    return results


# ==========================================
# 🔧 INTEGRATION EXAMPLE
# ==========================================

if __name__ == '__main__':
    # For testing purposes
    import requests
    import time

    # Set your data directory
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'assets', 'data')
    os.makedirs(DATA_DIR, exist_ok=True)

    # Run the update
    results = update_maps()

    # Print final status
    print("📋 Final Status:")
    print(
        f"   DeepState: {'✅ Fresh' if results['deepstate'] else ('⚠️ Cached' if results['deepstate_cached'] else '❌ Failed')}")
    print(
        f"   ISW: {'✅ Fresh' if results['isw'] else ('⚠️ Cached' if results['isw_cached'] else '❌ Failed')}")


# ==========================================
# 🧠 AI ENGINE
# ==========================================
client_ai = OpenAI(api_key=OPENAI_API_KEY)


def analyze_with_ai(text, source, platform, media_url=None):
    if len(text) < 30:
        return None
    print(f"   🤖 AI Analizza ({len(text)} chars) da {source}...")

    prompt = f"""
    Sei un Senior Intelligence Analyst specializzato nel conflitto Russo-Ucraino.
    Analizza questo testo grezzo da {source} ({platform}): "{text}"

    REGOLE DI FILTRO (CRUCIALE):
    - Se il testo NON riguarda la guerra Russia-Ucraina (es. Gaza, Taiwan, calcio, pubblicità), restituisci NULL (JSON vuoto).
    
    COMPITI DI ANALISI:
    1. TRADUZIONE: Italiano professionale, stile militare conciso.
    2. BIAS DETECTOR: Analizza il tono.
    3. NSFW CHECK: Se il testo descrive cadaveri, sangue, decapitazioni o violenza grafica estrema, imposta nsfw=true.
    4. GEOLOCALIZZAZIONE: Lat/Lon stimate.
    5. CLASSIFICAZIONE: [ground, air, missile, drone, artillery, naval, strategic, civil].
    6. INTENSITÀ: 0.1 (basso) -> 1.0 (nucleare/strategico).

    OUTPUT JSON (Strettamente questo formato):
    {{
        "valid": true,
        "title": "Titolo breve (max 60 chars)",
        "description": "Report completo tradotto...",
        "bias_note": "Analisi del bias...",
        "lat": 0.0,
        "lon": 0.0,
        "type": "ground",
        "intensity": 0.5,
        "actor_code": "UNK", 
        "nsfw": false
    }}
    """

    try:
        response = client_ai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        raw = response.choices[0].message.content.replace(
            "```json", "").replace("```", "").strip()

        if not raw or "null" in raw.lower():
            return None
        data = json.loads(raw)
        if not data.get('valid', True):
            return None

        # Arricchimento Dati
        data['date'] = datetime.now().strftime("%Y-%m-%d")
        data['timestamp'] = int(datetime.now().timestamp() * 1000)
        data['author'] = f"@{source} ({platform})"
        data['before_img'] = media_url if media_url else ""
        data['after_img'] = ""
        data['video'] = "null"

        return data

    except Exception as e:
        print(f"   ❌ Errore AI: {e}")
        return None

# ==========================================
# 🚨 ALERT SYSTEM
# ==========================================


async def send_telegram_alert(client, event):
    if not ALERT_CHAT_ID:
        return
    if event.get('intensity', 0) < 0.9:
        return

    msg = f"🚨 **ALLARME CRITICO**\n\n📍 **{event.get('title', 'N/A')}**\n⚠️ Intensità: {event.get('intensity', 'N/A')}\n🔗 {event.get('author', 'N/A')}"
    try:
        await client.send_message(ALERT_CHAT_ID, msg)
    except:
        pass

# ==========================================
# 🕵️ SCRAPERS
# ==========================================


async def scrape_telegram(existing_ids):
    print("\n📡 Telegram Scraper Avviato (Modalità Sicura)...")
    new_items = []

    # 1. Configurazione Client
    client = TelegramClient(
        SESSION_FILE_PATH, TELEGRAM_API_ID, TELEGRAM_API_HASH)

    try:
        # 2. Connessione e Autenticazione Robusta
        await client.connect()

        if not await client.is_user_authorized():
            print("⚠️ Autenticazione richiesta!")
            phone = input("Inserisci il tuo numero di telefono (+39...): ")
            await client.send_code_request(phone)

            try:
                code = input("Inserisci il codice ricevuto su Telegram: ")
                await client.sign_in(phone, code)
            except SessionPasswordNeededError:
                pw = input("Inserisci la password (2FA): ")
                await client.sign_in(password=pw)

        print("   ✅ Login effettuato con successo.")

        # 3. Scansione "Lenta" (Anti-Ban per account nuovi)
        # Mischia i canali per non fare sempre lo stesso percorso
        channels_shuffled = TELEGRAM_CHANNELS.copy()
        random.shuffle(channels_shuffled)

        for i, channel in enumerate(channels_shuffled):
            print(f"   [{i+1}/{len(channels_shuffled)}] Accesso a @{channel}...")

            try:
                msg_count = 0
                # SCARICA SOLO GLI ULTIMI 15 MESSAGGI (Per non stressare l'account nuovo)
                async for msg in client.iter_messages(channel, limit=15):
                    if not msg.text or len(msg.text) < 50:
                        continue

                    uid = f"tg_{channel}_{msg.id}"
                    if uid in existing_ids:
                        continue

                    # Pausa umana tra un messaggio e l'altro (0.5 - 1.5 secondi)
                    await asyncio.sleep(random.uniform(0.5, 1.5))

                    res = analyze_with_ai(msg.text, channel, "Telegram", None)
                    if res:
                        res['original_id'] = uid
                        res['source_url'] = f"https://t.me/{channel}/{msg.id}"
                        new_items.append(res)
                        existing_ids.add(uid)
                        # await send_telegram_alert(client, res) # Disabilita alert in uscita per ora
                        msg_count += 1

                if msg_count > 0:
                    print(f"      ↳ Trovati {msg_count} nuovi eventi.")

            except FloodWaitError as e:
                print(
                    f"      🛑 FLOOD WAIT RILEVATO: Devo dormire per {e.seconds} secondi.")
                await asyncio.sleep(e.seconds)
            except Exception as e:
                print(f"     ⚠️ Skip {channel}: {e}")

            # PAUSA LUNGA TRA I CANALI (5-10 secondi)
            wait_time = random.uniform(5, 10)
            print(f"      ⏳ Pausa caffè di {wait_time:.1f}s...")
            await asyncio.sleep(wait_time)

    except Exception as e:
        print(f"❌ Errore Critico Telegram: {e}")
        print("💡 SUGGERIMENTO: Se è un errore di Timeout o Asyncio, prova a usare Python 3.11 invece del 3.14.")
    finally:
        try:
            await client.disconnect()
        except:
            pass

    return new_items


def scrape_twitter_rss(existing_ids):
    print("\n🐦 Twitter RSS Scraper Avviato (Debug Mode)...")
    new_items = []

    # Lista aggiornata di istanze Nitter che spesso funzionano meglio
    # Puoi aggiungerne altre da: https://status.d420.de/
    active_instances = [
        "https://nitter.privacydev.net",
        "https://nitter.poast.org",
        "https://nitter.lucabased.xyz",
        "https://nitter.cz",
        "https://nitter.net"
    ]

    for user in TWITTER_ACCOUNTS:
        print(f"   🔍 Controllo {user}...")
        success = False

        for instance in active_instances:
            try:
                url = f"{instance}/{user}/rss"
                # Timeout breve per non bloccare tutto se l'istanza è lenta
                feed = feedparser.parse(url)

                if feed.bozo:  # feedparser segnala errori bozo se l'XML è rotto
                    # print(f"      ❌ {instance}: XML non valido o blocco anti-bot.")
                    continue

                if not feed.entries:
                    # print(f"      ⚠️ {instance}: Nessun tweet trovato (Feed vuoto).")
                    continue

                print(
                    f"      ✅ {instance}: Trovati {len(feed.entries)} tweet. Analisi...")

                # Analizza solo i 10 più recenti
                for entry in feed.entries[:10]:
                    uid = f"tw_{user}_{entry.id}"

                    if uid in existing_ids:
                        # print(f"         - {uid} già presente.")
                        continue

                    text = entry.summary.replace("<br>", "\n")

                    # Estrazione immagine se presente
                    img_url = None
                    if 'img src="' in entry.summary:
                        try:
                            img_url = entry.summary.split(
                                'img src="')[1].split('"')[0]
                        except:
                            pass

                    # Analisi AI
                    res = analyze_with_ai(text, user, "X", img_url)

                    if res:
                        print(
                            f"         ✨ NUOVO EVENTO RILEVATO: {res['title']}")
                        res['original_id'] = uid
                        res['source_url'] = entry.link
                        new_items.append(res)
                        existing_ids.add(uid)
                    else:
                        # print(f"         - Scartato dall'AI (Non rilevante).")
                        pass

                success = True
                break  # Se un'istanza funziona, passa al prossimo utente

            except Exception as e:
                # print(f"      ❌ Errore connessione {instance}: {e}")
                continue

        if not success:
            print(f"      ⚠️ Impossibile leggere @{user} da nessuna istanza.")

    return new_items

# ==========================================
# 🚀 MAIN (FIXED CRASH)
# ==========================================


async def main():
    print("=== 🌍 IMPACT ATLAS AGENT v2.2 (Anti-Crash) ===")

    # 1. Aggiorna Mappe
    update_maps()

    # 2. Carica DB
    existing_ids = set()
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            geojson = json.load(f)
            for f in geojson['features']:
                if 'original_id' in f['properties']:
                    existing_ids.add(f['properties']['original_id'])
            print(f"📂 Database caricato: {len(geojson['features'])} eventi.")
    else:
        geojson = {"type": "FeatureCollection", "features": []}

    # 3. Scraping
    tg = await scrape_telegram(existing_ids)
    # tw = scrape_twitter_rss(existing_ids) # DISABILITATO TEMPORANEAMENTE
    total = tg  # + tw

    if total:
        print(f"\n💾 Salvataggio {len(total)} nuovi rapporti intelligence...")
        count_saved = 0

        for item in total:
            props = item.copy()

            # --- FIX ANTI-CRASH ---
            # Usa .pop con default (None) per evitare KeyError se l'AI sbaglia
            lat = props.pop('lat', None)
            lon = props.pop('lon', None)

            # Se mancano le coordinate o sono None, salta l'evento o metti 0
            if lat is None or lon is None:
                print(
                    f"   ⚠️ Evento scartato (coordinate mancanti): {props.get('title', 'Senza titolo')}")
                continue
            # ----------------------

            geojson['features'].append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props
            })
            count_saved += 1

        if count_saved > 0:
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(geojson, f, indent=2, ensure_ascii=False)
            print(
                f"✅ DATABASE SALVATO CORRETTAMENTE ({count_saved} eventi aggiunti).")
        else:
            print("⚠️ Nessun evento valido da salvare.")
    else:
        print("\n💤 Nessun nuovo evento rilevante.")

if __name__ == '__main__':
    nest_asyncio.apply()
    asyncio.run(main())
