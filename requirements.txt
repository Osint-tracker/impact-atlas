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


def update_maps():
    print(f"\n🗺️ AVVIO AGGIORNAMENTO MAPPE (Safe Mode)...")

    # Header per sembrare un browser vero (Cruciale per DeepState)
    headers_browser = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }

    # --- 1. DEEPSTATE (Priorità API -> Fallback Mirror) ---
    ds_success = False

    # Tentativo A: API Ufficiale
    try:
        print("   📡 Tentativo 1: DeepState API...")
        # 1. Ottieni l'ID dell'ultima versione
        hist_url = "https://deepstatemap.live/api/history"
        r_hist = requests.get(hist_url, headers=headers_browser, timeout=10)

        if r_hist.status_code == 200:
            latest_id = r_hist.json()[0]['id']
            # 2. Scarica il GeoJSON specifico
            ds_url = f"https://deepstatemap.live/api/history/{latest_id}/geojson"
            ds_success = download_file(
                ds_url, 'frontline.geojson', headers_browser)
    except Exception as e:
        print(f"      API Error: {e}")

    # Tentativo B: Mirror (Se API fallisce)
    if not ds_success:
        print("   📡 Tentativo 2: DeepState Mirror...")
        # URL corretto senza typo 'daata'
        mirror_url = "https://raw.githubusercontent.com/UaMap/data/main/data.geojson"
        ds_success = download_file(mirror_url, 'frontline.geojson')

    # Se tutto fallisce, crea file vuoto per non rompere il sito
    if not ds_success:
        create_dummy_map('frontline.geojson')

    # --- 2. ISW (Mirror Lee Drake) ---
    print("   📡 Scaricamento ISW Data...")
    # URL corretto senza typo 'leedrakee5'
    isw_url = "https://raw.githubusercontent.com/leedrake5/Russia-Ukraine/master/data/russia_ukraine.geojson"
    isw_success = download_file(isw_url, 'frontline_isw.geojson')

    # Se fallisce, prova un mirror alternativo
    if not isw_success:
        print("      Fallback su mirror ISW alternativo...")
        alt_isw = "https://raw.githubusercontent.com/OwlDevs/ISW-Data/main/geojson/latest.geojson"
        isw_success = download_file(alt_isw, 'frontline_isw.geojson')

    if not isw_success:
        # Se non abbiamo ISW, copiamo DeepState (se esiste) o creiamo vuoto
        if os.path.exists(os.path.join(DATA_DIR, 'frontline.geojson')):
            shutil.copy(os.path.join(DATA_DIR, 'frontline.geojson'),
                        os.path.join(DATA_DIR, 'frontline_isw.geojson'))
            print("      ⚠️ Usata copia DeepState per ISW (Backup).")
        else:
            create_dummy_map('frontline_isw.geojson')

    print("🏁 Mappe gestite.\n")


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
    print("\n📡 Telegram Scraper Avviato (Deep Scan)...")
    new_items = []
    try:
        async with TelegramClient(SESSION_FILE_PATH, TELEGRAM_API_ID, TELEGRAM_API_HASH) as client:
            for channel in TELEGRAM_CHANNELS:
                print(f"   ↳ @{channel}...")
                try:
                    async for msg in client.iter_messages(channel, limit=50):
                        if not msg.text or len(msg.text) < 50:
                            continue
                        uid = f"tg_{channel}_{msg.id}"
                        if uid in existing_ids:
                            continue

                        res = analyze_with_ai(
                            msg.text, channel, "Telegram", None)
                        if res:
                            res['original_id'] = uid
                            res['source_url'] = f"https://t.me/{channel}/{msg.id}"
                            new_items.append(res)
                            existing_ids.add(uid)
                            await send_telegram_alert(client, res)
                except Exception as e:
                    print(f"     ⚠️ Errore {channel}: {e}")
    except Exception as e:
        print(f"❌ Errore Login Telegram: {e}")
    return new_items


def scrape_twitter_rss(existing_ids):
    print("\n🐦 Twitter RSS Scraper Avviato...")
    new_items = []
    for user in TWITTER_ACCOUNTS:
        for instance in NITTER_INSTANCES:
            try:
                feed = feedparser.parse(f"{instance}/{user}/rss")
                if not feed.entries:
                    continue
                for entry in feed.entries[:50]:
                    text = entry.summary.replace("<br>", "\n")
                    uid = f"tw_{user}_{entry.id}"
                    if uid in existing_ids:
                        continue

                    img_url = None
                    if 'img src="' in entry.summary:
                        try:
                            img_url = entry.summary.split(
                                'img src="')[1].split('"')[0]
                        except:
                            pass

                    res = analyze_with_ai(text, user, "X", img_url)
                    if res:
                        res['original_id'] = uid
                        res['source_url'] = entry.link
                        new_items.append(res)
                        existing_ids.add(uid)
                break
            except:
                continue
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
    tw = scrape_twitter_rss(existing_ids)
    total = tg + tw

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
