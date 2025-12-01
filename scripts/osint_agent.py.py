import asyncio
import json
import os
import random
import feedparser
import re
import requests
import gzip
import shutil
from datetime import datetime
from telethon import TelegramClient
from openai import OpenAI
import nest_asyncio

# ==========================================
# ⚙️ CONFIGURAZIONE PERCORSI
# ==========================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_FILE_PATH = os.path.join(SCRIPT_DIR, 'osint_session')

# Cerca il file dati
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

# Percorso per la Frontline
FRONTLINE_FILE = os.path.join(os.path.dirname(DATA_FILE), 'frontline.geojson')

# ==========================================
# ⚙️ CONFIGURAZIONE UTENTE
# ==========================================

# Credenziali (Legge dai Secrets)
try:
    TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID'))
except:
    TELEGRAM_API_ID = 0
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Canale per ALERTS (Opzionale)
ALERT_CHAT_ID = os.getenv('ALERT_CHAT_ID')

# ------------------------------------------
# 🎯 LISTE TARGET (AGGIORNATE)
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
    # Correzione per "БПО | Братья по оружию" (usare username, non titolo)
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
# 🗺️ MAP DOWNLOADER (FIXED)
# ==========================================


def update_frontline_data():
    print(f"\n🗺️ Scaricamento Frontline in: {FRONTLINE_FILE}...")
    # URL AGGIORNATO (Branch 'main' invece di 'master' spesso risolve, oppure mirror alternativo)
    # Se questo URL fallisce ancora, DeepState potrebbe aver cambiato API pubblica.
    url = "https://raw.githubusercontent.com/cyterat/deepstate-map-data/main/data/deepstate-map-data.geojson.gz"

    temp_gz = FRONTLINE_FILE + ".gz"

    try:
        # 1. Scarica
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(temp_gz, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        # 2. Decomprimi
        with gzip.open(temp_gz, 'rb') as f_in:
            with open(FRONTLINE_FILE, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # 3. Pulisci
        if os.path.exists(temp_gz):
            os.remove(temp_gz)

        print("✅ Frontline aggiornata e salvata.")
        return True
    except Exception as e:
        print(f"⚠️ Errore aggiornamento frontline: {e}")
        # Non blocchiamo lo script se la mappa fallisce, continuiamo con le news
        return False


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
    2. BIAS DETECTOR: Analizza il tono. La fonte è filo-russa o filo-ucraina? C'è propaganda? Scrivi una frase di analisi (es: "Fonte filo-russa, possibile esagerazione delle perdite nemico").
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

        # Se l'AI decide che non è pertinente
        if not raw or raw == "NULL" or "null" in raw.lower():
            print("      🗑️ Scartato: Non pertinente.")
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
    if event['intensity'] < 0.9:
        return

    msg = f"🚨 **ALLARME CRITICO**\n\n📍 **{event['title']}**\n⚠️ Intensità: {event['intensity']}\n🔗 {event['author']}"
    try:
        await client.send_message(ALERT_CHAT_ID, msg)
    except Exception as e:
        print(f"      ❌ Errore alert: {e}")

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
                    # LIMITE IMPOSTATO A 50 COME RICHIESTO
                    async for msg in client.iter_messages(channel, limit=50):
                        if not msg.text or len(msg.text) < 50:
                            continue
                        uid = f"tg_{channel}_{msg.id}"
                        if uid in existing_ids:
                            continue

                        media = None
                        res = analyze_with_ai(
                            msg.text, channel, "Telegram", media)
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
        success = False
        for instance in NITTER_INSTANCES:
            if success:
                break
            rss_url = f"{instance}/{user}/rss"
            try:
                feed = feedparser.parse(rss_url)
                if not feed.entries:
                    continue
                success = True

                # LIMITE IMPOSTATO A 50 (O massimo disponibile nel feed)
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
            except:
                continue
    return new_items

# ==========================================
# 🚀 MAIN
# ==========================================


async def main():
    print("=== 🌍 IMPACT ATLAS INTELLIGENCE AGENT ===")

    # 1. Aggiorna Frontline
    update_frontline_data()

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
        for item in total:
            props = item.copy()
            lat, lon = props.pop('lat'), props.pop('lon')
            geojson['features'].append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props
            })

        with open(DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2, ensure_ascii=False)
        print("✅ DATABASE AGGIORNATO.")
    else:
        print("\n💤 Nessun nuovo evento rilevante.")

if __name__ == '__main__':
    nest_asyncio.apply()
    asyncio.run(main())
