import os
import sys
import json
import asyncio
import sqlite3
import random
import time
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env')
load_dotenv(env_path)

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'war_tracker_v2', 'data', 'raw_events.db')
MEDIA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'war_tracker_v2', 'data', 'media')
SESSION_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'ingestion', 'telegram_session')

os.makedirs(MEDIA_DIR, exist_ok=True)

# ANTI-BAN CONFIGURATION
MIN_SLEEP = 3.0  # Minimum seconds to wait between downloads
MAX_SLEEP = 7.0  # Maximum seconds to wait between downloads
BATCH_SIZE = 15  # Pause after this many downloads
BATCH_SLEEP = 30 # Seconds to pause after a batch
MAX_DOWNLOADS = 200 # Max total files to download in one run

async def main():
    if not API_ID or not API_HASH:
        print("[FAIL] Missing Telegram API credentials.")
        return

    print("[INFO] Avviando client in modalita SICURA (Anti-Ban)...")
    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        print("[FAIL] Session is not authorized.")
        return
    print("[OK] Autenticato con successo. Inizio estrazione sicura.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT event_id, media_urls FROM unique_events WHERE media_urls IS NOT NULL AND media_urls != '[]' AND media_urls != ''")
    rows = cursor.fetchall()
    
    updated = 0
    downloaded_count = 0
    
    print(f"[STATS] Trovati {len(rows)} eventi con media potenziali.")

    for event_id, media_urls_str in rows:
        try:
            urls = json.loads(media_urls_str)
            new_urls = []
            changed = False
            
            for url in urls:
                if url.startswith('https://t.me/') and '/s/' not in url:
                    parts = url.split('t.me/')[1].split('/')
                    if len(parts) >= 2:
                        channel = parts[0]
                        msg_id = int(parts[1])
                        
                        file_path = os.path.join(MEDIA_DIR, f"{channel}_{msg_id}.mp4")
                        
                        # Verifica se esiste già per evitare scaricamenti doppi
                        found_local = False
                        for existing_ext in ['.mp4', '.jpg', '.jpeg', '.png']:
                            check_path = os.path.join(MEDIA_DIR, f"{channel}_{msg_id}{existing_ext}")
                            if os.path.exists(check_path):
                                file_path = check_path
                                found_local = True
                                break
                        
                        if not found_local:
                            if downloaded_count >= MAX_DOWNLOADS:
                                continue # Skip remainder if we hit max
                                
                            print(f"[DOWNLOAD] Scarico media {channel}/{msg_id} (attesa per sicurezza...)")
                            
                            # --- SLEEP ANTI-BAN ---
                            await asyncio.sleep(random.uniform(MIN_SLEEP, MAX_SLEEP))
                            
                            try:
                                msg = await client.get_messages(channel, ids=msg_id)
                                if msg and msg.media:
                                    downloaded = await client.download_media(msg, file=os.path.join(MEDIA_DIR, f"{channel}_{msg_id}"))
                                    if downloaded:
                                        file_path = downloaded
                                        print(f"  [OK] Salvato: {os.path.basename(file_path)}")
                                        downloaded_count += 1
                                        
                                        # Pausa lunga ogni batch
                                        if downloaded_count % BATCH_SIZE == 0:
                                            print(f"[PAUSE] Pausa di {BATCH_SLEEP}s per raffreddamento API...")
                                            await asyncio.sleep(BATCH_SLEEP)
                                            
                                    else:
                                        print("  [FAIL] Impossibile scaricare (nessun file utile)")
                                else:
                                    print("  [FAIL] Messaggio non trovato o senza media (eliminato?)")
                            except FloodWaitError as e:
                                print(f"  [FLOOD_WAIT] Telegram dice di aspettare {e.seconds}s.")
                                print(f"  [SLEEP] Dormo per {e.seconds + 10}s per sicurezza...")
                                await asyncio.sleep(e.seconds + 10)
                                print("  [RESUME] Riprendo lentamente...")
                            except Exception as e:
                                print(f"  [WARN] Errore generico (forse canale privato o bannato): {e}")
                                
                        if os.path.exists(file_path):
                            abs_path = os.path.abspath(file_path)
                            new_urls.append(abs_path)
                            changed = True
                        else:
                            new_urls.append(url)
                    else:
                        new_urls.append(url)
                else:
                    new_urls.append(url)
                    
            if changed:
                new_json = json.dumps(new_urls)
                cursor.execute("UPDATE unique_events SET media_urls = ? WHERE event_id = ?", (new_json, event_id))
                updated += 1
                conn.commit()
                
        except Exception as e:
            print(f"Errore DB per {event_id}: {e}")

    conn.close()
    await client.disconnect()
    print(f"\n[OPERAZIONE COMPLETATA] Media aggiornati per {updated} eventi. File scaricati: {downloaded_count}.")

if __name__ == '__main__':
    asyncio.run(main())
