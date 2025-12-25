import os
import csv
import asyncio
import random
from telethon import TelegramClient, errors
from datetime import datetime

# --- CONFIGURAZIONE ---
# Inserisci qui le tue credenziali API (da my.telegram.org)
API_ID = '24856485'
API_HASH = 'ccb58f4b099129d8e899d973b4fbf336'
SESSION_NAME = 'osint_session_v2'

# Quanti messaggi scaricare per ogni canale?
MSG_LIMIT = 100

# File di Output
OUTPUT_FILE = "scripts/telegram_fresh_data.csv"

# --- LISTA CANALI TARGET (Aggiornata con PlayfraOSINT) ---
TARGET_CHANNELS = [
    # --- I TUOI ORIGINALI (VERIFICATI) ---
    'DeepStateUA',          # UA: Mappe ufficiali
    'rybar',                # RU: Analisi top tier
    'CinCA_AFU',            # UA: Ufficiale Esercito
    'noel_reports',         # UA: Aggregatore (Mirror Twitter)
    'fighter_bomber',       # RU: Aviazione
    'lost_armour',          # RU: Perdite mezzi
    'karymat',              # UA: Footage crudo
    'strelkovii',           # RU: Critica (Strelkov)
    'stanislav_osman',      # UA: Combattente sul campo
    'officer_33',           # UA: Blogger militare
    'parabellumcommunity',  # IT: Analisi tecnica
    'dariodangelo',         # IT: Analisi geopolitica
    'PlayfraOSINT',         # IT: Analisi OSINT (Aggiunto su richiesta)

    # --- AGGIUNTE FONDAMENTALI ---

    # PRO-UCRAINA (Ufficiali & News)
    'V_Zelenskiy_official',  # Presidente Zelensky
    'insiderUKR',           # News veloci (Molto attivo)
    'uniannet',             # Agenzia Stampa
    'operativnoZSU',        # News dal fronte
    'azov_media',           # Brigata Azov
    'robert_magyar',        # Unità Droni (Magyar)
    'sstenenko',            # Droni FPV (Sternenko)
    'Tsaplienko',           # Giornalista embedded
    'lachentyt',            # OSINT/News (Molto affidabile)
    'myro_shnykov',          # Corrispondente di guerra

    # PRO-RUSSIA (Z-Channels & Corrispondenti)
    'boris_rozhin',         # Colonelcassad
    'vysokygovorit',        # Comandanti RU
    'voenkorKotenok',       # War Correspondent
    'sashakots',            # Kotsnews
    'grey_zone',            # Wagner (Archivio/Attivo)
    'wargonzo',             # Semen Pegov (Fronte)
    'dva_majors',           # Due Maggiori (Analisi tattica)
    'RVvoenkor',            # RusVesna (Operazioni)
    'milchronicles',        # Cronache militari

    # OSINT & AGGREGATORI (Footage & Mappe)
    'combat_ftg',           # Combat Footage (Neutrale/Misto)
    'faceofwar',            # Footage
    'conflictzone',         # Aggregatore
    'petrenko_IHS',         # Mapper (Simile a Deepstate)
    'yurasumy',             # Analista Pro-RU (Mappe)
    'monitor'               # Allerte aeree/Missili (Affidabile)
]

# --- PAROLE CHIAVE DA ESCLUDERE (FILTRO GEOPOLITICO) ---
# Se il messaggio contiene queste parole, viene scartato (case insensitive)
EXCLUDED_KEYWORDS = [
    "gaza", "israel", "hamas", "palestin", "hezbollah", "yemen", "houthi",  # Medio Oriente
    "taiwan", "china", "kina", "xi jinping",  # Asia
    "crypto", "bitcoin", "investment", "subscribe", "promo", "casino",  # Spam/Ads
    "armenia", "azerbaijan"  # Altri conflitti
]


def is_message_relevant(text):
    """
    Filtra i messaggi:
    1. Lunghezza minima (no spam/emoji singole)
    2. Contenuto off-topic (Gaza, Taiwan, ecc.)
    """
    if not text:
        return False

    text_lower = text.lower()

    # 1. Filtro Lunghezza (Min 50 caratteri)
    if len(text) < 50:
        return False

    # 2. Filtro Keyword Escluse
    for keyword in EXCLUDED_KEYWORDS:
        if keyword in text_lower:
            return False

    return True


async def main():
    print(
        f"🚀 Avvio Telegram Vacuum Cleaner (Target: {len(TARGET_CHANNELS)} canali)...")
    print(f"🛡️  Modalità Anti-Ban: ATTIVA")
    print(f"🧹 Filtro Off-Topic (Gaza/Taiwan): ATTIVO")

    # Connessione
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()

    # Setup CSV
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Header compatibile con l'analisi
        writer.writerow(['Date', 'Source_Channel',
                        'Raw_Text', 'Link', 'Title_Preview'])

        total_saved = 0

        for i, channel in enumerate(TARGET_CHANNELS):
            print(f"📡 [{i+1}/{len(TARGET_CHANNELS)}] Scansionando: {channel}...")

            try:
                entity = await client.get_entity(channel)
                saved_count = 0
                scanned_count = 0

                # Scarica messaggi
                async for msg in client.iter_messages(entity, limit=MSG_LIMIT):
                    scanned_count += 1
                    text = msg.text

                    # Applica i filtri
                    if is_message_relevant(text):
                        date_str = msg.date.strftime('%Y-%m-%d %H:%M:%S')
                        link = f"https://t.me/{channel}/{msg.id}"
                        # Titolo anteprima (prima riga o primi 100 char)
                        title_preview = text.split(
                            '\n')[0][:100].replace('"', "'")

                        writer.writerow(
                            [date_str, channel, text.strip(), link, title_preview])
                        saved_count += 1
                        total_saved += 1

                print(
                    f"   ✅ Salvati: {saved_count} / Scansionati: {scanned_count} (Filtrati: {scanned_count - saved_count})")

            except errors.FloodWaitError as e:
                print(
                    f"   🛑 FLOOD WAIT RILEVATO! Pausa forzata di {e.seconds} secondi.")
                await asyncio.sleep(e.seconds + 10)
            except Exception as e:
                print(f"   ❌ Errore su {channel}: {e}")

            # --- PAUSA ANTI-BAN TRA I CANALI ---
            if i < len(TARGET_CHANNELS) - 1:
                wait_time = random.uniform(5, 15)  # Pausa tra 5 e 15 secondi
                print(f"   ☕ Pausa tattica di {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)

    print(f"\n🎉 Finito! Totale messaggi rilevanti salvati: {total_saved}")
    print(f"📂 File salvato in: {OUTPUT_FILE}")
    await client.disconnect()

if __name__ == '__main__':
    if not os.path.exists('data'):
        os.makedirs('data')
    asyncio.run(main())
