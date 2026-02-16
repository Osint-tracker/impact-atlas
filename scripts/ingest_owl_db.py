import requests
import zipfile
import io
import json
import os
import re

# URL per scaricare l'intera repo come ZIP
REPO_ZIP_URL = "https://github.com/owlmaps/units/archive/refs/heads/main.zip"

# Percorsi di output
OUTPUT_ORBAT = os.path.join("assets", "data", "orbat_full.json")
OUTPUT_SOURCES = os.path.join("ingestion", "owl_telegram_sources.json")

def harvest_owl_units():
    print("Owl Unit Harvester v2.0 (DB Edition) avviato...")
    
    try:
        print("1. Scaricando il database unità (ZIP)...")
        r = requests.get(REPO_ZIP_URL, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"Errore download: {e}")
        return

    orbat_db = []
    telegram_sources = set()
    
    print("2. Decomprimendo e analizzando i JSON...")
    
    # Apri lo zip in memoria
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            # Itera su tutti i file nell'archivio
            for filename in z.namelist():
                # Ci interessano solo i file .json
                if not filename.endswith('.json') or "schema" in filename:
                    continue
                    
                # Determina la fazione basandosi sulla cartella (UA/RU)
                # Esempio path: units-main/UA/Brigades/47th.json
                side = "UNKNOWN"
                if "/UA/" in filename: side = "UA"
                elif "/RU/" in filename: side = "RU"
                
                try:
                    # Leggi il contenuto del JSON
                    with z.open(filename) as f:
                        data = json.load(f)
                        
                        # Normalizza i dati per il nostro sistema
                        unit_entry = {
                            "name": data.get("name", "Unknown Unit"),
                            "side": side,
                            "type": data.get("type", "Unit"),
                            "parent": data.get("parent", None),
                            "socials": data.get("socials", {})
                        }
                        
                        # --- INTELLIGENCE MINING ---
                        # Estrai i canali Telegram dai social
                        socials = data.get("socials", {})
                        if socials:
                            # Cerca chiavi come "telegram", "tg", ecc.
                            for key, value in socials.items():
                                if "telegram" in key.lower() and value:
                                    # Pulisci l'URL per ottenere solo lo username
                                    username = value.replace("https://t.me/", "").replace("t.me/", "").replace("@", "").strip()
                                    if username:
                                        telegram_sources.add(username)
                                        # Aggiungiamo anche il link pulito all'oggetto unità
                                        unit_entry["socials"]["telegram_clean"] = username

                        orbat_db.append(unit_entry)
                        
                except json.JSONDecodeError:
                    print(f"Errore lettura JSON: {filename}")
                    continue
    except Exception as e:
        print(f"Errore ZIP: {e}")
        return

    # 3. Salvataggio Output
    
    # A. ORBAT Completo per il Frontend
    os.makedirs(os.path.dirname(OUTPUT_ORBAT), exist_ok=True)
    with open(OUTPUT_ORBAT, 'w', encoding='utf-8') as f:
        json.dump(orbat_db, f, indent=2, ensure_ascii=False)
        
    # B. Lista Canali Telegram per lo Scraper
    os.makedirs(os.path.dirname(OUTPUT_SOURCES), exist_ok=True)
    
    # Formattiamo per lo scraper (che si aspetta un dizionario o lista)
    with open(OUTPUT_SOURCES, 'w', encoding='utf-8') as f:
        # Salviamo come lista semplice di stringhe
        json.dump(list(telegram_sources), f, indent=2)

    print(f"\nDATABASE GENERATO:")
    print(f"   - Unità Totali: {len(orbat_db)}")
    print(f"   - Canali Telegram estratti: {len(telegram_sources)}")
    print(f"   - File salvati in: assets/data/ e ingestion/")

if __name__ == "__main__":
    harvest_owl_units()
