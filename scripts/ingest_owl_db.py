import requests
import zipfile
import io
import json
import os
import re

# URL della repo
REPO_ZIP_URL = "https://github.com/owlmaps/units/archive/refs/heads/main.zip"

# Percorsi di output
OUTPUT_ORBAT = os.path.join("assets", "data", "orbat_full.json")
OUTPUT_SOURCES = os.path.join("ingestion", "owl_telegram_sources.json")

def clean_ts_to_json(ts_content):
    """
    Estrae il JSON grezzo da un file TypeScript (export const data = { ... })
    """
    # 1. Rimuove commenti
    text = re.sub(r'//.*', '', ts_content)
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
    
    # 2. Cerca l'oggetto principale { ... }
    # Cerchiamo la prima graffa aperta e l'ultima chiusa
    start = text.find('{')
    end = text.rfind('}')
    
    if start == -1 or end == -1:
        return None
        
    json_block = text[start:end+1]
    
    # 3. Trasforma JS object in JSON
    # Aggiunge quote alle chiavi (es. name: -> "name":)
    json_block = re.sub(r'(\w+)\s*:', r'"\1":', json_block)
    # Rimuove virgole trailing
    json_block = re.sub(r',\s*([}\]])', r'\1', json_block)
    # Rimpiazza single quotes con double quotes
    json_block = json_block.replace("'", '"')
    
    try:
        return json.loads(json_block)
    except:
        return None

def harvest_owl_units():
    print("🦅 Owl Unit Harvester v3.0 (Path Fix) avviato...")
    
    try:
        print("1. Scaricando la repo...")
        r = requests.get(REPO_ZIP_URL)
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Errore download: {e}")
        return

    orbat_db = []
    telegram_sources = set()
    
    # Contatori stats
    stats = {
        "ts_files_found": 0,
        "units_parsed": 0,
        "ru_units": 0,
        "ua_units": 0,
        "skipped": 0
    }
    
    print("2. Analisi ZIP...")
    
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        file_list = z.namelist()
        print(f"   Root folder rilevata: {file_list[0]}")
        
        for filename in file_list:
            # Ignora cartelle e file non .ts
            if filename.endswith('/') or not filename.endswith('.ts'):
                continue
                
            # Ignora file di sistema/test
            if "spec.ts" in filename or "index.ts" in filename or "types.ts" in filename:
                continue

            stats["ts_files_found"] += 1
            
            # --- RILEVAMENTO FAZIONE (FIXED) ---
            # Cerchiamo 'units-ua' o 'units-ru' nel path
            side = "UNKNOWN"
            lower_name = filename.lower()
            
            if "units-ua" in lower_name:
                side = "UA"
            elif "units-ru" in lower_name:
                side = "RU"
            else:
                # Se il file è fuori dalle cartelle note, lo saltiamo (o lo logghiamo come debug)
                # print(f"   [DEBUG] Skip file fuori path target: {filename}")
                stats["skipped"] += 1
                continue
            
            try:
                with z.open(filename) as f:
                    content = f.read().decode('utf-8')
                    data = clean_ts_to_json(content)
                    
                    if data and isinstance(data, dict):
                        # Controllo validità minima
                        unit_name = data.get("name") or data.get("ids", [None])[0]
                        
                        if unit_name:
                            # Entry valida trovata!
                            stats["units_parsed"] += 1
                            if side == "UA": stats["ua_units"] += 1
                            else: stats["ru_units"] += 1
                            
                            unit_entry = {
                                "name": unit_name,
                                "side": side,
                                "type": data.get("type", "Unit"),
                                "parent": data.get("parent"),
                                "socials": data.get("socials", {})
                            }
                            
                            # Estrazione Telegram
                            socials = data.get("socials", {})
                            if socials:
                                for k, v in socials.items():
                                    if "telegram" in k.lower() and v:
                                        clean = v.split('/')[-1].replace('@', '').strip()
                                        if clean: telegram_sources.add(clean)
                                        
                            orbat_db.append(unit_entry)
            except:
                pass

    # 3. Output
    if orbat_db:
        os.makedirs(os.path.dirname(OUTPUT_ORBAT), exist_ok=True)
        with open(OUTPUT_ORBAT, 'w', encoding='utf-8') as f:
            json.dump(orbat_db, f, indent=2, ensure_ascii=False)
            
        os.makedirs(os.path.dirname(OUTPUT_SOURCES), exist_ok=True)
        with open(OUTPUT_SOURCES, 'w', encoding='utf-8') as f:
            json.dump(list(telegram_sources), f, indent=2)

        print(f"\n✅ SUCCESSO! Dati estratti correttamente.")
        print(f"   - File .ts analizzati: {stats['ts_files_found']}")
        print(f"   - Unità UA processate: {stats['ua_units']}")
        print(f"   - Unità RU processate: {stats['ru_units']}")
        print(f"   - Totale Unità nel DB: {len(orbat_db)}")
        print(f"   - Canali Telegram TROVATI: {len(telegram_sources)}")
    else:
        print("\n❌ ANCORA NESSUNA UNITÀ. C'è un problema di path o regex.")
        print(f"   Debug Stats: {stats}")

if __name__ == "__main__":
    harvest_owl_units()