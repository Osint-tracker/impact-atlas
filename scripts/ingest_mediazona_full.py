import json
import os
import pandas as pd
from thefuzz import process
from datetime import datetime

# CONFIGURAZIONE
ORBAT_FILE = 'assets/data/orbat_full.json'
OUTPUT_DIR = 'assets/data/losses/ru/'
MEDIAZONA_URL = "https://raw.githubusercontent.com/Mediazona/data-202X/main/casualty_data.csv" # URL Ipotetico, va verificato quello attivo

def ingest_mediazona_nominal():
    print(">>> AVVIO PROTOCOLLO REAPER (Mediazona Nominal Ingestion)")
    
    # 1. Carica ORBAT per avere gli ID ufficiali
    with open(ORBAT_FILE, 'r') as f:
        orbat_data = json.load(f)
    
    # Crea mappa inversa: Nome Unità -> ID Unità (per matching veloce)
    # Esempio: "331st Guards Airborne Regiment" -> "ru_331_pdp"
    unit_map = {}
    for unit in orbat_data['units']:
        # Mappa sia il nome inglese che quello originale se presente
        unit_map[unit['name']] = unit['id']
        if 'name_ru' in unit:
            unit_map[unit['name_ru']] = unit['id']
            
    # 2. Scarica/Carica Dati Mediazona
    # Supponiamo un CSV con colonne: name, rank, date, unit, status
    # df = pd.read_csv(MEDIAZONA_URL) 
    # MOCK DATA per test
    data = [
        {"name": "Ivan Ivanov", "rank": "Lt. Col", "date": "2024-02-15", "unit": "331st Airborne Regiment", "source": "Obituary..."},
        {"name": "Petr Petrov", "rank": "Pvt", "date": "2024-02-10", "unit": "155th Naval Infantry", "source": "Social Media"},
        # ... altri 80k record
    ]
    df = pd.DataFrame(data)

    # 3. Processing e Sharding
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Statistiche per aggiornare l'ORBAT principale
    stats_update = {}

    # Raggruppa per unità (stringa raw di Mediazona)
    for raw_unit_name, group in df.groupby('unit'):
        
        # Tenta di trovare l'ID Project Owl corrispondente
        # Usa Fuzzy Matching se il match esatto fallisce
        match_name, score = process.extractOne(raw_unit_name, unit_map.keys())
        
        if score > 85:
            unit_id = unit_map[match_name]
            
            # Prepara la lista dei caduti per questa specifica unità
            casualty_list = []
            for _, row in group.iterrows():
                casualty_list.append({
                    "n": row['name'],       # Abbrevia le chiavi per risparmiare byte
                    "r": row['rank'],
                    "d": row['date'],
                    "s": row['source']
                })
            
            # SALVA IL FILE SHARD (es: assets/data/losses/ru/ru_331_pdp.json)
            file_path = os.path.join(OUTPUT_DIR, f"{unit_id}.json")
            with open(file_path, 'w') as f:
                json.dump(casualty_list, f, ensure_ascii=False)
                
            # Aggiorna il contatore per l'ORBAT principale
            stats_update[unit_id] = len(casualty_list)
            print(f"[MATCH] {raw_unit_name} -> {unit_id} ({len(casualty_list)} KIA)")
        else:
            # Opzionale: Salva in un file "Unknown Unit" o logga l'errore
            pass

    # 4. Aggiorna l'ORBAT principale SOLO con i conteggi (per le heatmap)
    for unit in orbat_data['units']:
        uid = unit['id']
        if uid in stats_update:
            unit['metrics']['kia_confirmed'] = stats_update[uid]
            unit['metrics']['has_list'] = True # Flag per il frontend
    
    with open(ORBAT_FILE, 'w') as f:
        json.dump(orbat_data, f, indent=2)

    print(">>> PROTOCOLLO REAPER COMPLETATO")

if __name__ == "__main__":
    ingest_mediazona_nominal()