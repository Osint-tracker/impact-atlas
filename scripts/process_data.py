import pandas as pd
import json
import sys
import numpy as np
import requests
import io
import re
import math
import os

# --- CONFIGURAZIONE ---
SHEET_URL = "https://docs.google.com/spreadsheets/d/1NEyNXzCSprGOw6gCmVVbtwvFmz8160Oag-WqG93ouoQ/export?format=csv"
OUTPUT_GEOJSON = "assets/data/events.geojson"
OUTPUT_TIMELINE = "assets/data/events_timeline.json"

def get_col(df, candidates):
    """Trova la colonna corretta tra le varianti possibili."""
    for c in candidates:
        if c in df.columns: return c
    return None

def safe_float(val):
    """
    Converte stringhe o numeri in float sicuro.
    Gestisce: 10.5 (US), 10,5 (IT), stringhe vuote, NaN.
    """
    try:
        if val is None or str(val).strip() == "":
            return None
        # Sostituisce virgola con punto per formato italiano
        val_str = str(val).replace(',', '.')
        f = float(val_str)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except:
        return None

def classify_actor(row, col_map):
    """
    Motore di Classificazione POLIGLOTTA (IT/EN).
    """
    # Recupera testi e converte in minuscolo
    desc = str(row.get(col_map['desc']) or "").lower()
    title = str(row.get(col_map['title']) or "").lower()
    loc = str(row.get(col_map['loc']) or "").lower()
    
    full_text = f"{desc} {title}"

    # --- 1. KEYWORDS (ITALIANO + INGLESE) ---
    
    # RUSSIA (Aggressore)
    # Cerchiamo: russo, russa, russi, russian, wagner, mosca, dpr, lpr, shahed, iskander, kinzhal
    if re.search(r'\b(russ[oaie]|russian|rf|fed\.? russa|mosca|moscow|wagner|dpr|lpr|vks)\b', full_text):
        return 'RUS'
    if re.search(r'\b(shahed|geran|iskander|kalibr|kinzhal|kh-\d+|fab-\d+|s-300|s-400)\b', full_text):
        return 'RUS'

    # UCRAINA (Difensore/Contrattacco)
    # Cerchiamo: ucraino, ucraina, ukrainian, kiev forces, zsu, uaf, himars, atacms
    if re.search(r'\b(ucrain[oaie]|ukrain[a-z]*|zsu|uaf|kiev troops|forze di kiev)\b', full_text):
        return 'UKR'
    if re.search(r'\b(himars|atacms|storm shadow|scalp|magura|sea baby|neptune)\b', full_text):
        return 'UKR'
    
    # --- 2. GEOGRAFIA INVERSA (Deduzione Logica) ---
    
    # Colpi in territorio Russo/Occupato -> Probabile UKR
    if re.search(r'belgorod|kursk|voronezh|rostov|crimea|sevastopol|kerch|mosc[oa]|krasnodar|bryansk|lipetsk|novorossiysk', loc):
        return 'UKR'
    
    # Colpi in territorio Ucraino "sicuro" -> Probabile RUS
    if re.search(r'kyiv|kiev|kharkiv|kharkov|odesa|odessa|lviv|lvov|dnipro|zaporizhzhia|vinnytsia|sumy|poltava|chernihiv|kryvyi rih', loc):
        return 'RUS'

    return 'UNK'

def main():
    print("🏭 AVVIO PROCESSAMENTO DATI (V. POLYGLOT)...")

    # 1. SCARICAMENTO
    try:
        print(f"⬇️ Scaricamento CSV...")
        response = requests.get(SHEET_URL)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text))
    except Exception as e:
        print(f"❌ Errore critico download: {e}")
        sys.exit(1)

    # 2. PULIZIA PRELIMINARE
    df.columns = df.columns.str.strip().str.lower()
    # Sostituisce NaN con stringa vuota per le colonne testo, per evitare errori
    df = df.fillna("")
    
    print(f"📋 Colonne trovate: {list(df.columns)}")

    # 3. MAPPATURA
    col_map = {
        'lat': get_col(df, ['latitude', 'lat']),
        'lon': get_col(df, ['longitude', 'lon', 'long']),
        'title': get_col(df, ['title', 'titolo']),
        'desc': get_col(df, ['description', 'descrizione', 'notes', 'note']),
        'loc': get_col(df, ['location', 'luogo', 'city']),
        'date': get_col(df, ['date', 'data']),
        'type': get_col(df, ['type', 'tipo', 'type of attack']),
        'link': get_col(df, ['source', 'link', 'fonte']),
        'video': get_col(df, ['video', 'video_url']),
        'ver': get_col(df, ['verification', 'verifica']),
        'int': get_col(df, ['intensity', 'intensità'])
    }

    if not col_map['lat'] or not col_map['lon']:
        print("❌ ERRORE: Coordinate mancanti nel CSV.")
        sys.exit(1)

    # 4. ELABORAZIONE
    features = []
    tl_events = []
    stats = {'RUS': 0, 'UKR': 0, 'UNK': 0}
    skipped = 0

    for index, row in df.iterrows():
        # Gestione Coordinate Robusta (IT/US)
        lat = safe_float(row[col_map['lat']])
        lon = safe_float(row[col_map['lon']])
        
        if lat is None or lon is None:
            skipped += 1
            continue # Salta righe senza coordinate valide

        # CLASSIFICAZIONE
        actor_code = classify_actor(row, col_map)
        stats[actor_code] += 1

        # Recupero Dati
        title = str(row[col_map['title']]).strip() or "Evento"
        desc = str(row[col_map['desc']]).strip()
        loc_str = str(row[col_map['loc']]).strip()
        date_str = str(row[col_map['date']]).strip()
        type_str = str(row[col_map['type']]).strip() or "General"
        video_str = str(row[col_map['video']]).strip()
        link_str = str(row[col_map['link']]).strip()
        ver_str = str(row[col_map['ver']]).strip().lower()
        if ver_str not in ['verified', 'not verified']: ver_str = 'not verified'
        
        intensity = safe_float(row[col_map['int']]) or 0.2

        # COSTRUZIONE GEOJSON
        props = {
            "title": title,
            "date": date_str,
            "type": type_str,
            "location": loc_str,
            "link": link_str,
            "verification": ver_str,
            "description": desc,
            "video": video_str,
            "intensity": intensity,
            "actor_code": actor_code # <--- CRUCIALE
        }

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props
        })

        # COSTRUZIONE TIMELINE
        # (Parsing data approssimativo per evitare crash)
        try:
            y, m, d = "2024", "01", "01"
            # Cerca pattern YYYY-MM-DD o DD/MM/YYYY
            if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                y, m, d = date_str.split('-')
            elif re.match(r'\d{1,2}/\d{1,2}/\d{2,4}', date_str):
                parts = date_str.split('/')
                d, m, y = parts[0], parts[1], parts[2]
                if len(y) == 2: y = "20" + y
            
            tl_obj = {
                "start_date": {"year": int(y), "month": int(m), "day": int(d)},
                "text": { 
                    "headline": title, 
                    "text": f"<b>Tipo:</b> {type_str}<br><b>Attore:</b> {actor_code}<br><b>Luogo:</b> {loc_str}<br><br>{desc}" 
                },
                "group": type_str
            }
            if video_str:
                tl_obj["media"] = {"url": video_str, "caption": "Fonte Video"}
            tl_events.append(tl_obj)
        except:
            pass # Se data invalida, niente timeline ma mappa ok

    # 5. OUTPUT
    os.makedirs(os.path.dirname(OUTPUT_GEOJSON), exist_ok=True)

    with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f, ensure_ascii=False, indent=2)
    
    with open(OUTPUT_TIMELINE, "w", encoding="utf-8") as f:
        json.dump({"title": {"text": {"headline": "Timeline"}}, "events": tl_events}, f, ensure_ascii=False, indent=2)

    print("\n=== REPORT ===")
    print(f"✅ Eventi Generati: {len(features)}")
    print(f"❌ Righe Saltate (No Lat/Lon): {skipped}")
    print(f"📊 Classificazione: {stats}")
    print("==============")

if __name__ == "__main__":
    main()
