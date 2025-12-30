import sqlite3
import json
import time
import os
import re
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# =============================================================================
# ⚙️ CONFIGURAZIONE
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')

# Inizializza il geocoder
geolocator = Nominatim(user_agent="osint_war_fixer_v3_dict")

# 1. CONFINI DI SICUREZZA
SAFE_MIN_LAT = 40.0
SAFE_MIN_LON = 10.0
SAFE_MAX_LON = 180.0

# 2. DIZIONARIO DI CORREZIONE MANUALE (Fixing the Log Failures)
# Mappa: "nome sbagliato/traslitterato male" -> "nome corretto per Nominatim"
MANUAL_FIXES = {
    "dobropillya": "Dobropillia",
    "gulyai-pole": "Huliaipole",
    "gulyaipole": "Huliaipole",
    "sotnitsky kazachok": "Sotnytskyi Kozachok",
    "sotnitsky kozachok": "Sotnytskyi Kozachok",
    "sotnytsky kozachok": "Sotnytskyi Kozachok",
    "kupyansk": "Kupiansk",
    "kostyantynivka": "Kostiantynivka",
    "grabovskoye": "Hrabovske",  # RU -> UA spelling
    "hrabokoske": "Hrabovske",  # Typo
    "pankiv": "Pankivka",       # Probabile villaggio
    "грабівському сумської області": "Грабівське",  # Fix grammatica
    "ispra": "Ispra",  # Nota: Ispra è in Italia, probabilmente un falso positivo dell'AI da ignorare o un centro ricerche
    "venezuela": "Venezuela",  # Da scartare
}

# 3. LISTA VIP RUSSA
RUSSIAN_PRIORITY_CITIES = [
    "rostov", "rostov-on-don", "rostov na donu",
    "belgorod", "kursk", "voronezh", "bryansk",
    "moscow", "moskva", "krasnodar", "sochi",
    "lipetsk", "volgograd", "saratov", "engels",
    "taganrog", "novorossiysk", "tuapse", "yeysk",
    "st. petersburg", "saint petersburg", "leningrad",
    "tatarstan", "yelabuga", "nizhny novgorod"
]

# 4. LUOGHI DA IGNORARE (Rumore)
IGNORE_TERMS = [
    "multiple locations", "romanian forest", "swedish waters",
    "ukraine", "russia", "frontline", "border", "europe",
    "unknown", "various", "direction"
]

# =============================================================================
# 🧮 FUNZIONI HELPER
# =============================================================================


def is_suspicious(lat, lon):
    if not lat or not lon:
        return True
    try:
        lat, lon = float(lat), float(lon)
        if lon < SAFE_MIN_LON:
            return True
        if lat < SAFE_MIN_LAT:
            return True
        return False
    except:
        return True


def smart_geocode(location_name):
    if not location_name:
        return None

    raw_name = location_name.strip()
    raw_lower = raw_name.lower()

    # 0. FILTRO RUMORE IMMEDIATO
    if raw_lower in IGNORE_TERMS:
        return None

    candidates = []

    # 1. CHECK DIZIONARIO MANUALE (Priorità Massima)
    if raw_lower in MANUAL_FIXES:
        candidates.append(MANUAL_FIXES[raw_lower])

    # Aggiunge anche l'originale
    candidates.append(raw_name)

    # 2. PULIZIA AUTOMATICA
    # Rimuovi parentesi
    no_parens = re.sub(r'\([^)]*\)', '', raw_name).strip()
    if no_parens != raw_name:
        candidates.append(no_parens)

    # Gestione liste (virgole)
    if ',' in raw_name:
        first_part = raw_name.split(',')[0].strip()
        candidates.append(first_part)
        if first_part.lower() in MANUAL_FIXES:  # Check dict anche sulla parte splittata
            candidates.append(MANUAL_FIXES[first_part.lower()])

    # Rimuovi parole rumore
    noise_words = [" region", " oblast", " area",
                   " district", " krai", " republic"]
    for word in noise_words:
        if word in raw_name.lower():
            candidates.append(re.sub(r'(?i)'+word, '', raw_name).strip())

    unique_candidates = list(dict.fromkeys(candidates))

    print(f"   🔍 Tentativi per '{location_name}': {unique_candidates}")

    # 3. ESECUZIONE RICERCA
    for candidate in unique_candidates:
        if len(candidate) < 3:
            continue

        candidate_lower = candidate.lower()
        is_russian_vip = any(
            vip in candidate_lower for vip in RUSSIAN_PRIORITY_CITIES)

        # Setup Priorità
        if is_russian_vip:
            priority_countries = [['ru'], ['ua'], ['by', 'md']]
        else:
            priority_countries = [['ua'], ['ru'], ['by', 'md']]

        # Tentativo Standard
        for country_list in priority_countries:
            try:
                location = geolocator.geocode(
                    candidate, country_codes=country_list, timeout=3)
                if location:
                    return location
            except:
                time.sleep(0.5)

        # 4. TENTATIVO DISPERATO: Aggiungi ", Ukraine" se non trovato
        # (Spesso Nominatim fallisce "Dobropillya" ma trova "Dobropillya, Ukraine")
        if not is_russian_vip:
            try:
                location = geolocator.geocode(
                    f"{candidate}, Ukraine", timeout=3)
                if location:
                    return location
            except:
                pass

    return None

# =============================================================================
# 🚀 MAIN LOOP
# =============================================================================


def main():
    print("🌍 AVVIO RIPARAZIONE COORDINATE (V3: DICTIONARY + FUZZY)...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        "SELECT event_id, ai_report_json FROM unique_events WHERE ai_report_json IS NOT NULL")
    rows = cursor.fetchall()

    fixed_count = 0
    skipped_count = 0

    print(f"📊 Analisi di {len(rows)} eventi...")

    for row in rows:
        try:
            event_id = row['event_id']
            try:
                data = json.loads(row['ai_report_json'])
            except:
                continue

            tactics = data.get('tactics', {})
            geo = tactics.get('geo_location', {})
            explicit = geo.get('explicit', {}) or {}
            inferred = geo.get('inferred', {}) or {}

            current_lat = explicit.get('lat')
            current_lon = explicit.get('lon')
            location_name = inferred.get('toponym_raw')

            # Fix necessario?
            needs_fix = is_suspicious(current_lat, current_lon)

            if needs_fix and location_name:

                print(
                    f"🔧 Fix richiesto per ID {event_id[:6]} ({location_name})")

                new_loc = smart_geocode(location_name)

                if new_loc:
                    print(
                        f"   ✅ Trovato in {new_loc.address.split(',')[-1].strip()}: {new_loc.latitude}, {new_loc.longitude}")

                    if 'explicit' not in data['tactics']['geo_location']:
                        data['tactics']['geo_location']['explicit'] = {}

                    if data['tactics']['geo_location']['explicit'] is None:
                        data['tactics']['geo_location']['explicit'] = {}

                    data['tactics']['geo_location']['explicit']['lat'] = new_loc.latitude
                    data['tactics']['geo_location']['explicit']['lon'] = new_loc.longitude

                    cursor.execute("UPDATE unique_events SET ai_report_json = ? WHERE event_id = ?",
                                   (json.dumps(data), event_id))
                    conn.commit()
                    fixed_count += 1
                else:
                    print("   ❌ Fallito.")

                time.sleep(1.1)
            else:
                skipped_count += 1

        except Exception as e:
            print(f"⚠️ Errore su evento {row['event_id']}: {e}")
            continue

    conn.close()
    print(f"\n🏁 FINITO. Corretti: {fixed_count} | Saltati: {skipped_count}")


if __name__ == "__main__":
    main()
