import requests
import pandas as pd
import io
import zipfile
import hashlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys

# --- SETUP IMPORT ---
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    from database_manager import save_batch_signals
except ImportError:
    # Fallback path
    sys.path.append(os.path.join(current_dir, 'scripts'))
    from database_manager import save_batch_signals

# --- CONFIGURAZIONE ---
START_DATE = "20220224000000"
END_DATE = datetime.now().strftime("%Y%m%d%H%M%S")
LIMIT_FILES = None
MAX_WORKERS = 5


def generate_hash(date, text, source):
    """Crea hash MD5 univoco."""
    clean_text = str(text).strip()[:100]
    raw_str = f"{date}|{source}|{clean_text}"
    return hashlib.md5(raw_str.encode('utf-8')).hexdigest()


def get_master_file_list():
    print("📜 Scaricando Master List GDELT...")
    url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    headers = {'User-Agent': 'Mozilla/5.0 (WarTrackerBot/1.0)'}
    try:
        r = requests.get(url, headers=headers, timeout=30)
        all_lines = r.text.splitlines()
    except Exception:
        return []

    target_urls = []
    print("🕵️  Filtrando file dal 2022 a oggi...")
    for line in all_lines:
        parts = line.split(' ')
        if len(parts) < 3:
            continue
        url_file = parts[2]
        if "export.CSV.zip" not in url_file:
            continue
        try:
            filename = url_file.split('/')[-1]
            date_str = filename.split('.')[0]
            if START_DATE <= date_str <= END_DATE:
                target_urls.append(url_file)
        except:
            continue

    target_urls.sort(reverse=True)
    if LIMIT_FILES:
        return target_urls[:LIMIT_FILES]
    return target_urls


def find_critical_columns(df):
    """
    Trova dinamicamente gli indici delle colonne Paese e Evento.
    Scansiona le colonne per vedere dove sono i dati 'UP'/'RS'.
    """
    country_idx = -1
    event_idx = -1

    # 1. Cerca la colonna Paese (Cerca 'UP' o 'RS')
    # Di solito è verso la fine (tra indice 50 e 60)
    # Scansioniamo solo le colonne stringa
    for col in df.columns:
        if df[col].dtype == 'object':
            unique_vals = df[col].dropna().unique()
            # Se contiene UP e RS, è quasi sicuramente la colonna giusta
            if 'UP' in unique_vals or 'RS' in unique_vals:
                # Controllo extra: i codici paese sono lunghi 2 caratteri
                sample = str(unique_vals[0])
                if len(sample) == 2:
                    country_idx = col
                    break

    # 2. Cerca la colonna EventRootCode (Codici '190', '180', ecc.)
    # Di solito è tra indice 25 e 30
    for col in df.columns:
        # Potrebbe essere letta come int o float o object
        try:
            # Convertiamo in stringa per controllare
            sample_series = df[col].dropna().astype(str)
            # Controlla se ci sono codici che iniziano con '19' o '18'
            matches = sample_series.str.startswith(('19', '18', '20')).sum()
            # Se almeno il 10% delle righe sembra un codice militare
            if matches > len(df) * 0.1:
                event_idx = col
                break
        except:
            continue

    return country_idx, event_idx


def process_single_url(url):
    signals = []
    headers = {'User-Agent': 'Mozilla/5.0 (WarTrackerBot/1.0)'}

    try:
        r = requests.get(url, headers=headers, timeout=20)

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            with z.open(z.namelist()[0]) as f:
                # Leggiamo SENZA header per avere indici numerici (0, 1, 2...)
                # Questo evita errori di nomi colonne.
                df = pd.read_csv(f, sep='\t', header=None, on_bad_lines='skip')

        # --- AUTO-RILEVAMENTO COLONNE ---
        country_col, event_col = find_critical_columns(df)

        # Se non riusciamo a identificare le colonne, saltiamo il file
        if country_col == -1 or event_col == -1:
            # print(f"Skipping {url}: Colonne non identificate.")
            return []

        # --- FILTRI ---
        # 1. GEO
        geo_mask = df[country_col].isin(['UP', 'RS'])

        # 2. TEMA
        target_codes = ('14', '15', '17', '18', '19', '20')
        df[event_col] = df[event_col].fillna(0).astype(int).astype(str)
        war_mask = df[event_col].str.startswith(target_codes)

        filtered = df[geo_mask & war_mask]

        if filtered.empty:
            return []

        # --- ESTRAZIONE DATI (Usando indici relativi) ---
        # Data è sempre l'ultima colonna - 1 (indice -2)
        # URL è sempre l'ultima colonna (indice -1)
        # Lat/Lon sono solitamente 3 e 2 colonne prima del CountryCode

        date_col_idx = df.columns[-2]
        url_col_idx = df.columns[-1]

        # Lat/Lon heuristic: Se Country è col X, Lat è X-2, Lon è X-1
        lat_idx = country_col - 2
        lon_idx = country_col - 1

        # Actor Names sono di solito col 6 e 16 (approssimativamente)
        # Ma per sicurezza usiamo stringhe generiche se non siamo sicuri
        act1_idx = 6
        act2_idx = 16
        loc_idx = country_col - 1  # FullName di solito è prima del country code

        for _, row in filtered.iterrows():
            try:
                synthetic_text = f"Event Code {row[event_col]}. Action in {row[country_col]} region."
                date_pub = str(row[date_col_idx])

                h = generate_hash(date_pub, synthetic_text, "GDELT")

                lat = row[lat_idx] if isinstance(
                    row[lat_idx], (int, float)) else None
                lon = row[lon_idx] if isinstance(
                    row[lon_idx], (int, float)) else None

                signal = {
                    'hash': h,
                    'type': 'GDELT',
                    'source': 'GDELT_Network',
                    'date': date_pub,
                    'text': synthetic_text,
                    'url': str(row[url_col_idx]),
                    'lat': lat,
                    'lon': lon
                }
                signals.append(signal)
            except:
                continue

    except Exception:
        return []

    return signals


def main():
    urls = get_master_file_list()
    if not urls:
        print("❌ Nessun file trovato.")
        return

    print(f"🚀 Avvio ingestione {len(urls)} file (Modalità Auto-Adattiva)...")

    total_saved = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(
            process_single_url, url): url for url in urls}

        for i, future in enumerate(as_completed(future_to_url)):
            signals = future.result()
            if signals:
                count = save_batch_signals(signals)
                total_saved += count

            if (i + 1) % 10 == 0:
                print(
                    f"   [{i + 1}/{len(urls)}] Elaborati. Eventi salvati: {total_saved}")

    print(f"\n🏁 Ingestione Completata. Totale eventi: {total_saved}")


if __name__ == "__main__":
    main()
