import requests
import datetime
import sys
import os
import time
from datetime import timedelta

# Import del database manager
try:
    from ingestion.db_manager import save_raw_events
except ImportError:
    sys.path.append(os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..')))
    from ingestion.db_manager import save_raw_events


def fetch_gdelt_window(start_str, end_str):
    """
    Esegue una singola chiamata API per una finestra temporale specifica.
    """
    url = "http://api.gdeltproject.org/api/v2/doc/doc"

    # QUERY AMPIA: Tutto ciò che menziona Ucraina, Russia, Putin o Zelensky.
    # Non filtriamo per paese di origine, vogliamo vedere tutto.
    query = "(Ukraine OR Russia OR Putin OR Zelensky OR Kyiv OR Moscow)"

    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": "250",  # Massimo consentito da GDELT per singola chiamata
        "format": "json",
        "startdatetime": start_str,
        "enddatetime": end_str,
        "sort": "DateDesc"  # I più recenti prima
    }

    # Header per sembrare un browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    try:
        # Retry loop for 429/503 errors with exponential backoff
        max_retries = 5
        for attempt in range(max_retries):
            response = requests.get(
                url, params=params, headers=headers, timeout=20)

            if response.status_code == 200:
                data = response.json()
                return data.get('articles', [])
            
            elif response.status_code in (429, 503):
                wait_time = min((2 ** attempt) * 5, 120)  # 5s, 10s, 20s, 40s, 80s (cap 120s)
                print(f"   [WARNING] HTTP {response.status_code}. Backoff {wait_time}s (attempt {attempt+1}/{max_retries})...")
                time.sleep(wait_time)
                continue
            
            else:
                print(f"   [ERROR] HTTP {response.status_code}")
                return []
        
        print(f"   [ERROR] GDELT rate-limit exhausted after {max_retries} retries. Skipping window.")
        return []

    except requests.exceptions.Timeout:
        print(f"   [ERROR] GDELT timeout (server unresponsive). Skipping window.")
        return []
    except requests.exceptions.ConnectionError as e:
        print(f"   [ERROR] GDELT connection failed: {e}")
        return []
    except Exception as e:
        print(f"   [ERROR] GDELT unexpected error: {type(e).__name__}: {e}")
        return []


def fetch_gdelt_news(start_date, end_date):
    """
    Scarica news iterando giorno per giorno per aggirare il limite di 250 risultati.
    Accetta stringhe YYYYMMDDHHMMSS.
    """
    print(f"[INFO] GDELT: Avvio scraping massivo da {start_date} a {end_date}...")

    # Convertiamo stringhe in oggetti datetime per fare i calcoli
    try:
        dt_start = datetime.datetime.strptime(start_date, "%Y%m%d%H%M%S")
        dt_end = datetime.datetime.strptime(end_date, "%Y%m%d%H%M%S")
    except ValueError:
        print("[ERROR] Errore formato data GDELT. Usa YYYYMMDDHHMMSS")
        return

    current_cursor = dt_start
    total_saved = 0

    # LOOP GIORNALIERO
    while current_cursor < dt_end:
        # Definiamo la finestra di 24 ore (o fino alla fine)
        next_cursor = current_cursor + timedelta(days=1)
        if next_cursor > dt_end:
            next_cursor = dt_end

        # Formattiamo per l'API
        s_str = current_cursor.strftime("%Y%m%d%H%M%S")
        e_str = next_cursor.strftime("%Y%m%d%H%M%S")

        print(f"   [FETCH] Scarico finestra: {s_str} -> {e_str} ... ", end="")

        articles = fetch_gdelt_window(s_str, e_str)

        if articles:
            clean_events = []
            for art in articles:
                clean_events.append({
                    'text': f"{art.get('title')} - {art.get('url')}",
                    'source': art.get('domain', 'GDELT_Network'),
                    'type': 'WEB_NEWS',
                    'date': art.get('seendate')
                })

            saved = save_raw_events(clean_events)
            total_saved += saved
            print(f"Trovati {len(articles)}, Nuovi Salvati: {saved}")
        else:
            print("Nessun dato.")

        # Avanziamo il cursore
        current_cursor = next_cursor
        # Pausa di cortesia tra finestre per evitare HTTP 429
        time.sleep(4.0)

    print(f"[SUCCESS] GDELT COMPLETATO: {total_saved} articoli totali salvati nel DB.")


# Test rapido
if __name__ == "__main__":
    fetch_gdelt_news("20260425000000", "20260510000000")
