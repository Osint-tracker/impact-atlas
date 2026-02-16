import requests
from bs4 import BeautifulSoup
import time
import csv
import sys

# Configurazione Base
BASE_URL = "https://ualosses.org"
UNITS_INDEX_URL = "https://ualosses.org/en/military_units/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def get_all_unit_links():
    """Scarica la lista completa delle unità militari."""
    print("Recupero indice delle unità...")
    try:
        response = requests.get(UNITS_INDEX_URL, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        units = []
        # La tabella delle unità è l'elemento principale della pagina
        # Cerchiamo i link dentro le righe della tabella
        rows = soup.find_all('tr')
        
        for row in rows:
            link_tag = row.find('a', href=True)
            if link_tag and "/military_unit/" in link_tag['href']:
                unit_name = link_tag.text.strip()
                unit_url = link_tag['href']
                if not unit_url.startswith("http"):
                    unit_url = BASE_URL + unit_url
                
                units.append({'name': unit_name, 'url': unit_url})
        
        # Deduplica (a volte ci sono link doppi)
        units = [dict(t) for t in {tuple(d.items()) for d in units}]
        print(f"Trovate {len(units)} unità militari da scansionare.")
        return units
    except Exception as e:
        print(f"Errore critico nel recupero indice unità: {e}")
        return []

def scrape_unit_casualties(unit_url, unit_name):
    """Scrapa TUTTE le pagine di una singola unità."""
    soldiers = []
    page = 1
    max_retries = 3
    
    while True:
        # Costruisci URL paginato (il sito usa ?page=X)
        target_url = f"{unit_url}?page={page}" if page > 1 else unit_url
        # print(f"   Processing Page {page}...", end='\r')
        
        for attempt in range(max_retries):
            try:
                resp = requests.get(target_url, headers=HEADERS, timeout=10)
                if resp.status_code == 404:
                    return soldiers # Fine pagine
                if resp.status_code != 200:
                    time.sleep(2)
                    continue
                break
            except:
                time.sleep(2)
        else:
            print(f"   Errore connessione su pag {page}, salto.")
            break

        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # --- SELETTORE CRUCIALE ---
        # I soldati sono elencati come link <a> con classe "bl" (Block Link)
        # Questo contiene sia il nome (dentro <b>) che il link al profilo
        soldier_links = soup.select('a.bl')
        
        if not soldier_links:
            # Se non ci sono soldati, abbiamo finito le pagine
            break
            
        for link in soldier_links:
            href = link.get('href')
            full_link = BASE_URL + href if href.startswith('/') else href
            name = link.get_text(strip=True)
            
            soldiers.append({
                'unit': unit_name,
                'soldier_name': name,
                'profile_url': full_link
            })
            
        page += 1
        # time.sleep(0.1) # Piccolo delay per non farsi bannare
        
    print(f"   -> Estratti {len(soldiers)} caduti per: {unit_name}")
    return soldiers

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    all_units = get_all_unit_links()
    
    # File di output
    csv_file = "ualosses_complete_dump.csv"
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['unit', 'soldier_name', 'profile_url'])
        writer.writeheader()
        
        total_extracted = 0
        
        # Iterazione su tutte le unità
        for i, unit in enumerate(all_units):
            print(f"[{i+1}/{len(all_units)}] Scraping: {unit['name']}")
            
            unit_casualties = scrape_unit_casualties(unit['url'], unit['name'])
            
            if unit_casualties:
                writer.writerows(unit_casualties)
                total_extracted += len(unit_casualties)
                f.flush() # Salva su disco in tempo reale
            
    print(f"\nScraping Completato! Totale caduti assegnati a unità: {total_extracted}")
    print(f"Dati salvati in: {csv_file}")