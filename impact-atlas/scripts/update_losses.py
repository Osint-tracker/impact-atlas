import requests
import json
import datetime
import re
import os
import time
import sys
from bs4 import BeautifulSoup

# Configuration
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), '../assets/data/external_losses.json')

# Helper for safe printing on Windows consoles
def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', 'ignore').decode('ascii'))

class LossProvider:
    def __init__(self, name):
        self.name = name
        self.losses = []

    def fetch(self):
        raise NotImplementedError("Subclasses must implement fetch()")

    def get_losses(self):
        return self.losses

class WarSpottingProvider(LossProvider):
    def __init__(self):
        super().__init__("WarSpotting")
        # Trying a slightly different search endpoint closer to their API usage or standard view
        self.url = "https://ukr.warspotting.net/search/?belligerent=2&weapon=0&model=&date_from=&date_to=&id_from=&id_to=&status=0"

    def fetch(self):
        safe_print(f"[{self.name}] Connecting to {self.url}...")
        try:
            # Enhanced Headers to mimic a real browser request to bypass 403
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://ukr.warspotting.net/',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'same-origin',
                'Sec-Fetch-User': '?1'
            }
            
            session = requests.Session()
            response = session.get(self.url, headers=headers, timeout=20)
            
            if response.status_code == 403:
                safe_print(f"[{self.name}] Access Denied (403). WarSpotting Cloudflare protection is active.")
                return 

            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            cards = soup.find_all('div', class_='card') 
            count = 0
            for card in cards:
                try:
                    text_content = card.get_text(" ", strip=True)
                    
                    model = "Unknown"
                    header = card.find('h5') or card.find('h4')
                    if header:
                        model = header.get_text(strip=True)
                    
                    date_match = re.search(r'\d{4}-\d{2}-\d{2}', text_content)
                    date = date_match.group(0) if date_match else "2024-01-01"

                    status = "Destroyed"
                    if "damaged" in text_content.lower(): status = "Damaged"
                    if "abandoned" in text_content.lower(): status = "Abandoned"
                    if "captured" in text_content.lower(): status = "Captured"
                    
                    link_tag = card.find('a', href=True)
                    proof_url = f"https://ukr.warspotting.net{link_tag['href']}" if link_tag else "#"

                    self.losses.append({
                        "date": date,
                        "model": model,
                        "type": "Ground Asset",
                        "country": "RUS",
                        "status": status,
                        "proof_url": proof_url
                    })
                    count += 1
                except:
                    continue
            
            safe_print(f"[{self.name}] Parsed {count} items.")
            
        except Exception as e:
            safe_print(f"[{self.name}] Scraping Error: {e}")

class DeepStateProvider(LossProvider):
    def __init__(self):
        super().__init__("DeepState")
        # DeepState API - trying multiple known endpoints
        self.endpoints = [
            "https://deepstatemap.live/api/history/public",
            "https://api.deepstatemap.live/api/v1/history",
            "https://deepstatemap.live/api/losses"  # Speculative losses endpoint
        ]

    def fetch(self):
        safe_print(f"[{self.name}] Attempting to connect to DeepState API...")
        
        for url in self.endpoints:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'application/json',
                    'Origin': 'https://deepstatemap.live',
                    'Referer': 'https://deepstatemap.live/'
                }
                
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    safe_print(f"[{self.name}] Connected to: {url}")
                    
                    try:
                        data = response.json()
                        
                        # Debug: Print the structure of the response
                        if isinstance(data, dict):
                            safe_print(f"[{self.name}] Response keys: {list(data.keys())[:5]}")
                            # Try to find a list of events/losses
                            for key in ['data', 'losses', 'events', 'items', 'history']:
                                if key in data and isinstance(data[key], list):
                                    items = data[key]
                                    safe_print(f"[{self.name}] Found '{key}' with {len(items)} items.")
                                    self._parse_items(items)
                                    return
                        elif isinstance(data, list):
                            safe_print(f"[{self.name}] Response is a list with {len(data)} items.")
                            self._parse_items(data)
                            return
                        else:
                            safe_print(f"[{self.name}] Unexpected response type: {type(data)}")
                            
                    except json.JSONDecodeError:
                        safe_print(f"[{self.name}] Response is not valid JSON.")
                        continue
                else:
                    safe_print(f"[{self.name}] {url} returned status {response.status_code}")
                    
            except Exception as e:
                safe_print(f"[{self.name}] Error with {url}: {e}")
                continue
        
        safe_print(f"[{self.name}] Could not retrieve usable data from any endpoint.")
    
    def _parse_items(self, items):
        """Attempt to parse items from DeepState into our loss format."""
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        count = 0
        
        for item in items[:100]:  # Limit to first 100 to avoid overwhelming
            try:
                # Try common field names
                model = item.get('name') or item.get('title') or item.get('type') or item.get('model') or "Unknown Unit"
                date = item.get('date') or item.get('timestamp') or item.get('created_at') or today
                status = item.get('status') or item.get('state') or "Reported"
                
                # Normalize date if it's a timestamp
                if isinstance(date, (int, float)):
                    date = datetime.datetime.fromtimestamp(date).strftime("%Y-%m-%d")
                
                self.losses.append({
                    "date": str(date)[:10],  # Ensure YYYY-MM-DD format
                    "model": str(model),
                    "type": "DeepState Report",
                    "country": "RUS",  # Assuming Russian losses
                    "status": str(status),
                    "proof_url": "https://deepstatemap.live/"
                })
                count += 1
            except Exception:
                continue
        
        safe_print(f"[{self.name}] Parsed {count} items.")

class OryxProvider(LossProvider):
    def __init__(self):
        super().__init__("Oryx")
        self.url = "https://www.oryxspioenkop.com/2022/02/attack-on-europe-documenting-equipment.html"

    def fetch(self):
        safe_print(f"[{self.name}] Connecting to Oryx...")
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(self.url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            list_items = soup.find_all('li')
            count = 0
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            
            for li in list_items:
                text = li.get_text(strip=True)
                if not any(k in text.lower() for k in ['destroyed', 'damaged', 'abandoned', 'captured']):
                    continue
                if not re.match(r'^\d+', text):
                    continue

                try:
                    parts = text.split(':')
                    if len(parts) < 2: continue
                    raw_model = parts[0].strip()
                    model = re.sub(r'^\d+\s+', '', raw_model)
                    
                    self.losses.append({
                        "date": today,
                        "model": model,
                        "type": "Vehicle",
                        "country": "RUS",
                        "status": "Verified Loss",
                        "proof_url": self.url
                    })
                    count += 1
                except:
                    continue
                    
            safe_print(f"[{self.name}] Found {count} lines of equipment.")

        except Exception as e:
            safe_print(f"[{self.name}] Error: {e}")

def main():
    safe_print("=== STARTING REAL SCRAPING ===")
    
    providers = [WarSpottingProvider(), OryxProvider(), DeepStateProvider()]
    all_losses = []

    for p in providers:
        p.fetch()
        all_losses.extend(p.get_losses())

    if not all_losses:
        safe_print("WARN: No data found. Check internet connection or site layout changes.")
    else:
        final_json = sorted(all_losses, key=lambda x: x['date'], reverse=True)
        
        try:
            os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                json.dump(final_json, f, indent=2)
            safe_print(f"[SUCCESS] Wrote {len(final_json)} items to {OUTPUT_FILE}")
        except Exception as e:
            safe_print(f"[ERROR] Failed to write JSON: {e}")

if __name__ == "__main__":
    main()
