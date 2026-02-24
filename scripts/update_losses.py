"""
Equipment Loss Scraper v3.0 — War Ledger Edition
Scrapes Oryx (RU + UA pages) and WarSpotting for verified equipment losses.
Produces:
  1. external_losses.json — Individual item-level losses (legacy format, enriched)
  2. net_losses_summary.json — Per-category Net Loss aggregation for the War Ledger UI
"""
import requests
import json
import datetime
import re
import os
import sys
from bs4 import BeautifulSoup

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, '../assets/data/external_losses.json')
NET_SUMMARY_FILE = os.path.join(BASE_DIR, '../assets/data/net_losses_summary.json')

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

def safe_print(msg):
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode('ascii', 'ignore').decode('ascii'))


# =============================================================================
# ORYX CATEGORY MAPPING — Normalize H3 titles to standard categories
# =============================================================================
CATEGORY_MAP = {
    'tanks': 'Tanks',
    'armoured fighting vehicles': 'AFVs',
    'infantry fighting vehicles': 'IFVs',
    'armoured personnel carriers': 'APCs',
    'mine-resistant ambush protected': 'MRAPs',
    'infantry mobility vehicles': 'IMVs',
    'communications stations': 'Comms',
    'engineering vehicles': 'Engineering',
    'command posts': 'Command Posts',
    'anti-tank guided missiles': 'ATGM',
    'man-portable air defence systems': 'MANPADS',
    'heavy mortars': 'Heavy Mortars',
    'towed artillery': 'Towed Artillery',
    'self-propelled artillery': 'SP Artillery',
    'multiple rocket launchers': 'MLRS',
    'anti-aircraft guns': 'AA Guns',
    'self-propelled anti-aircraft guns': 'SP AA Guns',
    'surface-to-air missile systems': 'SAM Systems',
    'radars': 'Radars',
    'jammers': 'EW/Jammers',
    'aircraft': 'Aircraft',
    'helicopters': 'Helicopters',
    'unmanned combat aerial vehicles': 'UCAV',
    'reconnaissance uavs': 'Recon UAVs',
    'naval ships': 'Naval',
    'trucks': 'Trucks',
    'jeeps': 'Jeeps',
}


class OryxDualProvider:
    """Scrapes both Oryx RU-loss and UA-loss pages, extracting per-category aggregates."""

    ORYX_PAGES = {
        'RU': 'https://www.oryxspioenkop.com/2022/02/attack-on-europe-documenting-equipment.html',
        'UA': 'https://www.oryxspioenkop.com/2022/02/attack-on-europe-documenting-ukrainian.html',
    }

    def __init__(self):
        self.name = "Oryx"
        self.item_losses = []       # Individual items (legacy format)
        self.category_stats = {}    # Per-faction, per-category aggregate {faction: {category: {...}}}

    def fetch(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        today = datetime.datetime.now().strftime("%Y-%m-%d")

        for faction, url in self.ORYX_PAGES.items():
            safe_print(f"[{self.name}] Scraping {faction} losses from Oryx...")
            try:
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')

                # ---- PHASE 1: Parse H3 category headers for aggregate counts ----
                h3_tags = soup.find_all('h3')
                faction_stats = {}
                global_total = None

                for h3 in h3_tags:
                    text = h3.get_text(strip=True)
                    if not text or len(text) < 10:
                        continue

                    # Match pattern: "CategoryName(COUNT, of which destroyed: X, damaged: Y, abandoned: Z, captured: W)"
                    # Or the global summary: "Russia - 24136, of which: destroyed: 18799, ..."
                    cat_match = re.match(
                        r'^(.+?)\s*\(?(\d[\d,]*)\s*,\s*of which\s*:?\s*destroyed:\s*(\d[\d,]*)\s*,?\s*damaged:\s*(\d[\d,]*)\s*,?\s*abandoned:\s*(\d[\d,]*)\s*,?\s*captured:\s*(\d[\d,]*)',
                        text, re.IGNORECASE
                    )
                    if not cat_match:
                        continue

                    raw_cat = cat_match.group(1).strip().rstrip('(').strip()
                    total = int(cat_match.group(2).replace(',', ''))
                    destroyed = int(cat_match.group(3).replace(',', ''))
                    damaged = int(cat_match.group(4).replace(',', ''))
                    abandoned = int(cat_match.group(5).replace(',', ''))
                    captured = int(cat_match.group(6).replace(',', ''))

                    # Skip aggregate lines (e.g. "Russia - 24136" or "Losses excluding...")
                    if any(skip in raw_cat.lower() for skip in ['russia', 'ukraine', 'losses excluding', 'losses of armoured combat']):
                        if global_total is None:
                            global_total = {
                                'total': total, 'destroyed': destroyed,
                                'damaged': damaged, 'abandoned': abandoned, 'captured': captured
                            }
                        continue

                    # Normalize category name
                    cat_key = raw_cat.lower()
                    std_cat = None
                    for pattern, mapped in CATEGORY_MAP.items():
                        if pattern in cat_key:
                            std_cat = mapped
                            break
                    if not std_cat:
                        std_cat = raw_cat[:30]

                    faction_stats[std_cat] = {
                        'total': total,
                        'destroyed': destroyed,
                        'damaged': damaged,
                        'abandoned': abandoned,
                        'captured': captured
                    }

                self.category_stats[faction] = {
                    'global': global_total or {},
                    'categories': faction_stats
                }

                safe_print(f"[{self.name}] {faction}: Global total = {global_total.get('total', '?') if global_total else '?'}, {len(faction_stats)} categories parsed")
                for cat, s in faction_stats.items():
                    safe_print(f"   {cat}: {s['total']} (D:{s['destroyed']} Dam:{s['damaged']} Ab:{s['abandoned']} Cap:{s['captured']})")

                # ---- PHASE 2: Parse individual <li> items — expand each entry ----
                li_items = soup.find_all('li')
                count = 0

                for li in li_items:
                    text = li.get_text(strip=True)
                    if not any(k in text.lower() for k in ['destroyed', 'damaged', 'abandoned', 'captured']):
                        continue
                    if not re.match(r'^\d+', text):
                        continue

                    try:
                        parts = text.split(':')
                        if len(parts) < 2:
                            continue
                        raw_model = parts[0].strip()
                        model = re.sub(r'^\d+\s+', '', raw_model)

                        # Classify category from model name
                        model_lower = model.lower()
                        cat = 'Vehicle'
                        if re.match(r'^t-\d', model_lower):
                            cat = 'Tanks'
                        elif any(k in model_lower for k in ['bmp', 'bmd', 'bradley', 'cv90', 'marder']):
                            cat = 'IFVs'
                        elif any(k in model_lower for k in ['btr', 'stryker', 'spartan', 'mastiff', 'wolfhound']):
                            cat = 'APCs'
                        elif any(k in model_lower for k in ['su-', 'mig-', 'tu-', 'an-', 'il-', 'f-16', 'a-10']):
                            cat = 'Aircraft'
                        elif any(k in model_lower for k in ['mi-', 'ka-', 'ah-', 'uh-']):
                            cat = 'Helicopters'
                        elif any(k in model_lower for k in ['s-300', 's-400', 'buk', 'tor', 'pantsir', 'osa', 'patriot', 'nasams', 'iris', 'gepard']):
                            cat = 'SAM Systems'
                        elif any(k in model_lower for k in ['msta', 'gvozdika', 'akatsiya', 'giatsint', 'pzh', 'caesar', 'krab', 'dana', 'nona', 'vasilek']):
                            cat = 'SP Artillery'
                        elif any(k in model_lower for k in ['grad', 'uragan', 'smerch', 'tornado', 'himars', 'mlrs', 'bm-21', 'bm-27']):
                            cat = 'MLRS'

                        # Collect proof URLs from <a> tags
                        link_tags = li.find_all('a', href=True)
                        proof_urls = [a['href'] for a in link_tags if a['href'].startswith('http')]
                        default_proof = proof_urls[0] if proof_urls else self.ORYX_PAGES[faction]

                        # EXPAND: Parse each parenthesized entry "(N, status)" 
                        entries = re.findall(r'\(([^)]+)\)', text)
                        proof_idx = 0
                        for entry in entries:
                            entry_lower = entry.lower()
                            # Determine status of this entry
                            if 'destroyed' in entry_lower:
                                status = 'Destroyed'
                            elif 'captured' in entry_lower:
                                status = 'Captured'
                            elif 'damaged' in entry_lower:
                                status = 'Damaged'
                            elif 'abandoned' in entry_lower:
                                status = 'Abandoned'
                            else:
                                continue

                            # Get matching proof URL if available
                            proof = proof_urls[proof_idx] if proof_idx < len(proof_urls) else default_proof
                            proof_idx += 1

                            self.item_losses.append({
                                "date": today,
                                "model": model,
                                "type": cat,
                                "country": faction,
                                "status": status,
                                "proof_url": proof,
                                "source_tag": "Oryx"
                            })
                            count += 1
                    except Exception:
                        continue

                safe_print(f"[{self.name}] {faction}: {count} individual items expanded")

            except Exception as e:
                safe_print(f"[{self.name}] Error scraping {faction}: {e}")

    def build_net_summary(self):
        """Calculate Net Loss for each faction/category.
        
        Net Loss = Total Losses - Captured from Enemy
        Where 'Captured from Enemy' = the opponent's 'captured' count for that category.
        """
        ru_stats = self.category_stats.get('RU', {}).get('categories', {})
        ua_stats = self.category_stats.get('UA', {}).get('categories', {})
        ru_global = self.category_stats.get('RU', {}).get('global', {})
        ua_global = self.category_stats.get('UA', {}).get('global', {})

        # All categories from both sides
        all_cats = sorted(set(list(ru_stats.keys()) + list(ua_stats.keys())))

        summary = {
            'generated_at': datetime.datetime.now().isoformat(),
            'source': 'Oryx (oryxspioenkop.com)',
            'disclaimer': 'These are visually confirmed minimums. Actual losses are likely higher for both sides.',
            'global': {
                'RU': {
                    'total_lost': ru_global.get('total', 0),
                    'destroyed': ru_global.get('destroyed', 0),
                    'damaged': ru_global.get('damaged', 0),
                    'abandoned': ru_global.get('abandoned', 0),
                    'captured_by_enemy': ru_global.get('captured', 0),
                    'captured_from_enemy': ua_global.get('captured', 0),
                    'net_loss': ru_global.get('total', 0) - ua_global.get('captured', 0),
                },
                'UA': {
                    'total_lost': ua_global.get('total', 0),
                    'destroyed': ua_global.get('destroyed', 0),
                    'damaged': ua_global.get('damaged', 0),
                    'abandoned': ua_global.get('abandoned', 0),
                    'captured_by_enemy': ua_global.get('captured', 0),
                    'captured_from_enemy': ru_global.get('captured', 0),
                    'net_loss': ua_global.get('total', 0) - ru_global.get('captured', 0),
                }
            },
            'categories': {}
        }

        for cat in all_cats:
            ru_cat = ru_stats.get(cat, {'total': 0, 'destroyed': 0, 'damaged': 0, 'abandoned': 0, 'captured': 0})
            ua_cat = ua_stats.get(cat, {'total': 0, 'destroyed': 0, 'damaged': 0, 'abandoned': 0, 'captured': 0})

            summary['categories'][cat] = {
                'RU': {
                    'total_lost': ru_cat['total'],
                    'destroyed': ru_cat['destroyed'],
                    'damaged': ru_cat['damaged'],
                    'abandoned': ru_cat['abandoned'],
                    'captured_by_enemy': ru_cat['captured'],
                    'captured_from_enemy': ua_cat['captured'],
                    'net_loss': ru_cat['total'] - ua_cat['captured'],
                },
                'UA': {
                    'total_lost': ua_cat['total'],
                    'destroyed': ua_cat['destroyed'],
                    'damaged': ua_cat['damaged'],
                    'abandoned': ua_cat['abandoned'],
                    'captured_by_enemy': ua_cat['captured'],
                    'captured_from_enemy': ru_cat['captured'],
                    'net_loss': ua_cat['total'] - ru_cat['captured'],
                }
            }

        return summary


def main():
    safe_print("╔══════════════════════════════════════════════╗")
    safe_print("║   WAR LEDGER — Equipment Loss Scraper v3.0  ║")
    safe_print("╚══════════════════════════════════════════════╝")

    oryx = OryxDualProvider()
    oryx.fetch()

    # Write item-level losses (legacy format)
    all_losses = sorted(oryx.item_losses, key=lambda x: x['date'], reverse=True)

    if all_losses:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(all_losses, f, indent=2, ensure_ascii=False)
        safe_print(f"\n[SUCCESS] Wrote {len(all_losses)} individual items to {OUTPUT_FILE}")
    else:
        safe_print("[WARN] No individual items scraped.")

    # Write Net Loss summary
    net_summary = oryx.build_net_summary()
    if net_summary['categories']:
        with open(NET_SUMMARY_FILE, 'w', encoding='utf-8') as f:
            json.dump(net_summary, f, indent=2, ensure_ascii=False)
        safe_print(f"[SUCCESS] Wrote Net Loss summary to {NET_SUMMARY_FILE}")

        # Print summary
        safe_print("\n═══ WAR LEDGER SUMMARY ═══")
        for faction in ['RU', 'UA']:
            g = net_summary['global'][faction]
            safe_print(f"\n  {faction} GLOBAL: Total Lost={g['total_lost']}, Captured from Enemy={g['captured_from_enemy']}, NET LOSS={g['net_loss']}")
    else:
        safe_print("[WARN] No summary generated.")


if __name__ == "__main__":
    main()
