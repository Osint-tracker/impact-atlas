"""Quick test scrape of Oryx to understand the current HTML structure."""
import requests
from bs4 import BeautifulSoup
import re, sys

sys.stdout.reconfigure(encoding='utf-8')

ORYX_RU = "https://www.oryxspioenkop.com/2022/02/attack-on-europe-documenting-equipment.html"
ORYX_UA = "https://www.oryxspioenkop.com/2022/02/attack-on-europe-documenting-ukrainian.html"

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

for label, url in [("RU LOSSES", ORYX_RU), ("UA LOSSES", ORYX_UA)]:
    print(f"\n=== {label} ===")
    try:
        r = requests.get(url, headers=headers, timeout=20)
        print(f"Status: {r.status_code}, Length: {len(r.text)}")
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Find all H3 headers (category headers like "Tanks", "AFVs", etc.)
        h3_tags = soup.find_all('h3')
        print(f"H3 category headers found: {len(h3_tags)}")
        for h in h3_tags[:8]:
            txt = h.get_text(strip=True)
            if len(txt) > 5 and len(txt) < 200:
                print(f"  H3: {txt[:120]}")
        
        # Find all <li> with status keywords
        li_items = soup.find_all('li')
        status_lis = [li for li in li_items if any(k in li.get_text(strip=True).lower() for k in ['destroyed','damaged','abandoned','captured'])]
        print(f"\nTotal <li>: {len(li_items)}, with status keywords: {len(status_lis)}")
        
        # Show first 5 examples
        for li in status_lis[:5]:
            text = li.get_text(strip=True)
            print(f"  LI: {text[:150]}")
        
        # Try to find the summary counts near each H3
        # Oryx format: "Russia - 12345, of which: destroyed: 6789, damaged: 1234, abandoned: 567, captured: 890"
        article = soup.find('article') or soup.find('div', class_='post-body')
        if article:
            full_text = article.get_text()
            # Look for summary lines
            summary_pattern = r'(\d[\d,]+)\s*,\s*of which:\s*destroyed:\s*(\d[\d,]*)\s*,?\s*damaged:\s*(\d[\d,]*)\s*,?\s*abandoned:\s*(\d[\d,]*)\s*,?\s*captured:\s*(\d[\d,]*)'
            matches = re.findall(summary_pattern, full_text, re.IGNORECASE)
            print(f"\nSummary lines (Total, D, Dam, Ab, Cap): {len(matches)} found")
            for m in matches[:10]:
                print(f"  {m}")
        
    except Exception as e:
        print(f"Error: {e}")
