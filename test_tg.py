import requests
from bs4 import BeautifulSoup
import re

def extract_telegram_cdn(url):
    embed_url = url + "?embed=1"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        r = requests.get(embed_url, headers=headers, timeout=10)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Print part of HTML to see what's actually there
        print("HTML length:", len(r.text))
        print(r.text[:500])
        
        video_tag = soup.find('video')
        if video_tag and video_tag.has_attr('src'):
            return video_tag['src']
            
        photo_tags = soup.find_all('a', class_='tgme_widget_message_photo_wrap')
        if photo_tags:
            style = photo_tags[0].get('style', '')
            match = re.search(r"url\('([^']+)'\)", style)
            if match:
                return match.group(1)
    except Exception as e:
        print("Error:", e)
    return None

if __name__ == "__main__":
    # Let's try a recent channel post
    test_urls = ["https://t.me/rybar/68500", "https://t.me/DeepStateUA/21600"]
    for u in test_urls:
        print(f"{u} -> {extract_telegram_cdn(u)}\n")
