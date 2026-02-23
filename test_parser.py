from urllib.parse import urlparse
import json

def parse_sources_to_list(sources_str):
    if not sources_str:
        return []
    
    # Simulate generate_output.py behavior
    if ' ||| ' in str(sources_str):
        urls = [u.strip() for u in str(sources_str).split(' ||| ') if u.strip()]
    elif ' | ' in str(sources_str):
        urls = [u.strip() for u in str(sources_str).split(' | ') if u.strip()]
    else:
        urls = [str(sources_str).strip()] if sources_str else []
    
    result = []
    for url in urls:
        url = str(url).strip()
        if len(url) < 5 or url.lower() in ['none', 'null', 'unknown', '[null]']:
            continue
        try:
            domain = urlparse(url).netloc.replace('www.', '')
            if not domain:
                domain = "Source"
        except:
            domain = "Source"
        result.append({"name": domain, "url": url})
    
    return result

# From DB test
print(parse_sources_to_list('["https://www.foxnews.com/world/...", "https://www.samaa.tv/..."]'))

# From Telegram
print(parse_sources_to_list('["Rybar"]'))
