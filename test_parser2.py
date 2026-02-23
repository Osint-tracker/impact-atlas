from urllib.parse import urlparse
import json

def parse_sources_to_list(sources_str):
    if not sources_str or sources_str == '[]':
        return []
    
    urls = []
    try:
        parsed = json.loads(sources_str)
        if isinstance(parsed, list):
            urls = parsed
    except:
        pass
    
    if not urls:
        if ' ||| ' in str(sources_str):
            urls = [u.strip() for u in str(sources_str).split(' ||| ') if u.strip()]
        elif ' | ' in str(sources_str):
            urls = [u.strip() for u in str(sources_str).split(' | ') if u.strip()]
        else:
            urls = [str(sources_str).strip()] if sources_str else []
    
    result = []
    for url in urls:
        url = str(url).strip()
        if len(url) < 3 or url.lower() in ['none', 'null', 'unknown', '[null]']:
            continue
            
        is_url = url.startswith('http') or url.startswith('www.')
        if is_url:
            try:
                domain = urlparse(url if url.startswith('http') else 'https://'+url).netloc.replace('www.', '')
                if not domain: domain = "Source"
            except:
                domain = "Source"
            result.append({"name": domain, "url": url})
        else:
            domain = url
            final_url = f"https://t.me/{domain}" if domain != 'GDELT_Network' else "#"
            result.append({"name": domain, "url": final_url})
            
    return result

print(parse_sources_to_list('["https://www.foxnews.com/world/...", "https://www.samaa.tv/..."]'))
print(parse_sources_to_list('["Rybar"]'))
print(parse_sources_to_list('["GDELT_Network"]'))
