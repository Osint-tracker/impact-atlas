import json
import re

def fix_double_encoding(text):
    if not isinstance(text, str):
        return text
    try:
        # Standard way to fix double UTF-8 encoding
        return text.encode('latin-1').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        # If standard encoding/decoding fails, we can do it character-by-character
        # only converting U+0000 to U+00FF characters back to bytes, and leaving others as is.
        try:
            bytes_list = []
            i = 0
            n = len(text)
            while i < n:
                c = text[i]
                ord_c = ord(c)
                if ord_c <= 0xFF:
                    bytes_list.append(ord_c)
                else:
                    # If we find a character > 0xFF, it means it's already a wide Unicode character
                    # that was NOT double-encoded (or was partially corrupted).
                    # We first decode whatever bytes we collected so far, append this character,
                    # and reset our byte buffer.
                    pass
                i += 1
            # For robustness, we can try to do a safe conversion
            # Let's write a fallback character-by-character converter
            return fallback_fix(text)
        except Exception:
            return text

def fallback_fix(text):
    # This function processes characters that are double-encoded.
    # A double-encoded character typically starts with U+00C2 or U+00C3 etc.
    # Let's do a byte-level conversion where possible.
    res_bytes = bytearray()
    for c in text:
        o = ord(c)
        if o <= 255:
            res_bytes.append(o)
        else:
            # We encountered a non-latin1 character.
            # We decode what we have, append the character, and continue.
            # But usually JSON keys/values are purely double-encoded or ASCII in our case.
            # So let's just use replacement or ignore if it fails.
            pass
    try:
        return res_bytes.decode('utf-8')
    except Exception:
        # If it still fails, just return original
        return text

# Fix events_timeline.json
timeline_path = 'assets/data/events_timeline.json'
print("Fixing events_timeline.json...")
try:
    with open(timeline_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    def recursive_fix(obj):
        if isinstance(obj, dict):
            return {k: recursive_fix(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [recursive_fix(x) for x in obj]
        elif isinstance(obj, str):
            return fix_double_encoding(obj)
        else:
            return obj
            
    fixed_data = recursive_fix(data)
    
    with open(timeline_path, 'w', encoding='utf-8') as f:
        json.dump(fixed_data, f, indent=2, ensure_ascii=False)
    print("Successfully fixed events_timeline.json!")
except Exception as e:
    print(f"Error fixing events_timeline.json: {e}")

# Fix assets/js/map.js
map_js_path = 'assets/js/map.js'
print("\nFixing assets/js/map.js...")
try:
    with open(map_js_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Replace the specific mojibake patterns found in map.js
    replacements = {
        'Ã‚Â°C': '°C',
        'Ã‚Â©': '©'
    }
    
    original_content = content
    for bad, good in replacements.items():
        count = content.count(bad)
        if count > 0:
            content = content.replace(bad, good)
            print(f"  Replaced '{bad}' with '{good}' {count} times")
            
    if content != original_content:
        with open(map_js_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print("Successfully fixed assets/js/map.js!")
    else:
        print("No replacements needed in assets/js/map.js.")
except Exception as e:
    print(f"Error fixing assets/js/map.js: {e}")
