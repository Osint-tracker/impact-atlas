import json
import os
import math

def fix_nan_in_value(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    elif isinstance(value, str) and value.lower() in ['nan', 'NaN']:
        return None
    elif isinstance(value, dict):
        return {k: fix_nan_in_value(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [fix_nan_in_value(item) for item in value]
    return value

def process_json_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        cleaned_data = fix_nan_in_value(data)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
        print(f"✓ {filepath}")
        return True
    except Exception as e:
        print(f"✗ {filepath}: {e}")
        return False

for dirpath, dirnames, filenames in os.walk('.'):
    dirnames[:] = [d for d in dirnames if d not in ['.git', 'node_modules', '__pycache__']]
    for filename in filenames:
        if filename.endswith(('.json', '.geojson')):
            process_json_file(os.path.join(dirpath, filename))
