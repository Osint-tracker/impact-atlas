
import json
from collections import Counter

def inspect():
    try:
        with open('assets/data/owl_layer.geojson', 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        names = Counter()
        descriptions = Counter()
        types = Counter()

        for feature in data.get('features', []):
            props = feature.get('properties', {})
            geom = feature.get('geometry', {})
            
            if geom.get('type') in ['LineString', 'MultiLineString']:
                name = props.get('name', 'Unknown')
                desc = props.get('description', 'Unknown')
                names[name] += 1
                descriptions[desc] += 1
                types[geom.get('type')] += 1

        print("=== UNIQUE NAMES (LineString) ===")
        for name, count in names.most_common(50):
            print(f"{name}: {count}")

        print("\n=== UNIQUE DESCRIPTIONS (LineString) ===")
        for desc, count in descriptions.most_common(20):
            print(f"{desc}: {count}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspect()
