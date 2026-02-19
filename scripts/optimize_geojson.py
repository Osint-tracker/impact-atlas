
import json

def optimize_geojson():
    input_file = 'assets/data/fortifications_parabellum.geojson'
    output_file = 'assets/data/fortifications_parabellum_optimized.geojson'
    
    print(f"Optimizing {input_file}...")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        original_size = len(json.dumps(data))
        print(f"   Original Size: {original_size / 1024 / 1024:.2f} MB")
        
        # Round coordinates function
        def round_coords(coords):
            if isinstance(coords, float):
                return round(coords, 5)
            elif isinstance(coords, list):
                return [round_coords(c) for c in coords]
            return coords

        count = 0
        for feature in data.get('features', []):
            geom = feature.get('geometry', {})
            if 'coordinates' in geom:
                geom['coordinates'] = round_coords(geom['coordinates'])
            
            # Remove unnecessary properties to save space
            props = feature['properties']
            keep_keys = ['id', 'name'] # minimal set
            feature['properties'] = {k: v for k, v in props.items() if k in keep_keys}
            count += 1

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, separators=(',', ':'), ensure_ascii=False)
            
        new_size = len(json.dumps(data, separators=(',', ':')))
        print(f"   Optimized Size: {new_size / 1024 / 1024:.2f} MB")
        print(f"   Reduction: {100 - (new_size/original_size*100):.1f}%")
        print(f"   Processed {count} features")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    optimize_geojson()
