
import json

def repair_json():
    input_file = 'assets/data/trincee_debug.txt'
    output_file = 'assets/data/fortifications_parabellum.geojson'
    
    print(f"Repairing {input_file}...")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Find the start of the features array
        start_index = content.find('"features": [')
        if start_index == -1:
            print("❌ Could not find 'features' array start.")
            return

        # Robust Logic: Find the last start of a new feature
        # Standard GeoJSON format: ..., {"type": "Feature", ...
        # We want to keep everything BEFORE the last incomplete feature.
        
        # Normalize slightly to handle potential whitespace variations if needed, 
        # but standard serializers are usually consistent.
        last_feature_start = content.rfind('{"type": "Feature"')
        if last_feature_start == -1:
            last_feature_start = content.rfind('{"type":"Feature"')
            
        if last_feature_start == -1:
             print("❌ Could not find any feature start.")
             return
             
        # Now look backwards from there for the comma
        # ... }, { "type": ...
        # We want the '}' before the comma.
        
        # Slice up to the start of the broken feature
        valid_chunk = content[:last_feature_start]
        
        # Strip trailing whitespace and comma
        valid_chunk = valid_chunk.rstrip().rstrip(',')
        
        # Now we should have valid JSON array content ending in '}'
        valid_content = valid_chunk + ']}'
        
        # Verify it parses
        try:
            data = json.loads(valid_content)
            feature_count = len(data.get('features', []))
            print(f"Successfully repaired JSON! Recovered {feature_count} features.")
            
            with open(output_file, 'w', encoding='utf-8') as out:
                json.dump(data, out, ensure_ascii=False, indent=2)
            print(f"Saved to {output_file}")
            
        except json.JSONDecodeError as e:
            print(f"Repair failed: Resulting JSON still invalid. {e}")
            # Debug: show the end of our constructed string
            print(f"End of string context: {valid_content[-100:]}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    repair_json()
