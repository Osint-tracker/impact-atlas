
import requests
import json
import sys

# Windows Unicode Fix
sys.stdout.reconfigure(encoding='utf-8')

WFS_URL = "https://geo.parabellumthinktank.com/index.php/lizmap/service"

def fetch_trenches():
    print("🚀 Fetching Trenches from Parabellum WFS...")
    
    params = {
        "repository": "russoukrainianwar",
        "project": "russian_invasion_of_ukraine",
        "SERVICE": "WFS",
        "REQUEST": "GetFeature",
        "TYPENAME": "Trincee",  # Trying the name provided by user
        "VERSION": "1.1.0",
        "OUTPUTFORMAT": "GeoJSON"
    }

    try:
        response = requests.get(WFS_URL, params=params, timeout=60)
        
        if response.status_code != 200:
            print(f"❌ Error: HTTP {response.status_code}")
            return

        # Save RAW content for debugging
        with open('assets/data/trincee_debug.txt', 'w', encoding='utf-8') as f:
            f.write(response.text)
        print("💾 Saved raw response to assets/data/trincee_debug.txt")

        try:
            content = response.text
            # Aggressive cleanup: find first { and last }
            start = content.find('{')
            end = content.rfind('}')
            
            if start != -1 and end != -1:
                content = content[start:end+1]
                
            data = json.loads(content)
            features = data.get('features', [])
            print(f"✅ Retrieved {len(features)} fortification features")
            
            if features:
                with open('assets/data/fortifications_parabellum.geojson', 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"💾 Saved {len(features)} features to geojson")
                
        except Exception as e:
            print(f"❌ Parsing Error: {e}")

            
            if features:
                # Save to file
                output_file = 'assets/data/fortifications_parabellum.geojson'
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"💾 Saved to {output_file}")
                
                # Inspect first feature
                print("\n🧐 First Feature Properties:")
                print(json.dumps(features[0]['properties'], indent=2, ensure_ascii=False))
            else:
                print("⚠️ No features found. Layer name might be incorrect.")
                
        except json.JSONDecodeError:
            print("❌ Response is not valid JSON")
            print(response.text[:500])

    except Exception as e:
        print(f"❌ Exception: {e}")

if __name__ == "__main__":
    fetch_trenches()
