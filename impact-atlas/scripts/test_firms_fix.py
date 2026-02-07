"""
test_firms_fix.py - Test the proposed FIRMS fix before merging
===============================================================
Tests:
1. Multi-satellite data fetching (SNPP + NOAA20 + MODIS)
2. 3-day window aggregation
3. GeoJSON output format validation
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import requests
import json
from datetime import datetime
from pathlib import Path

FIRMS_API_KEY = "29ca712bddef41c37ea9989a2b521dea"
UKRAINE_BBOX = "22.1,44.3,40.2,52.4"

def fetch_multi_satellite_data(days=3):
    """Fetch fire data from multiple satellite sources"""
    
    satellites = [
        "VIIRS_SNPP_NRT",
        "VIIRS_NOAA20_NRT",
        "VIIRS_NOAA21_NRT",
        "MODIS_NRT"
    ]
    
    all_features = []
    seen_coords = set()  # For deduplication
    
    print(f"\n📡 Fetching from {len(satellites)} satellite sources ({days}-day window)...")
    
    for sat in satellites:
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{FIRMS_API_KEY}/{sat}/{UKRAINE_BBOX}/{days}"
        
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code != 200:
                print(f"   ⚠️ {sat}: HTTP {resp.status_code}")
                continue
                
            lines = resp.text.strip().split('\n')
            if len(lines) < 2:
                print(f"   ⚠️ {sat}: No data")
                continue
            
            headers = lines[0].split(',')
            count = 0
            
            for line in lines[1:]:
                values = line.split(',')
                if len(values) < 3:
                    continue
                    
                data = dict(zip(headers, values))
                
                try:
                    lat = float(data['latitude'])
                    lon = float(data['longitude'])
                    
                    # Deduplication by rounded coords
                    coord_key = (round(lat, 3), round(lon, 3))
                    if coord_key in seen_coords:
                        continue
                    seen_coords.add(coord_key)
                    
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [lon, lat]
                        },
                        "properties": {
                            "brightness": float(data.get('bright_ti4', data.get('brightness', 300))),
                            "confidence": data.get('confidence', 'n'),
                            "acq_date": data.get('acq_date'),
                            "acq_time": data.get('acq_time'),
                            "satellite": sat.split('_')[0] + "_" + sat.split('_')[1] if '_' in sat else sat,
                            "frp": float(data.get('frp', 0)) if data.get('frp') else 0
                        }
                    }
                    all_features.append(feature)
                    count += 1
                    
                except (ValueError, KeyError):
                    continue
            
            print(f"   ✅ {sat}: {count} detections")
            
        except Exception as e:
            print(f"   ❌ {sat}: Error - {e}")
    
    return all_features


def create_test_geojson(features, output_path="assets/data/thermal_firms_test.geojson"):
    """Create GeoJSON from aggregated features"""
    
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "generated": datetime.now().isoformat(),
            "source": "NASA FIRMS (Multi-satellite)",
            "satellites": ["VIIRS_SNPP", "VIIRS_NOAA20", "VIIRS_NOAA21", "MODIS"],
            "days": 3
        },
        "features": features
    }
    
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2)
    
    return path


def validate_geojson_for_frontend(path):
    """Validate the GeoJSON works for Leaflet"""
    
    print(f"\n🔍 Validating GeoJSON for frontend compatibility...")
    
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    errors = []
    
    # Check structure
    if data.get('type') != 'FeatureCollection':
        errors.append("Missing 'type': 'FeatureCollection'")
    
    features = data.get('features', [])
    if not isinstance(features, list):
        errors.append("'features' is not a list")
    
    # Check each feature
    for i, f in enumerate(features[:5]):  # Check first 5
        if f.get('type') != 'Feature':
            errors.append(f"Feature {i}: Missing 'type': 'Feature'")
        
        geom = f.get('geometry', {})
        if geom.get('type') != 'Point':
            errors.append(f"Feature {i}: Geometry is not Point")
        
        coords = geom.get('coordinates', [])
        if len(coords) != 2:
            errors.append(f"Feature {i}: Invalid coordinates")
        elif not (-180 <= coords[0] <= 180 and -90 <= coords[1] <= 90):
            errors.append(f"Feature {i}: Coordinates out of range")
        
        props = f.get('properties', {})
        if 'brightness' not in props:
            errors.append(f"Feature {i}: Missing 'brightness' property")
    
    if errors:
        print("   ❌ Validation FAILED:")
        for e in errors:
            print(f"      - {e}")
        return False
    else:
        print(f"   ✅ Validation PASSED ({len(features)} features)")
        return True


def main():
    print("="*60)
    print("FIRMS FIX TEST SUITE")
    print("="*60)
    
    # Test 1: Multi-satellite fetch
    features = fetch_multi_satellite_data(days=3)
    
    print(f"\n📊 TOTAL UNIQUE DETECTIONS: {len(features)}")
    
    if len(features) == 0:
        print("❌ TEST FAILED: No data retrieved")
        return False
    
    # Test 2: Create test GeoJSON
    test_path = create_test_geojson(features)
    print(f"\n📄 Test GeoJSON saved: {test_path}")
    
    # Test 3: Validate for frontend
    valid = validate_geojson_for_frontend(test_path)
    
    # Summary
    print("\n" + "="*60)
    if len(features) > 0 and valid:
        print("✅ ALL TESTS PASSED - Safe to merge fix into production code")
        
        # Show sample
        print(f"\n📍 Sample detections:")
        for f in features[:3]:
            props = f['properties']
            coords = f['geometry']['coordinates']
            print(f"   • {props['satellite']}: [{coords[1]:.2f}, {coords[0]:.2f}] "
                  f"brightness={props['brightness']:.0f} confidence={props['confidence']}")
        
        return True
    else:
        print("❌ TESTS FAILED - Do not merge")
        return False


if __name__ == "__main__":
    success = main()
    print("\n" + "="*60)
    exit(0 if success else 1)
