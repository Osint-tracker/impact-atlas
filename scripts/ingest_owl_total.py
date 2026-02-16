import requests
import zipfile
import io
import json
import os
import re
from bs4 import BeautifulSoup

# Config
KMZ_URL = "https://github.com/owlmaps/UAControlMapBackups/raw/master/latest.kmz"
OUTPUT_FILE = os.path.join("assets", "data", "owl_layer.geojson")

def parse_coordinates(coord_str):
    """Converts KML coordinates string 'lon,lat,alt ...' to list of [lon, lat]"""
    coords = []
    # KML coords are space-separated tuples: lon,lat,alt
    # e.g. "36.374,50.300,0 36.378,50.293,0"
    for pair in coord_str.strip().split():
        parts = pair.split(',')
        if len(parts) >= 2:
            try:
                lon = float(parts[0])
                lat = float(parts[1])
                # Ignore altitude for 2D map
                coords.append([lon, lat])
            except ValueError:
                continue
    return coords

def harvest_owl_layers():
    print("Owl Map Harvester v2.0 (KMZ Edition)...")
    
    try:
        print(f"1. Downloading KMZ from {KMZ_URL}...")
        r = requests.get(KMZ_URL, timeout=60)
        r.raise_for_status()
    except Exception as e:
        print(f"Download failed: {e}")
        return

    print("2. Decompressing KMZ...")
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            # Find the .kml file inside
            kml_files = [f for f in z.namelist() if f.endswith('.kml')]
            if not kml_files:
                print("No KML found inside KMZ")
                return
            
            print(f"   Found KML: {kml_files[0]}")
            kml_content = z.read(kml_files[0])
    except Exception as e:
        print(f"Decompression failed: {e}")
        return

    print("3. Parsing KML...")
    # Using 'lxml' if available, else 'xml' (html.parser is bad for case sensitive tags)
    try:
        soup = BeautifulSoup(kml_content, 'xml')
    except:
        soup = BeautifulSoup(kml_content, 'lxml-xml')
    
    features = []
    
    # Iterate through all Placemarks
    placemarks = soup.find_all('Placemark')
    print(f"   Found {len(placemarks)} placemarks. Processing...")

    for pm in placemarks:
        name = pm.find('name').text if pm.find('name') else "Unknown"
        description = pm.find('description').text if pm.find('description') else ""
        
        # Determine styling/folder context 
        # In simple KML parsing, getting parent Folder name is hard without full tree traversal.
        # We rely on text content and styleUrl.
        style_url = pm.find('styleUrl').text if pm.find('styleUrl') else ""
        
        side = "NEUTRAL"
        full_text = f"{name} {description} {style_url}".lower()
        
        if "ukrain" in full_text or "ua" in style_url.lower():
            side = "UA"
        elif "russia" in full_text or "ru" in style_url.lower():
            side = "RU"
        
        # Geometry extraction
        geo_json_geometry = None
        
        # 1. LineString
        line = pm.find('LineString')
        if line:
            coords_tag = line.find('coordinates')
            if coords_tag:
                coords = parse_coordinates(coords_tag.text)
                if coords:
                    geo_json_geometry = {
                        "type": "LineString",
                        "coordinates": coords
                    }
        
        # 2. Polygon
        if not geo_json_geometry:
            poly = pm.find('Polygon')
            if poly:
                outer = poly.find('outerBoundaryIs')
                if outer:
                    # KML Polygon -> outerBoundaryIs -> LinearRing -> coordinates
                    lr = outer.find('LinearRing')
                    if lr and lr.find('coordinates'):
                        coords = parse_coordinates(lr.find('coordinates').text)
                        if coords:
                            geo_json_geometry = {
                                "type": "Polygon",
                                "coordinates": [coords] # Polygon needs 3D nesting (array of rings)
                            }

        # 3. Point
        if not geo_json_geometry:
            point = pm.find('Point')
            if point:
                coords_tag = point.find('coordinates')
                if coords_tag:
                    c = parse_coordinates(coords_tag.text)
                    if c and len(c) > 0:
                        geo_json_geometry = {
                            "type": "Point",
                            "coordinates": c[0]
                        }

        # Only add if we have geometry
        if geo_json_geometry:
            feature = {
                "type": "Feature",
                "properties": {
                    "name": name,
                    "description": description[:100], # Trucate for sanity
                    "side": side,
                    "layer": "OwlMap", # Generic tag
                    "style_url": style_url
                },
                "geometry": geo_json_geometry
            }
            features.append(feature)

    # Save
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, indent=2, ensure_ascii=False)
        
    print(f"\nSaved {len(features)} features to {OUTPUT_FILE}")

if __name__ == "__main__":
    harvest_owl_layers()