import json
import os
import sys
import math
from datetime import datetime

try:
    from shapely.geometry import shape, LineString, MultiLineString, Point
    from shapely.ops import nearest_points
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False

# Reconfigure stdout for UTF-8
sys.stdout.reconfigure(encoding='utf-8')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Check if running from scripts or root
if os.path.basename(BASE_DIR) == 'scripts':
    ROOT_DIR = os.path.dirname(BASE_DIR)
else:
    ROOT_DIR = BASE_DIR

OLD_FILE = os.path.join(ROOT_DIR, 'assets/data/owl_layer_old.geojson')
NEW_FILE = os.path.join(ROOT_DIR, 'assets/data/owl_layer.geojson')
SECTORS_FILE = os.path.join(ROOT_DIR, 'assets/data/operational_sectors.geojson')
OUTPUT_FILE = os.path.join(ROOT_DIR, 'assets/data/frontline_delta.json')

def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def get_frontline_geom(geojson_path):
    if not os.path.exists(geojson_path):
        return None
    try:
        with open(geojson_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"[!] Error loading {geojson_path}: {e}")
        return None
    
    geoms = []
    for feature in data.get('features', []):
        if feature.get('properties', {}).get('name') == 'Frontline':
            try:
                geom = shape(feature['geometry'])
                if isinstance(geom, (LineString, MultiLineString)):
                    geoms.append(geom)
            except:
                continue
    
    if not geoms:
        return None
    
    # Merge into a single MultiLineString for easier distance calculation
    if len(geoms) == 1:
        return geoms[0]
    return MultiLineString(geoms)

def calculate_delta():
    print(f"[*] Starting Frontline Delta Calculation...")
    
    if not SHAPELY_AVAILABLE:
        print("[!] Shapely not installed. Cannot perform geometric delta calculation.")
        sys.exit(1)

    if not os.path.exists(OLD_FILE):
        print(f"[!] Old frontline file {OLD_FILE} not found. Skipping delta calculation.")
        return
    
    if not os.path.exists(NEW_FILE):
        print(f"[!] New frontline file {NEW_FILE} not found.")
        return

    print(f"[*] Loading old frontline: {OLD_FILE}")
    old_geom = get_frontline_geom(OLD_FILE)
    print(f"[*] Loading new frontline: {NEW_FILE}")
    new_geom = get_frontline_geom(NEW_FILE)
    
    if old_geom is None or new_geom is None:
        print("[!] Could not extract frontline geometry from one of the files.")
        return

    # Load sectors
    if not os.path.exists(SECTORS_FILE):
        print(f"[!] Sectors file {SECTORS_FILE} missing.")
        return
        
    with open(SECTORS_FILE, 'r', encoding='utf-8') as f:
        sectors_data = json.load(f)
    
    results = {}
    total_delta = 0.0
    sectors_count = 0
    
    print("[*] Computing displacement per operational sector...")
    for feature in sectors_data.get('features', []):
        props = feature.get('properties', {})
        sector_name = props.get('operational_sector', props.get('name', 'Unknown'))
        
        try:
            sector_poly = shape(feature['geometry'])
        except:
            continue
        
        # Clip frontline to sector
        try:
            # We use buffer(0) to fix potential invalid geometries
            old_sector_line = old_geom.intersection(sector_poly.buffer(0))
            new_sector_line = new_geom.intersection(sector_poly.buffer(0))
        except Exception as e:
            print(f"    [!] Error clipping sector {sector_name}: {e}")
            continue
            
        if old_sector_line.is_empty or new_sector_line.is_empty:
            results[sector_name] = 0.0
            continue
            
        # Calculate displacement
        total_dist = 0.0
        sample_count = 0
        
        # Collect points from the NEW line to measure distance to the OLD one
        points_to_check = []
        if isinstance(new_sector_line, (LineString, MultiLineString)):
            if hasattr(new_sector_line, 'geoms'):
                for g in new_sector_line.geoms:
                    points_to_check.extend(list(g.coords))
            else:
                points_to_check.extend(list(new_sector_line.coords))
        elif hasattr(new_sector_line, 'geoms'): # GeometryCollection
            for g in new_sector_line.geoms:
                if isinstance(g, (LineString, MultiLineString)):
                    points_to_check.extend(list(g.coords))
        
        # Sub-sample points if too many (to keep performance decent)
        if len(points_to_check) > 200:
            points_to_check = points_to_check[::len(points_to_check)//100]

        for lon, lat in points_to_check:
            pt = Point(lon, lat)
            # Find the closest point on the OLD frontline within THIS sector
            try:
                _, closest_pt = nearest_points(pt, old_sector_line)
                dist = haversine_km(lat, lon, closest_pt.y, closest_pt.x)
                total_dist += dist
                sample_count += 1
            except:
                continue
        
        avg_delta = total_dist / sample_count if sample_count > 0 else 0.0
        results[sector_name] = round(avg_delta, 3)
        total_delta += avg_delta
        sectors_count += 1
        print(f"    [+] {sector_name}: {avg_delta:.3f} km")

    # Save results
    final_output = {
        "generated_at": datetime.now().isoformat(),
        "method": "geometric_sampling",
        "sectors": results,
        "global_average_delta": round(total_delta / sectors_count, 3) if sectors_count > 0 else 0
    }
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_output, f, indent=2)
    
    print(f"\n[SUCCESS] Frontline delta saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    calculate_delta()
