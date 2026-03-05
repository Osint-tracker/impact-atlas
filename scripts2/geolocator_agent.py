import os
import json
import logging
try:
    from shapely.geometry import Point, shape
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False

logger = logging.getLogger("geolocator_agent")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SECTORS_PATH = os.path.join(BASE_DIR, '../assets/data/operational_sectors.geojson')
BORDERS_PATH = os.path.join(BASE_DIR, '../assets/data/national_borders.geojson')

class GeolocatorAgent:
    """
    Deterministically assigns an operational sector to event coordinates using pure math (Point-in-Polygon).
    No LLMs used to avoid geographical hallucinations.
    """
    def __init__(self):
        self.sectors = []
        self.russia_shape = None
        self.ukraine_shape = None
        
        if not SHAPELY_AVAILABLE:
            logger.error("Shapely library is not available. Point-in-polygon will fall back to UNKNOWN_SECTOR.")
        else:
            self._load_data()

    def _load_data(self):
        try:
            if os.path.exists(SECTORS_PATH):
                with open(SECTORS_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for feature in data.get('features', []):
                        # Ensure names are strictly in English (e.g. "Pokrovsk Axis")
                        props = feature.get('properties', {})
                        name = props.get('operational_sector', props.get('name', 'Unknown Sector'))
                        geom = shape(feature['geometry'])
                        self.sectors.append({'name': name, 'polygon': geom})
                logger.info(f"Loaded {len(self.sectors)} operational sectors from geojson.")
            else:
                logger.warning(f"Sectors file missing: {SECTORS_PATH}")
                
            if os.path.exists(BORDERS_PATH):
                with open(BORDERS_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for feature in data.get('features', []):
                        name = feature.get('properties', {}).get('name', '').lower()
                        geom = shape(feature['geometry'])
                        if 'russia' in name or name == 'ru':
                            self.russia_shape = geom
                        elif 'ukraine' in name or name == 'ua':
                            self.ukraine_shape = geom
            else:
                logger.warning(f"Borders file missing: {BORDERS_PATH}")
        except Exception as e:
            logger.error(f"Error loading geofencing data: {e}")

    def assign_sector(self, lon, lat):
        """
        Assigns an English operational sector based on longitude and latitude.
        Falls back to national borders logic (DEEP_STRIKE_RU, REAR_AREA_UA).
        Returns string.
        """
        if lon is None or lat is None or not SHAPELY_AVAILABLE:
            return 'UNKNOWN_SECTOR'
            
        try:
            pt = Point(float(lon), float(lat))
            
            # 1. Check core Tactical Sectors first (highly precise)
            for sector in self.sectors:
                if sector['polygon'].contains(pt):
                    return sector['name']
                    
            # 2. Check National Borders Fallback (lower precision)
            if self.russia_shape and self.russia_shape.contains(pt):
                return 'DEEP_STRIKE_RU'
            elif self.ukraine_shape and self.ukraine_shape.contains(pt):
                return 'REAR_AREA_UA'
                
        except (ValueError, TypeError):
            pass
            
        return 'UNKNOWN_SECTOR'

# Create a global instance for easy import
geolocator = GeolocatorAgent()
