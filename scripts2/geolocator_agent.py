import os
import json
import logging

try:
    from shapely.geometry import Point, shape
    from shapely.prepared import prep
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False

logger = logging.getLogger("geolocator_agent")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SECTORS_PATH = os.path.join(BASE_DIR, '../assets/data/operational_sectors.geojson')
BORDERS_PATH = os.path.join(BASE_DIR, '../assets/data/national_borders.geojson')
FALLBACK_RU = "Deep_Strike_RU"
FALLBACK_UA = "Rear_Area_UA"


class GeolocatorAgent:
    """
    Deterministically assigns an operational sector to event coordinates using pure math
    (Point-in-Polygon). No LLM is involved in sector assignment.
    """

    def __init__(self):
        self.sectors = []
        self.russia_shape = None
        self.ukraine_shape = None

        if not SHAPELY_AVAILABLE:
            logger.error("Shapely library is not available. Falling back to UNKNOWN_SECTOR.")
        else:
            self._load_data()

    def _load_data(self):
        try:
            if os.path.exists(SECTORS_PATH):
                with open(SECTORS_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for feature in data.get('features', []):
                        if not feature.get('geometry'):
                            continue
                        props = feature.get('properties', {})
                        name = props.get('operational_sector', props.get('name', 'Unknown Sector'))
                        geom = shape(feature['geometry'])
                        self.sectors.append({'name': name, 'polygon': prep(geom)})
                logger.info("Loaded %d operational sectors from geojson.", len(self.sectors))
            else:
                logger.warning("Sectors file missing: %s", SECTORS_PATH)

            if os.path.exists(BORDERS_PATH):
                with open(BORDERS_PATH, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    for feature in data.get('features', []):
                        if not feature.get('geometry'):
                            continue
                        name = feature.get('properties', {}).get('name', '').lower()
                        geom = prep(shape(feature['geometry']))
                        if 'russia' in name or name == 'ru':
                            self.russia_shape = geom
                        elif 'ukraine' in name or name == 'ua':
                            self.ukraine_shape = geom
            else:
                logger.warning("Borders file missing: %s", BORDERS_PATH)
        except Exception as e:
            logger.error("Error loading geofencing data: %s", e)

    def assign_sector(self, lon, lat):
        """
        Assigns an operational sector deterministically.
        Fallback logic is mandatory:
        - if inside RU territory -> Deep_Strike_RU
        - otherwise -> Rear_Area_UA
        """
        if lon is None or lat is None or not SHAPELY_AVAILABLE:
            return 'UNKNOWN_SECTOR'

        try:
            pt = Point(float(lon), float(lat))

            for sector in self.sectors:
                if sector['polygon'].contains(pt) or sector['polygon'].covers(pt):
                    return sector['name']

            if self.russia_shape and (self.russia_shape.contains(pt) or self.russia_shape.covers(pt)):
                return FALLBACK_RU
            if self.ukraine_shape and (self.ukraine_shape.contains(pt) or self.ukraine_shape.covers(pt)):
                return FALLBACK_UA
            return FALLBACK_UA
        except (ValueError, TypeError):
            return 'UNKNOWN_SECTOR'


geolocator = GeolocatorAgent()
