import os
import json
import logging
import sqlite3
import aiohttp
import asyncio

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
DB_PATH = os.path.join(BASE_DIR, '../war_tracker_v2/data/raw_events.db')
FALLBACK_RU = "Deep_Strike_RU"
FALLBACK_UA = "Rear_Area_UA"


class GazetteerCache:
    """
    Middleware for geocoding that uses a local SQLite cache (Canonical + Aliases)
    before falling back to external APIs (Photon).
    """

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        try:
            conn = sqlite3.connect(self.db_path, timeout=60.0)
            conn.execute("PRAGMA journal_mode=WAL;")
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS locations_registry (
                    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_name TEXT NOT NULL,
                    region TEXT NOT NULL,
                    aliases TEXT,
                    country TEXT DEFAULT 'UA',
                    lat REAL NOT NULL,
                    lon REAL NOT NULL,
                    confidence_score INTEGER DEFAULT 100,
                    hit_count INTEGER DEFAULT 1,
                    UNIQUE(canonical_name, region)
                );
            """)
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"GazetteerCache: DB Init error: {e}")

    async def get_coordinates(self, location: str, region: str) -> tuple:
        """
        Main entry point: Cache Hit (DB) -> Cache Miss (Photon) -> Store
        Returns (lat, lon, canonical_name)
        """
        if not location:
            return None, None, None

        loc_norm = location.lower().strip()
        reg_norm = region.lower().strip() if region else "unknown"

        # 1. Cache Hit (Search Aliases)
        result = await self._check_cache(loc_norm, reg_norm)
        if result:
            lat, lon, canonical = result
            await self._increment_hit(canonical, reg_norm)
            return lat, lon, canonical

        # 2. Cache Miss (Call Photon)
        logger.info(f"Gazetteer: Cache miss for '{loc_norm}' in '{reg_norm}'. Calling Photon...")
        photon_res = await self._call_photon(loc_norm, reg_norm)
        
        if photon_res:
            lat, lon, canonical, aliases = photon_res
            # 3. Store new entry
            await self._store_entry(canonical, reg_norm, aliases, lat, lon)
            return lat, lon, canonical

        return None, None, None

    async def _check_cache(self, location: str, region: str):
        try:
            # Use thread to avoid blocking loop with sqlite3
            return await asyncio.to_thread(self._check_cache_sync, location, region)
        except Exception as e:
            logger.error(f"Gazetteer: Cache check error: {e}")
            return None

    def _check_cache_sync(self, location: str, region: str):
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        cursor = conn.cursor()
        
        # We search inside the JSON aliases array
        # SQLite doesn't always have JSON1, so we use a robust LIKE pattern
        # '["київ", "kiev"]' -> %"київ"%
        query = "SELECT lat, lon, canonical_name FROM locations_registry WHERE aliases LIKE ? AND LOWER(region) = ?"
        cursor.execute(query, (f'%"{location}"%', region))
        row = cursor.fetchone()
        conn.close()
        return row if row else None

    async def _increment_hit(self, canonical: str, region: str):
        try:
            await asyncio.to_thread(self._increment_hit_sync, canonical, region)
        except: pass

    def _increment_hit_sync(self, canonical: str, region: str):
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("UPDATE locations_registry SET hit_count = hit_count + 1 WHERE canonical_name = ? AND LOWER(region) = ?", 
                    (canonical, region))
        conn.commit()
        conn.close()

    async def _call_photon(self, location: str, region: str):
        query = f"{location}, {region}, Ukraine"
        url = "https://photon.komoot.io/api/"
        params = {"q": query, "limit": 1}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        features = data.get('features', [])
                        if not features:
                            return None
                        
                        feat = features[0]
                        coords = feat['geometry']['coordinates'] # [lon, lat]
                        props = feat['properties']
                        
                        # Extract canonical name (defaulting to name or international name)
                        canonical = props.get('name', location.title())
                        
                        # Gather all available translations as aliases
                        aliases = {location.lower()}
                        for key, val in props.items():
                            if key.startswith('name:') or key == 'name':
                                aliases.add(val.lower())
                        
                        return coords[1], coords[0], canonical, list(aliases)
        except Exception as e:
            logger.error(f"Gazetteer: Photon API error: {e}")
        return None

    async def _store_entry(self, canonical: str, region: str, aliases: list, lat: float, lon: float):
        try:
            await asyncio.to_thread(self._store_entry_sync, canonical, region, aliases, lat, lon)
        except Exception as e:
            logger.error(f"Gazetteer: Store entry error: {e}")

    def _store_entry_sync(self, canonical: str, region: str, aliases: list, lat: float, lon: float):
        conn = sqlite3.connect(self.db_path, timeout=60.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        try:
            conn.execute("""
                INSERT OR IGNORE INTO locations_registry 
                (canonical_name, region, aliases, lat, lon)
                VALUES (?, ?, ?, ?, ?)
            """, (canonical, region.title(), json.dumps(aliases, ensure_ascii=False), lat, lon))
            conn.commit()
        finally:
            conn.close()


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
                with open(SECTORS_PATH, 'r', encoding='utf-8-sig') as f:
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
                with open(BORDERS_PATH, 'r', encoding='utf-8-sig') as f:
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
gazetteer = GazetteerCache()
