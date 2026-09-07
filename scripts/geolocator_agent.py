"""Deterministic geolocation services for the Impact Atlas pipeline.

Provides:
    * ``GeolocatorAgent`` -- pure point-in-polygon sector assignment
      (Shapely based, no LLM involved).
    * ``GazetteerCache`` -- SQLite-backed geocoding cache with a Photon
      fallback for cache misses.

The module intentionally performs no I/O at import time. Obtain shared
instances through :func:`get_geolocator` / :func:`get_gazetteer`, which
build them lazily and cache them for the process lifetime.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Any
from collections.abc import Sequence

import aiohttp

# Resolve the project root so the shared config imports when this module is
# used from a directly-executed sibling script.
_PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

try:
    from shapely.geometry import Point, shape
    from shapely.prepared import prep

    SHAPELY_AVAILABLE = True
except ImportError:  # pragma: no cover - optional runtime dependency
    SHAPELY_AVAILABLE = False

logger = logging.getLogger("geolocator_agent")

Coords = tuple[float | None, float | None, str | None]

FALLBACK_RU = "Deep_Strike_RU"
FALLBACK_UA = "Rear_Area_UA"
PHOTON_API_URL = "https://photon.komoot.io/api/"
PHOTON_TIMEOUT_SECONDS = 10
SQLITE_TIMEOUT_SECONDS = 60.0


def _resolve_paths() -> tuple[Path, Path, Path]:
    """Resolve sector/border assets and the raw-events database location."""
    from impact_atlas.config import ProjectPaths

    paths = ProjectPaths.discover()
    return (
        paths.assets_data / "operational_sectors.geojson",
        paths.assets_data / "national_borders.geojson",
        paths.raw_events_database,
    )


class GazetteerCache:
    """Geocoding middleware backed by a local SQLite cache with Photon fallback.

    Lookup order: local ``locations_registry`` table (canonical names and
    alias arrays) followed by the Photon API for cache misses. Successful
    external resolutions are written back to the cache.
    """

    def __init__(self, db_path: Path) -> None:
        """Bind the cache to ``db_path`` and ensure its schema exists."""
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        """Open a short-lived WAL connection with a bounded lock wait."""
        conn = sqlite3.connect(str(self.db_path), timeout=SQLITE_TIMEOUT_SECONDS)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self) -> None:
        """Create the ``locations_registry`` table when it is missing."""
        try:
            conn = self._connect()
            try:
                conn.execute(
                    """
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
                    """
                )
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error as error:
            logger.error("GazetteerCache: DB init error: %s", error)

    async def get_coordinates(self, location: str, region: str) -> Coords:
        """Resolve ``location`` to ``(lat, lon, canonical_name)``.

        Returns ``(None, None, None)`` when neither the cache nor Photon can
        resolve the query.
        """
        if not location:
            return None, None, None

        loc_norm = location.lower().strip()
        reg_norm = region.lower().strip() if region else "unknown"

        cached = await self._check_cache(loc_norm, reg_norm)
        if cached:
            lat, lon, canonical = cached
            await self._increment_hit(canonical, reg_norm)
            return lat, lon, canonical

        logger.info("Gazetteer: cache miss for '%s' in '%s'; querying Photon.", loc_norm, reg_norm)
        photon_result = await self._call_photon(loc_norm, reg_norm)
        if photon_result:
            lat, lon, canonical, aliases = photon_result
            await self._store_entry(canonical, reg_norm, aliases, lat, lon)
            return lat, lon, canonical

        return None, None, None

    async def _check_cache(self, location: str, region: str) -> tuple[float, float, str] | None:
        """Query the alias cache off the event loop thread."""
        try:
            return await asyncio.to_thread(self._check_cache_sync, location, region)
        except sqlite3.Error as error:
            logger.error("Gazetteer: cache check error: %s", error)
            return None

    def _check_cache_sync(self, location: str, region: str) -> tuple[float, float, str] | None:
        """Return ``(lat, lon, canonical_name)`` for a cached alias, if any."""
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT lat, lon, canonical_name FROM locations_registry "
                "WHERE aliases LIKE ? AND LOWER(region) = ?",
                (f'%"{location}"%', region),
            )
            row = cursor.fetchone()
            return (row[0], row[1], row[2]) if row else None
        finally:
            conn.close()

    async def _increment_hit(self, canonical: str, region: str) -> None:
        """Bump the hit counter for a cache entry without blocking the loop."""
        try:
            await asyncio.to_thread(self._increment_hit_sync, canonical, region)
        except sqlite3.Error as error:
            logger.warning("Gazetteer: hit counter update failed: %s", error)

    def _increment_hit_sync(self, canonical: str, region: str) -> None:
        """Execute the hit-counter update on a worker thread."""
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE locations_registry SET hit_count = hit_count + 1 "
                "WHERE canonical_name = ? AND LOWER(region) = ?",
                (canonical, region),
            )
            conn.commit()
        finally:
            conn.close()

    async def _call_photon(
        self, location: str, region: str
    ) -> tuple[float, float, str, list[str]] | None:
        """Query Photon and return ``(lat, lon, canonical, aliases)`` or ``None``."""
        query = f"{location}, {region}, Ukraine"
        params: dict[str, Any] = {"q": query, "limit": 1}
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(PHOTON_API_URL, params=params, timeout=PHOTON_TIMEOUT_SECONDS) as resp,
            ):
                if resp.status != 200:
                    logger.warning("Gazetteer: Photon returned HTTP %s.", resp.status)
                    return None
                data = await resp.json()
                features = data.get("features", [])
                if not features:
                    return None

                feature = features[0]
                coords = feature["geometry"]["coordinates"]  # [lon, lat]
                props = feature["properties"]
                canonical = props.get("name", location.title())

                aliases = {location.lower()}
                for key, value in props.items():
                    if key.startswith("name:") or key == "name":
                        aliases.add(str(value).lower())

                return float(coords[1]), float(coords[0]), canonical, list(aliases)
        except (TimeoutError, aiohttp.ClientError, KeyError, TypeError, ValueError) as error:
            logger.error("Gazetteer: Photon API error: %s", error)
        return None

    async def _store_entry(
        self, canonical: str, region: str, aliases: Sequence[str], lat: float, lon: float
    ) -> None:
        """Persist a newly resolved location off the event loop thread."""
        try:
            await asyncio.to_thread(self._store_entry_sync, canonical, region, aliases, lat, lon)
        except sqlite3.Error as error:
            logger.error("Gazetteer: store entry error: %s", error)

    def _store_entry_sync(
        self, canonical: str, region: str, aliases: Sequence[str], lat: float, lon: float
    ) -> None:
        """Insert the resolved location into the cache (idempotent)."""
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO locations_registry
                (canonical_name, region, aliases, lat, lon)
                VALUES (?, ?, ?, ?, ?)
                """,
                (canonical, region.title(), json.dumps(list(aliases), ensure_ascii=False), lat, lon),
            )
            conn.commit()
        finally:
            conn.close()


class GeolocatorAgent:
    """Deterministic operational-sector assignment via point-in-polygon tests.

    Falls back to national border polygons when no operational sector
    matches, and ultimately to ``Rear_Area_UA`` / ``UNKNOWN_SECTOR``.
    """

    def __init__(self, sectors_path: Path, borders_path: Path) -> None:
        """Load sector and border polygons from ``sectors_path``/``borders_path``."""
        self.sectors: list[dict[str, object]] = []
        self.russia_shape: object | None = None
        self.ukraine_shape: object | None = None

        if not SHAPELY_AVAILABLE:
            logger.error("Shapely is not installed; sector assignment will degrade to UNKNOWN_SECTOR.")
            return
        self._load_data(sectors_path, borders_path)

    def _load_data(self, sectors_path: Path, borders_path: Path) -> None:
        """Load and prepare geofence polygons, logging any structural errors."""
        try:
            if sectors_path.exists():
                with open(sectors_path, encoding="utf-8-sig") as handle:
                    data = json.load(handle)
                for feature in data.get("features", []):
                    if not feature.get("geometry"):
                        continue
                    props = feature.get("properties", {})
                    name = props.get("operational_sector", props.get("name", "Unknown Sector"))
                    self.sectors.append({"name": name, "polygon": prep(shape(feature["geometry"]))})
                logger.info("Loaded %d operational sectors.", len(self.sectors))
            else:
                logger.warning("Sectors file missing: %s", sectors_path)

            if borders_path.exists():
                with open(borders_path, encoding="utf-8-sig") as handle:
                    data = json.load(handle)
                for feature in data.get("features", []):
                    if not feature.get("geometry"):
                        continue
                    name = feature.get("properties", {}).get("name", "").lower()
                    geom = prep(shape(feature["geometry"]))
                    if "russia" in name or name == "ru":
                        self.russia_shape = geom
                    elif "ukraine" in name or name == "ua":
                        self.ukraine_shape = geom
            else:
                logger.warning("Borders file missing: %s", borders_path)
        except (OSError, json.JSONDecodeError, ValueError) as error:
            logger.error("Error loading geofencing data: %s", error)

    def assign_sector(self, lon: float | None, lat: float | None) -> str:
        """Assign the operational sector for ``lon``/``lat`` coordinates.

        Fallback policy: inside Russia -> ``Deep_Strike_RU``; otherwise
        ``Rear_Area_UA``; invalid input -> ``UNKNOWN_SECTOR``.
        """
        if lon is None or lat is None or not SHAPELY_AVAILABLE:
            return "UNKNOWN_SECTOR"

        try:
            point = Point(float(lon), float(lat))
        except (ValueError, TypeError):
            return "UNKNOWN_SECTOR"

        for sector in self.sectors:
            polygon = sector["polygon"]
            if polygon.contains(point) or polygon.covers(point):
                return str(sector["name"])

        if self.russia_shape is not None and (
            self.russia_shape.contains(point) or self.russia_shape.covers(point)
        ):
            return FALLBACK_RU
        return FALLBACK_UA


_geolocator: GeolocatorAgent | None = None
_gazetteer: GazetteerCache | None = None


def get_geolocator() -> GeolocatorAgent | None:
    """Return the shared sector-assignment agent, building it on first use."""
    global _geolocator
    if _geolocator is None:
        sectors_path, borders_path, _db_path = _resolve_paths()
        _geolocator = GeolocatorAgent(sectors_path, borders_path)
    return _geolocator


def get_gazetteer() -> GazetteerCache | None:
    """Return the shared geocoding cache, building it on first use."""
    global _gazetteer
    if _gazetteer is None:
        _sectors, _borders, db_path = _resolve_paths()
        _gazetteer = GazetteerCache(db_path)
    return _gazetteer
