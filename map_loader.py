"""
map_loader.py - Legitimate OSINT Data Sources for Impact Atlas
===============================================================
Author: Impact Atlas Project (Modified via Gemini)
"""

import csv
import json
import logging
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any

import requests
from shapely.geometry import Point, Polygon

from impact_atlas.config import ProjectPaths, RuntimeSettings
from impact_atlas.http import ResilientHttpClient, RetryPolicy
from impact_atlas.logging import configure_logging

logger = logging.getLogger(__name__)
PROJECT_ROOT = ProjectPaths.discover().root


class MapDataLoader:
    """Fetches and converts map data from legitimate public sources."""

    def __init__(
        self,
        output_dir: str | Path = PROJECT_ROOT / "assets" / "data",
        *,
        settings: RuntimeSettings | None = None,
        http_client: ResilientHttpClient | None = None,
    ) -> None:
        """Create a map loader with explicit configuration and an injectable transport."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.settings = settings or RuntimeSettings.from_environment(dotenv_path=PROJECT_ROOT / ".env")
        self._http = http_client or ResilientHttpClient(
            name="map-loader",
            user_agent=self.settings.user_agent,
            retry_policy=RetryPolicy(
                max_attempts=self.settings.max_retries,
                timeout_seconds=self.settings.request_timeout_seconds,
                backoff_seconds=self.settings.retry_backoff_seconds,
            ),
        )
        self.firms_api_key = self.settings.firms_api_key
        self.borders = self._load_borders()

    def _load_borders(self) -> list[Polygon]:
        """Loads UA and RU borders from GeoJSON for filtering."""
        borders_path = PROJECT_ROOT / "assets" / "data" / "national_borders.geojson"
        polygons = []
        if not borders_path.exists():
            logger.warning(f"Borders file not found: {borders_path}")
            return polygons

        try:
            with open(borders_path, encoding='utf-8-sig') as f:
                data = json.load(f)
                for feature in data.get('features', []):
                    geom = feature.get('geometry', {})
                    if not geom:
                        continue

                    if geom.get('type') == 'Polygon':
                        # Validating coordinates structure
                        coords = geom.get('coordinates', [])
                        if coords and isinstance(coords[0], list):
                            polygons.append(Polygon(coords[0]))
                    elif geom.get('type') == 'MultiPolygon':
                        for poly_coords in geom.get('coordinates', []):
                            if poly_coords and isinstance(poly_coords[0], list):
                                polygons.append(Polygon(poly_coords[0]))

            logger.info(f"Loaded {len(polygons)} boundary polygons for filtering.")
        except Exception as e:
            logger.error(f"Failed to load borders: {e}")
        return polygons

    def is_in_theater(self, lat: float, lon: float) -> bool:
        """Checks if a point is within allowed UA/RU boundaries."""
        if not self.borders:
            return True  # Fallback if borders can't be loaded
        point = Point(lon, lat)
        return any(poly.covers(point) for poly in self.borders)

    def create_dummy_geojson(self, filename: str, feature_type: str = "LineString") -> dict[str, Any]:
        """Creates a valid empty GeoJSON file as fallback."""
        dummy = {
            "type": "FeatureCollection",
            "metadata": {
                "generated": datetime.now().isoformat(),
                "source": "fallback",
                "status": "No data available"
            },
            "features": []
        }

        output_path = self.output_dir / filename
        # Scrive solo se il file non esiste per non sovrascrivere dati manuali
        if not output_path.exists():
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(dummy, f, indent=2)
            logger.info(f"Created fallback GeoJSON: {filename}")
        return dummy

    def fetch_with_retry(self, url: str) -> requests.Response | None:
        """Fetch a URL through the shared timeout and retry policy."""
        return self._http.request("GET", url)

    def load_nasa_firms(self, days: int = 3) -> bool:
        """
        Fetch NASA FIRMS fire data from multiple satellite sources.
        Combines VIIRS SNPP, NOAA-20, NOAA-21, and MODIS for maximum coverage.
        """
        if not self.firms_api_key:
            logger.warning("FIRMS API Key missing!")
            return False

        # Area: Ukraine + Russia expanded bounding box
        # Large BBox: [22.1, 44.0, 60.0, 65.0]
        # We split it into two to avoid API limits (400 Bad Request)
        bboxes = [
            "22.1,44.0,41.0,65.0", # Western Sector (UA + Border)
            "41.0,44.0,60.0,65.0"  # Eastern Sector (RU Deep)
        ]

        # Multiple satellite sources for better coverage
        satellites = [
            ("VIIRS_SNPP_NRT", "VIIRS_SNPP"),
            ("VIIRS_NOAA20_NRT", "VIIRS_NOAA20"),
            ("VIIRS_NOAA21_NRT", "VIIRS_NOAA21"),
            ("MODIS_NRT", "MODIS"),
        ]

        all_features = []
        seen_coords = set()  # For deduplication

        logger.info(f"Fetching NASA FIRMS from {len(satellites)} satellite sources ({days}-day window)...")

        for source_id, source_label in satellites:
            count = 0
            for bbox in bboxes:
                url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{self.firms_api_key}/{source_id}/{bbox}/{days}"

                response = self.fetch_with_retry(url)
                if not response:
                    logger.warning(f"  {source_label} ({bbox}): No response")
                    continue

                reader = csv.DictReader(StringIO(response.text.lstrip("\ufeff")))
                if not reader.fieldnames:
                    logger.info(f"  {source_label} ({bbox}): No data")
                    continue

                for data in reader:
                    try:
                        lat = float(data['latitude'])
                        lon = float(data['longitude'])

                        # Mandatory Geo-Filtering (Backend)
                        if not self.is_in_theater(lat, lon):
                            continue

                        # Deduplication by rounded coordinates (prevent near-duplicates)
                        coord_key = (round(lat, 3), round(lon, 3))
                        if coord_key in seen_coords:
                            continue
                        seen_coords.add(coord_key)

                        # Get brightness - field name varies by satellite
                        brightness = float(data.get('bright_ti4', data.get('brightness', 300)))

                        feature = {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [lon, lat]
                            },
                            "properties": {
                                "brightness": brightness,
                                "confidence": data.get('confidence', 'n'),
                                "acq_date": data.get('acq_date'),
                                "acq_time": data.get('acq_time'),
                                "satellite": source_label,
                                "frp": float(data.get('frp', 0)) if data.get('frp') else 0
                            }
                        }
                        all_features.append(feature)
                        count += 1

                    except (TypeError, ValueError, KeyError):
                        continue

            logger.info(f"  {source_label}: {count} detections")

        if not all_features:
            logger.warning("NASA FIRMS returned no data from any satellite.")
            return False

        geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "generated": datetime.now().isoformat(),
                "source": "NASA FIRMS (Multi-satellite)",
                "satellites": [s[1] for s in satellites],
                "days": days
            },
            "features": all_features
        }

        output_path = self.output_dir / "thermal_firms.geojson"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2)

        logger.info(f"Saved {len(all_features)} unique thermal hotspots from NASA FIRMS")
        return True

    def convert_kml_to_geojson(self, kml_filename: str, output_name: str) -> bool:
        """
        Converte un file KML (scaricato manualmente) in GeoJSON.
        Cerca il file KML nella cartella principale o in assets/data.
        """
        try:
            import geopandas as gpd
            import fiona

            # Enable KML driver
            fiona.drvsupport.supported_drivers['KML'] = 'rw'

            # Cerca il file
            kml_path = Path(kml_filename)
            if not kml_path.exists():
                kml_path = self.output_dir / kml_filename
                if not kml_path.exists():
                    logger.warning(f"KML file not found: {kml_filename}")
                    return False

            logger.info(f"Converting {kml_path} to GeoJSON...")

            # Read KML
            gdf = gpd.read_file(kml_path, driver='KML')

            # Convert to GeoJSON
            output_path = self.output_dir / output_name
            gdf.to_file(output_path, driver='GeoJSON')

            logger.info(f"SUCCESS: Converted KML to {output_name}")
            return True

        except ImportError:
            logger.error(
                "ERRORE: geopandas non installato. Esegui: pip install geopandas fiona")
            return False
        except Exception as e:
            logger.error(f"KML conversion failed: {e}")
            return False

    def run_all(self):
        """Run all data fetchers."""
        logger.info("=" * 60)
        logger.info("STARTING IMPACT ATLAS LEGITIMATE DATA LOADER")
        logger.info("=" * 60)

        # 1. NASA FIRMS (Multi-satellite, 3-day window)
        self.load_nasa_firms(days=3)

        # 2. CONVERSIONE MANUALE (Se hai scaricato un file KML)
        # Se metti un file chiamato 'manual_frontline.kml' nella cartella, lui lo converte
        self.convert_kml_to_geojson(
            "manual_frontline.kml", "frontline.geojson")

        # 3. Creazione file fallback se mancano
        self.create_dummy_geojson("frontline.geojson", "LineString")
        self.create_dummy_geojson("events.geojson", "Point")

        logger.info("\nData loading complete!")


if __name__ == "__main__":
    configure_logging("map_loader", PROJECT_ROOT / "logs" / "map_loader.log")
    loader = MapDataLoader()
    loader.run_all()
