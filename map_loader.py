"""
map_loader.py - Legitimate OSINT Data Sources for Impact Atlas
===============================================================
Author: Impact Atlas Project (Modified via Gemini)
"""

import json
import requests
import time
from pathlib import Path
from typing import Dict, Optional, List
import logging
from datetime import datetime, timedelta
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MapDataLoader:
    """Fetches and converts map data from legitimate public sources."""

    def __init__(self, output_dir: str = "assets/data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ImpactAtlas/1.0 (Educational OSINT Project)'
        })
        # --- CONFIGURAZIONE CHIAVI ---
        self.firms_api_key = "29ca712bddef41c37ea9989a2b521dea"

    def create_dummy_geojson(self, filename: str, feature_type: str = "LineString") -> Dict:
        """Creates a valid empty GeoJSON file as fallback."""
        dummy = {
            "type": "FeatureCollection",
            "metadata": {
                "generated": datetime.utcnow().isoformat(),
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

    def fetch_with_retry(self, url: str, max_retries: int = 3, backoff: float = 2.0) -> Optional[requests.Response]:
        """Fetch URL with exponential backoff retry logic."""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)

                if response.status_code == 200:
                    return response
                elif response.status_code in [403, 401]:
                    logger.warning(
                        f"Access denied ({response.status_code}): {url}")
                    return None
                elif response.status_code == 404:
                    logger.warning(f"Resource not found (404): {url}")
                    return None
                else:
                    logger.warning(
                        f"HTTP {response.status_code} on attempt {attempt + 1}/{max_retries}")

            except requests.exceptions.RequestException as e:
                logger.error(
                    f"Request failed on attempt {attempt + 1}/{max_retries}: {e}")

            if attempt < max_retries - 1:
                time.sleep(backoff ** attempt)

        return None

    def load_nasa_firms(self, days: int = 1) -> bool:
        """
        Fetch NASA FIRMS fire data using the user provided key.
        """
        if not self.firms_api_key:
            logger.warning("FIRMS API Key missing!")
            return False

        # Area: Ukraine bounding box (West, South, East, North)
        bbox = "22.1,44.3,40.2,52.4"

        # VIIRS_SNPP_NRT è il satellite standard per dati in near-real-time
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{self.firms_api_key}/VIIRS_SNPP_NRT/{bbox}/{days}"

        logger.info(f"Contacting NASA FIRMS...")
        response = self.fetch_with_retry(url)
        if not response:
            return False

        # Convert CSV to GeoJSON
        features = []
        lines = response.text.strip().split('\n')

        if len(lines) < 2:
            logger.warning("NASA FIRMS returned no data rows.")
            return False

        headers = lines[0].split(',')

        for line in lines[1:]:
            values = line.split(',')
            if len(values) < 3:
                continue

            data = dict(zip(headers, values))

            try:
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(data['longitude']), float(data['latitude'])]
                    },
                    "properties": {
                        "brightness": float(data.get('bright_ti4', 0)),
                        "confidence": data.get('confidence', 'unknown'),
                        "acq_date": data.get('acq_date'),
                        "acq_time": data.get('acq_time'),
                        "satellite": "VIIRS"
                    }
                }
                features.append(feature)
            except (ValueError, KeyError) as e:
                continue

        geojson = {
            "type": "FeatureCollection",
            "metadata": {
                "generated": datetime.utcnow().isoformat(),
                "source": "NASA FIRMS",
                "days": days
            },
            "features": features
        }

        output_path = self.output_dir / "thermal_firms.geojson"
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(geojson, f, indent=2)

        logger.info(f"Saved {len(features)} thermal hotspots from NASA FIRMS")
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

        # 1. NASA FIRMS (Automatico)
        self.load_nasa_firms(days=1)

        # 2. CONVERSIONE MANUALE (Se hai scaricato un file KML)
        # Se metti un file chiamato 'manual_frontline.kml' nella cartella, lui lo converte
        self.convert_kml_to_geojson(
            "manual_frontline.kml", "frontline.geojson")

        # 3. Creazione file fallback se mancano
        self.create_dummy_geojson("frontline.geojson", "LineString")
        self.create_dummy_geojson("events.geojson", "Point")

        logger.info("\nData loading complete!")


if __name__ == "__main__":
    loader = MapDataLoader()
    loader.run_all()
