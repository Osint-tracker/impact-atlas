import time
import logging
import requests
import re
import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
import random
from typing import List, Dict, Any, Optional

# Configure module-level logger
logger = logging.getLogger("connectors")

class BaseConnector(ABC):
    """Abstract base class for all data connectors."""
    
    def __init__(self, name: str, rate_limit_sec: float = 1.0):
        self.name = name
        self.rate_limit_sec = rate_limit_sec
        self.last_request_time = 0.0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

    def _wait_for_rate_limit(self):
        """Enforce leaky bucket rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_sec:
            sleep_time = self.rate_limit_sec - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()

    def safe_request(self, url: str, method: str = "GET", **kwargs) -> Optional[requests.Response]:
        """Execute HTTP request with error handling and rate limiting."""
        self._wait_for_rate_limit()
        try:
            resp = self.session.request(method, url, timeout=30, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            logger.error(f"[{self.name}] HTTP Error on {url}: {e}")
            return None

    @abstractmethod
    def fetch_events(self) -> List[Dict[str, Any]]:
        """Main method to retrieve normalized events."""
        pass


class WarSpottingClient(BaseConnector):
    """
    Connector for WarSpotting API.
    Authoritative Source: /api/stats/russia
    """
    BASE_URL = "https://ukr.warspotting.net/api"

    def __init__(self):
        # Strict rate limit: 10 requests / 10 seconds = 1 req/sec
        super().__init__("WarSpotting", rate_limit_sec=1.5)

    def fetch_events(self) -> List[Dict[str, Any]]:
        url = f"{self.BASE_URL}/losses/russia"
        logger.info(f"[{self.name}] Fetching authoritative data from {url}")
        
        # Use full headers to avoid 520 errors
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://ukr.warspotting.net/',
            'Accept': 'application/json, text/plain, */*'
        }
        
        resp = self.safe_request(url, headers=headers)
        if not resp:
            return []

        try:
            data = resp.json()
        except json.JSONDecodeError:
            logger.error(f"[{self.name}] Failed to parse JSON response")
            return []

        # API v0.4 returns {"losses": [...]}
        items = data.get('losses', [])
        logger.info(f"[{self.name}] Retrieved {len(items)} raw items")
        
        normalized_events = []
        for item in items:
            event = self._normalize_item(item)
            if event:
                normalized_events.append(event)
                
        return normalized_events

    def _normalize_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert raw WarSpotting item to internal schema."""
        try:
            # Mandate ID presence
            raw_id = item.get('id')
            if not raw_id:
                return None

            # Extract fields
            model = item.get('model', 'Unknown')
            status = item.get('status', 'Unknown')
            timestamp = item.get('date') or item.get('created_at')
            
            # Geo handling
            geo = item.get('geo') or {}
            lat = None
            lon = None
            if geo:
                lat = float(geo.get('lat')) if geo.get('lat') else None
                lon = float(geo.get('lng') or geo.get('lon')) if (geo.get('lng') or geo.get('lon')) else None
            
            # Unit info
            unit_raw = item.get('unit')

            return {
                "source_id": str(raw_id),
                "event_id": f"ws_{raw_id}",
                "date": timestamp,  # Caller needs to normalize this date string
                "lat": lat,
                "lon": lon,
                "unit_raw": unit_raw,
                "raw_data": {
                    "model": model,
                    "status": status,
                    "tags": item.get('tags', []),
                    "type": "equipment_loss"
                }
            }
        except Exception as e:
            logger.warning(f"[{self.name}] Error normalizing item {item.get('id')}: {e}")
            return None


class ParabellumGeoExtractor(BaseConnector):
    """
    Heuristic Map Scraper for Parabellum Think Tank.
    Target: https://geo.parabellumthinktank.com
    Strategies:
    1. Inspect HTML for embedded JSON blobs (json_data, config, etc.)
    2. Fallback to WFS layers if discovery fails
    """
    TARGET_URL = "https://geo.parabellumthinktank.com/index.php/view/map?repository=russoukrainianwar&project=russian_invasion_of_ukraine"
    WFS_BASE = "https://geo.parabellumthinktank.com/index.php/lizmap/service?repository=russoukrainianwar&project=russian_invasion_of_ukraine"

    def __init__(self):
        super().__init__("Parabellum", rate_limit_sec=2.0)

    def fetch_events(self) -> List[Dict[str, Any]]:
        logger.info(f"[{self.name}] Starting heuristic extraction...")
        
        # Strategy 1: HTML Inspection
        events = self._strategy_html_embedded()
        if events:
            logger.info(f"[{self.name}] Strategy 1 (HTML) success: {len(events)} events")
            return events
            
        # Strategy 2: WFS Fallback (Legacy method)
        logger.info(f"[{self.name}] Strategy 1 failed. Falling back to WFS...")
        return self._strategy_wfs()

    def _strategy_html_embedded(self) -> List[Dict[str, Any]]:
        """Inspect HTML for 'var json_data = ...' or similar patterns."""
        resp = self.safe_request(self.TARGET_URL)
        if not resp:
            return []

        html = resp.text
        # Regex to find JSON assigned to variables or passed to functions
        # Look for: var someData = {...}; OR param = {...}
        # Common pattern in Lizmap/Leaflet: "options": {...} or "features": [...]
        
        # 1. Look for explicit GeoJSON structure in scripts
        # Pattern: {"type":"FeatureCollection" ... }
        geojson_matches = re.findall(r'(\{"type"\s*:\s*"FeatureCollection".+?\})\s*(?:;|\))', html, re.DOTALL)
        
        all_events = []
        
        for match in geojson_matches:
            try:
                data = json.loads(match)
                features = data.get('features', [])
                logger.info(f"[{self.name}] Found embedded FeatureCollection with {len(features)} features")
                for feat in features:
                    parsed = self._parse_feature(feat, layer_name="embedded_html")
                    if parsed:
                        all_events.append(parsed)
            except json.JSONDecodeError:
                continue

        # 2. Look for config objects that point to data files
        # (This is more complex, skipping for MVP unless needed)

        return all_events

    def _strategy_wfs(self) -> List[Dict[str, Any]]:
        """Fallback to WFS scraping."""
        # Known useful layers
        layers = ["russian_invasion_of_ukraine", "frontline", "events", "units"]
        all_events = []
        
        for layer in layers:
            wfs_url = (f"{self.WFS_BASE}&SERVICE=WFS&REQUEST=GetFeature"
                       f"&TYPENAME={layer}&OUTPUTFORMAT=GeoJSON&SRSNAME=EPSG:4326")
            
            resp = self.safe_request(wfs_url)
            if not resp:
                continue
                
            try:
                data = resp.json()
                features = data.get('features', [])
                logger.info(f"[{self.name}] WFS Layer '{layer}': {len(features)} features")
                
                for feat in features:
                    parsed = self._parse_feature(feat, layer_name=layer)
                    if parsed:
                        all_events.append(parsed)
            except json.JSONDecodeError:
                continue
                
        return all_events

    def _parse_feature(self, feature: Dict[str, Any], layer_name: str) -> Optional[Dict[str, Any]]:
        """Normalize GeoJSON feature."""
        try:
            props = feature.get('properties', {})
            geom = feature.get('geometry', {})
            
            # ID extraction
            raw_id = feature.get('id') or props.get('id') or props.get('fid')
            if not raw_id:
                # Generate hash from properties if no ID
                prop_str = json.dumps(props, sort_keys=True)
                raw_id = f"gen_{hash(prop_str) % 10000000}"

            # Geometry extraction (Centroid for non-points)
            lat, lon = None, None
            coords = geom.get('coordinates')
            if geom.get('type') == 'Point' and coords:
                lon, lat = coords[0], coords[1]
            elif coords:
                # Very rough centroid for MVP
                # Flatten coords to find first point
                flat = self._flatten_coords(coords)
                if len(flat) >= 2:
                    lon, lat = flat[0], flat[1]

            # Date extraction
            date_val = None
            for key in ['date', 'timestamp', 'time', 'Date']:
                if key in props:
                    date_val = props[key]
                    break

            # Unit extraction
            unit_raw = None
            for key in ['unit', 'name', 'Name', 'label']:
                if key in props:
                    unit_raw = str(props[key])
                    break

            return {
                "source_id": str(raw_id),
                "event_id": f"pb_{layer_name}_{raw_id}",
                "date": date_val,
                "lat": lat,
                "lon": lon,
                "unit_raw": unit_raw,
                "raw_data": {
                    "layer": layer_name,
                    "description": props.get('description', ''),
                    "properties": props
                }
            }

        except Exception as e:
            return None

    def _flatten_coords(self, coords):
        """Recursively get first coordinate pair."""
        if not coords:
            return []
        if not isinstance(coords[0], list):
            return coords
        return self._flatten_coords(coords[0])
