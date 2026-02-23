"""
geo_instrument.py - Geographic Sanity Probe for OSINT Tracker
==============================================================
The "Sanfilippo Method" Instrument: Provides observability into AI-extracted
coordinates before they're saved to the database.

This module validates that coordinates fall within the Ukraine/Russia
theatre of operations and optionally reverse-geocodes them for additional
country-level validation.
"""

import logging
from typing import Optional

# Configure logging for the instrument
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GeoProbe")


class GeoProbe:
    """
    Geographic validation instrument for the Impact Atlas pipeline.
    
    Acts as the "eyes" of the system, checking if AI-predicted coordinates
    make physical and contextual sense before saving to the database.
    """
    
    # =========================================================================
    # THEATRE OF OPERATIONS - Bounding Box Definition
    # =========================================================================
    # Rough bounds for Ukraine/Western Russia conflict zone
    # Encompasses: Ukraine, Crimea, Western Russia (Belgorod, Kursk, Rostov)
    # Excludes: Poland, Belarus (mostly), Black Sea (deep offshore)
    
    THEATRE_BOUNDS = {
        'min_lat': 44.0,   # South: Crimean coast / Sea of Azov
        'max_lat': 53.0,   # North: Sumy Oblast / Kursk border
        'min_lon': 22.0,   # West: Zakarpattia / Polish border
        'max_lon': 42.0    # East: Rostov Oblast / Don River
    }
    
    # Valid country codes for the conflict zone
    VALID_COUNTRY_CODES = {'ua', 'ru'}
    
    # Extended tolerance zone (for border regions, Belarus logistics)
    EXTENDED_BOUNDS = {
        'min_lat': 43.5,
        'max_lat': 56.0,
        'min_lon': 21.0,
        'max_lon': 45.0
    }
    
    def __init__(self, use_reverse_geocoding: bool = True, timeout: int = 5):
        """
        Initialize the GeoProbe.
        
        Args:
            use_reverse_geocoding: Whether to use Photon for country validation
            timeout: Timeout in seconds for geocoding API calls
        """
        import os
        self.use_reverse_geocoding = use_reverse_geocoding
        self.timeout = timeout
        
        # Load local gazetteer and frontline data for fast, free checks
        self.gazetteer = None
        self.frontline_geometry = None
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.gazetteer_path = os.path.join(base_dir, '../assets/data/gazetteer.json')
        self.frontline_path = os.path.join(base_dir, '../assets/data/owl_layer.geojson')
        
        self._load_local_datasets()
        
    def _load_local_datasets(self):
        import json
        
        # Load Gazetteer
        try:
            with open(self.gazetteer_path, 'r', encoding='utf-8') as f:
                self.gazetteer = json.load(f)
                logger.info(f"Loaded local gazetteer with {len(self.gazetteer)} entries.")
        except FileNotFoundError:
            # Gazetteer not found, will rely on Photon
            self.gazetteer = None
            
        # Load Frontline for Soft Anomaly Filters
        try:
            from shapely.geometry import shape, MultiLineString
            with open(self.frontline_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                lines = []
                for feat in data.get('features', []):
                    # We assume frontline is represented by LineStrings
                     geom = feat.get('geometry', {})
                     if geom.get('type') in ['LineString', 'MultiLineString']:
                         lines.append(shape(geom))
                if lines:
                    from shapely.ops import linemerge
                    self.frontline_geometry = linemerge(lines)
                    logger.info("Loaded frontline geometry for distance checks.")
        except Exception as e:
            logger.warning(f"Could not load frontline geometry: {e}")

    def gazetteer_lookup(self, location_name: str) -> Optional[dict]:
        """Fast fuzzy lookup in local JSON gazetteer (Zero API Cost)."""
        if not self.gazetteer or not location_name:
            return None
        
        loc_lower = location_name.lower().strip()
        # Direct match check
        for entry in self.gazetteer:
            name_en = entry.get('name_en', '').lower()
            name_ua = entry.get('name_ua', '').lower()
            name_ru = entry.get('name_ru', '').lower()
            if loc_lower in [name_en, name_ua, name_ru]:
                return {'lat': entry['lat'], 'lon': entry['lon']}
                
        # Fuzzy match logic could be added here (e.g. rapidfuzz)
        return None
    
    def _is_in_bounding_box(self, lat: float, lon: float, strict: bool = True) -> bool:
        """
        Check if coordinates fall within the theatre bounding box.
        
        Args:
            lat: Latitude
            lon: Longitude
            strict: If True, use tight bounds. If False, use extended bounds.
        """
        bounds = self.THEATRE_BOUNDS if strict else self.EXTENDED_BOUNDS
        
        return (bounds['min_lat'] <= lat <= bounds['max_lat'] and
                bounds['min_lon'] <= lon <= bounds['max_lon'])
    
    def _reverse_geocode(self, lat: float, lon: float) -> Optional[dict]:
        """
        Reverse geocode coordinates to get location details using Photon API.
        
        Returns:
            dict with 'address', 'country_code', 'region', 'country' or None on failure
        """
        import requests
        
        try:
            url = f"https://photon.komoot.io/reverse?lon={lon}&lat={lat}"
            resp = requests.get(url, timeout=self.timeout)
            data = resp.json()
            
            if data and data.get('features'):
                props = data['features'][0].get('properties', {})
                country_code = props.get('countrycode', '').lower()
                
                return {
                    'address': props.get('name', 'Unknown'),
                    'country_code': country_code,
                    'region': props.get('state') or props.get('county') or props.get('city', 'Unknown'),
                    'country': props.get('country', 'Unknown')
                }
        except Exception as e:
            logger.warning(f"Photon reverse geocoding failed for ({lat}, {lon}): {e}")
            
        return None
    
    def probe_coordinates(self, lat: float, lon: float) -> dict:
        """
        Main validation function - probes coordinates for validity.
        
        This is the primary interface for the Geographic Sanity Loop.
        
        Args:
            lat: Latitude extracted by the AI
            lon: Longitude extracted by the AI
            
        Returns:
            dict with:
                - is_valid: bool - Whether coordinates should be accepted
                - region: str - Human-readable region name
                - country_code: str - ISO country code
                - error_msg: str - Specific error if invalid (empty if valid)
                - method: str - How validation was performed
                - suspicious: bool - True if event is suspiciously far from action
        """
        result = {
            'is_valid': False,
            'region': 'Unknown',
            'country_code': 'unknown',
            'country': 'Unknown',
            'error_msg': '',
            'method': 'none',
            'suspicious': False
        }
        
        # =====================================================================
        # VALIDATION CHECK 1: Basic Sanity (null, zero, invalid types)
        # =====================================================================
        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            result['error_msg'] = f"Invalid coordinate types: lat={type(lat)}, lon={type(lon)}"
            return result
        
        # Check for null island (0, 0) or obviously wrong coordinates
        if lat == 0.0 and lon == 0.0:
            result['error_msg'] = "Coordinates are (0, 0) - 'Null Island'. AI likely failed to extract."
            return result
        
        # Check valid ranges
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            result['error_msg'] = f"Coordinates out of valid range: ({lat}, {lon})"
            return result
        
        # =====================================================================
        # VALIDATION CHECK 2: Bounding Box (Fast, No API)
        # =====================================================================
        in_strict_box = self._is_in_bounding_box(lat, lon, strict=True)
        in_extended_box = self._is_in_bounding_box(lat, lon, strict=False)
        
        if not in_extended_box:
            # Definitely outside the theatre - no need for reverse geocoding
            result['error_msg'] = (
                f"Coordinates ({lat:.4f}, {lon:.4f}) are far outside the theatre of operations. "
                f"Expected lat {self.THEATRE_BOUNDS['min_lat']}-{self.THEATRE_BOUNDS['max_lat']}, "
                f"lon {self.THEATRE_BOUNDS['min_lon']}-{self.THEATRE_BOUNDS['max_lon']}."
            )
            result['method'] = 'bounding_box'
            return result
        
        # =====================================================================
        # VALIDATION CHECK 3: Reverse Geocoding (Accurate, API Cost)
        # =====================================================================
        if self.use_reverse_geocoding and self._geolocator:
            geo_result = self._reverse_geocode(lat, lon)
            
            if geo_result:
                result['region'] = geo_result['region']
                result['country_code'] = geo_result['country_code']
                result['country'] = geo_result['country']
                result['method'] = 'reverse_geocoding'
                
                # FIX: Check if country is valid AND coordinates are in strict bounding box
                # This prevents Moscow (ru, but lat 55.7 > max 53.0) from passing
                is_valid_country = geo_result['country_code'] in self.VALID_COUNTRY_CODES
                
                if is_valid_country and in_strict_box:
                    # Both country and bounding box valid - high confidence
                    result['is_valid'] = True
                    result['error_msg'] = ''
                    return result
                elif is_valid_country and not in_strict_box:
                    # Valid country but outside strict bounding box (e.g., Moscow, St. Petersburg)
                    result['error_msg'] = (
                        f"Coordinates ({lat:.4f}, {lon:.4f}) reverse-geocode to "
                        f"'{geo_result['region']}, {geo_result['country']}' which is in {geo_result['country_code'].upper()}, "
                        f"but OUTSIDE the active conflict zone (lat {self.THEATRE_BOUNDS['min_lat']}-{self.THEATRE_BOUNDS['max_lat']}). "
                        f"The event was likely in a different location."
                    )
                    return result
                else:
                    # Outside UA/RU entirely (e.g., Poland, Moldova, Belarus)
                    result['error_msg'] = (
                        f"Coordinates ({lat:.4f}, {lon:.4f}) reverse-geocode to "
                        f"'{geo_result['region']}, {geo_result['country']}' (country code: {geo_result['country_code']}). "
                        f"This is OUTSIDE the theatre of operations (Ukraine/Russia)."
                    )
                    return result
            else:
                # Reverse geocoding failed - graceful degradation
                logger.warning(f"Reverse geocoding unavailable for ({lat}, {lon}). Using bounding box only.")
        
        # =====================================================================
        # FALLBACK: Bounding Box Only (When Geocoding Unavailable/Failed)
        # =====================================================================
        result['method'] = 'bounding_box_fallback'
        
        if in_strict_box:
            # Inside strict bounds - high confidence it's valid
            result['is_valid'] = True
            result['region'] = 'Ukraine/Russia Theatre (unverified)'
            result['error_msg'] = ''
        else:
            # FIX: In extended but not strict bounds - this is likely an error
            # Examples: Black Sea center, borderlands
            # We now REJECT these rather than allow with warning
            logger.warning(
                f"Coordinates ({lat}, {lon}) in extended bounds only (geocoding failed). "
                "Rejecting as likely offshore or border error."
            )
            result['is_valid'] = False
            result['region'] = 'Extended Zone (rejected - geocoding failed)'
            result['error_msg'] = (
                f"Coordinates ({lat:.4f}, {lon:.4f}) are in an uncertain zone (extended bounds only) "
                f"and reverse geocoding failed. This could be offshore (Black Sea) or a border error. "
                f"Please provide coordinates within the strict theatre bounds."
            )
        # =====================================================================
        # VALIDATION CHECK 5: Soft Anomaly Distance (100km from front)
        # =====================================================================
        if self.frontline_geometry and result['is_valid']:
            from shapely.geometry import Point
            p = Point(lon, lat)
            # Distance in degrees: 1 degree approx 100km at this latitude
            # If distance > 1.0 degrees, it's > 100km from the frontline
            dist = self.frontline_geometry.distance(p)
            if dist > 1.0:
                result['suspicious'] = True
                logger.info(f"Anomaly Filter: Event is ~{dist*100:.1f}km from frontline. Flagged as suspicious.")
        
        return result
    
    def format_feedback_prompt(self, original_text: str, extracted_data: dict, 
                                probe_result: dict, attempt: int) -> str:
        """
        Generate a feedback prompt for the AI retry loop.
        
        This prompt tells the AI exactly what went wrong and asks for correction.
        
        Args:
            original_text: The original cluster text
            extracted_data: The JSON the AI extracted
            probe_result: The result from probe_coordinates()
            attempt: Current attempt number (1-indexed)
            
        Returns:
            str: Formatted prompt for the retry call
        """
        geo_data = extracted_data.get('geo_location') or {}
        explicit = geo_data.get('explicit') or {}
        inferred = geo_data.get('inferred') or {}
        
        lat = explicit.get('lat', 'N/A')
        lon = explicit.get('lon', 'N/A')
        toponym = inferred.get('toponym_raw', 'Unknown')
        
        prompt = f"""
CORRECTION REQUIRED (Attempt {attempt}/3)
=========================================

You previously extracted the following geolocation:
- Coordinates: ({lat}, {lon})
- Location Name: "{toponym}"

VALIDATION ERROR:
{probe_result['error_msg']}

INSTRUCTION:
The location name '{toponym}' in the original text likely refers to a DIFFERENT place
with the same name that is actually inside the conflict zone (Donetsk, Zaporizhzhia, 
Kherson, Kharkiv, Luhansk, or adjacent Russian oblasts like Belgorod/Kursk).

Common issues:
1. Multiple villages named '{toponym}' exist - choose the one in the war zone
2. The name could be a metonymy (e.g., "Kyiv says" doesn't mean the strike was in Kyiv)
3. The coordinates might be from a different country entirely

Re-analyze the ORIGINAL TEXT below and provide CORRECTED coordinates:

=== ORIGINAL TEXT (Re-analyze this) ===
{original_text[:8000]}
=== END ORIGINAL TEXT ===

Respond with corrected JSON in the same format as before.
"""
        return prompt


# =============================================================================
# STANDALONE TEST HARNESS
# =============================================================================
if __name__ == "__main__":
    import sys
    # Force UTF-8 output on Windows
    if sys.platform == 'win32':
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    
    print("=" * 70)
    print("GeoProbe Self-Validation Test Suite")
    print("=" * 70)
    
    # Initialize probe
    probe = GeoProbe(use_reverse_geocoding=True, timeout=5)
    
    # Test cases: (lat, lon, expected_valid, description)
    test_cases = [
        # Valid locations (Ukraine)
        (48.4647, 35.0462, True, "Dnipro, Ukraine (city center)"),
        (47.0958, 37.5533, True, "Mariupol, Ukraine (occupied)"),
        (50.4501, 30.5234, True, "Kyiv, Ukraine (capital)"),
        (48.0159, 37.8028, True, "Donetsk, Ukraine (occupied)"),
        (46.4775, 30.7326, True, "Odesa, Ukraine (port city)"),
        (49.9935, 36.2304, True, "Kharkiv, Ukraine (front line)"),
        
        # Valid locations (Russia - conflict zone)
        (51.7304, 36.1920, True, "Kursk, Russia (border region)"),
        (50.5997, 36.5983, True, "Belgorod, Russia (shelled frequently)"),
        
        # Invalid locations (Outside theatre)
        (52.2297, 21.0122, False, "Warsaw, Poland (outside)"),
        (53.9006, 27.5590, False, "Minsk, Belarus (outside strict box)"),
        (55.7558, 37.6173, False, "Moscow, Russia (too far north)"),
        (41.0082, 28.9784, False, "Istanbul, Turkey (way outside)"),
        (48.8566, 2.3522, False, "Paris, France (completely outside)"),
        
        # Edge cases
        (0.0, 0.0, False, "Null Island (extraction failure)"),
        (43.5, 34.0, False, "Black Sea center (offshore)"),
        (44.5, 33.5, True, "Sevastopol, Crimea (borderline)"),
    ]
    
    print(f"\nRunning {len(test_cases)} test cases...\n")
    
    passed = 0
    failed = 0
    
    for lat, lon, expected_valid, description in test_cases:
        result = probe.probe_coordinates(lat, lon)
        
        actual_valid = result['is_valid']
        status = "[PASS]" if actual_valid == expected_valid else "[FAIL]"
        
        if actual_valid == expected_valid:
            passed += 1
        else:
            failed += 1
        
        print(f"{status} | {description}")
        print(f"       Coords: ({lat}, {lon})")
        print(f"       Expected: {expected_valid}, Got: {actual_valid}")
        print(f"       Region: {result['region']}, Country: {result['country_code']}")
        if result['error_msg']:
            print(f"       Error: {result['error_msg'][:80]}...")
        print(f"       Method: {result['method']}")
        print()
    
    print("=" * 70)
    print(f"RESULTS: {passed} passed, {failed} failed out of {len(test_cases)} tests")
    print("=" * 70)
    
    if failed > 0:
        print("\n[WARNING] Some tests failed. Review the results above.")
        exit(1)
    else:
        print("\n[OK] All tests passed! GeoProbe is ready for integration.")
        exit(0)
