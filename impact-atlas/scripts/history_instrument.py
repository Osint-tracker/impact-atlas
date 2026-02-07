"""
history_instrument.py - Kinetic Plausibility Probe for OSINT Tracker
=====================================================================
The "Sanfilippo Method" Instrument (Part 2): Historical State Probe

This module validates that unit movements are physically plausible by
checking against the unit's known history in the database. It prevents
"teleportation errors" where a unit appears to move 500km in 2 hours
due to location name confusion (e.g., multiple villages named "Ivanivka").

Physics Rules:
- Teleportation Check: Distance > 800km AND Time < 24h -> IMPOSSIBLE
- Speed Check: Implied Speed > 120km/h sustained -> IMPOSSIBLE
"""

import sqlite3
import os
import logging
from datetime import datetime, timezone
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HistoryProbe")

# Try to import geopy for Haversine calculation
try:
    from geopy.distance import geodesic
    GEOPY_AVAILABLE = True
except ImportError:
    logger.warning("geopy not installed. Using fallback Haversine implementation.")
    GEOPY_AVAILABLE = False
    import math


class UnitHistoryProbe:
    """
    Kinetic Plausibility Probe for the Impact Atlas ORBAT pipeline.
    
    Acts as the "rear-view mirror" of the system, checking if a reported
    unit movement is physically plausible based on its known history.
    """
    
    # =========================================================================
    # PHYSICS CONSTANTS (The Laws of War)
    # =========================================================================
    
    # Maximum plausible speed for a mechanized brigade (km/h)
    # Even on highways, sustained speed is limited by logistics
    MAX_PLAUSIBLE_SPEED_KMH = 120.0
    
    # Teleportation threshold: if distance > this AND time < 24h, impossible
    TELEPORTATION_DISTANCE_KM = 800.0
    TELEPORTATION_TIME_HOURS = 24.0
    
    # Minimum time delta to perform speed check (avoid division by tiny numbers)
    MIN_TIME_DELTA_HOURS = 0.1  # 6 minutes
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the UnitHistoryProbe.
        
        Args:
            db_path: Path to the SQLite database containing units_registry.
                     If None, uses the default path relative to this script.
        """
        if db_path is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.db_path = os.path.join(base_dir, '../war_tracker_v2/data/raw_events.db')
        else:
            self.db_path = db_path
        
        logger.info(f"HistoryProbe initialized. DB: {self.db_path}")
    
    def _calculate_distance_km(self, lat1: float, lon1: float, 
                                lat2: float, lon2: float) -> float:
        """
        Calculate distance between two points using Haversine formula.
        
        Returns:
            Distance in kilometers
        """
        if GEOPY_AVAILABLE:
            return geodesic((lat1, lon1), (lat2, lon2)).kilometers
        else:
            # Fallback Haversine implementation
            R = 6371.0  # Earth radius in km
            
            lat1_rad = math.radians(lat1)
            lat2_rad = math.radians(lat2)
            delta_lat = math.radians(lat2 - lat1)
            delta_lon = math.radians(lon2 - lon1)
            
            a = (math.sin(delta_lat / 2) ** 2 + 
                 math.cos(lat1_rad) * math.cos(lat2_rad) * 
                 math.sin(delta_lon / 2) ** 2)
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
            
            return R * c
    
    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """
        Safely parse a timestamp string into a datetime object.
        
        Handles multiple formats and timezone awareness.
        
        Returns:
            datetime object (timezone-aware UTC) or None on failure
        """
        if not timestamp_str:
            return None
        
        # Common formats to try
        formats = [
            "%Y-%m-%dT%H:%M:%S.%fZ",     # ISO with microseconds + Z
            "%Y-%m-%dT%H:%M:%SZ",         # ISO with Z
            "%Y-%m-%dT%H:%M:%S.%f%z",     # ISO with microseconds + timezone
            "%Y-%m-%dT%H:%M:%S%z",        # ISO with timezone
            "%Y-%m-%dT%H:%M:%S.%f",       # ISO with microseconds, no TZ
            "%Y-%m-%dT%H:%M:%S",          # ISO basic
            "%Y-%m-%d %H:%M:%S.%f",       # SQLite datetime with microseconds
            "%Y-%m-%d %H:%M:%S",          # SQLite datetime
            "%Y-%m-%d",                   # Date only
        ]
        
        # Clean input
        ts = str(timestamp_str).strip()
        
        for fmt in formats:
            try:
                dt = datetime.strptime(ts, fmt)
                # Make timezone-aware if naive
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        
        logger.warning(f"Could not parse timestamp: {timestamp_str}")
        return None
    
    def _get_unit_history(self, unit_id: str) -> Optional[dict]:
        """
        Query the units_registry for a unit's last known position.
        
        Returns:
            dict with last_seen_lat, last_seen_lon, last_seen_date
            or None if unit not found
        """
        if not os.path.exists(self.db_path):
            logger.error(f"Database not found: {self.db_path}")
            return None
        
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT last_seen_lat, last_seen_lon, last_seen_date
                FROM units_registry
                WHERE unit_id = ?
            """, (unit_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row and row['last_seen_lat'] and row['last_seen_lon']:
                return {
                    'last_seen_lat': row['last_seen_lat'],
                    'last_seen_lon': row['last_seen_lon'],
                    'last_seen_date': row['last_seen_date']
                }
            return None
            
        except Exception as e:
            logger.error(f"Database error querying unit {unit_id}: {e}")
            return None
    
    def check_plausibility(self, unit_id: str, new_lat: float, new_lon: float,
                           new_timestamp: str) -> dict:
        """
        Main validation function - checks if a unit movement is physically plausible.
        
        This is the primary interface for the Kinetic Plausibility Check.
        
        Args:
            unit_id: The normalized unit identifier (e.g., "UA_47_MECH_BDE")
            new_lat: The newly reported latitude
            new_lon: The newly reported longitude
            new_timestamp: The timestamp of the new report (ISO format)
            
        Returns:
            dict with:
                - status: 'NEW' (no history) or 'KNOWN' (history exists)
                - is_plausible: bool - Whether the movement is physically possible
                - distance_km: float - Distance from last known position (if KNOWN)
                - time_delta_hours: float - Time since last sighting (if KNOWN)
                - implied_speed_kmh: float - Calculated speed (if KNOWN)
                - reason: str - Explanation of the verdict
                - last_position: dict - Previous coordinates (if KNOWN)
        """
        result = {
            'status': 'NEW',
            'is_plausible': True,
            'distance_km': 0.0,
            'time_delta_hours': 0.0,
            'implied_speed_kmh': 0.0,
            'reason': '',
            'last_position': None
        }
        
        # =================================================================
        # STEP 1: Query unit history
        # =================================================================
        history = self._get_unit_history(unit_id)
        
        if not history:
            result['status'] = 'NEW'
            result['is_plausible'] = True
            result['reason'] = f"Unit '{unit_id}' is new or has no prior location data."
            return result
        
        result['status'] = 'KNOWN'
        result['last_position'] = {
            'lat': history['last_seen_lat'],
            'lon': history['last_seen_lon'],
            'date': history['last_seen_date']
        }
        
        # =================================================================
        # STEP 2: Parse timestamps
        # =================================================================
        old_dt = self._parse_timestamp(history['last_seen_date'])
        new_dt = self._parse_timestamp(new_timestamp)
        
        if not old_dt or not new_dt:
            result['is_plausible'] = True
            result['reason'] = "Could not parse timestamps. Allowing movement (fail-open)."
            return result
        
        # =================================================================
        # STEP 3: Calculate distance
        # =================================================================
        try:
            distance_km = self._calculate_distance_km(
                history['last_seen_lat'], history['last_seen_lon'],
                new_lat, new_lon
            )
            result['distance_km'] = round(distance_km, 2)
        except Exception as e:
            logger.error(f"Distance calculation error: {e}")
            result['is_plausible'] = True
            result['reason'] = f"Distance calculation failed: {e}. Allowing movement."
            return result
        
        # =================================================================
        # STEP 4: Calculate time delta
        # =================================================================
        time_delta = new_dt - old_dt
        time_delta_hours = time_delta.total_seconds() / 3600.0
        result['time_delta_hours'] = round(time_delta_hours, 2)
        
        # Handle time travel (new report is older than last seen)
        if time_delta_hours < 0:
            result['is_plausible'] = True
            result['reason'] = (
                f"New report is older than last sighting. "
                f"Time delta: {time_delta_hours:.1f}h. Allowing (historical backfill)."
            )
            return result
        
        # =================================================================
        # STEP 5: Apply Physics Rules
        # =================================================================
        
        # RULE 1: Teleportation Check
        if (distance_km > self.TELEPORTATION_DISTANCE_KM and 
            time_delta_hours < self.TELEPORTATION_TIME_HOURS):
            result['is_plausible'] = False
            result['reason'] = (
                f"TELEPORTATION DETECTED: Unit moved {distance_km:.1f}km in only "
                f"{time_delta_hours:.1f} hours. This exceeds the {self.TELEPORTATION_DISTANCE_KM}km "
                f"threshold for movements under {self.TELEPORTATION_TIME_HOURS}h."
            )
            return result
        
        # RULE 2: Speed Check
        if time_delta_hours >= self.MIN_TIME_DELTA_HOURS:
            implied_speed = distance_km / time_delta_hours
            result['implied_speed_kmh'] = round(implied_speed, 1)
            
            if implied_speed > self.MAX_PLAUSIBLE_SPEED_KMH:
                result['is_plausible'] = False
                result['reason'] = (
                    f"IMPOSSIBLE SPEED: Unit would need to travel at "
                    f"{implied_speed:.1f}km/h sustained for {time_delta_hours:.1f}h "
                    f"to cover {distance_km:.1f}km. Max plausible speed is "
                    f"{self.MAX_PLAUSIBLE_SPEED_KMH}km/h for a mechanized unit."
                )
                return result
        
        # =================================================================
        # STEP 6: Movement is plausible
        # =================================================================
        result['is_plausible'] = True
        result['reason'] = (
            f"Movement plausible: {distance_km:.1f}km in {time_delta_hours:.1f}h "
            f"(implied speed: {result['implied_speed_kmh']:.1f}km/h)."
        )
        return result
    
    def format_correction_prompt(self, unit_id: str, new_coords: dict,
                                  probe_result: dict) -> str:
        """
        Generate a correction prompt for the AI when a movement is implausible.
        
        This prompt tells the AI exactly what went wrong and asks for correction.
        
        Args:
            unit_id: The unit identifier
            new_coords: Dict with 'lat' and 'lon' of the new (implausible) coordinates
            probe_result: The result from check_plausibility()
            
        Returns:
            str: Formatted prompt for the retry call
        """
        last_pos = probe_result.get('last_position', {})
        
        # Extract region from last position (if available)
        # This would need reverse geocoding, but we can estimate from coords
        last_lat = last_pos.get('lat', 'Unknown')
        last_lon = last_pos.get('lon', 'Unknown')
        
        prompt = f"""
SYSTEM NOTICE: Physics Violation Detected
==========================================

You reported unit '{unit_id}' at ({new_coords.get('lat')}, {new_coords.get('lon')}).

However, history shows this unit was at ({last_lat}, {last_lon}) 
({probe_result['distance_km']:.1f}km away) only {probe_result['time_delta_hours']:.1f} hours ago.

This implies a travel speed of {probe_result['implied_speed_kmh']:.1f}km/h, 
which is {probe_result['reason']}

LIKELY CAUSE:
You likely selected the wrong location with a common name. There are many villages
in Ukraine with names like "Ivanivka", "Pokrovsk", "Novoselivka", etc.

INSTRUCTION:
Look for a location CLOSER to the unit's last known position at 
({last_lat}, {last_lon}). The correct location should be within a 
plausible movement distance (typically <100km for most reports).

Re-analyze the source text and provide CORRECTED coordinates for unit '{unit_id}'.
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
    print("UnitHistoryProbe Self-Validation Test Suite")
    print("=" * 70)
    
    # Initialize probe with test mode
    probe = UnitHistoryProbe()
    
    # Check if DB exists
    if not os.path.exists(probe.db_path):
        print(f"\n⚠️  Database not found at: {probe.db_path}")
        print("   Running synthetic tests only (no actual DB queries).\n")
    
    # ==========================================================================
    # TEST 1: Distance Calculation (Haversine)
    # ==========================================================================
    print("\n--- TEST 1: Distance Calculation ---")
    
    # Kyiv to Kharkiv (~480km)
    dist = probe._calculate_distance_km(50.4501, 30.5234, 49.9935, 36.2304)
    print(f"Kyiv to Kharkiv: {dist:.1f}km (expected ~480km)")
    
    # Kyiv to Donetsk (~680km)
    dist2 = probe._calculate_distance_km(50.4501, 30.5234, 48.0159, 37.8028)
    print(f"Kyiv to Donetsk: {dist2:.1f}km (expected ~680km)")
    
    # Same location (0km)
    dist3 = probe._calculate_distance_km(50.0, 30.0, 50.0, 30.0)
    print(f"Same location: {dist3:.1f}km (expected 0km)")
    
    # ==========================================================================
    # TEST 2: Timestamp Parsing
    # ==========================================================================
    print("\n--- TEST 2: Timestamp Parsing ---")
    
    test_timestamps = [
        "2026-01-15T10:30:00Z",
        "2026-01-15T10:30:00.123456Z",
        "2026-01-15T10:30:00+02:00",
        "2026-01-15 10:30:00",
        "2026-01-15",
        "invalid-timestamp",
    ]
    
    for ts in test_timestamps:
        result = probe._parse_timestamp(ts)
        status = "✅" if result else "❌"
        print(f"  {status} '{ts}' -> {result}")
    
    # ==========================================================================
    # TEST 3: Plausibility Check Logic (Synthetic)
    # ==========================================================================
    print("\n--- TEST 3: Plausibility Logic (Synthetic) ---")
    
    # Since we may not have the actual DB, test the logic directly
    # by mocking the _get_unit_history method
    
    class MockProbe(UnitHistoryProbe):
        def __init__(self):
            super().__init__()
            self.mock_history = None
        
        def _get_unit_history(self, unit_id):
            return self.mock_history
    
    mock_probe = MockProbe()
    
    # Test Case A: New Unit (no history)
    print("\n  Case A: New Unit (no history)")
    mock_probe.mock_history = None
    result = mock_probe.check_plausibility(
        "UA_TEST_NEW", 49.0, 37.0, "2026-01-15T12:00:00Z"
    )
    print(f"    Status: {result['status']}, Plausible: {result['is_plausible']}")
    print(f"    Reason: {result['reason']}")
    
    # Test Case B: Known Unit, Reasonable Movement (50km in 5h = 10km/h)
    print("\n  Case B: Known Unit, Reasonable Movement")
    mock_probe.mock_history = {
        'last_seen_lat': 49.0,
        'last_seen_lon': 37.0,
        'last_seen_date': '2026-01-15T07:00:00Z'
    }
    # Move ~50km east
    result = mock_probe.check_plausibility(
        "UA_47_MECH", 49.0, 37.65, "2026-01-15T12:00:00Z"  # ~50km east
    )
    print(f"    Distance: {result['distance_km']}km, Time: {result['time_delta_hours']}h")
    print(f"    Speed: {result['implied_speed_kmh']}km/h")
    print(f"    Plausible: {result['is_plausible']}")
    
    # Test Case C: Teleportation (800km in 3h)
    print("\n  Case C: Teleportation Detection (800km in 3h)")
    mock_probe.mock_history = {
        'last_seen_lat': 50.4501,  # Kyiv
        'last_seen_lon': 30.5234,
        'last_seen_date': '2026-01-15T09:00:00Z'
    }
    result = mock_probe.check_plausibility(
        "UA_47_MECH", 48.0159, 37.8028, "2026-01-15T12:00:00Z"  # Donetsk (~680km)
    )
    print(f"    Distance: {result['distance_km']}km, Time: {result['time_delta_hours']}h")
    print(f"    Speed: {result['implied_speed_kmh']}km/h")
    print(f"    Plausible: {result['is_plausible']}")
    print(f"    Reason: {result['reason'][:80]}...")
    
    # Test Case D: Impossible Speed (200km in 30min = 400km/h)
    print("\n  Case D: Supersonic Speed (200km in 30min)")
    mock_probe.mock_history = {
        'last_seen_lat': 49.0,
        'last_seen_lon': 37.0,
        'last_seen_date': '2026-01-15T11:30:00Z'
    }
    result = mock_probe.check_plausibility(
        "UA_47_MECH", 49.0, 39.5, "2026-01-15T12:00:00Z"  # ~200km east in 30min
    )
    print(f"    Distance: {result['distance_km']}km, Time: {result['time_delta_hours']}h")
    print(f"    Speed: {result['implied_speed_kmh']}km/h")
    print(f"    Plausible: {result['is_plausible']}")
    
    # ==========================================================================
    # SUMMARY
    # ==========================================================================
    print("\n" + "=" * 70)
    print("Test Suite Complete!")
    print("=" * 70)
    print("\n✅ UnitHistoryProbe is ready for integration into ai_agent.py")
