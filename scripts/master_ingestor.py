#!/usr/bin/env python3
"""
master_ingestor.py -- Production-Grade OSINT Multi-Source Ingestor
=================================================================
Ingests data from 10 OSINT sources into a local SQLite database (impact_atlas.db).
Critical feature: Entity Resolution via UnitResolver class.

Sources:
  1. WarSpotting     (API)   -- Equipment losses
  2. MilitaryLand    (HTML)  -- ORBAT / force structure
  3. DeepState       (API)   -- GeoJSON frontline snapshots
  4. LostArmour      (HTML)  -- Lancet/FPV strike targets
  5. Oryx            (HTML)  -- Verified equipment losses
  6. UkrDailyUpdate  (HTML)  -- Frontline change events
  7. TopCargo200     (HTML)  -- RU senior officer casualties
  8. UALosses        (HTML)  -- UA soldier records
  9. Motolko         (HTML)  -- Belarus military intel
 10. GeoConfirmed    (KML)   -- Geolocated conflict events
 11. Parabellum      (WFS)   -- Geolocated military data

Usage:
  python scripts/master_ingestor.py                # Full run (all sources)
  python scripts/master_ingestor.py --dry-run      # Schema init only, no network
  python scripts/master_ingestor.py --source ws    # Single source
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "impact_atlas.db"
LOG_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "data"
DEEPSTATE_DIR = DATA_DIR / "deepstate"
GEOCONFIRMED_DIR = DATA_DIR / "geoconfirmed"

RATE_LIMIT_SECONDS = 1.5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("master_ingestor")
logger.setLevel(logging.DEBUG)

fh = logging.FileHandler(LOG_DIR / "master_ingestor.log", encoding="utf-8")
fh.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)

fmt = logging.Formatter(
    "[%(asctime)s] %(levelname)-8s %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
fh.setFormatter(fmt)
ch.setFormatter(fmt)
logger.addHandler(fh)
logger.addHandler(ch)


# ===========================================================================
# UNIT RESOLVER -- Entity Resolution Engine
# ===========================================================================
class UnitResolver:
    """
    Resolves diverse unit naming conventions into a canonical unit_id.
    Uses string normalization + keyword matching against a master alias dictionary.
    """

    # Master alias dictionary: canonical_id -> [list of known aliases/patterns]
    ALIAS_MAP: dict[str, list[str]] = {
        # ── Ukrainian Ground Forces ──
        "ua_1_tank": ["1st tank brigade", "1 otbr", "1-a otbr", "1st tank bde"],
        "ua_3_assault": ["3rd assault brigade", "3rd assault", "3 oshbr", "azov brigade"],
        "ua_10_mtn_assault": ["10th mountain assault brigade", "10 ogshbr", "10th mtn assault"],
        "ua_12_azov": ["12th azov brigade", "12 obr azov", "12th brigade azov"],
        "ua_24_mech": ["24th mechanized brigade", "24 ombr", "24th mech", "king danylo"],
        "ua_28_mech": ["28th mechanized brigade", "28 ombr", "28th mech"],
        "ua_30_mech": ["30th mechanized brigade", "30 ombr", "30th mech"],
        "ua_32_mech": ["32nd mechanized brigade", "32 ombr", "32nd mech"],
        "ua_36_marine": ["36th marine brigade", "36 obmp", "36th marines"],
        "ua_47_mech": ["47th mechanized brigade", "47 ombr", "47th mech", "magura"],
        "ua_53_mech": ["53rd mechanized brigade", "53 ombr", "53rd mech"],
        "ua_59_mech": ["59th mechanized brigade", "59 ombr", "59th mech"],
        "ua_72_mech": ["72nd mechanized brigade", "72 ombr", "72nd mech", "black zaporozhians"],
        "ua_80_air_assault": ["80th air assault brigade", "80 odshbr", "80th air assault"],
        "ua_82_air_assault": ["82nd air assault brigade", "82 odshbr", "82nd air assault"],
        "ua_92_mech": ["92nd mechanized brigade", "92 ombr", "92nd mech", "ivan sirko"],
        "ua_93_mech": ["93rd mechanized brigade", "93 ombr", "93rd mech", "kholodny yar"],
        "ua_110_mech": ["110th mechanized brigade", "110 ombr", "110th mech"],
        "ua_128_mtn_assault": ["128th mountain assault brigade", "128 ogshbr", "128th mtn"],
        # ── Russian Ground Forces ──
        "ru_1_gta": ["1st guards tank army", "1 gta", "1st tank army", "1-ya gvardeiskaya"],
        "ru_2_gma": ["2nd guards motor rifle army", "2nd combined arms army", "2 oa"],
        "ru_4_tank": ["4th guards tank division", "4 gtd", "4th kantemirovskaya"],
        "ru_5_tank": ["5th guards tank brigade", "5 otbr", "5th tank bde"],
        "ru_76_vdv": ["76th guards air assault division", "76 gv vdd", "76th vdv", "pskov vdv"],
        "ru_155_marine": ["155th marine brigade", "155 obmp", "155th marines", "pacific marines"],
        "ru_200_motor": ["200th motor rifle brigade", "200 omsbr", "200th arctic"],
        "ru_810_marine": ["810th marine brigade", "810 obmp", "810th marines", "sevastopol marines"],
        "ru_storm_z": ["storm-z", "shtorm z", "штурм-z", "storm z detachment"],
        "ru_wagner": ["wagner group", "pmc wagner", "wagner pmc", "чвк вагнер", "prigozhin"],
    }

    def __init__(self):
        """Build a flattened lookup: normalized_alias -> canonical_id."""
        self._lookup: dict[str, str] = {}
        for canonical_id, aliases in self.ALIAS_MAP.items():
            for alias in aliases:
                normalized = self._normalize(alias)
                self._lookup[normalized] = canonical_id
        logger.info(
            "UnitResolver initialized: %d canonical units, %d total aliases",
            len(self.ALIAS_MAP),
            len(self._lookup),
        )

    @staticmethod
    def _normalize(text: str) -> str:
        """Lowercase, strip punctuation, collapse whitespace, normalize ordinals."""
        text = text.lower().strip()
        # Remove common punctuation
        text = re.sub(r"[''\".,;:!?()\[\]{}/\\-]", " ", text)
        # Normalize ordinals: "93rd" -> "93", "1st" -> "1", "2nd" -> "2"
        text = re.sub(r"(\d+)(?:st|nd|rd|th)\b", r"\1", text)
        # Collapse whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def resolve_unit_id(self, raw_name: str | None) -> str | None:
        """
        Resolve a raw unit name to a canonical unit_id.
        Returns None and logs a warning if no match is found.
        """
        if not raw_name or not raw_name.strip():
            return None

        normalized = self._normalize(raw_name)

        # ── Direct match ──
        if normalized in self._lookup:
            return self._lookup[normalized]

        # ── Substring / keyword match ──
        for alias_norm, canonical_id in self._lookup.items():
            if alias_norm in normalized or normalized in alias_norm:
                logger.debug(
                    "UnitResolver: fuzzy match '%s' -> '%s' via alias '%s'",
                    raw_name, canonical_id, alias_norm,
                )
                return canonical_id

        # ── Ordinal extraction heuristic ──
        numbers = re.findall(r"\d+", normalized)
        if numbers:
            for num in numbers:
                for alias_norm, canonical_id in self._lookup.items():
                    if num in alias_norm.split():
                        type_keywords = ["mech", "tank", "marine", "assault", "vdv",
                                         "motor", "brigade", "division", "bde", "brig"]
                        for kw in type_keywords:
                            if kw in normalized and kw in alias_norm:
                                logger.debug(
                                    "UnitResolver: ordinal+keyword match '%s' -> '%s'",
                                    raw_name, canonical_id,
                                )
                                return canonical_id

        # ── No match ──
        logger.warning("UnitResolver: UNRESOLVED unit '%s' -- needs human review", raw_name)
        return None


# ===========================================================================
# DATABASE MANAGER
# ===========================================================================
class DatabaseManager:
    """SQLite wrapper with schema init and upsert helpers."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        logger.info("DatabaseManager connected: %s", self.db_path)

    def _init_schema(self):
        cursor = self.conn.cursor()
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS units_registry (
                unit_id             TEXT PRIMARY KEY,
                official_name       TEXT,
                aliases             TEXT,
                faction             TEXT,
                status              TEXT DEFAULT 'ACTIVE',
                equipment_manifest  TEXT
            );

            CREATE TABLE IF NOT EXISTS kinetic_events (
                event_id        TEXT PRIMARY KEY,
                unit_id         TEXT,
                source          TEXT NOT NULL,
                date            TEXT,
                lat             REAL,
                lon             REAL,
                intensity_score REAL,
                raw_data        TEXT,
                FOREIGN KEY (unit_id) REFERENCES units_registry(unit_id)
                    ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_ke_date ON kinetic_events(date);
            CREATE INDEX IF NOT EXISTS idx_ke_source ON kinetic_events(source);
            CREATE INDEX IF NOT EXISTS idx_ke_unit ON kinetic_events(unit_id);
            CREATE INDEX IF NOT EXISTS idx_ke_geo ON kinetic_events(lat, lon);
        """)
        self.conn.commit()

    def upsert_unit(self, unit_id: str, official_name: str = None,
                    aliases: str = None, faction: str = None,
                    status: str = "ACTIVE", equipment_manifest: str = None):
        self.conn.execute("""
            INSERT INTO units_registry (unit_id, official_name, aliases, faction, status, equipment_manifest)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(unit_id) DO UPDATE SET
                official_name = COALESCE(excluded.official_name, units_registry.official_name),
                aliases = COALESCE(excluded.aliases, units_registry.aliases),
                faction = COALESCE(excluded.faction, units_registry.faction),
                status = COALESCE(excluded.status, units_registry.status),
                equipment_manifest = COALESCE(excluded.equipment_manifest, units_registry.equipment_manifest)
        """, (unit_id, official_name, aliases, faction, status, equipment_manifest))
        self.conn.commit()

    def upsert_event(self, event_id: str, unit_id: str = None,
                     source: str = None, date: str = None,
                     lat: float = None, lon: float = None,
                     intensity_score: float = None, raw_data: str = None):
        self.conn.execute("""
            INSERT INTO kinetic_events (event_id, unit_id, source, date, lat, lon, intensity_score, raw_data)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                unit_id = COALESCE(excluded.unit_id, kinetic_events.unit_id),
                source = COALESCE(excluded.source, kinetic_events.source),
                date = COALESCE(excluded.date, kinetic_events.date),
                lat = COALESCE(excluded.lat, kinetic_events.lat),
                lon = COALESCE(excluded.lon, kinetic_events.lon),
                intensity_score = COALESCE(excluded.intensity_score, kinetic_events.intensity_score),
                raw_data = COALESCE(excluded.raw_data, kinetic_events.raw_data)
        """, (event_id, unit_id, source, date, lat, lon, intensity_score, raw_data))
        self.conn.commit()

    def close(self):
        self.conn.close()
        logger.info("Database connection closed.")

    def get_stats(self) -> dict:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM units_registry")
        units = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM kinetic_events")
        events = cur.fetchone()[0]
        return {"units_registry": units, "kinetic_events": events}


# ===========================================================================
# HTTP HELPER
# ===========================================================================
def safe_request(url: str, method: str = "GET", retries: int = MAX_RETRIES,
                 **kwargs) -> requests.Response | None:
    """Execute an HTTP request with retry logic, rate limiting, and error handling."""
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", USER_AGENT)
    kwargs["headers"] = headers
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)

    for attempt in range(1, retries + 1):
        try:
            time.sleep(RATE_LIMIT_SECONDS)
            resp = requests.request(method, url, **kwargs)
            if resp.status_code == 200:
                return resp
            elif resp.status_code >= 500:
                logger.warning(
                    "Server error %d on %s (attempt %d/%d)",
                    resp.status_code, url, attempt, retries,
                )
                time.sleep(attempt * 2)
                continue
            else:
                logger.error("HTTP %d on %s -- not retrying", resp.status_code, url)
                return None
        except requests.exceptions.RequestException as e:
            logger.error("Request failed for %s (attempt %d/%d): %s",
                         url, attempt, retries, e)
            time.sleep(attempt * 2)

    logger.error("All %d retries exhausted for %s", retries, url)
    return None


# ===========================================================================
# NORMALIZATION HELPERS
# ===========================================================================
def normalize_date(raw_date: str | None) -> str | None:
    """Convert various date formats to ISO-8601 (YYYY-MM-DD)."""
    if not raw_date:
        return None
    raw_date = raw_date.strip()

    formats = [
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d.%m.%Y",
        "%d %B %Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%Y%m%d",
    ]
    for fmt_str in formats:
        try:
            dt = datetime.strptime(raw_date, fmt_str)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Try pandas as last resort
    try:
        dt = pd.to_datetime(raw_date, utc=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        logger.warning("Could not parse date: '%s'", raw_date)
        return None


def safe_float(value) -> float | None:
    """Safely convert a value to float or return None."""
    if value is None:
        return None
    try:
        f = float(value)
        if abs(f) < 0.001:
            return None
        return round(f, 6)
    except (ValueError, TypeError):
        return None


def make_event_id(prefix: str, raw_id) -> str:
    """Create a prefixed event ID to avoid collisions across sources."""
    return f"{prefix}_{raw_id}"


def hash_text(text: str) -> str:
    """SHA-256 hash of text content for deduplication."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


# ===========================================================================
# SOURCE 1: WARSPOTTING (API)
# ===========================================================================
def ingest_warspotting(db: DatabaseManager, resolver: UnitResolver):
    """Fetch WarSpotting /api/releases/all and upsert into kinetic_events."""
    logger.info("=== SOURCE 1: WarSpotting -- Starting ingestion ===")
    url = "https://ukr.warspotting.net/api/releases/all"
    resp = safe_request(url)
    if not resp:
        logger.error("WarSpotting: failed to fetch API")
        return

    try:
        data = resp.json()
    except json.JSONDecodeError:
        logger.error("WarSpotting: invalid JSON response")
        return

    items = data if isinstance(data, list) else data.get("data", data.get("results", []))
    count = 0
    for item in items:
        try:
            ws_id = item.get("id")
            if ws_id is None:
                continue

            event_id = make_event_id("ws", ws_id)
            raw_unit = item.get("unit", "")
            unit_id = resolver.resolve_unit_id(raw_unit) if raw_unit else None

            geo = item.get("geo", {}) or {}
            lat = safe_float(geo.get("lat"))
            lon = safe_float(geo.get("lng") or geo.get("lon"))

            model = item.get("model", "Unknown")
            status = item.get("status", "Unknown")
            date_str = normalize_date(item.get("date") or item.get("created_at"))

            raw_data = json.dumps({
                "model": model,
                "status": status,
                "unit": raw_unit,
                "tags": item.get("tags", []),
                "location": item.get("location", {}),
            }, ensure_ascii=False)

            db.upsert_event(
                event_id=event_id,
                unit_id=unit_id,
                source="WarSpotting",
                date=date_str,
                lat=lat,
                lon=lon,
                intensity_score=None,
                raw_data=raw_data,
            )
            count += 1
        except Exception as e:
            logger.error("WarSpotting: error processing item %s: %s", item.get("id"), e)

    logger.info("WarSpotting: upserted %d events", count)


# ===========================================================================
# SOURCE 2: MILITARYLAND (HTML CRAWLER)
# ===========================================================================
def ingest_militaryland(db: DatabaseManager, resolver: UnitResolver):
    """Crawl MilitaryLand /ukraine/armed-forces/ to extract brigade ORBAT data."""
    logger.info("=== SOURCE 2: MilitaryLand -- Starting ingestion ===")
    root_url = "https://www.militaryland.net/ukraine/armed-forces/"
    resp = safe_request(root_url)
    if not resp:
        logger.error("MilitaryLand: failed to fetch root page")
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    # Find links to brigade/unit pages
    links = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "/ukraine/" in href and href != root_url:
            full_url = urljoin(root_url, href)
            if "militaryland.net/ukraine/" in full_url:
                links.add(full_url)

    logger.info("MilitaryLand: found %d unit page links", len(links))
    count = 0

    for link in sorted(links):
        try:
            page_resp = safe_request(link)
            if not page_resp:
                continue

            page_soup = BeautifulSoup(page_resp.text, "html.parser")

            # Extract title (official unit name)
            title_tag = page_soup.find("h1", class_="entry-title") or page_soup.find("h1")
            if not title_tag:
                continue
            official_name = title_tag.get_text(strip=True)

            unit_id = resolver.resolve_unit_id(official_name)
            if not unit_id:
                # Generate a slug-based ID for unresolved units
                slug = re.sub(r"[^a-z0-9]+", "_", official_name.lower()).strip("_")
                unit_id = f"ml_{slug[:40]}"

            # Extract equipment from <ul> lists in the article body
            equipment_items = []
            article = page_soup.find("article") or page_soup.find("div", class_="entry-content")
            if article:
                for ul in article.find_all("ul"):
                    for li in ul.find_all("li"):
                        text = li.get_text(strip=True)
                        if text and len(text) > 3:
                            equipment_items.append(text)

            equipment_manifest = json.dumps(equipment_items, ensure_ascii=False) if equipment_items else None

            # Determine faction from URL
            faction = "UA" if "/ukraine/" in link else "RU"

            db.upsert_unit(
                unit_id=unit_id,
                official_name=official_name,
                aliases=official_name,
                faction=faction,
                status="ACTIVE",
                equipment_manifest=equipment_manifest,
            )
            count += 1

        except Exception as e:
            logger.error("MilitaryLand: error processing %s: %s", link, e)

    logger.info("MilitaryLand: upserted %d units", count)


# ===========================================================================
# SOURCE 3: DEEPSTATE (GeoJSON API -- Save to Disk Only)
# ===========================================================================
def ingest_deepstate(db: DatabaseManager, resolver: UnitResolver):
    """Fetch DeepState GeoJSON and save raw JSON to disk (no DB parse)."""
    logger.info("=== SOURCE 3: DeepState -- Starting ingestion ===")
    DEEPSTATE_DIR.mkdir(parents=True, exist_ok=True)

    # Use current timestamp rounded to the day
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    endpoints = [
        f"https://deepstatemap.live/api/history/{timestamp}/geojson",
        "https://deepstatemap.live/api/history/public",
        "https://api.deepstatemap.live/api/v1/history",
    ]

    for url in endpoints:
        resp = safe_request(url)
        if resp:
            filename = f"deepstate_{timestamp}_{hash_text(url)}.geojson"
            filepath = DEEPSTATE_DIR / filename
            try:
                # Validate it's actual JSON
                data = resp.json()
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logger.info("DeepState: saved GeoJSON to %s (%d bytes)", filepath, filepath.stat().st_size)
                return
            except (json.JSONDecodeError, IOError) as e:
                logger.error("DeepState: failed to save from %s: %s", url, e)
        else:
            logger.warning("DeepState: endpoint %s unreachable", url)

    logger.error("DeepState: all endpoints failed")


# ===========================================================================
# SOURCE 4: LOSTARMOUR (HTML Parsing)
# ===========================================================================
def ingest_lostarmour(db: DatabaseManager, resolver: UnitResolver):
    """Scrape LostArmour /tags/lancet and /tags/fpv for strike data."""
    logger.info("=== SOURCE 4: LostArmour -- Starting ingestion ===")
    tag_pages = [
        "https://lostarmour.info/tags/lancet",
        "https://lostarmour.info/tags/fpv",
    ]
    total_count = 0

    for tag_url in tag_pages:
        resp = safe_request(tag_url)
        if not resp:
            logger.error("LostArmour: failed to fetch %s", tag_url)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        tag_name = tag_url.split("/")[-1]

        # Parse article/card items
        items = soup.find_all(["article", "div"], class_=re.compile(r"item|card|entry|post"))
        if not items:
            # Fallback: look for list items or table rows
            items = soup.find_all("tr") or soup.find_all("li")

        for idx, item in enumerate(items):
            try:
                text = item.get_text(" ", strip=True)
                if len(text) < 10:
                    continue

                event_id = make_event_id("la", f"{tag_name}_{hash_text(text)}")

                # Try to extract unit mentions
                unit_id = None
                unit_patterns = [
                    r"(\d+)\s*(?:th|st|nd|rd)?\s*(?:bde|brigade|brig|ombr|otbr)",
                    r"(?:attack on|strike on|hit on)\s+(.+?)(?:\.|,|$)",
                ]
                for pattern in unit_patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        unit_id = resolver.resolve_unit_id(match.group(0))
                        if unit_id:
                            break

                raw_data = json.dumps({
                    "tag": tag_name,
                    "description": text[:500],
                    "source_url": tag_url,
                }, ensure_ascii=False)

                db.upsert_event(
                    event_id=event_id,
                    unit_id=unit_id,
                    source=f"LostArmour_{tag_name}",
                    date=normalize_date(datetime.now(timezone.utc).strftime("%Y-%m-%d")),
                    raw_data=raw_data,
                )
                total_count += 1
            except Exception as e:
                logger.error("LostArmour: error processing item %d: %s", idx, e)

    logger.info("LostArmour: upserted %d events", total_count)


# ===========================================================================
# SOURCE 5: ORYX (HTML Parsing -- Equipment Losses)
# ===========================================================================
def ingest_oryx(db: DatabaseManager, resolver: UnitResolver):
    """Scrape Oryx Blogspot page for verified equipment losses."""
    logger.info("=== SOURCE 5: Oryx -- Starting ingestion ===")
    url = "https://www.oryxspioenkop.com/2022/02/attack-on-europe-documenting-equipment.html"
    resp = safe_request(url)
    if not resp:
        logger.error("Oryx: failed to fetch page")
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    content = soup.find("div", class_="post-body") or soup.find("div", id="post-body")
    if not content:
        logger.error("Oryx: could not find post body")
        return

    # Parse category headers and list items ONLY (no <a> tags)
    current_category = "Unknown"
    count = 0
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for element in content.find_all(["h3", "h2", "li"]):
        tag_name = element.name

        if tag_name in ("h3", "h2"):
            header_text = element.get_text(strip=True)
            if header_text and len(header_text) > 3:
                # Extract category name before parenthetical stats
                # e.g. "Tanks (1234, of which ...)" -> "Tanks"
                cat_match = re.match(r"^([A-Za-z\s\-/]+)", header_text)
                current_category = cat_match.group(1).strip() if cat_match else header_text
            continue

        if tag_name == "li":
            text = element.get_text(strip=True)
            if not text or len(text) < 5:
                continue

            # FILTER: Must contain a status keyword
            status_match = re.search(
                r"(destroyed|damaged|abandoned|captured)",
                text, re.IGNORECASE
            )
            if not status_match:
                continue

            # FILTER: Entry must start with a number (Oryx format: "1 T-72B3: destroyed")
            if not re.match(r"^\d+", text):
                continue

            status = status_match.group(1).title()

            # Extract model name: "1 T-72B3: destroyed, ..." -> "T-72B3"
            parts = text.split(":")
            if len(parts) < 2:
                continue
            raw_model = parts[0].strip()
            model = re.sub(r"^\d+\s+", "", raw_model)  # Remove leading count

            event_id = make_event_id("ox", hash_text(f"{current_category}_{text}"))

            raw_data = json.dumps({
                "category": current_category,
                "entry": model,
                "status": status,
            }, ensure_ascii=False)

            db.upsert_event(
                event_id=event_id,
                unit_id=None,
                source="Oryx",
                date=today,
                raw_data=raw_data,
            )
            count += 1

    logger.info("Oryx: upserted %d equipment loss entries", count)


# ===========================================================================
# SOURCE 6: UKRDAILYUPDATE (Frontline Map)
# ===========================================================================
def ingest_ukrdailyupdate(db: DatabaseManager, resolver: UnitResolver):
    """Scrape UkrDailyUpdate map for frontline change events."""
    logger.info("=== SOURCE 6: UkrDailyUpdate -- Starting ingestion ===")
    # The map is JS-rendered; try known API/data endpoints
    data_urls = [
        "https://map.ukrdailyupdate.com/api/events",
        "https://map.ukrdailyupdate.com/data/events.json",
        "https://map.ukrdailyupdate.com/api/changes",
    ]

    for url in data_urls:
        resp = safe_request(url)
        if resp:
            try:
                data = resp.json()
                items = data if isinstance(data, list) else data.get("events", data.get("data", []))
                count = 0
                for item in items:
                    try:
                        raw_id = item.get("id") or hash_text(json.dumps(item, sort_keys=True))
                        event_id = make_event_id("udu", raw_id)

                        lat = safe_float(item.get("lat") or item.get("latitude"))
                        lon = safe_float(item.get("lng") or item.get("lon") or item.get("longitude"))
                        date_str = normalize_date(
                            item.get("date") or item.get("timestamp") or item.get("created_at")
                        )

                        raw_data = json.dumps(item, ensure_ascii=False, default=str)

                        db.upsert_event(
                            event_id=event_id,
                            source="UkrDailyUpdate",
                            date=date_str,
                            lat=lat,
                            lon=lon,
                            raw_data=raw_data,
                        )
                        count += 1
                    except Exception as e:
                        logger.error("UkrDailyUpdate: item error: %s", e)
                logger.info("UkrDailyUpdate: upserted %d events from %s", count, url)
                return
            except json.JSONDecodeError:
                continue

    # Fallback: scrape the HTML page for any embedded JSON data
    page_url = "https://map.ukrdailyupdate.com/"
    resp = safe_request(page_url)
    if resp:
        # Look for embedded JSON in script tags
        soup = BeautifulSoup(resp.text, "html.parser")
        for script in soup.find_all("script"):
            script_text = script.string or ""
            json_match = re.search(r'(?:events|data)\s*=\s*(\[.+?\]);', script_text, re.DOTALL)
            if json_match:
                try:
                    items = json.loads(json_match.group(1))
                    count = 0
                    for item in items:
                        event_id = make_event_id("udu", hash_text(json.dumps(item, sort_keys=True)))
                        db.upsert_event(
                            event_id=event_id,
                            source="UkrDailyUpdate",
                            raw_data=json.dumps(item, ensure_ascii=False, default=str),
                        )
                        count += 1
                    logger.info("UkrDailyUpdate: upserted %d events from embedded data", count)
                    return
                except json.JSONDecodeError:
                    continue

    logger.warning("UkrDailyUpdate: no parseable data found (JS-rendered map)")


# ===========================================================================
# SOURCE 7: TOPCARGO200 (RU Officer Casualties)
# ===========================================================================
def ingest_topcargo200(db: DatabaseManager, resolver: UnitResolver):
    """Scrape TopCargo200 for confirmed Russian senior officer casualties."""
    logger.info("=== SOURCE 7: TopCargo200 -- Starting ingestion ===")
    url = "https://topcargo200.com/"
    resp = safe_request(url)
    if not resp:
        logger.error("TopCargo200: failed to fetch page")
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    count = 0

    # Find officer cards/entries -- they're typically <a> links to individual pages
    officer_links = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if re.match(r"https?://topcargo200\.com/\d+", href):
            name = a_tag.get_text(strip=True)
            if name and len(name) > 3:
                officer_links.append((href, name))

    # Deduplicate by URL
    seen_urls = set()
    unique_officers = []
    for href, name in officer_links:
        if href not in seen_urls:
            seen_urls.add(href)
            unique_officers.append((href, name))

    logger.info("TopCargo200: found %d unique officer entries", len(unique_officers))

    for href, name in unique_officers:
        try:
            # Extract numeric ID from URL
            id_match = re.search(r"/(\d+)/?$", href)
            raw_id = id_match.group(1) if id_match else hash_text(href)
            event_id = make_event_id("tc", raw_id)

            # Parse individual officer page for details
            detail_resp = safe_request(href)
            rank = None
            unit_raw = None
            date_str = None

            if detail_resp:
                detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
                detail_text = detail_soup.get_text(" ", strip=True)

                # Try to extract rank
                rank_match = re.search(
                    r"(General|Colonel|Lieutenant Colonel|Major|Captain|Commander|Admiral)",
                    detail_text, re.IGNORECASE
                )
                rank = rank_match.group(1) if rank_match else None

                # Try to extract unit
                unit_match = re.search(
                    r"(?:unit|brigade|division|regiment|army|corps|fleet)[:\s]+(.+?)(?:\.|,|\n|$)",
                    detail_text, re.IGNORECASE
                )
                if unit_match:
                    unit_raw = unit_match.group(1).strip()

                # Try to extract date
                date_match = re.search(
                    r"(\d{1,2}[./]\d{1,2}[./]\d{2,4}|\d{4}-\d{2}-\d{2}|\w+ \d{1,2},?\s*\d{4})",
                    detail_text
                )
                if date_match:
                    date_str = normalize_date(date_match.group(1))

            unit_id = resolver.resolve_unit_id(unit_raw) if unit_raw else None

            raw_data = json.dumps({
                "name": name,
                "rank": rank,
                "unit_raw": unit_raw,
                "source_url": href,
            }, ensure_ascii=False)

            db.upsert_event(
                event_id=event_id,
                unit_id=unit_id,
                source="TopCargo200",
                date=date_str,
                raw_data=raw_data,
            )
            count += 1
        except Exception as e:
            logger.error("TopCargo200: error processing %s: %s", href, e)

    logger.info("TopCargo200: upserted %d officer casualty records", count)


# ===========================================================================
# SOURCE 8: UALOSSES (UA Soldier Records)
# ===========================================================================
def _parse_ualosses_slug(slug: str) -> dict:
    """Extract name, unit, and rank from a UALosses soldier URL slug.
    Example slug: 'koval-oleksandr-dmytrovych-1975-04-13-47-krushynivka-lieutenant'
    """
    result = {"name": None, "rank": None, "unit_raw": None}
    if not slug:
        return result

    # Known rank keywords (ordered longest-first to match multi-word ranks)
    rank_keywords = [
        "lieutenant-colonel", "senior-lieutenant", "junior-sergeant",
        "senior-soldier", "senior-seaman", "junior-lieutenant",
        "lieutenant", "colonel", "captain", "major", "general",
        "sergeant", "soldier", "seaman", "corporal", "private",
    ]
    for rk in rank_keywords:
        if slug.endswith(rk) or f"-{rk}-" in slug:
            result["rank"] = rk.replace("-", " ").title()
            slug = slug.replace(f"-{rk}", "")  # strip rank from end
            break

    # Known unit patterns in slugs (e.g., "79th-separate-airborne-assault-brigade")
    unit_match = re.search(
        r"(\d+(?:st|nd|rd|th)-[a-z-]*(?:brigade|battalion|regiment|division|guard|detachment|corps)[a-z-]*)",
        slug
    )
    if unit_match:
        result["unit_raw"] = unit_match.group(1).replace("-", " ").title()
        slug = slug.replace(unit_match.group(0), "")

    # Also check for named units (e.g., "azov", "national-guard")
    named_unit_match = re.search(
        r"(national-guard-of-ukraine[a-z-]*|azov[a-z-]*|right-sector[a-z-]*)",
        slug
    )
    if named_unit_match:
        named = named_unit_match.group(1).replace("-", " ").title()
        if result["unit_raw"]:
            result["unit_raw"] += " " + named
        else:
            result["unit_raw"] = named

    # The remaining beginning part is the name (before date patterns)
    name_match = re.match(r"^([a-z]+-[a-z]+-[a-z]+(?:-[a-z]+)?)", slug)
    if name_match:
        result["name"] = name_match.group(1).replace("-", " ").title()

    return result


def ingest_ualosses(db: DatabaseManager, resolver: UnitResolver):
    """Scrape UALosses.org for Ukrainian soldier loss records."""
    logger.info("=== SOURCE 8: UALosses -- Starting ingestion ===")
    base_url = "https://ualosses.org/en/soldiers/"
    count = 0
    page = 1
    max_pages = 50  # Safety cap -- site has ~1861 pages

    while page <= max_pages:
        url = f"{base_url}?page={page}" if page > 1 else base_url
        resp = safe_request(url)
        if not resp:
            break

        soup = BeautifulSoup(resp.text, "html.parser")

        # UALosses lists soldiers as <li> items containing <a> links
        # Link URLs are like: /en/soldier/koval-oleksandr-dmytrovych-1975-04-13-...-lieutenant/
        # Surrounding text shows: "Name\n Date of Birth - Date of Death\n Location"
        soldier_links = soup.find_all("a", href=re.compile(r"/en/soldier/"))

        if not soldier_links:
            logger.debug("UALosses: no soldier links found on page %d", page)
            break

        page_count = 0
        seen_hrefs = set()

        for link in soldier_links:
            try:
                href = link.get("href", "")
                if href in seen_hrefs:
                    continue
                seen_hrefs.add(href)

                # Extract slug from URL
                slug_match = re.search(r"/en/soldier/([^/]+)/?", href)
                if not slug_match:
                    continue
                slug = slug_match.group(1)

                # Generate unique event ID from slug
                event_id = make_event_id("ual", hash_text(slug))

                # Parse structured data from slug
                parsed = _parse_ualosses_slug(slug)
                name = parsed["name"] or link.get_text(strip=True)
                rank = parsed["rank"]
                unit_raw = parsed["unit_raw"]

                # Extract dates from surrounding text
                parent = link.parent
                parent_text = parent.get_text(" ", strip=True) if parent else ""

                # Date patterns: "April 13, 1975 - Sept. 29, 2022"
                date_of_death = None
                date_patterns = [
                    # "Sept. 29, 2022" or "(Feb. 26, 2022)"
                    r"-\s*\(?([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})\)?",
                    # ISO format
                    r"(\d{4}-\d{2}-\d{2})",
                ]
                for dp in date_patterns:
                    dm = re.search(dp, parent_text)
                    if dm:
                        date_of_death = normalize_date(dm.group(1))
                        if date_of_death:
                            break

                unit_id = resolver.resolve_unit_id(unit_raw) if unit_raw else None

                raw_data = json.dumps({
                    "name": name,
                    "rank": rank,
                    "unit_raw": unit_raw,
                    "slug": slug,
                    "source_url": f"https://ualosses.org{href}",
                    "context": parent_text[:300],
                }, ensure_ascii=False)

                db.upsert_event(
                    event_id=event_id,
                    unit_id=unit_id,
                    source="UALosses",
                    date=date_of_death,
                    raw_data=raw_data,
                )
                page_count += 1
                count += 1
            except Exception as e:
                logger.error("UALosses: error processing soldier link: %s", e)

        # Check for next page
        next_link = soup.find("a", string=re.compile(r"Next", re.IGNORECASE))
        if not next_link or page_count == 0:
            break
        page += 1

    logger.info("UALosses: upserted %d soldier records across %d pages", count, page)


# ===========================================================================
# SOURCE 9: MOTOLKO (Belarus Military Intel)
# ===========================================================================
def ingest_motolko(db: DatabaseManager, resolver: UnitResolver):
    """Scrape Motolko.help for Belarus military intelligence articles."""
    logger.info("=== SOURCE 9: Motolko -- Starting ingestion ===")
    url = "https://motolko.help/en-news/"
    resp = safe_request(url)
    if not resp:
        logger.error("Motolko: failed to fetch news page")
        return

    soup = BeautifulSoup(resp.text, "html.parser")
    count = 0

    # Find article links
    article_links = []
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "/en-news/" in href and href != url and href.endswith("/"):
            title = a_tag.get_text(strip=True)
            if title and len(title) > 10:
                full_url = urljoin(url, href)
                article_links.append((full_url, title))

    # Deduplicate
    seen = set()
    unique_articles = []
    for href, title in article_links:
        if href not in seen:
            seen.add(href)
            unique_articles.append((href, title))

    logger.info("Motolko: found %d unique article links", len(unique_articles))

    for href, title in unique_articles[:30]:  # Process up to 30 articles
        try:
            event_id = make_event_id("mk", hash_text(href))

            # Check for military relevance keywords
            military_keywords = [
                "military", "army", "weapon", "drone", "uav", "tank", "brigade",
                "soldier", "missile", "artillery", "aviation", "airforce", "war",
                "sanction", "defense", "nato", "russia", "belarus", "armed",
            ]
            title_lower = title.lower()
            is_military = any(kw in title_lower for kw in military_keywords)

            raw_data = json.dumps({
                "title": title,
                "source_url": href,
                "is_military_relevant": is_military,
            }, ensure_ascii=False)

            db.upsert_event(
                event_id=event_id,
                source="Motolko",
                date=normalize_date(datetime.now(timezone.utc).strftime("%Y-%m-%d")),
                raw_data=raw_data,
            )
            count += 1
        except Exception as e:
            logger.error("Motolko: error processing %s: %s", href, e)

    logger.info("Motolko: upserted %d articles", count)


# ===========================================================================
# SOURCE 10: GEOCONFIRMED (Geolocated Conflict Events)
# ===========================================================================
def _parse_kml_placemarks(kml_content: bytes, db: DatabaseManager):
    """Parse KML XML content for Placemark elements and upsert events."""
    import xml.etree.ElementTree as ET

    count = 0
    try:
        root = ET.fromstring(kml_content)
    except ET.ParseError as e:
        logger.error("GeoConfirmed: KML XML parse error: %s", e)
        return 0

    # KML namespace
    ns = {"kml": "http://www.opengis.net/kml/2.2"}

    # Try with and without namespace
    placemarks = root.findall(".//kml:Placemark", ns)
    if not placemarks:
        placemarks = root.findall(".//Placemark")

    for pm in placemarks:
        try:
            name_el = pm.find("kml:name", ns) or pm.find("name")
            name = name_el.text.strip() if name_el is not None and name_el.text else "Unknown"

            coords_el = pm.find(".//kml:coordinates", ns) or pm.find(".//coordinates")
            lat, lon = None, None
            if coords_el is not None and coords_el.text:
                coords_text = coords_el.text.strip()
                parts = coords_text.split(",")
                if len(parts) >= 2:
                    lon = safe_float(parts[0])
                    lat = safe_float(parts[1])

            desc_el = pm.find("kml:description", ns) or pm.find("description")
            description = desc_el.text.strip() if desc_el is not None and desc_el.text else ""

            # Try to extract date from description
            date_str = None
            date_match = re.search(
                r"(\d{4}-\d{2}-\d{2}|\d{1,2}[./]\d{1,2}[./]\d{2,4})",
                description
            )
            if date_match:
                date_str = normalize_date(date_match.group(1))

            event_id = make_event_id("gc", hash_text(f"{name}_{coords_text if coords_el is not None else ''}"))

            raw_data = json.dumps({
                "name": name,
                "description": description[:500],
            }, ensure_ascii=False)

            db.upsert_event(
                event_id=event_id,
                source="GeoConfirmed",
                date=date_str,
                lat=lat,
                lon=lon,
                raw_data=raw_data,
            )
            count += 1
        except Exception as e:
            logger.error("GeoConfirmed: error processing placemark: %s", e)

    return count


def ingest_geoconfirmed(db: DatabaseManager, resolver: UnitResolver):
    """Download GeoConfirmed KML/data and ingest geolocated events."""
    logger.info("=== SOURCE 10: GeoConfirmed -- Starting ingestion ===")
    GEOCONFIRMED_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d")

    # Strategy 1: The CDN KML is a NetworkLink redirect -- follow it to get the real KML
    real_kml_url = "https://geoconfirmed.org/api/map/ExportAsKml/Ukraine"
    stub_kml_url = "https://cdn.geoconfirmed.org/geoconfirmed/kmls/ukraine-google-earth.kml"

    kml_urls = [real_kml_url, stub_kml_url]
    total_count = 0

    for kml_url in kml_urls:
        resp = safe_request(kml_url)
        if not resp:
            logger.warning("GeoConfirmed: KML endpoint unreachable: %s", kml_url)
            continue

        # Save raw KML to disk
        url_tag = "api" if "api" in kml_url else "cdn"
        kml_path = GEOCONFIRMED_DIR / f"ukraine_{url_tag}_{ts}.kml"
        try:
            with open(kml_path, "wb") as f:
                f.write(resp.content)
            logger.info("GeoConfirmed: saved KML to %s (%d bytes)", kml_path, len(resp.content))
        except IOError as e:
            logger.error("GeoConfirmed: failed to save KML: %s", e)

        # Check if it's a NetworkLink (small file = redirect stub)
        content_str = resp.content.decode("utf-8", errors="ignore")
        if "<NetworkLink>" in content_str:
            # Extract the href from the NetworkLink
            href_match = re.search(r"<href>(.+?)</href>", content_str)
            if href_match:
                redirect_url = href_match.group(1).strip()
                logger.info("GeoConfirmed: following NetworkLink to %s", redirect_url)
                redirect_resp = safe_request(redirect_url)
                if redirect_resp and len(redirect_resp.content) > 500:
                    redirect_path = GEOCONFIRMED_DIR / f"ukraine_live_{ts}.kml"
                    try:
                        with open(redirect_path, "wb") as f:
                            f.write(redirect_resp.content)
                    except IOError:
                        pass
                    parsed = _parse_kml_placemarks(redirect_resp.content, db)
                    total_count += parsed
                    logger.info("GeoConfirmed: upserted %d events from redirected KML", parsed)
            continue

        # Parse directly if it's real KML content
        if len(resp.content) > 500:
            parsed = _parse_kml_placemarks(resp.content, db)
            total_count += parsed
            logger.info("GeoConfirmed: upserted %d events from %s", parsed, url_tag)
            if total_count > 0:
                break  # Got data, no need to try other URLs

    # Strategy 2: Try the GeoConfirmed JSON API directly
    if total_count == 0:
        json_urls = [
            "https://geoconfirmed.org/api/map/GetMapItems/Ukraine",
            "https://geoconfirmed.org/api/map/Ukraine",
        ]
        for jurl in json_urls:
            resp = safe_request(jurl)
            if not resp:
                continue
            try:
                data = resp.json()
                items = data if isinstance(data, list) else data.get("features", data.get("items", []))
                for item in items:
                    props = item.get("properties", item)
                    geom = item.get("geometry", {})
                    coords = geom.get("coordinates", [])

                    lat = safe_float(coords[1]) if len(coords) > 1 else safe_float(props.get("lat"))
                    lon = safe_float(coords[0]) if len(coords) > 0 else safe_float(props.get("lng"))

                    eid = make_event_id("gc", hash_text(json.dumps(props, sort_keys=True, default=str)))
                    date_str = normalize_date(
                        str(props.get("date") or props.get("timestamp") or "")
                    )

                    db.upsert_event(
                        event_id=eid,
                        source="GeoConfirmed",
                        date=date_str,
                        lat=lat,
                        lon=lon,
                        raw_data=json.dumps(props, ensure_ascii=False, default=str),
                    )
                    total_count += 1
                logger.info("GeoConfirmed: upserted %d events from JSON API %s", total_count, jurl)
                break
            except (json.JSONDecodeError, TypeError):
                continue

    logger.info("GeoConfirmed: total upserted %d events", total_count)


# ===========================================================================
# SOURCE 11: PARABELLUM THINK TANK (Lizmap WFS -- Geolocated Military Data)
# ===========================================================================
PARABELLUM_DIR = DATA_DIR / "parabellum"

def ingest_parabellum(db: DatabaseManager, resolver: UnitResolver):
    """Fetch Parabellum Think Tank conflict map data via Lizmap WFS endpoint."""
    logger.info("=== SOURCE 11: Parabellum Think Tank -- Starting ingestion ===")
    PARABELLUM_DIR.mkdir(parents=True, exist_ok=True)

    # Lizmap exposes a standard OGC WFS endpoint.
    # First: GetCapabilities to discover available layer names.
    base_wfs = (
        "https://geo.parabellumthinktank.com/index.php/lizmap/service"
        "?repository=russoukrainianwar&project=russian_invasion_of_ukraine"
    )
    capabilities_url = f"{base_wfs}&SERVICE=WFS&REQUEST=GetCapabilities"

    resp = safe_request(capabilities_url)
    layer_names: list[str] = []
    if resp:
        # WFS GetCapabilities returns XML -- use xml.etree.ElementTree
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(resp.content)
            # WFS namespaces vary; search for FeatureType/Name with and without ns
            # Common WFS namespace
            wfs_ns = {"wfs": "http://www.opengis.net/wfs"}
            feature_types = root.findall(".//wfs:FeatureType/wfs:Name", wfs_ns)
            if not feature_types:
                # Try WFS 2.0 namespace
                wfs_ns2 = {"wfs": "http://www.opengis.net/wfs/2.0"}
                feature_types = root.findall(".//wfs:FeatureType/wfs:Name", wfs_ns2)
            if not feature_types:
                # Try without namespace
                feature_types = root.findall(".//FeatureType/Name")
            if not feature_types:
                # Brute-force: find any element called Name under FeatureType
                for ft in root.iter():
                    if ft.tag.endswith("FeatureType"):
                        for child in ft:
                            if child.tag.endswith("Name") and child.text:
                                layer_names.append(child.text.strip())
            else:
                for name_el in feature_types:
                    if name_el.text:
                        layer_names.append(name_el.text.strip())
        except ET.ParseError as e:
            logger.error("Parabellum: XML parse error on GetCapabilities: %s", e)

    if not layer_names:
        logger.warning("Parabellum: No WFS layers discovered -- falling back to known layer names")
        # Fallback: common layer names observed on Parabellum maps
        layer_names = [
            "russian_invasion_of_ukraine",
            "frontline",
            "events",
            "positions",
            "units",
        ]

    logger.info("Parabellum: discovered %d WFS layers: %s",
                len(layer_names), layer_names[:15])

    total_count = 0
    for layer in layer_names:
        wfs_url = (
            f"{base_wfs}"
            f"&SERVICE=WFS&REQUEST=GetFeature"
            f"&TYPENAME={layer}"
            f"&OUTPUTFORMAT=GeoJSON"
            f"&SRSNAME=EPSG:4326"
        )
        resp = safe_request(wfs_url)
        if not resp:
            logger.debug("Parabellum: layer '%s' not available", layer)
            continue

        try:
            geojson = resp.json()
        except json.JSONDecodeError:
            logger.debug("Parabellum: layer '%s' returned non-JSON", layer)
            continue

        features = geojson.get("features", [])
        if not features:
            continue

        # Save raw GeoJSON to disk for reference
        ts = datetime.now(timezone.utc).strftime("%Y%m%d")
        safe_layer = re.sub(r"[^a-z0-9_]", "_", layer.lower())
        save_path = PARABELLUM_DIR / f"{safe_layer}_{ts}.geojson"
        try:
            with open(save_path, "w", encoding="utf-8") as f:
                json.dump(geojson, f, ensure_ascii=False, indent=2)
        except IOError as e:
            logger.error("Parabellum: failed to save %s: %s", save_path, e)

        layer_count = 0
        for feat in features:
            try:
                props = feat.get("properties", {})
                geom = feat.get("geometry", {})
                coords = geom.get("coordinates", [])

                # Handle Point geometries (lon, lat)
                lat, lon = None, None
                geom_type = geom.get("type", "")
                if geom_type == "Point" and len(coords) >= 2:
                    lon = safe_float(coords[0])
                    lat = safe_float(coords[1])
                elif geom_type in ("Polygon", "MultiPolygon", "LineString"):
                    # Use centroid of first coordinate for approximate location
                    flat_coords = coords
                    while flat_coords and isinstance(flat_coords[0], list):
                        flat_coords = flat_coords[0]
                    if len(flat_coords) >= 2:
                        lon = safe_float(flat_coords[0])
                        lat = safe_float(flat_coords[1])

                # Build unique ID from layer + feature ID or properties hash
                feat_id = feat.get("id") or props.get("id") or props.get("fid")
                if feat_id:
                    event_id = make_event_id("pb", f"{safe_layer}_{feat_id}")
                else:
                    event_id = make_event_id("pb", hash_text(
                        f"{safe_layer}_{json.dumps(props, sort_keys=True)}"
                    ))

                # Try to extract date from properties
                date_str = None
                for date_key in ["date", "Date", "DATE", "timestamp", "time", "day"]:
                    if date_key in props and props[date_key]:
                        date_str = normalize_date(str(props[date_key]))
                        if date_str:
                            break

                # Try to extract unit from properties
                unit_raw = None
                for unit_key in ["unit", "Unit", "name", "Name", "label", "description"]:
                    if unit_key in props and props[unit_key]:
                        unit_raw = str(props[unit_key])
                        break
                unit_id = resolver.resolve_unit_id(unit_raw) if unit_raw else None

                raw_data = json.dumps({
                    "layer": layer,
                    "properties": props,
                }, ensure_ascii=False, default=str)

                db.upsert_event(
                    event_id=event_id,
                    unit_id=unit_id,
                    source=f"Parabellum_{safe_layer}",
                    date=date_str,
                    lat=lat,
                    lon=lon,
                    raw_data=raw_data,
                )
                layer_count += 1
            except Exception as e:
                logger.error("Parabellum: error processing feature in '%s': %s", layer, e)

        total_count += layer_count
        logger.info("Parabellum: layer '%s' -- upserted %d features", layer, layer_count)

    logger.info("Parabellum: total upserted %d events across %d layers",
                total_count, len(layer_names))


# ===========================================================================
# ORCHESTRATOR
# ===========================================================================
SOURCE_REGISTRY = {
    "ws":   ("WarSpotting",       ingest_warspotting),
    "ml":   ("MilitaryLand",      ingest_militaryland),
    "ds":   ("DeepState",         ingest_deepstate),
    "la":   ("LostArmour",        ingest_lostarmour),
    "ox":   ("Oryx",              ingest_oryx),
    "udu":  ("UkrDailyUpdate",    ingest_ukrdailyupdate),
    "tc":   ("TopCargo200",       ingest_topcargo200),
    "ual":  ("UALosses",          ingest_ualosses),
    "mk":   ("Motolko",           ingest_motolko),
    "gc":   ("GeoConfirmed",      ingest_geoconfirmed),
    "pb":   ("Parabellum",        ingest_parabellum),
}


def run_pipeline(sources: list[str] | None = None, dry_run: bool = False):
    """Execute the full ingestion pipeline."""
    logger.info("=====================================================")
    logger.info("    MASTER INGESTOR -- Pipeline Start")
    logger.info("    Mode: %s", "DRY RUN" if dry_run else "LIVE")
    logger.info("=====================================================")

    resolver = UnitResolver()
    db = DatabaseManager()

    if dry_run:
        stats = db.get_stats()
        logger.info("DRY RUN: Schema initialized successfully.")
        logger.info("DRY RUN: Current DB state -- %s", stats)
        logger.info("DRY RUN: All %d source modules registered.", len(SOURCE_REGISTRY))
        for key, (name, _) in SOURCE_REGISTRY.items():
            logger.info("  [%s] %s -- READY", key, name)
        db.close()
        return

    # Determine which sources to run
    if sources:
        targets = {k: v for k, v in SOURCE_REGISTRY.items() if k in sources}
    else:
        targets = SOURCE_REGISTRY

    logger.info("Running %d source(s): %s", len(targets),
                ", ".join(name for name, _ in targets.values()))

    # Seeding known units to prevent FK errors if MilitaryLand source isn't run
    logger.info("Seeding %d known units into units_registry...", len(resolver.ALIAS_MAP))
    seed_count = 0
    for unit_id, aliases in resolver.ALIAS_MAP.items():
        # Use first alias as official name placeholder; don't overwrite if exists
        # We use INSERT OR IGNORE logic via upsert_unit but we only want to ensure existence.
        # Actually upsert_unit updates if exists. Let's strictly ensure it exists.
        # Since we don't have full metadata here, we'll just upsert with status='ACTIVE'
        # and the first alias as name. This is safe as MilitaryLand will overwrite with better data later.
        official_name = aliases[0].title()
        db.upsert_unit(unit_id=unit_id, official_name=official_name, faction="Unknown", aliases=",".join(aliases))
        seed_count += 1


    for key, (name, func) in targets.items():
        try:
            logger.info("--- Starting source: %s [%s] ---", name, key)
            start_time = time.time()
            func(db, resolver)
            elapsed = time.time() - start_time
            logger.info("--- Completed %s in %.1fs ---", name, elapsed)
        except Exception as e:
            logger.error("CRITICAL: Source %s failed with unhandled exception: %s", name, e,
                         exc_info=True)

    # Final stats
    stats = db.get_stats()
    logger.info("=====================================================")
    logger.info("    Pipeline Complete")
    logger.info("    units_registry:  %6d rows", stats["units_registry"])
    logger.info("    kinetic_events:  %6d rows", stats["kinetic_events"])
    logger.info("=====================================================")

    db.close()


# ===========================================================================
# CLI ENTRYPOINT
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(
        description="OSINT Master Ingestor -- Multi-source data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Source codes:
  ws   WarSpotting           ox   Oryx
  ml   MilitaryLand          udu  UkrDailyUpdate
  ds   DeepState             tc   TopCargo200
  la   LostArmour            ual  UALosses
  mk   Motolko               gc   GeoConfirmed
  pb   Parabellum

Examples:
  python master_ingestor.py                    # Run all sources
  python master_ingestor.py --dry-run          # Init DB only
  python master_ingestor.py --source ws ox     # WarSpotting + Oryx only
        """,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Initialize DB schema and validate configuration without network calls.",
    )
    parser.add_argument(
        "--source",
        nargs="+",
        choices=list(SOURCE_REGISTRY.keys()),
        help="Run specific source(s) only. Use source codes listed below.",
    )
    args = parser.parse_args()
    run_pipeline(sources=args.source, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
