#!/usr/bin/env python3
"""Export pipeline: normalized database rows to UI-consumable artifacts.

Reads analyzed events (``unique_events``) from the raw-events database and
the entity database, then writes the full artifact set consumed by the
dashboard:

* ``events.geojson`` / ``events_latest.json`` -- sanitized public payloads
  (full history and the rolling latest window).
* ``events_export.csv`` -- flat analyst exchange format.
* ``units.json`` -- ORBAT-enriched unit dossiers with engagement statistics.
* ``external_losses.json`` -- verified equipment-loss records.
* ``strategic_trends.json`` -- daily per-sector T.I.E. aggregates.
* ``sector_anomalies.json`` / ``asymmetry_index.json`` / ``glocs.geojson`` --
  analytic layers derived from the exported features.
* ``campaigns_geo.json`` / ``campaign_reports.json`` -- campaign views.

Every record passes PII redaction and OPSEC publication gating before it is
written to disk.
"""

from __future__ import annotations

import contextlib
import csv
import datetime as _dt
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from collections.abc import Iterable, Mapping, Sequence
from urllib.parse import urlparse

from dotenv import load_dotenv

try:
    import orjson

    def _fast_json_loads(payload: str | bytes) -> Any:
        """Deserialize JSON with orjson when it is available."""
        return orjson.loads(payload)

except ImportError:  # pragma: no cover - orjson is an optional accelerator

    def _fast_json_loads(payload: str | bytes) -> Any:
        """Deserialize JSON with the standard library."""
        return json.loads(payload)


try:
    from scripts.campaigns_engine import (
        build_campaign_reports,
        build_campaigns_geo,
        ensure_campaign_columns,
        load_campaign_definitions,
    )
    from scripts.v42_analytics import (
        apply_anomaly_flags,
        apply_reputation_decay,
        build_glocs_geojson,
        compute_asymmetry_index,
        compute_sector_volume_anomalies,
        ensure_sources_reputation_schema,
        extract_classification,
        extract_faction,
        write_json,
    )
except ImportError:  # executed directly from the scripts/ directory
    from campaigns_engine import (
        build_campaign_reports,
        build_campaigns_geo,
        ensure_campaign_columns,
        load_campaign_definitions,
    )
    from v42_analytics import (
        apply_anomaly_flags,
        apply_reputation_decay,
        build_glocs_geojson,
        compute_asymmetry_index,
        compute_sector_volume_anomalies,
        ensure_sources_reputation_schema,
        extract_classification,
        extract_faction,
        write_json,
    )

from impact_atlas.config import ProjectPaths
from impact_atlas.logging import configure_logging

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = ProjectPaths.discover()

DB_PATH = PATHS.raw_events_database
IMPACT_ATLAS_DB_PATH = PATHS.impact_database
GEOJSON_PATH = PATHS.assets_data / "events.geojson"
EVENTS_LATEST_PATH = PATHS.assets_data / "events_latest.json"
CSV_PATH = PATHS.assets_data / "events_export.csv"
UNITS_JSON_PATH = PATHS.assets_data / "units.json"
ORBAT_JSON_PATH = PATHS.assets_data / "orbat_units.json"
STRATEGIC_TRENDS_PATH = PATHS.assets_data / "strategic_trends.json"
EXTERNAL_LOSSES_PATH = PATHS.assets_data / "external_losses.json"
SECTOR_ANOMALIES_PATH = PATHS.assets_data / "sector_anomalies.json"
ASYMMETRY_INDEX_PATH = PATHS.assets_data / "asymmetry_index.json"
GLOCS_PATH = PATHS.assets_data / "glocs.geojson"
CAMPAIGN_DEFINITIONS_CACHE_PATH = PATHS.assets_data / "campaign_definitions.json"
CAMPAIGN_REPORTS_PATH = PATHS.assets_data / "campaign_reports.json"
CAMPAIGNS_GEO_PATH = PATHS.assets_data / "campaigns_geo.json"

OPSEC_CUTOFF_HOURS = 24
LATEST_WINDOW_DAYS = 7
SENSITIVE_MOVEMENT_CLASSES = {
    "MANOEUVRE",
    "MANEUVER",
    "SHAPING_MANOEUVRE",
    "SHAPING_MANEUVER",
}

PII_REDACTION = "[REDACTED]"
PERSON_TITLE_PATTERN = re.compile(
    r"\b(?:Lt\.?\s*Gen\.?|Lieutenant\s+General|Major\s+General|Brigadier\s+General|"
    r"Colonel|Col\.?|Lieutenant|Lt\.?|Captain|Capt\.?|Major|Sgt\.?|Sergeant|"
    r"Commander|President|Minister|Governor|General),?\s+"
    r"[A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+){0,3}\b"
)
CYRILLIC_PERSON_TITLE_PATTERN = re.compile(
    r"\b(?:генерал|полковник|майор|капітан|лейтенант|командир|міністр|губернатор)\s+"
    r"[А-ЯЁІЇЄҐ][а-яёіїєґ'-]+(?:\s+[А-ЯЁІЇЄҐ][а-яёіїєґ'-]+){0,3}\b",
    re.IGNORECASE,
)
LICENSE_PLATE_PATTERN = re.compile(
    r"\b(?:[A-ZА-ЯІЇЄҐ]{1,3}[-\s]?\d{3,5}[-\s]?[A-ZА-ЯІЇЄҐ]{1,3}|"
    r"\d{2,4}[-\s]?[A-ZА-ЯІЇЄҐ]{2,4}[-\s]?\d{2,4})\b"
)

# Pre-compiled regex patterns (avoid re-compilation in hot loops)
_ASSET_RE = re.compile(
    r"\b(T-(?:72|80|90|64|55)[A-Z0-9]*|BMP-[123][A-Z]*|BTR-[0-9]+[A-Z]*|"
    r"2S(?:1|3|5|7|19|35)[A-Z\- ]*|HIMARS|GMLRS|M270|M142|Grad|Smerch|Uragan|"
    r"TOS-1[A]?|S-[234]00[A-Z0-9]*|Buk[- ]?[A-Z0-9]*|Patriot|NASAMS|IRIS-T|Gepard|"
    r"Iskander[- ]?[MK]?|Kalibr|Kinzhal|Shahed[- ]?1[0-9]{2}|Lancet[- ]?[0-9]*|"
    r"FPV|Orlan[- ]?10|Ka-52|Su-[0-9]+[A-Z]*|Leopard[- ]?[12][A-Z0-9]*|Bradley|"
    r"CV90|CAESAR|PzH[- ]?2000|Krab|M777|Storm Shadow|ATACMS|Javelin|NLAW|"
    r"Stugna[- ]?P?|Kornet)\b",
    re.IGNORECASE,
)
_ALPHANUM_RE = re.compile(r"^[A-Za-z0-9_]+$")

logger = logging.getLogger("generate_output")


# =============================================================================
# PARSING / COERCION HELPERS
# =============================================================================
def _date_to_epoch_ms(date_str: Any) -> int:
    """Convert a date string to epoch milliseconds (0 when unparsable)."""
    if not date_str or not isinstance(date_str, str):
        return 0
    date_str = date_str.strip()
    if not date_str or date_str.lower() in ("nat", "none", "null", "unknown"):
        return 0
    try:
        parsed = _dt.datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_dt.UTC)
        return int(parsed.timestamp() * 1000)
    except (ValueError, OverflowError):
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = _dt.datetime.strptime(date_str[: len(fmt) + 5], fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=_dt.UTC)
            return int(parsed.timestamp() * 1000)
        except (ValueError, OverflowError):
            continue
    return 0


def _safe_float(val: Any, default: float = 0.0) -> float:
    """Coerce ``val`` to float, returning ``default`` for junk input."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    try:
        clean = str(val).strip()
        match = re.match(r"^[-+]?\d*\.?\d+", clean)
        if match:
            return float(match.group(0))
        return float(clean)
    except (ValueError, TypeError):
        return default


def _safe_int(val: Any, default: int = 0) -> int:
    """Coerce ``val`` to int, returning ``default`` for junk input."""
    if val is None:
        return default
    if isinstance(val, int):
        return val
    try:
        clean = str(val).strip()
        match = re.match(r"^[-+]?\d+", clean)
        if match:
            return int(match.group(0))
        return int(clean)
    except (ValueError, TypeError):
        return default


def _parse_event_datetime_utc(date_str: Any) -> _dt.datetime | None:
    """Parse an event date to an aware UTC datetime, or ``None``."""
    if not date_str or not isinstance(date_str, str):
        return None
    clean = date_str.strip()
    if not clean or clean.lower() in {"unknown", "none", "nat", "null"}:
        return None
    try:
        parsed = _dt.datetime.fromisoformat(clean.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_dt.UTC)
        return parsed.astimezone(_dt.UTC)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            parsed = _dt.datetime.strptime(clean[: len(fmt) + 5], fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=_dt.UTC)
            return parsed.astimezone(_dt.UTC)
        except (ValueError, OverflowError):
            continue
    return None


# =============================================================================
# OPSEC / PII SANITIZATION
# =============================================================================
def _is_sensitive_movement_event(
    classification: Any, category: Any, title: Any, description: Any
) -> bool:
    """Return True when any field marks the event as a sensitive movement."""
    text = " ".join(str(v or "") for v in (classification, category, title, description)).upper()
    return any(token in text for token in SENSITIVE_MOVEMENT_CLASSES)


def _should_publish_event(
    date_str: Any,
    classification: Any,
    category: Any,
    title: Any,
    description: Any,
    export_now: _dt.datetime,
) -> bool:
    """Apply the OPSEC hold-back rule for recent sensitive-movement events."""
    if not _is_sensitive_movement_event(classification, category, title, description):
        return True
    event_dt = _parse_event_datetime_utc(date_str)
    if not event_dt:
        return False
    return event_dt <= export_now - _dt.timedelta(hours=OPSEC_CUTOFF_HOURS)


def sanitize_public_text(value: Any) -> Any:
    """Redact person names (by title) and license plates from free text."""
    if value is None:
        return value
    text = str(value)
    text = PERSON_TITLE_PATTERN.sub(PII_REDACTION, text)
    text = CYRILLIC_PERSON_TITLE_PATTERN.sub(PII_REDACTION, text)
    text = LICENSE_PLATE_PATTERN.sub(PII_REDACTION, text)
    return text


def sanitize_public_object(value: Any) -> Any:
    """Recursively redact PII and drop identity-bearing keys from structures."""
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        drop_keys = {
            "name", "rank", "commander", "source_url", "context", "person",
            "full_name", "first_name", "last_name", "patronymic", "license_plate",
        }
        for key, item in value.items():
            if str(key).lower() in drop_keys:
                continue
            sanitized[key] = sanitize_public_object(item)
        return sanitized
    if isinstance(value, list):
        return [sanitize_public_object(item) for item in value]
    if isinstance(value, str):
        return sanitize_public_text(value)
    return value


def sanitize_public_feature(feature: dict[str, Any]) -> dict[str, Any]:
    """Sanitize the presentation fields of one GeoJSON feature in place."""
    props = feature.get("properties", {})
    for key in ("title", "description", "ai_reasoning", "visual_analysis"):
        if key in props:
            props[key] = sanitize_public_text(props.get(key))
    if props.get("units"):
        try:
            units = json.loads(props["units"]) if isinstance(props["units"], str) else props["units"]
            props["units"] = json.dumps(sanitize_public_object(units), ensure_ascii=False)
        except (json.JSONDecodeError, TypeError):
            pass
    return feature


# =============================================================================
# PUBLIC PAYLOAD BUILDERS
# =============================================================================
def build_public_payload(
    features: Sequence[dict[str, Any]],
    generated_at: str,
    opsec_withheld_count: int,
) -> dict[str, Any]:
    """Wrap sanitized features in the public FeatureCollection envelope."""
    latest_dt: _dt.datetime | None = None
    for feature in features:
        event_dt = _parse_event_datetime_utc(feature.get("properties", {}).get("date"))
        if event_dt and (latest_dt is None or event_dt > latest_dt):
            latest_dt = event_dt
    return {
        "type": "FeatureCollection",
        "metadata": {
            "generated_at": generated_at,
            "latest_event_date": latest_dt.date().isoformat() if latest_dt else None,
            "latest_window_days": LATEST_WINDOW_DAYS,
            "opsec_cutoff_hours": OPSEC_CUTOFF_HOURS,
            "opsec_withheld_count": opsec_withheld_count,
            "pii_sanitized": True,
        },
        "features": list(features),
    }


def build_latest_features(features: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return only features inside the rolling latest-event window."""
    dated: list[tuple[_dt.datetime, dict[str, Any]]] = []
    for feature in features:
        event_dt = _parse_event_datetime_utc(feature.get("properties", {}).get("date"))
        if event_dt:
            dated.append((event_dt, feature))
    if not dated:
        return []
    latest_dt = max(dt for dt, _ in dated)
    cutoff = latest_dt - _dt.timedelta(days=LATEST_WINDOW_DAYS)
    return [feature for event_dt, feature in dated if event_dt >= cutoff]


# =============================================================================
# SOURCE PARSING
# =============================================================================
def parse_sources_to_list(sources_str: Any) -> list[dict[str, str]]:
    """Parse a raw sources column into structured ``{"name", "url"}`` entries.

    Accepts JSON arrays, `` ||| `` / `` | `` separated strings, bare domains,
    and Telegram handles; unknown junk values are dropped.
    """
    if not sources_str or sources_str == "[]":
        return []
    items: list[Any] = []
    try:
        parsed = json.loads(sources_str)
        if isinstance(parsed, list):
            items = parsed
    except (json.JSONDecodeError, TypeError):
        pass
    if not items:
        raw = str(sources_str)
        if " ||| " in raw:
            items = [u.strip() for u in raw.split(" ||| ") if u.strip()]
        elif " | " in raw:
            items = [u.strip() for u in raw.split(" | ") if u.strip()]
        else:
            items = [raw.strip()] if sources_str else []

    result: list[dict[str, str]] = []
    for item in items:
        item = str(item).strip()
        if len(item) < 3 or item.lower() in ["none", "null", "unknown", "[null]"]:
            continue
        is_url = item.startswith("http") or item.startswith("www.")
        if is_url:
            url = item
            if "t.me/" in url:
                try:
                    parts = url.split("t.me/")[1].split("/")
                    channel_name = parts[0] if parts else "t.me"
                    result.append({"name": channel_name, "url": url})
                except (IndexError, AttributeError):
                    result.append({"name": "Telegram", "url": url})
            else:
                try:
                    domain = urlparse(url if url.startswith("http") else "https://" + url).netloc.replace("www.", "")
                    if not domain:
                        domain = "Source"
                except ValueError:
                    domain = "Source"
                result.append({"name": domain, "url": url})
        else:
            if item == "GDELT_Network":
                result.append({"name": "GDELT", "url": "#"})
            elif "." in item and not item.startswith("@"):
                url = f"https://{item}" if not item.startswith("http") else item
                result.append({"name": item, "url": url})
            elif _ALPHANUM_RE.match(item):
                result.append({"name": item, "url": f"https://t.me/{item}"})

    seen_names: dict[str, dict[str, str]] = {}
    unique_result: list[dict[str, str]] = []
    for entry in result:
        key = entry["name"].lower()
        if key not in seen_names:
            seen_names[key] = entry
            unique_result.append(entry)
        elif seen_names[key]["url"] == "#" and entry["url"] != "#":
            seen_names[key]["url"] = entry["url"]
    return unique_result


# =============================================================================
# ORBAT ENRICHMENT
# =============================================================================
def load_orbat_data() -> list[dict[str, Any]]:
    """Load the ORBAT unit dataset, returning an empty list on failure."""
    try:
        if ORBAT_JSON_PATH.exists():
            with open(ORBAT_JSON_PATH, encoding="utf-8") as handle:
                data = json.load(handle)
            return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError) as error:
        logger.warning("Failed to load ORBAT data: %s", error)
    return []


def build_orbat_index(
    orbat_data: Sequence[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Pre-build a hashmap for O(1) ORBAT lookups instead of O(n*m) scans."""
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for ob in orbat_data or []:
        faction = (ob.get("faction") or "").upper()
        ob_name = (ob.get("unit_name") or "").lower()
        if ob_name:
            index[(faction, ob_name)] = ob
    return index


def enrich_units(
    ai_units: Any,
    orbat_data: Sequence[dict[str, Any]],
    orbat_index: Mapping[tuple[str, str], dict[str, Any]] | None = None,
) -> Any:
    """Attach ORBAT metadata to AI-detected units in place.

    Uses the pre-built index for exact matches and falls back to a bounded
    substring scan only when no exact match exists.
    """
    if not ai_units or not orbat_data:
        return ai_units
    transfer_keys = [
        "echelon", "echelon_symbol", "type", "branch", "sub_branch",
        "garrison", "district", "commander", "superior",
    ]
    if orbat_index is not None:
        for u in ai_units:
            u_name = (u.get("unit_name") or "").lower()
            u_id = (u.get("unit_id") or "").lower()
            u_faction = (u.get("faction") or "UNKNOWN").upper()
            best_match: dict[str, Any] | None = None
            best_score = 0
            for candidate in (u_name, u_id):
                if candidate:
                    ob = orbat_index.get((u_faction, candidate))
                    if ob:
                        best_match, best_score = ob, 100
                        break
            if best_score < 80:
                for (fac, ob_name), ob in orbat_index.items():
                    if fac != u_faction:
                        continue
                    if ob_name in u_name or ob_name in u_id:
                        best_match, best_score = ob, 80
                        break
            if best_match and best_score >= 80:
                u["orbat_id"] = best_match.get("orbat_id")
                ob_name = best_match.get("unit_name") or best_match.get("full_name_en")
                if ob_name:
                    u["display_name"] = ob_name
                for key in transfer_keys:
                    u[key] = best_match.get(key)
                best_match["_used"] = True
        return ai_units

    # Legacy fallback (original O(n*m) behavior) when no index is supplied.
    for u in ai_units:
        best_match = None
        best_score = 0
        u_name = (u.get("unit_name") or "").lower()
        u_id = (u.get("unit_id") or "").lower()
        u_faction = (u.get("faction") or "UNKNOWN").upper()
        for ob in orbat_data:
            if (ob.get("faction") or "").upper() != u_faction:
                continue
            ob_name = (ob.get("unit_name") or "").lower()
            if not ob_name:
                continue
            if ob_name in (u_name, u_id):
                score = 100
            elif ob_name in u_name or ob_name in u_id:
                score = 80
            else:
                score = 0
            if score > best_score:
                best_score, best_match = score, ob
        if best_match and best_score >= 80:
            u["orbat_id"] = best_match.get("orbat_id")
            ob_name = best_match.get("unit_name") or best_match.get("full_name_en")
            if ob_name:
                u["display_name"] = ob_name
            for key in transfer_keys:
                u[key] = best_match.get(key)
            best_match["_used"] = True
    return ai_units


def get_marker_style(tie_score: Any, effect_score: Any) -> tuple[float, str]:
    """Derive marker radius and color from T.I.E. and effect scores."""
    try:
        tie_score = float(tie_score or 0)
        effect_score = float(effect_score or 0)
    except (TypeError, ValueError):
        tie_score, effect_score = 0, 0
    radius = 4 + (tie_score / 10)
    if effect_score >= 8:
        color = "#ef4444"
    elif effect_score >= 5:
        color = "#f59e0b"
    elif effect_score >= 3:
        color = "#eab308"
    else:
        color = "#64748b"
    return radius, color


# =============================================================================
# MEDIA EXTRACTION
# =============================================================================
def save_media_frame(event_id: str, frame_id: Any, base64_str: Any) -> str:
    """Decode a base64 frame and persist it under ``assets/data/media/``.

    Returns the web-relative path of the saved frame, or an empty string when
    the payload is absent or undecodable. Existing files are not rewritten.
    """
    import base64

    if not base64_str or not isinstance(base64_str, str):
        return ""

    if base64_str.startswith("data:image/"):
        try:
            header, raw_b64 = base64_str.split(";base64,", 1)
            ext = header.split("image/", 1)[1]
            if ext == "jpeg":
                ext = "jpg"
        except (ValueError, IndexError):
            return ""
    else:
        raw_b64 = base64_str
        ext = "jpg"

    try:
        img_data = base64.b64decode(raw_b64)
    except (ValueError, TypeError) as error:
        logger.warning("Failed to decode base64 for event %s frame %s: %s", event_id, frame_id, error)
        return ""

    media_dir = (PATHS.assets_data / "media").resolve()
    media_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{event_id}_{frame_id}.{ext}"
    filepath = media_dir / filename
    try:
        if not filepath.exists():
            with open(filepath, "wb") as handle:
                handle.write(img_data)
        return f"assets/data/media/{filename}"
    except OSError as error:
        logger.warning("Failed to save frame image to %s: %s", filepath, error)
        return ""


# =============================================================================
# SECTOR CLASSIFICATION & UNIT STATISTICS
# =============================================================================
def classify_sector(lat: Any, lon: Any, target_type: Any) -> str:
    """Map an event to a strategic sector using target type and geography."""
    target_type_lower = (target_type or "").lower()
    energy_keywords = ["power", "grid", "dam", "plant", "refinery", "substation", "transformer", "energy"]
    if any(kw in target_type_lower for kw in energy_keywords):
        return "ENERGY_COERCION"
    if "airfield" in target_type_lower or "airbase" in target_type_lower:
        return "DEEP_STRIKES_RU"
    try:
        lat_f = float(lat or 0)
        lon_f = float(lon or 0)
    except (TypeError, ValueError):
        return "EASTERN_FRONT"
    if lat_f and lon_f and lat_f > 50.0 and lon_f > 36.0:
        return "DEEP_STRIKES_RU"
    if lon_f <= 36.0 and lat_f < 48.0:
        return "SOUTHERN_FRONT"
    return "EASTERN_FRONT"


def update_unit_stats(
    stats_acc: dict[str, dict[str, Any]],
    unit: Mapping[str, Any],
    event_data: Mapping[str, Any],
) -> None:
    """Accumulate engagement statistics for one unit from one event."""
    key = str(
        unit.get("orbat_id") or unit.get("unit_id") or unit.get("unit_name") or "UNKNOWN"
    ).lower()
    if key not in stats_acc:
        stats_acc[key] = {
            "engagement_count": 0,
            "last_active": "2000-01-01",
            "total_tie": 0,
            "tactics_hist": {},
            "roles_hist": {},
            "orbat_id": unit.get("orbat_id"),
            "tie_vectors": [],
            "assets_set": set(),
            "daily_dates": [],
            "recent_events": [],
        }
    entry = stats_acc[key]
    entry["engagement_count"] += 1
    evt_date = event_data.get("date", "2000-01-01")
    if evt_date and evt_date > entry["last_active"]:
        entry["last_active"] = evt_date
    entry["total_tie"] += event_data.get("tie_score", 0)
    cls = event_data.get("classification", "UNKNOWN")
    entry["tactics_hist"][cls] = entry["tactics_hist"].get(cls, 0) + 1
    k, t, e = event_data.get("kinetic_score", 0), event_data.get("target_score", 0), event_data.get("effect_score", 0)
    if k or t or e:
        entry["tie_vectors"].append({"kinetic": float(k), "target": float(t), "effect": float(e)})
    detected_assets = event_data.get("detected_assets", [])
    if detected_assets:
        for asset in detected_assets:
            atype = asset.get("type", "") if isinstance(asset, dict) else str(asset)
            if atype and atype not in ("UNKNOWN_ARMOR", "UNKNOWN_VEHICLE", "UNKNOWN_SYSTEM", "UNKNOWN_AIRCRAFT"):
                entry["assets_set"].add(atype)
    else:
        text_blob = f"{event_data.get('title', '')} {event_data.get('description', '')}"
        for match in _ASSET_RE.findall(text_blob):
            entry["assets_set"].add(match.strip())
    if evt_date and evt_date != "2000-01-01":
        entry["daily_dates"].append(evt_date[:10])
    entry["recent_events"].append({
        "date": evt_date,
        "title": event_data.get("title", ""),
        "location": event_data.get("location", ""),
        "lat": event_data.get("lat"),
        "lon": event_data.get("lon"),
        "url": event_data.get("url", ""),
        "event_id": event_data.get("event_id", ""),
    })


def _build_dossier_fields(stats_entry: Mapping[str, Any]) -> dict[str, Any]:
    """Derive presentation-ready dossier fields from a unit stats entry."""
    result: dict[str, Any] = {}
    vecs = stats_entry.get("tie_vectors", [])
    if vecs:
        n = len(vecs)
        result["avg_tie"] = {
            "kinetic": round(sum(v["kinetic"] for v in vecs) / n, 2),
            "target": round(sum(v["target"] for v in vecs) / n, 2),
            "effect": round(sum(v["effect"] for v in vecs) / n, 2),
        }
    else:
        result["avg_tie"] = {"kinetic": 0, "target": 0, "effect": 0}
    result["assets_detected"] = sorted(list(stats_entry.get("assets_set", set())))
    raw_dates = stats_entry.get("daily_dates", [])
    if raw_dates:
        valid_dates = sorted([d for d in raw_dates if d and len(d) >= 10])
        anchor = _dt.datetime.strptime(valid_dates[-1], "%Y-%m-%d").date() if valid_dates else _dt.date.today()
    else:
        anchor = _dt.date.today()
    trend = [0] * 30
    date_counter: dict[str, int] = {}
    for d in raw_dates:
        date_counter[d] = date_counter.get(d, 0) + 1
    for i in range(30):
        day_key = (anchor - _dt.timedelta(days=29 - i)).strftime("%Y-%m-%d")
        trend[i] = date_counter.get(day_key, 0)
    result["engagement_trend_30d"] = trend
    result["engagement_trend_anchor"] = anchor.isoformat()
    total_30d = sum(trend)
    result["engagement_freq_label"] = "High" if total_30d > 8 else ("Medium" if total_30d >= 3 else "Low")
    recent = stats_entry.get("recent_events", [])
    result["recent_engagements"] = sorted(recent, key=lambda x: x.get("date", ""), reverse=True)[:5]
    return result


# =============================================================================
# CASUALTIES & LOSSES EXPORTS
# =============================================================================
def preload_casualties_index() -> dict[str, int]:
    """Pre-load UALosses casualty counts as ``{unit_raw_lower: count}``."""
    if not IMPACT_ATLAS_DB_PATH.exists():
        return {}
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(IMPACT_ATLAS_DB_PATH))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT raw_data FROM kinetic_events WHERE source = 'UALosses'"
        ).fetchall()
        casualties_by_unit_raw: dict[str, int] = {}
        for row in rows:
            try:
                data = _fast_json_loads(row["raw_data"])
                unit_raw = (data.get("unit_raw") or "").strip().lower()
                if unit_raw:
                    casualties_by_unit_raw[unit_raw] = casualties_by_unit_raw.get(unit_raw, 0) + 1
            except (ValueError, TypeError, AttributeError):
                continue
        return casualties_by_unit_raw
    except sqlite3.Error as error:
        logger.error("Failed to preload casualties: %s", error)
        return {}
    finally:
        if conn is not None:
            conn.close()


def enrich_units_with_casualties(
    units_list: list[dict[str, Any]],
    casualties_index: Mapping[str, int] | None = None,
) -> list[dict[str, Any]]:
    """Attach best-match casualty counts to Ukrainian units in place."""
    if casualties_index is None:
        casualties_index = preload_casualties_index()
    if not casualties_index:
        return units_list
    for unit in units_list:
        if (unit.get("faction") or "").upper() != "UA":
            continue
        display_name = (unit.get("display_name") or "").strip().lower()
        unit_id = (unit.get("unit_id") or "").replace("UA_", "").replace("_", " ").lower()
        best_count = 0
        for unit_key, cas_count in casualties_index.items():
            name_match = display_name and (display_name in unit_key or unit_key in display_name)
            id_match = unit_id and (unit_id in unit_key or unit_key in unit_id)
            if (name_match or id_match) and cas_count > best_count:
                best_count = cas_count
        if best_count > 0:
            unit["casualty_count"] = best_count
            unit["missing_count"] = best_count
    return units_list


def export_equipment_losses() -> None:
    """Write verified equipment-loss records from the entity database."""
    if not IMPACT_ATLAS_DB_PATH.exists():
        return
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(IMPACT_ATLAS_DB_PATH))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT event_id, source, date, raw_data FROM kinetic_events "
            "WHERE source IN ('Oryx', 'LostArmour_fpv', 'LostArmour_lancet')"
        ).fetchall()
        losses: list[dict[str, Any]] = []
        for row in rows:
            try:
                data = _fast_json_loads(row["raw_data"])
                source = row["source"]
                if source == "Oryx":
                    loss = {
                        "date": row["date"] or "Unknown",
                        "model": data.get("entry", "Unknown"),
                        "type": data.get("category", "Vehicle"),
                        "country": "RUS",
                        "status": data.get("status", "Verified Loss"),
                        "proof_url": "https://www.oryxspioenkop.com/2022/02/attack-on-europe-documenting-equipment.html",
                        "source_tag": "Oryx",
                    }
                else:
                    weapon_type = "Lancet" if "lancet" in source.lower() else "FPV Drone"
                    loss = {
                        "date": row["date"] or "Unknown",
                        "model": weapon_type,
                        "type": f"Precision Strike ({data.get('tag', weapon_type)})",
                        "country": "UA",
                        "status": "Verified Strike",
                        "proof_url": data.get("source_url", "https://lostarmour.info"),
                        "source_tag": "LostArmour",
                        "description": data.get("description", ""),
                    }
                losses.append(loss)
            except (ValueError, TypeError, AttributeError):
                continue
        losses.sort(key=lambda x: x.get("date", ""), reverse=True)
        with open(EXTERNAL_LOSSES_PATH, "w", encoding="utf-8") as handle:
            json.dump(losses, handle, indent=2, ensure_ascii=False)
    except (sqlite3.Error, OSError) as error:
        logger.error("Failed to export equipment losses: %s", error)
    finally:
        if conn is not None:
            conn.close()


def export_units(
    unit_stats: dict[str, dict[str, Any]] | None = None,
    orbat_data: Sequence[dict[str, Any]] | None = None,
    casualties_index: Mapping[str, int] | None = None,
) -> None:
    """Write ``units.json`` with ORBAT enrichment and dossier statistics."""
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='units_registry'")
        if not cursor.fetchone():
            conn.close()
            conn = None
            return
        cursor.execute("SELECT * FROM units_registry ORDER BY last_seen_date DESC")
        rows = cursor.fetchall()
        units: list[dict[str, Any]] = []
        for row in rows:
            u = dict(row)
            if u.get("last_seen_date"):
                u["last_seen_date"] = str(u["last_seen_date"])
            if not u.get("display_name"):
                u["display_name"] = u.get("unit_id") or "Unknown Unit"
            matches = enrich_units([u], orbat_data or [])
            u = matches[0] if matches else u
            if unit_stats:
                key = str(u.get("orbat_id") or u.get("unit_id") or u.get("unit_name") or "").lower()
                stats = unit_stats.get(key)
                if stats:
                    u["engagement_count"] = stats["engagement_count"]
                    u["last_active"] = stats["last_active"]
                    sorted_tactics = sorted(stats["tactics_hist"].items(), key=lambda x: x[1], reverse=True)
                    u["primary_tactic"] = sorted_tactics[0][0] if sorted_tactics else "UNKNOWN"
                    dossier = _build_dossier_fields(stats)
                    for k in [
                        "avg_tie", "assets_detected", "engagement_trend_30d",
                        "engagement_trend_anchor", "engagement_freq_label", "recent_engagements",
                    ]:
                        u[k] = dossier[k]
                else:
                    u["engagement_count"] = 0
                    u["avg_tie"] = {"kinetic": 0, "target": 0, "effect": 0}
                    u["assets_detected"] = []
                    u["engagement_trend_30d"] = [0] * 30
                    u["engagement_freq_label"] = "Low"
                    u["recent_engagements"] = []
            units.append(u)
        if orbat_data:
            for ob in orbat_data:
                if not ob.get("_used"):
                    new_u = {
                        "unit_id": ob.get("orbat_id") or ob.get("unit_name"),
                        "display_name": ob.get("unit_name"),
                        "faction": ob.get("faction"),
                        "type": ob.get("type") or "UNKNOWN",
                        "echelon": ob.get("echelon"),
                        "branch": ob.get("branch"),
                        "sub_branch": ob.get("sub_branch"),
                        "garrison": ob.get("garrison"),
                        "district": ob.get("district"),
                        "commander": ob.get("commander"),
                        "superior": ob.get("superior"),
                        "last_seen_lat": ob.get("lat"),
                        "last_seen_lon": ob.get("lon"),
                        "last_seen_date": ob.get("updated_at"),
                        "status": "ACTIVE",
                        "source": "PARABELLUM",
                        "engagement_count": 0,
                        "avg_tie": 0,
                    }
                    if new_u["last_seen_lat"] and new_u["last_seen_lon"]:
                        units.append(new_u)
        units = enrich_units_with_casualties(units, casualties_index)
        units = sanitize_public_object(units)
        with open(UNITS_JSON_PATH, "w", encoding="utf-8") as handle:
            json.dump(units, handle, indent=2, ensure_ascii=False)
    except (sqlite3.Error, OSError) as error:
        logger.error("Failed to export units: %s", error)
    finally:
        if conn is not None:
            conn.close()


# =============================================================================
# STRATEGIC TRENDS
# =============================================================================
def generate_strategic_trends(features: Sequence[dict[str, Any]]) -> None:
    """Aggregate daily T.I.E. pressure per strategic sector to disk."""
    from collections import defaultdict

    daily_sectors: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for feature in features:
        props = feature.get("properties", {})
        date_str = props.get("date", "")
        if not date_str or date_str == "Unknown":
            continue
        if "/" in date_str:
            parts = date_str.split("/")
            if len(parts) == 3:
                date_str = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        date_str = date_str[:10]
        coords = feature.get("geometry", {}).get("coordinates", [])
        lon = coords[0] if len(coords) > 0 else None
        lat = coords[1] if len(coords) > 1 else None
        target_type, tie_score = props.get("target_type", ""), props.get("tie_total", 0)
        if not tie_score or tie_score == 0:
            intensity = props.get("intensity_score", 0) or props.get("vec_k", 0)
            reliability = props.get("reliability", 50)
            try:
                tie_score = float(intensity) * float(reliability) / 10
            except (TypeError, ValueError):
                tie_score = 0
        sector = classify_sector(lat, lon, target_type)
        daily_sectors[date_str][sector] += float(tie_score)

    sorted_dates = sorted(daily_sectors.keys())
    sectors = ["ENERGY_COERCION", "DEEP_STRIKES_RU", "EASTERN_FRONT", "SOUTHERN_FRONT"]
    datasets: dict[str, list[float]] = {sector: [] for sector in sectors}
    for date in sorted_dates:
        for sector in sectors:
            datasets[sector].append(round(daily_sectors[date].get(sector, 0), 1))
    output = {"dates": sorted_dates, "datasets": datasets}
    with open(STRATEGIC_TRENDS_PATH, "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2, ensure_ascii=False)


# =============================================================================
# DEEP-LINK LOOKUP (PARALLEL)
# =============================================================================
def _fetch_tg_deeplinks(db_path: Path) -> dict[str, dict[str, str]]:
    """Fetch Telegram deep-links per cluster in a background-safe manner."""
    tg_deeplinks: dict[str, dict[str, str]] = {}
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT cluster_id, url
            FROM raw_signals
            WHERE url LIKE '%t.me/%/%' AND cluster_id IS NOT NULL
            """
        ).fetchall()
        for sig in rows:
            cid, url = sig["cluster_id"], sig["url"]
            if not cid or not url:
                continue
            try:
                channel = url.split("t.me/")[1].split("/")[0]
                tg_deeplinks.setdefault(cid, {})[channel] = url
            except IndexError:
                continue
    except sqlite3.Error as error:
        logger.warning("Could not build deep-link lookup: %s", error)
    finally:
        if conn is not None:
            conn.close()
    return tg_deeplinks


# =============================================================================
# MAIN PIPELINE
# =============================================================================
def _dedupe_sources(combined_sources: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    """Deduplicate structured sources, preferring concrete URLs over ``#``."""
    seen: dict[str, dict[str, str]] = {}
    structured_sources: list[dict[str, str]] = []
    for source in combined_sources:
        key = source["name"].lower()
        if key not in seen:
            seen[key] = source
            structured_sources.append(source)
        else:
            existing = seen[key]
            needs_url = (
                existing["url"] == "#"
                or ("t.me/" in existing["url"] and "/" not in existing["url"].split("t.me/")[-1])
            )
            if needs_url and source["url"] != "#":
                existing["url"] = source["url"]
    return structured_sources


def _inject_deeplinks(
    structured_sources: list[dict[str, str]],
    event_deeplinks: Mapping[str, str],
) -> None:
    """Replace channel-root URLs with concrete message deep-links."""
    for source in structured_sources:
        url = source.get("url", "")
        if "t.me/" in url:
            try:
                channel = url.split("t.me/")[1].split("/")[0]
            except IndexError:
                continue
            if channel in event_deeplinks and "/" not in url.split("t.me/" + channel)[-1]:
                source["url"] = event_deeplinks[channel]


def _resolve_event_coordinates(
    ai_data: Mapping[str, Any],
) -> tuple[Any, Any]:
    """Recover explicit or inferred coordinates from the AI report."""
    tactics = ai_data.get("tactics") or {}
    geo = (tactics.get("geo_location") or {}).get("explicit") or {}
    lat, lon = geo.get("lat"), geo.get("lon")
    if not lat or not lon:
        inferred = (tactics.get("geo_location") or {}).get("inferred") or {}
        lat, lon = inferred.get("lat"), inferred.get("lon")
    return lat, lon


def _recover_text_fallbacks(
    row: Mapping[str, Any],
    ai_data: Mapping[str, Any],
    title: str,
    description: str,
) -> tuple[str, str]:
    """Fill missing title/description from AI report editorial fields."""
    try:
        tactics = ai_data.get("tactics") or {}
        if not title:
            title = (ai_data.get("editorial") or {}).get("title_en", "")
        if not description:
            description = (ai_data.get("editorial") or {}).get("description_en", "")
        if not description:
            description = (tactics.get("event_analysis") or {}).get("summary_en", "")
        if not description:
            description = (ai_data.get("strategy") or {}).get("strategic_value_assessment", "")
        if not description:
            ai_summary = row.get("ai_summary") or ""
            if ai_summary:
                description = ai_summary.split("[IT]")[0].replace("[EN]", "").strip()[:300]
    except (KeyError, AttributeError, TypeError):
        pass
    return title, description


def _build_visual_analysis(
    ai_data: Mapping[str, Any], event_id: str
) -> list[dict[str, Any]]:
    """Extract IMINT frame analyses, persisting base64 frames to disk."""
    visual_analysis: list[dict[str, Any]] = []
    if not ai_data:
        return visual_analysis
    visionary_report = (ai_data.get("tactics") or {}).get("visionary_report") or {}
    if not isinstance(visionary_report, dict):
        return visual_analysis
    analyzed_frames = visionary_report.get("analyzed_frames") or visionary_report.get("per_frame_analysis") or []
    v_status = (visionary_report.get("visual_confirmation") or {}).get("verification_status", "")
    for af in analyzed_frames:
        if not isinstance(af, dict):
            continue
        frame_id = af.get("frame_id", 0)
        b64 = af.get("base64_data", "")
        img_path = save_media_frame(event_id, frame_id, b64) if b64 else ""
        visual_analysis.append({
            "frame_id": frame_id,
            "confidence": af.get("confidence", 0),
            "selection_reason": af.get("selection_reason", ""),
            "explanation": af.get("explanation", ""),
            "base64_data": img_path,
            "verification_status": v_status,
        })
    return visual_analysis


def main() -> int:
    """Run the full export pipeline and return a process exit code."""
    import time as _time

    with contextlib.suppress(AttributeError, OSError):
        sys.stdout.reconfigure(encoding="utf-8")

    configure_logging("generate_output", PATHS.logs / "generate_output.log")
    load_dotenv(PATHS.root / ".env", override=False)

    _t0 = _time.perf_counter()
    logger.info("Connecting to database %s", DB_PATH)

    if not DB_PATH.exists():
        logger.error("Database not found: %s", DB_PATH)
        return 1

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    ensure_campaign_columns(conn)
    ensure_sources_reputation_schema(conn)
    apply_reputation_decay(conn)

    orbat_data = load_orbat_data()
    orbat_index = build_orbat_index(orbat_data)
    logger.info("Loaded %d ORBAT units for enrichment (indexed).", len(orbat_data))

    casualties_index = preload_casualties_index()
    logger.info("Pre-loaded %d casualty unit entries.", len(casualties_index))

    campaign_definitions = load_campaign_definitions(
        sheet_url=os.getenv("SHEET_CSV_URL", ""),
        cache_path=str(CAMPAIGN_DEFINITIONS_CACHE_PATH),
        tab_name="campaign_definitions",
    )
    campaign_index = {c.get("campaign_id"): c for c in campaign_definitions}
    logger.info("Loaded %d campaign definitions.", len(campaign_definitions))

    _t_db = _time.perf_counter()
    with ThreadPoolExecutor(max_workers=1) as tg_pool:
        tg_future = tg_pool.submit(_fetch_tg_deeplinks, DB_PATH)
        cursor.execute(
            """
            SELECT
                event_id, last_seen_date, title, description, tie_score, tie_status,
                kinetic_score, target_score, effect_score, reliability, bias_score,
                ai_summary, has_video, urls_list, sources_list, ai_report_json,
                operational_sector, image_phash, source_reputation_score,
                ai_analysis_status, campaign_id, campaign_match_meta, campaign_tagged_at
            FROM unique_events
            WHERE ai_analysis_status = 'COMPLETED'
            """
        )
        rows = cursor.fetchall()
        tg_deeplinks = tg_future.result()

    logger.info(
        "Found %d completed events, %d TG deep-links (DB: %.0fms)",
        len(rows), len(tg_deeplinks), (_time.perf_counter() - _t_db) * 1000,
    )

    unit_stats_acc: dict[str, dict[str, Any]] = {}
    geojson_features: list[dict[str, Any]] = []
    csv_rows: list[dict[str, Any]] = []
    export_now = _dt.datetime.now(_dt.UTC)
    generated_at = export_now.isoformat(timespec="seconds").replace("+00:00", "Z")
    opsec_withheld_count = 0
    csv_headers = [
        "ID", "Date", "Title", "Lat", "Lon", "TIE", "K", "T", "E",
        "Reliability", "Bias", "HasVideo", "Sources",
    ]

    _t_loop = _time.perf_counter()
    for db_row in rows:
        try:
            row = dict(db_row)
            ai_data = (
                _fast_json_loads(row["ai_report_json"]) if row.get("ai_report_json") else {}
            )
            if not isinstance(ai_data, dict):
                ai_data = {}

            event_id = row["event_id"]
            date = row["last_seen_date"]
            if not date or str(date).lower() in ["none", "nat", "null", ""]:
                if ai_data:
                    date = (ai_data.get("timestamp_generated") or "")[:10]
                if not date:
                    date = "Unknown"

            title = row.get("title") or ""
            description = row.get("description") or ""
            tie_score = _safe_float(row.get("tie_score"))
            k_score = _safe_float(row.get("kinetic_score"))
            t_score = _safe_float(row.get("target_score"))
            e_score = _safe_float(row.get("effect_score"))
            reliability = _safe_int(row.get("reliability"))
            bias_score = _safe_float(row.get("bias_score"))
            ai_summary = row.get("ai_summary") or ""
            has_video = bool(row.get("has_video"))

            all_url_strs = []
            if row.get("urls_list"):
                all_url_strs.append(row["urls_list"])
            if row.get("sources_list"):
                all_url_strs.append(row["sources_list"])

            combined_sources: list[dict[str, str]] = []
            for src_str in all_url_strs:
                combined_sources.extend(parse_sources_to_list(src_str))
            structured_sources = _dedupe_sources(combined_sources)
            _inject_deeplinks(structured_sources, tg_deeplinks.get(event_id, {}))

            lat, lon = _resolve_event_coordinates(ai_data) if ai_data else (None, None)
            title, description = _recover_text_fallbacks(row, ai_data, title, description)
            visual_analysis = _build_visual_analysis(ai_data, event_id)

            classification = extract_classification(ai_data)
            faction = extract_faction(ai_data, f"{title} {description}")
            category_hint = ai_data.get("classification") or ""

            if not _should_publish_event(date, classification, category_hint, title, description, export_now):
                opsec_withheld_count += 1
                continue

            # Geo jitter fallback for IMINT-only events without coordinates.
            if not lat or not lon or float(lat) == 0 or float(lon) == 0:
                if visual_analysis:
                    h1 = int(hashlib.md5(event_id.encode("utf-8"), usedforsecurity=False).hexdigest()[:8], 16)  # noqa: S324
                    h2 = int(hashlib.md5(event_id.encode("utf-8")[::-1], usedforsecurity=False).hexdigest()[:8], 16)  # noqa: S324
                    lat = 48.3 + (h1 % 1000) / 400.0
                    lon = 31.1 + (h2 % 1000) / 400.0
                else:
                    continue

            radius, color = get_marker_style(tie_score, e_score)
            raw_units = (ai_data.get("tactics") or {}).get("military_units_detected", []) if ai_data else []
            enriched_units = enrich_units(raw_units, orbat_data, orbat_index)

            campaign_id = (row.get("campaign_id") or "").strip().lower() or None
            campaign_info = campaign_index.get(campaign_id) if campaign_id else None

            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
                "properties": {
                    "id": event_id,
                    "title": title,
                    "description": description,
                    "date": date,
                    "timestamp": _date_to_epoch_ms(date),
                    "tie_total": round(tie_score, 1),
                    "vec_k": k_score,
                    "vec_t": t_score,
                    "vec_e": e_score,
                    "reliability": reliability,
                    "bias_score": bias_score,
                    "classification": classification,
                    "target_type": ai_data.get("target_type", "UNKNOWN"),
                    "faction": faction,
                    "ai_reasoning": ai_summary,
                    "has_video": has_video,
                    "sources_list": json.dumps(structured_sources),
                    "source_reputation_score": row.get("source_reputation_score", 50),
                    "image_phash": row.get("image_phash") or "",
                    "units": json.dumps(enriched_units),
                    "visual_analysis": json.dumps(visual_analysis) if visual_analysis else "",
                    "marker_radius": radius,
                    "marker_color": color,
                    "operational_sector": row.get("operational_sector", "UNKNOWN_SECTOR"),
                    "campaign_id": campaign_id,
                    "campaign_name": campaign_info.get("name") if campaign_info else None,
                    "campaign_color": campaign_info.get("color") if campaign_info else None,
                },
            }
            geojson_features.append(sanitize_public_feature(feature))

            first_url = structured_sources[0].get("url", "") if structured_sources else ""
            detected_assets_raw = (
                (ai_data.get("tactics") or {}).get("visionary_report", {}).get("detected_assets", [])
                if ai_data else []
            )
            for u in enriched_units:
                update_unit_stats(
                    unit_stats_acc,
                    u,
                    {
                        "date": date,
                        "event_id": event_id,
                        "tie_score": tie_score,
                        "kinetic_score": k_score,
                        "target_score": t_score,
                        "effect_score": e_score,
                        "classification": classification,
                        "title": title,
                        "description": description,
                        "location": row.get("operational_sector", ""),
                        "lat": lat,
                        "lon": lon,
                        "url": first_url,
                        "detected_assets": detected_assets_raw,
                    },
                )

            csv_rows.append({
                "ID": event_id,
                "Date": date,
                "Title": title[:50],
                "Lat": lat,
                "Lon": lon,
                "TIE": round(tie_score, 1),
                "K": k_score,
                "T": t_score,
                "E": e_score,
                "Reliability": reliability,
                "Bias": bias_score,
                "HasVideo": 1 if has_video else 0,
                "Sources": len(structured_sources),
            })
        except Exception as error:
            event_id = db_row["event_id"] if "event_id" in db_row.keys() else "UNKNOWN"  # noqa: SIM118
            logger.error("Error processing %s: %s", event_id, error)
            continue

    conn.close()
    logger.info(
        "Event loop: %.0fms (%d features, %d withheld)",
        (_time.perf_counter() - _t_loop) * 1000, len(geojson_features), opsec_withheld_count,
    )

    _t_out = _time.perf_counter()
    PATHS.assets_data.mkdir(parents=True, exist_ok=True)

    public_payload = build_public_payload(geojson_features, generated_at, opsec_withheld_count)
    with open(GEOJSON_PATH, "w", encoding="utf-8") as handle:
        json.dump(public_payload, handle, indent=2, ensure_ascii=False)

    latest_features = build_latest_features(geojson_features)
    latest_payload = build_public_payload(latest_features, generated_at, opsec_withheld_count)
    with open(EVENTS_LATEST_PATH, "w", encoding="utf-8") as handle:
        json.dump(latest_payload, handle, indent=2, ensure_ascii=False)

    anomalies = compute_sector_volume_anomalies(geojson_features, lookback_days=14)
    geojson_features = apply_anomaly_flags(geojson_features, anomalies)
    write_json(SECTOR_ANOMALIES_PATH, {
        "generated_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
        "anomalies": list(anomalies.values()),
    })
    write_json(ASYMMETRY_INDEX_PATH, compute_asymmetry_index(geojson_features))
    write_json(GLOCS_PATH, build_glocs_geojson(geojson_features))

    build_campaigns_geo(geojson_features, campaign_definitions, str(CAMPAIGNS_GEO_PATH))
    build_campaign_reports(
        geojson_features,
        campaign_definitions,
        str(CAMPAIGN_REPORTS_PATH),
        sparkline_days=30,
        weekly_window_days=7,
    )
    generate_strategic_trends(geojson_features)

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_headers)
        writer.writeheader()
        writer.writerows(csv_rows)

    export_units(unit_stats_acc, orbat_data, casualties_index)
    export_equipment_losses()
    logger.info("Output writes + analytics: %.0fms", (_time.perf_counter() - _t_out) * 1000)
    logger.info(
        "Export complete: %d events (%d latest, %d OPSEC-withheld) in %.0fms total.",
        len(geojson_features), len(latest_features), opsec_withheld_count,
        (_time.perf_counter() - _t0) * 1000,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
