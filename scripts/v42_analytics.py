"""V4.2 analytics layer: source reputation, sector anomalies, and asymmetry.

Pure functions (plus two schema/maintenance helpers) that derive analytic
layers from exported event features and maintain the source-reputation
ledger in the raw-events database.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import logging
import math
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any
from collections.abc import Iterable, Mapping, Sequence
from urllib.parse import urlparse

logger = logging.getLogger("v42_analytics")

INSTITUTIONAL_DOMAINS = {
    "isw.pub", "mod.gov.ua", "mil.gov.ua", "defence-ua.com",
    "mod.mil.ru", "government.ru", "nato.int", "europa.eu",
    "osce.org", "un.org",
}


def _utcnow_naive() -> dt.datetime:
    """Return the current UTC time as a naive datetime (legacy DB semantics)."""
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def parse_event_datetime(date_str: Any) -> dt.datetime:
    """Parse an event date to a naive UTC datetime, defaulting to now."""
    if not date_str:
        return _utcnow_naive()
    value = str(date_str).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = dt.datetime.strptime(value[: len(fmt) + 5], fmt)
            if parsed.tzinfo:
                parsed = parsed.astimezone(dt.UTC).replace(tzinfo=None)
            return parsed
        except ValueError:
            continue
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return _utcnow_naive()


def normalize_domain(value: Any) -> str:
    """Normalize a URL or bare string to a lowercase registrable domain."""
    if not value:
        return ""
    item = str(value).strip()
    if not item:
        return ""
    try:
        if item.startswith("http://") or item.startswith("https://"):
            netloc = urlparse(item).netloc
        else:
            netloc = urlparse("https://" + item).netloc
    except ValueError:
        netloc = item
    return netloc.lower().replace("www.", "").split("/")[0].split("?")[0]


def domains_from_structured_sources(
    structured_sources: Iterable[Any] | None,
) -> list[str]:
    """Extract a sorted list of unique domains from structured source entries."""
    domains: set[str] = set()
    for source in structured_sources or []:
        if isinstance(source, dict):
            domain = normalize_domain(source.get("url") or source.get("name"))
        else:
            domain = normalize_domain(source)
        if domain:
            domains.add(domain)
    return sorted(domains)


def ensure_sources_reputation_schema(conn: sqlite3.Connection) -> None:
    """Create the reputation ledger and event score column when missing."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sources_reputation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT UNIQUE,
            score INTEGER DEFAULT 50,
            last_verified TEXT
        )
        """
    )
    with contextlib.suppress(sqlite3.OperationalError):
        conn.execute("ALTER TABLE unique_events ADD COLUMN source_reputation_score REAL")
    conn.commit()


def _decay_to_center(
    score: Any, last_verified: Any, current_dt: dt.datetime
) -> int:
    """Pull a reputation score one step toward the neutral 50 baseline."""
    base = int(score or 50)
    if not last_verified:
        return base
    try:
        previous = parse_event_datetime(last_verified)
        steps = max(0, (current_dt.date() - previous.date()).days // 15)
        if steps == 0:
            return base
        if base > 50:
            return max(50, base - steps)
        if base < 50:
            return min(50, base + steps)
        return base
    except (ValueError, TypeError):
        return base


def apply_reputation_decay(
    conn: sqlite3.Connection, current_dt: dt.datetime | None = None
) -> None:
    """Decay every source reputation score toward 50 in a single transaction."""
    now_dt = current_dt or _utcnow_naive()
    cursor = conn.cursor()
    cursor.execute("SELECT id, score, last_verified FROM sources_reputation")
    rows = cursor.fetchall()
    updates: list[tuple[int, str, int]] = []
    for row_id, score, last_verified in rows:
        new_score = _decay_to_center(score, last_verified, now_dt)
        if int(score or 50) != int(new_score):
            updates.append((int(new_score), now_dt.isoformat(timespec="seconds"), row_id))
    if updates:
        cursor.executemany(
            "UPDATE sources_reputation SET score = ?, last_verified = ? WHERE id = ?",
            updates,
        )
        conn.commit()
        logger.info("Decayed %d source reputation scores.", len(updates))


def update_event_reputation(
    conn: sqlite3.Connection,
    event_id: str,
    domains: Sequence[str],
    event_dt: dt.datetime | None = None,
    discrepancy: bool = False,
    hash_duplicate: bool = False,
    institutional: bool = False,
) -> float:
    """Fold event signals into the reputation ledger and score the event.

    Returns the event's new source-reputation score (the minimum across its
    domains, or 50 when no domain scored).
    """
    cursor = conn.cursor()
    event_dt = event_dt or _utcnow_naive()

    delta = 0
    if discrepancy or hash_duplicate:
        delta -= 10
    if institutional:
        delta += 2

    scores: list[int] = []
    for domain in domains:
        if not domain:
            continue
        cursor.execute("SELECT score, last_verified FROM sources_reputation WHERE domain = ?", (domain,))
        row = cursor.fetchone()
        if row:
            decayed = _decay_to_center(row[0], row[1], event_dt)
            new_score = max(0, min(100, int(decayed + delta)))
            cursor.execute(
                "UPDATE sources_reputation SET score = ?, last_verified = ? WHERE domain = ?",
                (new_score, event_dt.isoformat(timespec="seconds"), domain),
            )
            scores.append(new_score)
        else:
            new_score = max(0, min(100, int(50 + delta)))
            cursor.execute(
                "INSERT INTO sources_reputation(domain, score, last_verified) VALUES (?, ?, ?)",
                (domain, new_score, event_dt.isoformat(timespec="seconds")),
            )
            scores.append(new_score)

    event_score = min(scores) if scores else 50
    cursor.execute(
        "UPDATE unique_events SET source_reputation_score = ? WHERE event_id = ?",
        (event_score, event_id),
    )
    conn.commit()
    return float(event_score)


def extract_classification(ai_data: Any) -> str:
    """Extract the event classification from the AI report's nested fields."""
    if not isinstance(ai_data, dict):
        return "UNKNOWN"
    tactics = ai_data.get("tactics", {}) if isinstance(ai_data.get("tactics"), dict) else {}
    strategy = ai_data.get("strategy", {}) if isinstance(ai_data.get("strategy"), dict) else {}
    event_analysis = (
        tactics.get("event_analysis", {}) if isinstance(tactics.get("event_analysis"), dict) else {}
    )
    candidates = [
        ai_data.get("classification"),
        event_analysis.get("classification"),
        strategy.get("event_category"),
        tactics.get("event_category"),
    ]
    for candidate in candidates:
        if candidate and isinstance(candidate, str):
            return candidate.strip().upper()
    return "UNKNOWN"


def extract_faction(ai_data: Any, fallback_text: str = "") -> str:
    """Infer the aggressor faction (RU/UA) from the report or fallback text."""
    if isinstance(ai_data, dict):
        tactics = ai_data.get("tactics", {}) if isinstance(ai_data.get("tactics"), dict) else {}
        strategy = ai_data.get("strategy", {}) if isinstance(ai_data.get("strategy"), dict) else {}
        actors = tactics.get("actors", strategy.get("actors", {}))
        if isinstance(actors, dict):
            aggressor = actors.get("aggressor", {}) if isinstance(actors.get("aggressor"), dict) else {}
            side = str(aggressor.get("side") or "").upper()
            if "RUS" in side or "RU" in side:
                return "RU"
            if "UKR" in side or "UA" in side:
                return "UA"

    text = (fallback_text or "").upper()
    ru_hits = sum(keyword in text for keyword in ["RUSSIA", "RUSSIAN", "MOSCOW", "KREMLIN"])
    ua_hits = sum(keyword in text for keyword in ["UKRAINE", "UKRAINIAN", "KYIV", "AFU", "ZSU"])
    if ru_hits > ua_hits:
        return "RU"
    if ua_hits > ru_hits:
        return "UA"
    return "UNK"


def compute_sector_volume_anomalies(
    features: Sequence[Mapping[str, Any]], lookback_days: int = 14
) -> dict[str, dict[str, Any]]:
    """Flag sectors whose latest-day volume exceeds mean + 2 sigma."""
    per_sector_per_day: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    all_dates: set[str] = set()

    for feature in features:
        props = feature.get("properties", {})
        sector = props.get("operational_sector") or "UNKNOWN_SECTOR"
        date_str = str(props.get("date") or "")[:10]
        if len(date_str) != 10:
            continue
        per_sector_per_day[sector][date_str] += 1
        all_dates.add(date_str)

    if not all_dates:
        return {}

    latest_date = max(all_dates)
    latest_dt = dt.datetime.strptime(latest_date, "%Y-%m-%d").date()

    anomalies: dict[str, dict[str, Any]] = {}
    for sector, by_day in per_sector_per_day.items():
        current_count = by_day.get(latest_date, 0)
        history: list[int] = []
        for i in range(1, lookback_days + 1):
            day = (latest_dt - dt.timedelta(days=i)).strftime("%Y-%m-%d")
            history.append(by_day.get(day, 0))

        if not history:
            continue

        mean = sum(history) / len(history)
        variance = sum((x - mean) ** 2 for x in history) / len(history)
        std = math.sqrt(variance)
        threshold = mean + (2 * std)

        if current_count > threshold and current_count > 0:
            anomalies[sector] = {
                "sector": sector,
                "date": latest_date,
                "current_volume": current_count,
                "mean_14d": round(mean, 2),
                "std_14d": round(std, 2),
                "threshold": round(threshold, 2),
            }

    return anomalies


def apply_anomaly_flags(
    features: Sequence[dict[str, Any]], anomalies: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Tag each feature with ``is_anomaly_sector`` in place."""
    anomaly_set = set(anomalies.keys())
    for feature in features:
        props = feature.get("properties", {})
        sector = props.get("operational_sector") or "UNKNOWN_SECTOR"
        props["is_anomaly_sector"] = sector in anomaly_set
    return list(features)


def compute_asymmetry_index(
    features: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Compute the per-sector/faction asymmetry index.

    Asymmetry Index = sum(K * E) / sum(max(T, 0)) aggregated by sector and
    faction, with a global roll-up.
    """
    accum: dict[str, dict[str, dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: {"num": 0.0, "den": 0.0, "events": 0})
    )

    for feature in features:
        props = feature.get("properties", {})
        sector = props.get("operational_sector") or "UNKNOWN_SECTOR"
        faction = props.get("faction") or "UNK"
        try:
            k = float(props.get("vec_k") or 0)
            e = float(props.get("vec_e") or 0)
            t = float(props.get("vec_t") or 0)
        except (TypeError, ValueError):
            continue

        accum[sector][faction]["num"] += k * e
        accum[sector][faction]["den"] += max(t, 0)
        accum[sector][faction]["events"] += 1

    out: dict[str, Any] = {"sectors": {}, "global": {}}
    global_num: dict[str, float] = defaultdict(float)
    global_den: dict[str, float] = defaultdict(float)

    for sector, by_faction in accum.items():
        out["sectors"][sector] = {}
        for faction, vals in by_faction.items():
            den = vals["den"]
            idx = (vals["num"] / den) if den > 0 else 0.0
            out["sectors"][sector][faction] = {
                "asymmetry_index": round(idx, 4),
                "events": vals["events"],
                "numerator": round(vals["num"], 4),
                "denominator": round(den, 4),
            }
            global_num[faction] += vals["num"]
            global_den[faction] += den

    for faction in sorted(global_num.keys()):
        den = global_den[faction]
        out["global"][faction] = round((global_num[faction] / den), 4) if den > 0 else 0.0

    return out


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in kilometers."""
    radius = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def build_glocs_geojson(
    features: Sequence[Mapping[str, Any]],
    max_km: float = 40.0,
    max_hours: float = 24.0,
) -> dict[str, Any]:
    """Cluster LOGISTICS events into ground-lines-of-communication features."""
    logistics: list[dict[str, Any]] = []
    for feature in features:
        props = feature.get("properties", {})
        classification = str(props.get("classification") or "").upper()
        if classification != "LOGISTICS":
            continue
        coords = feature.get("geometry", {}).get("coordinates", [])
        if len(coords) < 2:
            continue
        timestamp = props.get("timestamp") or 0
        if not timestamp:
            continue
        logistics.append({
            "id": props.get("id"),
            "sector": props.get("operational_sector") or "UNKNOWN_SECTOR",
            "lon": float(coords[0]),
            "lat": float(coords[1]),
            "timestamp": int(timestamp),
        })

    logistics.sort(key=lambda x: x["timestamp"])

    clusters: list[list[dict[str, Any]]] = []
    for event in logistics:
        placed = False
        for cluster in clusters:
            last = cluster[-1]
            hours = abs(event["timestamp"] - last["timestamp"]) / 3600000.0
            dist = _haversine_km(event["lat"], event["lon"], last["lat"], last["lon"])
            if hours <= max_hours and dist <= max_km:
                cluster.append(event)
                placed = True
                break
        if not placed:
            clusters.append([event])

    lines: list[dict[str, Any]] = []
    for idx, cluster in enumerate(clusters, start=1):
        if len(cluster) < 2:
            continue
        cluster_sorted = sorted(cluster, key=lambda x: x["timestamp"])
        coords = [[x["lon"], x["lat"]] for x in cluster_sorted]
        sectors = sorted({x["sector"] for x in cluster_sorted})
        lines.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {
                "id": f"gloc_{idx}",
                "event_count": len(cluster_sorted),
                "start_ts": cluster_sorted[0]["timestamp"],
                "end_ts": cluster_sorted[-1]["timestamp"],
                "sectors": sectors,
            },
        })

    return {"type": "FeatureCollection", "features": lines}


def write_json(path: str | Path, data: Any) -> None:
    """Write ``data`` as pretty-printed UTF-8 JSON to ``path``."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
