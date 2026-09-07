"""Campaign definition loading, event tagging, and campaign report builders.

Campaign definitions load through a three-tier fallback chain: the Google
Sheet CSV export, the local JSON cache, and finally the curated bootstrap
CSV. Events are tagged with campaigns via deterministic target-type and
keyword matching; reports and geo views are then derived from tagged
features.
"""

import csv
import io
import json
import logging
import os
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, UTC
from typing import Any
from urllib.parse import quote

logger = logging.getLogger("campaigns_engine")

try:
    import requests
except Exception:  # pragma: no cover
    requests = None

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None

DEFAULT_CAMPAIGN_COLOR = "#f59e0b"


def _now_utc() -> datetime:
    """Return the current UTC time as an aware datetime."""
    return datetime.now(UTC)


def _normalize_text(value: Any) -> str:
    """Lowercase, collapse whitespace, and strip a value to comparable text."""
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _normalize_hex_color(value: Any) -> str:
    """Normalize arbitrary color input to a canonical ``#rrggbb`` string."""
    raw = str(value or "").strip()
    if not raw:
        return DEFAULT_CAMPAIGN_COLOR

    if not raw.startswith("#"):
        raw = f"#{raw}"

    if re.fullmatch(r"#[0-9a-fA-F]{3}", raw):
        return "#" + "".join(ch * 2 for ch in raw[1:]).lower()

    if re.fullmatch(r"#[0-9a-fA-F]{6}", raw):
        return raw.lower()

    return DEFAULT_CAMPAIGN_COLOR


def _split_tokens(value: Any) -> list[str]:
    """Split a list/JSON/pipe-separated value into unique normalized tokens."""
    if value is None:
        return []

    if isinstance(value, list):
        items = value
    else:
        raw = str(value).strip()
        if not raw:
            return []
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                items = parsed if isinstance(parsed, list) else [raw]
            except json.JSONDecodeError:
                items = [raw]
        else:
            items = re.split(r"[|,;]", raw)

    out = []
    seen = set()
    for item in items:
        token = _normalize_text(item)
        if len(token) < 2:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _parse_event_date(value: Any) -> datetime | None:
    """Parse an event date to an aware UTC datetime, or ``None``."""
    if value is None:
        return None

    raw = str(value).strip()
    if not raw or raw.lower() in {"unknown", "none", "null", "nat"}:
        return None

    candidate = raw
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d/%m/%y",
        "%d-%m-%Y",
        "%Y/%m/%d",
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            continue

    return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce ``value`` to float with a fallback default."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_json_dump(path: str, payload: dict[str, Any]) -> None:
    """Write ``payload`` as pretty JSON, creating parent directories."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def build_campaign_sheet_csv_url(sheet_url: str, tab_name: str = "campaign_definitions") -> str:
    """Convert a Google Sheets URL into its CSV export URL, or ``'``."""
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", str(sheet_url or ""))
    if not match:
        return ""

    sheet_id = match.group(1)
    encoded_tab = quote(tab_name)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={encoded_tab}"


def normalize_campaign_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Validate and normalize raw sheet/CSV rows into campaign definitions."""
    campaigns: list[dict[str, Any]] = []

    for row in rows:
        lowered = {str(k or "").strip().lower(): row[k] for k in row}

        campaign_id = _normalize_text(lowered.get("campaign_id"))
        name = str(lowered.get("name") or "").strip()
        target_types = _split_tokens(lowered.get("target_types"))
        keywords = _split_tokens(lowered.get("keywords"))
        color = _normalize_hex_color(lowered.get("color"))

        if not campaign_id or not name:
            continue
        if not target_types or not keywords:
            continue

        campaigns.append(
            {
                "campaign_id": campaign_id,
                "name": name,
                "target_types": target_types,
                "keywords": keywords,
                "color": color,
            }
        )

    return campaigns


def load_campaign_definitions_from_csv(csv_path: str) -> list[dict[str, Any]]:
    """Load and normalize campaign definitions from a local CSV file."""
    if not csv_path or not os.path.exists(csv_path):
        return []
    try:
        with open(csv_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
        return normalize_campaign_rows(rows)
    except (OSError, csv.Error) as error:
        logger.warning("Failed to load campaign CSV %s: %s", csv_path, error)
        return []


def load_campaign_definitions(
    sheet_url: str,
    cache_path: str,
    tab_name: str = "campaign_definitions",
    timeout_seconds: int = 10,
) -> list[dict[str, Any]]:
    """Load campaign definitions using the sheet -> cache -> bootstrap chain."""
    campaigns: list[dict[str, Any]] = []

    csv_url = build_campaign_sheet_csv_url(sheet_url, tab_name=tab_name)
    if csv_url and requests is not None:
        try:
            response = requests.get(csv_url, timeout=timeout_seconds)
            response.raise_for_status()
            reader = csv.DictReader(io.StringIO(response.text))
            campaigns = normalize_campaign_rows(list(reader))
            if campaigns:
                try:
                    _safe_json_dump(
                        cache_path,
                        {"generated_at": _now_utc().isoformat(), "campaigns": campaigns},
                    )
                except OSError as error:
                    logger.warning("Failed to refresh campaign cache %s: %s", cache_path, error)
        except Exception as error:
            logger.warning("Campaign sheet fetch failed, falling back to cache: %s", error)
            campaigns = []

    if campaigns:
        return campaigns

    try:
        if os.path.exists(cache_path):
            with open(cache_path, encoding="utf-8") as handle:
                payload = json.load(handle)
            cached = payload.get("campaigns", payload if isinstance(payload, list) else [])
            if isinstance(cached, list) and cached:
                logger.info("Loaded %d campaign definitions from cache.", len(cached))
                return normalize_campaign_rows(cached)
    except (OSError, json.JSONDecodeError, AttributeError) as error:
        logger.warning("Failed to read campaign cache %s: %s", cache_path, error)

    # Fallback to bootstrap curated csv
    local_csv = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'bootstrap', 'campaign_definitions.curated.csv',
    )
    fallback = load_campaign_definitions_from_csv(local_csv)
    if fallback:
        logger.info("Loaded %d campaign definitions from bootstrap CSV.", len(fallback))
        return fallback

    logger.warning("No campaign definitions available from any source.")
    return []


def ensure_campaign_columns(conn: sqlite3.Connection) -> None:
    """Add the campaign tagging columns to ``unique_events`` when missing."""
    conn.execute("PRAGMA journal_mode=WAL;")
    for ddl in (
        "ALTER TABLE unique_events ADD COLUMN campaign_id TEXT",
        "ALTER TABLE unique_events ADD COLUMN campaign_match_meta TEXT",
        "ALTER TABLE unique_events ADD COLUMN campaign_tagged_at TEXT",
    ):
        try:
            conn.execute(ddl)
        except sqlite3.OperationalError:
            continue
    conn.commit()


def match_event_campaign(
    campaigns: list[dict[str, Any]],
    target_type: Any,
    event_text: Any,
) -> dict[str, Any] | None:
    """Return the best campaign match for an event, or ``None``.

    A campaign matches when the event's target type matches one of the
    campaign's target types and at least one campaign keyword appears in
    the event text. The best match maximizes keyword count, then length.
    """
    norm_target = _normalize_text(target_type)
    norm_text = _normalize_text(event_text)

    if not norm_target or not norm_text or not campaigns:
        return None

    best: dict[str, Any] | None = None
    best_score = -1

    for campaign in campaigns:
        target_hits = [
            t for t in campaign.get("target_types", [])
            if t == norm_target or t in norm_target or norm_target in t
        ]
        if not target_hits:
            continue

        keyword_hits = [kw for kw in campaign.get("keywords", []) if kw in norm_text]
        if not keyword_hits:
            continue

        score = len(keyword_hits) * 10 + max(len(k) for k in keyword_hits)
        if score <= best_score:
            continue

        best_score = score
        best = {
            "campaign_id": campaign["campaign_id"],
            "name": campaign["name"],
            "color": campaign["color"],
            "match_meta": {
                "target_type_input": norm_target,
                "matched_target_types": target_hits,
                "matched_keywords": keyword_hits,
                "score": score,
                "rule": "target_type_and_keyword",
            },
        }

    return best


def _campaign_status(last_event_dt: datetime | None, live_days: int = 30) -> str:
    """Classify a campaign as LIVE or STANDBY from its most recent event."""
    if not last_event_dt:
        return "STANDBY"
    return "LIVE" if (_now_utc() - last_event_dt) <= timedelta(days=live_days) else "STANDBY"


def _build_fallback_brief(name: str, weekly_tie: float, sum_vec_e: float, status: str, total_events: int) -> str:
    """Produce a deterministic brief used when the LLM is unavailable."""
    return (
        f"{name}: {status} posture with {total_events} tagged events. "
        f"Weekly T.I.E. cumulative is {weekly_tie:.1f} and cumulative E-vector impact is {sum_vec_e:.1f}."
    )


def _maybe_generate_llm_brief(
    campaign_name: str,
    status: str,
    total_events: int,
    weekly_tie_cumulative: float,
    sum_vec_e: float,
) -> str | None:
    """Generate an LLM strategic brief, or ``None`` when unavailable."""
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key or OpenAI is None:
        return None

    try:
        client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)
        prompt = (
            "Write a concise strategic brief (max 3 sentences) for a military campaign dashboard. "
            f"Campaign: {campaign_name}. Status: {status}. Tagged events: {total_events}. "
            f"Weekly cumulative TIE: {weekly_tie_cumulative:.1f}. Cumulative Effect vector: {sum_vec_e:.1f}. "
            "Focus on operational implication, not narrative storytelling."
        )
        response = client.chat.completions.create(
            model="deepseek/deepseek-v4-flash",
            messages=[
                {"role": "system", "content": "You are The Strategist, a strict military analyst."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=120,
        )
        content = (response.choices[0].message.content or "").strip()
        return content or None
    except Exception as error:
        logger.warning("LLM brief generation failed for '%s': %s", campaign_name, error)
        return None


def build_campaign_reports(
    features: list[dict[str, Any]],
    campaigns: list[dict[str, Any]],
    output_path: str,
    sparkline_days: int = 30,
    weekly_window_days: int = 7,
) -> dict[str, Any]:
    """Build per-campaign report payloads and write them to ``output_path``.

    Briefs are generated concurrently (LLM with deterministic fallback) and
    each report includes sparkline series, weekly T.I.E., and status.
    """
    now_utc = _now_utc()
    weekly_start = now_utc - timedelta(days=weekly_window_days)
    sparkline_start = now_utc - timedelta(days=sparkline_days - 1)

    sparkline_dates = [
        (sparkline_start + timedelta(days=idx)).date().isoformat()
        for idx in range(sparkline_days)
    ]

    by_campaign: dict[str, list[dict[str, Any]]] = {}
    for feature in features:
        props = feature.get("properties", {})
        campaign_id = _normalize_text(props.get("campaign_id"))
        if not campaign_id:
            continue
        by_campaign.setdefault(campaign_id, []).append(feature)

    # Collect all LLM tasks, then execute in parallel
    report_items = []
    llm_tasks = []
    for idx, campaign in enumerate(campaigns):
        cid = campaign["campaign_id"]
        entries = by_campaign.get(cid, [])

        parsed_rows = []
        for entry in entries:
            props = entry.get("properties", {})
            dt = _parse_event_date(props.get("date"))
            parsed_rows.append(
                {
                    "date": dt,
                    "vec_e": _safe_float(props.get("vec_e"), 0.0),
                    "tie_total": _safe_float(props.get("tie_total"), 0.0),
                }
            )

        last_event_dt = max((row["date"] for row in parsed_rows if row["date"]), default=None)
        status = _campaign_status(last_event_dt, live_days=30)
        sum_vec_e = sum(row["vec_e"] for row in parsed_rows)

        weekly_rows = [
            row for row in parsed_rows
            if row["date"] and row["date"] >= weekly_start
        ]
        weekly_tie_cumulative = sum(row["tie_total"] for row in weekly_rows)

        daily_vec_e_map = {date_key: 0.0 for date_key in sparkline_dates}
        for row in parsed_rows:
            if not row["date"]:
                continue
            day_key = row["date"].date().isoformat()
            if day_key in daily_vec_e_map:
                daily_vec_e_map[day_key] += row["vec_e"]

        sparkline_values = [round(daily_vec_e_map[d], 2) for d in sparkline_dates]

        report_items.append(
            {
                "campaign_id": cid,
                "name": campaign["name"],
                "color": campaign["color"],
                "status": status,
                "total_events": len(parsed_rows),
                "last_event_date": last_event_dt.isoformat() if last_event_dt else None,
                "sum_vec_e": round(sum_vec_e, 2),
                "weekly_tie_cumulative": round(weekly_tie_cumulative, 2),
                "sparkline_daily_vec_e": {
                    "dates": sparkline_dates,
                    "values": sparkline_values,
                },
                "brief_text": "",  # placeholder, filled by LLM or fallback
            }
        )

        llm_tasks.append((
            idx,
            campaign["name"],
            status,
            len(parsed_rows),
            weekly_tie_cumulative,
            sum_vec_e,
        ))

    # Execute LLM briefs in parallel (5 concurrent requests)
    def _generate_brief(task):
        idx, name, status, total_events, weekly_tie, sum_e = task
        brief = _maybe_generate_llm_brief(
            campaign_name=name,
            status=status,
            total_events=total_events,
            weekly_tie_cumulative=weekly_tie,
            sum_vec_e=sum_e,
        )
        if not brief:
            brief = _build_fallback_brief(
                name=name,
                weekly_tie=weekly_tie,
                sum_vec_e=sum_e,
                status=status,
                total_events=total_events,
            )
        return idx, brief

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [pool.submit(_generate_brief, task) for task in llm_tasks]
        for future in as_completed(futures):
            try:
                idx, brief = future.result()
                report_items[idx]["brief_text"] = brief
            except Exception as error:
                logger.warning("Campaign brief worker failed: %s", error)

    payload = {
        "generated_at": now_utc.isoformat(),
        "weekly_window_days": weekly_window_days,
        "sparkline_window_days": sparkline_days,
        "campaigns": report_items,
    }
    _safe_json_dump(output_path, payload)
    return payload


def build_campaigns_geo(
    features: list[dict[str, Any]],
    campaigns: list[dict[str, Any]],
    output_path: str,
) -> dict[str, Any]:
    """Group tagged event points per campaign and write the geo payload."""
    campaign_index = {c["campaign_id"]: c for c in campaigns}
    grouped: dict[str, dict[str, Any]] = {}

    for feature in features:
        props = feature.get("properties", {})
        geometry = feature.get("geometry", {})

        campaign_id = _normalize_text(props.get("campaign_id"))
        if not campaign_id:
            continue

        coords = geometry.get("coordinates", [])
        if not isinstance(coords, list) or len(coords) < 2:
            continue

        lon = _safe_float(coords[0], None)
        lat = _safe_float(coords[1], None)
        if lon is None or lat is None:
            continue

        dt = _parse_event_date(props.get("date"))

        if campaign_id not in grouped:
            meta = campaign_index.get(campaign_id, {})
            grouped[campaign_id] = {
                "campaign_id": campaign_id,
                "name": meta.get("name", campaign_id.upper()),
                "color": meta.get("color", DEFAULT_CAMPAIGN_COLOR),
                "points": [],
                "last_event_date": None,
            }

        grouped[campaign_id]["points"].append(
            {
                "event_id": props.get("id") or props.get("event_id"),
                "lat": lat,
                "lon": lon,
                "date": props.get("date"),
                "vec_e": round(_safe_float(props.get("vec_e"), 0.0), 2),
                "tie_total": round(_safe_float(props.get("tie_total"), 0.0), 2),
            }
        )

        current_last = grouped[campaign_id]["last_event_date"]
        if dt and (current_last is None or dt > current_last):
            grouped[campaign_id]["last_event_date"] = dt

    campaigns_payload = []
    for cid, item in grouped.items():
        last_dt = item.get("last_event_date")
        campaigns_payload.append(
            {
                "campaign_id": cid,
                "name": item["name"],
                "color": item["color"],
                "status": _campaign_status(last_dt, live_days=30),
                "total_events": len(item["points"]),
                "last_event_date": last_dt.isoformat() if last_dt else None,
                "points": item["points"],
            }
        )

    payload = {
        "generated_at": _now_utc().isoformat(),
        "campaigns": campaigns_payload,
    }
    _safe_json_dump(output_path, payload)
    return payload
