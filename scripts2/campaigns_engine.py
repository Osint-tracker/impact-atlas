import csv
import io
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

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
    return datetime.now(timezone.utc)


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _normalize_hex_color(value: Any) -> str:
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


def _split_tokens(value: Any) -> List[str]:
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
                if isinstance(parsed, list):
                    items = parsed
                else:
                    items = [raw]
            except Exception:
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


def _parse_event_date(value: Any) -> Optional[datetime]:
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
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
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
            dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_json_dump(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def build_campaign_sheet_csv_url(sheet_url: str, tab_name: str = "campaign_definitions") -> str:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", str(sheet_url or ""))
    if not match:
        return ""

    sheet_id = match.group(1)
    encoded_tab = quote(tab_name)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={encoded_tab}"


def normalize_campaign_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    campaigns: List[Dict[str, Any]] = []

    for row in rows:
        lowered = {str(k or "").strip().lower(): row[k] for k in row.keys()}

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


def load_campaign_definitions_from_csv(csv_path: str) -> List[Dict[str, Any]]:
    if not csv_path or not os.path.exists(csv_path):
        return []
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
        return normalize_campaign_rows(rows)
    except Exception:
        return []


def load_campaign_definitions(
    sheet_url: str,
    cache_path: str,
    tab_name: str = "campaign_definitions",
    timeout_seconds: int = 10,
) -> List[Dict[str, Any]]:
    campaigns: List[Dict[str, Any]] = []

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
                except Exception:
                    pass
        except Exception:
            campaigns = []

    if campaigns:
        return campaigns

    try:
        if os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            cached = payload.get("campaigns", payload if isinstance(payload, list) else [])
            if isinstance(cached, list):
                return normalize_campaign_rows(cached)
    except Exception:
        pass

    return []


def ensure_campaign_columns(conn: sqlite3.Connection) -> None:
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
    campaigns: List[Dict[str, Any]],
    target_type: Any,
    event_text: Any,
) -> Optional[Dict[str, Any]]:
    norm_target = _normalize_text(target_type)
    norm_text = _normalize_text(event_text)

    if not norm_target or not norm_text or not campaigns:
        return None

    best: Optional[Dict[str, Any]] = None
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


def _campaign_status(last_event_dt: Optional[datetime], live_days: int = 30) -> str:
    if not last_event_dt:
        return "STANDBY"
    return "LIVE" if (_now_utc() - last_event_dt) <= timedelta(days=live_days) else "STANDBY"


def _build_fallback_brief(name: str, weekly_tie: float, sum_vec_e: float, status: str, total_events: int) -> str:
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
) -> Optional[str]:
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
            model="deepseek/deepseek-chat",
            messages=[
                {"role": "system", "content": "You are The Strategist, a strict military analyst."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=120,
        )
        content = (response.choices[0].message.content or "").strip()
        return content or None
    except Exception:
        return None


def build_campaign_reports(
    features: List[Dict[str, Any]],
    campaigns: List[Dict[str, Any]],
    output_path: str,
    sparkline_days: int = 30,
    weekly_window_days: int = 7,
) -> Dict[str, Any]:
    now_utc = _now_utc()
    weekly_start = now_utc - timedelta(days=weekly_window_days)
    sparkline_start = now_utc - timedelta(days=sparkline_days - 1)

    sparkline_dates = [
        (sparkline_start + timedelta(days=idx)).date().isoformat()
        for idx in range(sparkline_days)
    ]

    by_campaign: Dict[str, List[Dict[str, Any]]] = {}
    for feature in features:
        props = feature.get("properties", {})
        campaign_id = _normalize_text(props.get("campaign_id"))
        if not campaign_id:
            continue
        by_campaign.setdefault(campaign_id, []).append(feature)

    report_items = []
    for campaign in campaigns:
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

        brief_text = _maybe_generate_llm_brief(
            campaign_name=campaign["name"],
            status=status,
            total_events=len(parsed_rows),
            weekly_tie_cumulative=weekly_tie_cumulative,
            sum_vec_e=sum_vec_e,
        )
        if not brief_text:
            brief_text = _build_fallback_brief(
                name=campaign["name"],
                weekly_tie=weekly_tie_cumulative,
                sum_vec_e=sum_vec_e,
                status=status,
                total_events=len(parsed_rows),
            )

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
                "brief_text": brief_text,
            }
        )

    payload = {
        "generated_at": now_utc.isoformat(),
        "weekly_window_days": weekly_window_days,
        "sparkline_window_days": sparkline_days,
        "campaigns": report_items,
    }
    _safe_json_dump(output_path, payload)
    return payload


def build_campaigns_geo(
    features: List[Dict[str, Any]],
    campaigns: List[Dict[str, Any]],
    output_path: str,
) -> Dict[str, Any]:
    campaign_index = {c["campaign_id"]: c for c in campaigns}
    grouped: Dict[str, Dict[str, Any]] = {}

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
