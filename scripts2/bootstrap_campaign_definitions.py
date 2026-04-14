"""
Bootstrap campaign_definitions generator.

Scans historical events in SQLite unique_events, extracts dominant target types,
and suggests campaign rows with keywords.

Outputs:
- CSV compatible with campaign_definitions sheet
- JSON diagnostics with richer stats
"""

import argparse
import csv
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "../war_tracker_v2/data/raw_events.db")
DEFAULT_CSV_PATH = os.path.join(BASE_DIR, "../bootstrap/campaign_definitions.bootstrap.csv")
DEFAULT_JSON_PATH = os.path.join(BASE_DIR, "../bootstrap/campaign_definitions.bootstrap.json")
DEFAULT_PUBLISH_CSV_PATH = os.path.join(BASE_DIR, "../bootstrap/campaign_definitions.csv")
DEFAULT_CACHE_JSON_PATH = os.path.join(BASE_DIR, "../bootstrap/campaign_definitions.cache.json")

PALETTE = [
    "#f59e0b", "#ef4444", "#22c55e", "#06b6d4", "#f97316",
    "#eab308", "#38bdf8", "#a3e635", "#fb7185", "#94a3b8",
]

STOPWORDS = {
    "the", "and", "for", "that", "with", "from", "this", "were", "have", "has", "had", "into", "over", "was", "are",
    "near", "after", "before", "while", "across", "under", "between", "against", "about", "than", "their",
    "event", "events", "reported", "report", "according", "sources", "source", "update", "latest", "new",
    "area", "region", "city", "village", "town", "district", "sector", "front", "frontline", "line",
    "russia", "russian", "ukraine", "ukrainian", "moscow", "kyiv", "kiev", "forces", "troops",
    "strike", "strikes", "attack", "attacks", "drone", "drones", "missile", "missiles", "artillery",
    "military", "operation", "operations", "target", "targets", "system", "systems", "facility", "facilities",
    "damage", "destroyed", "destruction", "impact", "high", "low", "medium", "critical",
    "content", "date", "broader", "aligns", "underscores", "potentially", "reports", "analysis",
    "assessment", "operational", "strategic", "tactical", "implication", "implies", "likely", "suggests",
    "indicates", "highlight", "highlights", "demonstrates", "show", "shows", "noted", "reportedly",
    "http", "https", "www", "com", "org", "net", "tme", "telegram", "twitter", "xcom", "channel",
    "nafo", "substack", "youtube", "instagram", "facebook", "reddit", "threads", "whatsapp",
    "january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december",
    "il", "lo", "la", "gli", "le", "di", "del", "della", "delle", "dello", "dei", "degli", "da", "in", "su", "per",
    "con", "tra", "fra", "un", "una", "uno", "sono", "stato", "stata", "stati", "state", "evento", "eventi",
}

UNKNOWN_TARGET_VALUES = {"", "unknown", "null", "none", "n/a", "na", "other"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate bootstrap campaign_definitions from historical events")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="Path to SQLite DB")
    parser.add_argument("--out-csv", default=DEFAULT_CSV_PATH, help="Output CSV path")
    parser.add_argument("--out-json", default=DEFAULT_JSON_PATH, help="Output JSON path")
    parser.add_argument("--publish-csv", default=DEFAULT_PUBLISH_CSV_PATH, help="Published CSV path used by pipeline")
    parser.add_argument("--cache-json", default=DEFAULT_CACHE_JSON_PATH, help="Published JSON cache used by pipeline")
    parser.add_argument("--top", type=int, default=0, help="Max number of suggested campaigns (0 = all)")
    parser.add_argument("--min-events", type=int, default=1, help="Minimum events per target_type")
    parser.add_argument("--limit", type=int, default=0, help="Optional SQL limit (0 = all completed events)")
    parser.add_argument("--keywords", type=int, default=8, help="Number of suggested keywords per campaign")
    return parser.parse_args()


def safe_json_load(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def parse_date(raw: Any) -> Optional[datetime]:
    if not raw:
        return None
    txt = str(raw).strip()
    if not txt or txt.lower() in {"nat", "none", "null", "unknown"}:
        return None
    if txt.endswith("Z"):
        txt = txt[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            dt = datetime.strptime(txt, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None


def normalize_target_type(value: Any) -> Optional[str]:
    if value is None:
        return None
    txt = str(value).strip().lower()
    txt = re.sub(r"[^a-z0-9_\-/ ]+", " ", txt)
    txt = txt.replace("-", "_").replace("/", "_")
    txt = re.sub(r"\s+", "_", txt)
    txt = re.sub(r"_+", "_", txt).strip("_")
    if txt in UNKNOWN_TARGET_VALUES:
        return None
    if len(txt) < 3:
        return None
    return txt


def pick_target_type(ai_data: Dict[str, Any]) -> Optional[str]:
    tactics = ai_data.get("tactics") or {}
    strategy = ai_data.get("strategy") or {}
    titan_metrics = ai_data.get("titan_metrics") or {}
    strategy_verified = strategy.get("verified_data") or {}

    candidates = [
        ai_data.get("target_type"),
        (titan_metrics.get("target_type_category")),
        ((ai_data.get("titan_assessment") or {}).get("target_type_category")),
        ((tactics.get("titan_assessment") or {}).get("target_type_category")),
        ((strategy.get("titan_assessment") or {}).get("target_type_category")),
        (strategy.get("target_type")),
        (strategy_verified.get("target_type")),
    ]
    for cand in candidates:
        normalized = normalize_target_type(cand)
        if normalized:
            return normalized
    return None


def tokenize(text: str) -> List[str]:
    tokens = re.findall(r"[a-zA-Z][a-zA-Z0-9-]{2,}", (text or "").lower())
    out: List[str] = []
    for t in tokens:
        t = t.strip("-_")
        if len(t) < 3:
            continue
        if t in STOPWORDS:
            continue
        if re.search(r"\d", t):
            continue
        if t.isdigit():
            continue
        out.append(t)
    return out


def palette_color(seed: str) -> str:
    idx = abs(hash(seed)) % len(PALETTE)
    return PALETTE[idx]


def slugify_campaign_id(target_type: str) -> str:
    base = target_type.lower().replace("__", "_").strip("_")
    base = re.sub(r"[^a-z0-9_]+", "_", base)
    base = re.sub(r"_+", "_", base).strip("_")
    if not base:
        base = "campaign"
    return base


def split_token_field(raw: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for token in re.split(r"[|,;]", str(raw or "")):
        cleaned = token.strip().lower()
        if not cleaned:
            continue
        if cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def title_from_target(target_type: str) -> str:
    return " ".join(part.capitalize() for part in target_type.split("_") if part)


def iter_rows(conn: sqlite3.Connection, limit: int = 0) -> Iterable[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    sql = """
        SELECT
            event_id,
            last_seen_date,
            title,
            description,
            ai_summary,
            full_text_dossier,
            ai_report_json,
            tie_score,
            effect_score
        FROM unique_events
        WHERE ai_analysis_status = 'COMPLETED'
    """
    if limit and limit > 0:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return cur.fetchall()


def build_bootstrap(rows: Iterable[sqlite3.Row], keywords_count: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    agg: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "count": 0,
        "token_counter": Counter(),
        "tie_sum": 0.0,
        "effect_sum": 0.0,
        "last_seen": None,
        "event_ids": [],
    })

    total_rows = 0
    tagged_rows = 0

    for row in rows:
        total_rows += 1
        ai_data = safe_json_load(row["ai_report_json"])
        target_type = pick_target_type(ai_data)
        if not target_type:
            continue

        tagged_rows += 1
        bucket = agg[target_type]
        bucket["count"] += 1
        bucket["tie_sum"] += float(row["tie_score"] or 0)
        bucket["effect_sum"] += float(row["effect_score"] or 0)

        dt = parse_date(row["last_seen_date"])
        if dt and (bucket["last_seen"] is None or dt > bucket["last_seen"]):
            bucket["last_seen"] = dt

        if len(bucket["event_ids"]) < 20:
            bucket["event_ids"].append(row["event_id"])

        target_tokens = set(target_type.split("_"))
        combined_text = " ".join([
            str(row["title"] or ""),
            str(row["description"] or ""),
            str(row["full_text_dossier"] or "")[:1200],
        ])

        for tk in tokenize(combined_text):
            if tk in target_tokens:
                continue
            bucket["token_counter"][tk] += 1

    suggestions: List[Dict[str, Any]] = []
    for target_type, data in agg.items():
        frequent_terms = [(w, c) for w, c in data["token_counter"].most_common(max(keywords_count * 4, 40)) if c >= 2]
        pool = frequent_terms if frequent_terms else data["token_counter"].most_common(max(keywords_count * 2, 20))
        top_keywords = [w for w, _ in pool]
        top_keywords = [w for w in top_keywords if w not in set(target_type.split("_"))][:keywords_count]

        campaign_id = slugify_campaign_id(target_type)
        target_variants = [target_type.replace("_", " "), target_type]
        target_types_field = "|".join(dict.fromkeys([v.strip().lower() for v in target_variants if v.strip()]))

        suggestions.append({
            "campaign_id": campaign_id,
            "name": title_from_target(target_type),
            "target_types": target_types_field,
            "keywords": ";".join(top_keywords),
            "color": palette_color(campaign_id),
            "stats": {
                "event_count": data["count"],
                "avg_tie": round(data["tie_sum"] / data["count"], 2) if data["count"] else 0,
                "avg_effect": round(data["effect_sum"] / data["count"], 2) if data["count"] else 0,
                "last_seen": data["last_seen"].isoformat() if data["last_seen"] else None,
                "sample_event_ids": data["event_ids"],
                "top_terms": [w for w, _ in data["token_counter"].most_common(20)],
            },
        })

    suggestions.sort(key=lambda x: x["stats"]["event_count"], reverse=True)

    diagnostics = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_completed_rows_scanned": total_rows,
        "rows_with_target_type": tagged_rows,
        "unique_target_types": len(agg),
    }

    return suggestions, diagnostics


def write_csv(path: str, suggestions: List[Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["campaign_id", "name", "target_types", "keywords", "color"],
        )
        writer.writeheader()
        for item in suggestions:
            writer.writerow({
                "campaign_id": item["campaign_id"],
                "name": item["name"],
                "target_types": item["target_types"],
                "keywords": item["keywords"],
                "color": item["color"],
            })


def write_json(path: str, payload: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def publish_cache_json(path: str, suggestions: List[Dict[str, Any]]) -> None:
    campaigns = []
    for item in suggestions:
        campaigns.append({
            "campaign_id": item["campaign_id"],
            "name": item["name"],
            "target_types": split_token_field(item["target_types"]),
            "keywords": split_token_field(item["keywords"]),
            "color": item["color"],
        })

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "campaigns": campaigns,
    }
    write_json(path, payload)


def try_write_csv(path: str, suggestions: List[Dict[str, Any]]) -> Tuple[bool, str]:
    try:
        write_csv(path, suggestions)
        return True, ""
    except Exception as exc:
        return False, str(exc)


def try_write_cache_json(path: str, suggestions: List[Dict[str, Any]]) -> Tuple[bool, str]:
    try:
        publish_cache_json(path, suggestions)
        return True, ""
    except Exception as exc:
        return False, str(exc)


def main() -> None:
    args = parse_args()

    if not os.path.exists(args.db):
        raise FileNotFoundError(f"Database not found: {args.db}")

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL;")
    rows = iter_rows(conn, limit=args.limit)

    suggestions, diagnostics = build_bootstrap(rows, keywords_count=args.keywords)
    conn.close()

    filtered = [s for s in suggestions if s["stats"]["event_count"] >= args.min_events]
    if args.top and args.top > 0:
        filtered = filtered[: max(1, args.top)]

    write_csv(args.out_csv, filtered)
    published_csv_ok, published_csv_error = try_write_csv(args.publish_csv, filtered)
    write_json(
        args.out_json,
        {
            "diagnostics": diagnostics,
            "applied_filters": {
                "min_events": args.min_events,
                "top": args.top,
                "keywords_per_campaign": args.keywords,
            },
            "campaigns": filtered,
        },
    )
    cache_ok, cache_error = try_write_cache_json(args.cache_json, filtered)

    print("[BOOTSTRAP] Campaign suggestions generated")
    print(f"  DB: {args.db}")
    print(f"  Bootstrap CSV: {args.out_csv}")
    print(f"  Published CSV: {args.publish_csv}")
    print(f"  JSON: {args.out_json}")
    print(f"  Cache JSON: {args.cache_json}")
    print(f"  Suggested campaigns: {len(filtered)}")
    print(f"  Rows scanned: {diagnostics['total_completed_rows_scanned']}")
    print(f"  Rows with target_type: {diagnostics['rows_with_target_type']}")
    if not published_csv_ok:
        print(f"  [WARN] Could not write published CSV: {published_csv_error}")
    if not cache_ok:
        print(f"  [WARN] Could not write cache JSON: {cache_error}")


if __name__ == "__main__":
    main()
