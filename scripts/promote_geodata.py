import argparse
import json
import os
import sqlite3
from typing import Optional, Tuple


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "../war_tracker_v2/data/raw_events.db")


INVALID_TOKENS = {"", "0", "0.0", "null", "none", "unknown", "n/a"}


def _to_float(value) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        num = float(value)
    else:
        token = str(value).strip().lower()
        if token in INVALID_TOKENS:
            return None
        try:
            num = float(token)
        except ValueError:
            return None

    if num == 0.0:
        return None
    return num


def _extract_pair(candidate) -> Optional[Tuple[float, float]]:
    if not isinstance(candidate, dict):
        return None

    lat = _to_float(candidate.get("lat"))
    lon = _to_float(candidate.get("lon"))
    if lat is None or lon is None:
        return None
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return lat, lon


def _extract_best_pair(ai_report_json: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    try:
        report = json.loads(ai_report_json)
    except (TypeError, ValueError):
        return None, None, None

    geo = ((report.get("tactics") or {}).get("geo_location") or {}) if isinstance(report, dict) else {}
    explicit = (geo.get("explicit") or {}) if isinstance(geo, dict) else {}
    verified = (geo.get("verified") or {}) if isinstance(geo, dict) else {}
    inferred = (geo.get("inferred") or {}) if isinstance(geo, dict) else {}

    for source_name, candidate in (("explicit", explicit), ("verified", verified), ("inferred", inferred)):
        pair = _extract_pair(candidate)
        if pair:
            return pair[0], pair[1], source_name
    return None, None, None


def main():
    parser = argparse.ArgumentParser(description="Promote geodata from ai_report_json into unique_events.lat/lon")
    parser.add_argument("--db", default=DB_PATH, help="Path to SQLite DB")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N candidate events")
    parser.add_argument("--batch-size", type=int, default=500, help="Commit every N updates")
    parser.add_argument("--dry-run", action="store_true", help="Do not write changes")
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"[ERROR] Database not found: {args.db}")
        return

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = """
        SELECT event_id, ai_report_json, lat, lon
        FROM unique_events
        WHERE ai_analysis_status = 'COMPLETED'
          AND ai_report_json IS NOT NULL
          AND TRIM(ai_report_json) != ''
          AND (lat IS NULL OR lon IS NULL OR lat = 0 OR lon = 0)
        ORDER BY last_seen_date DESC
    """
    params = []
    if args.limit and args.limit > 0:
        query += " LIMIT ?"
        params.append(args.limit)

    cursor.execute(query, params)
    rows = cursor.fetchall()

    scanned = len(rows)
    updated = 0
    skipped_invalid_json = 0
    skipped_no_coords = 0
    source_counts = {"explicit": 0, "verified": 0, "inferred": 0}

    for row in rows:
        event_id = row["event_id"]
        ai_report_json = row["ai_report_json"]

        lat, lon, source_name = _extract_best_pair(ai_report_json)
        if source_name is None:
            try:
                json.loads(ai_report_json)
            except (TypeError, ValueError):
                skipped_invalid_json += 1
            else:
                skipped_no_coords += 1
            continue

        source_counts[source_name] += 1

        if args.dry_run:
            updated += 1
            continue

        cursor.execute(
            """
            UPDATE unique_events
            SET lat = COALESCE(?, lat),
                lon = COALESCE(?, lon)
            WHERE event_id = ?
              AND ai_analysis_status = 'COMPLETED'
              AND (lat IS NULL OR lon IS NULL OR lat = 0 OR lon = 0)
            """,
            (lat, lon, event_id),
        )
        if cursor.rowcount:
            updated += 1
            if updated % max(1, args.batch_size) == 0:
                conn.commit()

    if not args.dry_run:
        conn.commit()
    conn.close()

    mode = "DRY-RUN" if args.dry_run else "WRITE"
    print(f"[{mode}] Candidate rows scanned: {scanned}")
    print(f"[{mode}] Rows with promoted coordinates: {updated}")
    print(f"[{mode}] Skipped invalid JSON: {skipped_invalid_json}")
    print(f"[{mode}] Skipped without numeric coordinates: {skipped_no_coords}")
    print(
        f"[{mode}] Coordinate source usage: explicit={source_counts['explicit']}, "
        f"verified={source_counts['verified']}, inferred={source_counts['inferred']}"
    )


if __name__ == "__main__":
    main()
