from __future__ import annotations

import argparse
import csv
import json
import os
import re
from typing import Any, Dict, List

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv(*_args, **_kwargs):
        return False

try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
except Exception:  # pragma: no cover
    gspread = None
    ServiceAccountCredentials = None

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CREDS_PATH = os.path.join(BASE_DIR, "service_account.json")
DEFAULT_PROPOSALS_PATH = os.path.join(BASE_DIR, "../bootstrap/campaign_new_candidates.json")

REQUIRED_HEADERS = ["campaign_id", "name", "target_types", "keywords", "color"]

HEADER_ALIASES = {
    "campaign_id": {"campaign_id", "campaignid", "id_campaign", "campaign"},
    "name": {"name", "campaign_name", "nome", "campaignname"},
    "target_types": {
        "target_types",
        "target_type",
        "targettypes",
        "targettype",
        "target types",
        "tipo_target",
        "tipi_target",
    },
    "keywords": {"keywords", "keyword", "parole_chiave", "parolechiave"},
    "color": {"color", "colour", "colore"},
}


def _load_env_file_fallback(path: str) -> None:
    if not path or not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = val
    except Exception:
        return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append/upsert campaign definitions to Google Sheets")
    parser.add_argument("--credentials", default=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", DEFAULT_CREDS_PATH))
    parser.add_argument("--sheet-url", default=os.getenv("SHEET_CSV_URL", ""), help="Google Sheets URL")
    parser.add_argument("--worksheet", default="campaign_definitions", help="Target worksheet/tab")
    parser.add_argument("--from-json", default=DEFAULT_PROPOSALS_PATH, help="JSON file with AI proposals")
    parser.add_argument("--from-csv", default="", help="CSV file with campaign definitions")
    parser.add_argument("--campaign-id", default="", help="Single campaign id to append/upsert")
    parser.add_argument("--name", default="", help="Single campaign display name")
    parser.add_argument("--target-types", default="", help="Pipe/comma/semicolon-separated target_types")
    parser.add_argument("--keywords", default="", help="Pipe/comma/semicolon-separated keywords")
    parser.add_argument("--color", default="#f59e0b", help="Hex color")
    parser.add_argument("--upsert", action="store_true", help="Update existing campaign_id rows")
    parser.add_argument("--dry-run", action="store_true", help="Do not write to sheet")
    parser.add_argument("--verbose", action="store_true", help="Print diagnostics (sheet id/title and updated ranges)")
    return parser.parse_args()


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _split_tokens(value: Any) -> List[str]:
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = re.split(r"[|,;]", str(value or ""))
    out: List[str] = []
    seen = set()
    for item in raw_items:
        token = _normalize_text(item).lower()
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _normalize_hex(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "#f59e0b"
    if not raw.startswith("#"):
        raw = f"#{raw}"
    if re.fullmatch(r"#[0-9a-fA-F]{3}", raw):
        return "#" + "".join(c * 2 for c in raw[1:]).lower()
    if re.fullmatch(r"#[0-9a-fA-F]{6}", raw):
        return raw.lower()
    return "#f59e0b"


def _normalize_row(row: Dict[str, Any]) -> Dict[str, str]:
    campaign_id = _normalize_text(row.get("campaign_id")).lower()
    name = _normalize_text(row.get("name")) or campaign_id.replace("_", " ").title()
    target_types = _split_tokens(row.get("target_types"))
    keywords = _split_tokens(row.get("keywords"))
    color = _normalize_hex(row.get("color"))

    if not campaign_id or not target_types or not keywords:
        return {}

    return {
        "campaign_id": campaign_id,
        "name": name,
        "target_types": "|".join(target_types),
        "keywords": ";".join(keywords),
        "color": color,
    }


def _load_rows_from_json(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        if isinstance(payload.get("proposals"), list):
            rows = payload["proposals"]
        elif isinstance(payload.get("campaigns"), list):
            rows = payload["campaigns"]
        else:
            rows = []
    else:
        rows = []
    return [r for r in rows if isinstance(r, dict)]


def _load_rows_from_csv(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: List[Dict[str, Any]] = []
        for row in reader:
            if not isinstance(row, dict):
                continue
            rows.append(
                {
                    "campaign_id": row.get("campaign_id", ""),
                    "name": row.get("name", ""),
                    "target_types": row.get("target_types", ""),
                    "keywords": row.get("keywords", ""),
                    "color": row.get("color", ""),
                }
            )
        return rows


def _authorize(credentials_path: str):
    if gspread is None or ServiceAccountCredentials is None:
        raise RuntimeError("Missing dependencies: pip install gspread oauth2client")
    if not credentials_path or not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Service account JSON not found: {credentials_path}")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    return gspread.authorize(creds)


def _ensure_headers(worksheet) -> Dict[str, int]:
    values = worksheet.get_all_values()
    if not values:
        worksheet.append_row(REQUIRED_HEADERS, value_input_option="USER_ENTERED")
        return {header: idx for idx, header in enumerate(REQUIRED_HEADERS)}

    raw_headers = [_normalize_text(x).lower() for x in values[0]]
    normalized_headers = []
    for item in raw_headers:
        no_paren = re.sub(r"\([^)]*\)", "", item).strip()
        compact = re.sub(r"[^a-z0-9_ ]+", "", no_paren).strip()
        compact = re.sub(r"\s+", " ", compact)
        normalized_headers.append(compact)

    header_map: Dict[str, int] = {}
    for canonical, aliases in HEADER_ALIASES.items():
        for idx, h in enumerate(normalized_headers):
            h_norm = h.strip()
            h_underscored = h_norm.replace(" ", "_")
            if (
                h_norm in aliases
                or h_underscored in aliases
                or any(
                    h_norm.startswith(f"{alias} ")
                    or h_norm.startswith(f"{alias}_")
                    or h_underscored.startswith(f"{alias}_")
                    for alias in aliases
                )
            ):
                header_map[canonical] = idx
                break

    missing = [h for h in REQUIRED_HEADERS if h not in header_map]
    if missing:
        raise RuntimeError(
            f"Worksheet missing required headers: {missing}. "
            f"Expected canonical headers: {REQUIRED_HEADERS}. "
            f"Detected headers: {raw_headers}"
        )

    return header_map


def _existing_row_index(worksheet, header_map: Dict[str, int]) -> Dict[str, int]:
    values = worksheet.get_all_values()
    out: Dict[str, int] = {}
    for idx, row in enumerate(values[1:], start=2):
        cid = ""
        try:
            cid = _normalize_text(row[header_map["campaign_id"]]).lower()
        except Exception:
            cid = ""
        if cid and cid not in out:
            out[cid] = idx
    return out


def main() -> None:
    load_dotenv()
    load_dotenv(os.path.join(BASE_DIR, "..", ".env"))
    load_dotenv(os.path.join(BASE_DIR, "..", "war_tracker_v2", ".env"))
    _load_env_file_fallback(os.path.join(BASE_DIR, "..", ".env"))
    _load_env_file_fallback(os.path.join(BASE_DIR, "..", "war_tracker_v2", ".env"))

    args = parse_args()
    if not args.sheet_url:
        raise RuntimeError("Missing --sheet-url (or SHEET_CSV_URL env)")

    payload_rows = _load_rows_from_json(args.from_json)
    payload_rows.extend(_load_rows_from_csv(args.from_csv))
    if args.campaign_id:
        payload_rows.append(
            {
                "campaign_id": args.campaign_id,
                "name": args.name,
                "target_types": args.target_types,
                "keywords": args.keywords,
                "color": args.color,
            }
        )

    normalized: List[Dict[str, str]] = []
    seen = set()
    for row in payload_rows:
        clean = _normalize_row(row)
        if not clean:
            continue
        cid = clean["campaign_id"]
        if cid in seen:
            continue
        seen.add(cid)
        normalized.append(clean)

    if not normalized:
        print("[GSHEET] No valid campaign rows to push.")
        return

    client = _authorize(args.credentials)
    sheet = client.open_by_url(args.sheet_url)
    worksheet = sheet.worksheet(args.worksheet)
    print(f"[GSHEET] spreadsheet_title={sheet.title}")
    print(f"[GSHEET] spreadsheet_id={sheet.id}")
    print(f"[GSHEET] sheet_url={args.sheet_url}")
    header_map = _ensure_headers(worksheet)
    existing = _existing_row_index(worksheet, header_map)

    created = 0
    updated = 0
    skipped = 0

    for row in normalized:
        cid = row["campaign_id"]
        row_values = [row["campaign_id"], row["name"], row["target_types"], row["keywords"], row["color"]]

        if cid in existing:
            if not args.upsert:
                skipped += 1
                continue
            if args.dry_run:
                updated += 1
                continue
            row_idx = existing[cid]
            resp = worksheet.update(f"A{row_idx}:E{row_idx}", [row_values], value_input_option="USER_ENTERED")
            if args.verbose:
                print(f"[GSHEET][UPDATE] campaign_id={cid} row={row_idx} resp={resp}")
            updated += 1
        else:
            if args.dry_run:
                created += 1
                continue
            resp = worksheet.append_row(row_values, value_input_option="USER_ENTERED")
            if args.verbose:
                print(f"[GSHEET][APPEND] campaign_id={cid} resp={resp}")
            created += 1

    print("[GSHEET] Done")
    print(f"[GSHEET] worksheet={args.worksheet}")
    print(f"[GSHEET] created={created} updated={updated} skipped={skipped}")


if __name__ == "__main__":
    main()
