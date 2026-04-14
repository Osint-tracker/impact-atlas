from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False

from campaign_admission import (
    admit_event_two_agents,
    get_llm_runtime_status,
    merge_campaign_into_ai_report,
)
from campaigns_engine import (
    ensure_campaign_columns,
    load_campaign_definitions,
    load_campaign_definitions_from_csv,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "../war_tracker_v2/data/raw_events.db")
DEFAULT_CACHE_PATH = os.path.join(BASE_DIR, "../bootstrap/campaign_definitions.cache.json")
DEFAULT_PROPOSALS_PATH = os.path.join(BASE_DIR, "../bootstrap/campaign_new_candidates.json")
DEFAULT_CURATED_CSV_PATH = os.path.join(BASE_DIR, "../bootstrap/campaign_definitions.csv")

# WAL-safe update on canonical master rows (ai_analysis_status='COMPLETED').
# MERGED rows are excluded by WHERE clause and cannot overwrite master tags.
MASTER_UPDATE_SQL = """
UPDATE unique_events
SET
    campaign_id = CASE
        WHEN :overwrite_existing = 1 THEN :campaign_id
        WHEN campaign_id IS NULL OR TRIM(campaign_id) = '' THEN :campaign_id
        ELSE campaign_id
    END,
    campaign_match_meta = :campaign_match_meta,
    campaign_tagged_at = CASE
        WHEN :overwrite_existing = 1 THEN :campaign_tagged_at
        WHEN campaign_tagged_at IS NULL OR TRIM(campaign_tagged_at) = '' THEN :campaign_tagged_at
        ELSE campaign_tagged_at
    END,
    ai_report_json = :ai_report_json
WHERE event_id = :event_id
  AND ai_analysis_status = 'COMPLETED'
  AND (
      :overwrite_existing = 1
      OR campaign_id IS NULL
      OR TRIM(campaign_id) = ''
      OR campaign_id = :campaign_id
  )
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Strategic Campaigns admission backfill (two-agent pipeline)")
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite DB path")
    parser.add_argument("--sheet-url", default=os.getenv("SHEET_CSV_URL", ""), help="Google Sheet URL for campaign_definitions")
    parser.add_argument("--sheet-tab", default="campaign_definitions", help="Worksheet/tab name")
    parser.add_argument("--cache-path", default=DEFAULT_CACHE_PATH, help="Local campaign definitions cache path")
    parser.add_argument(
        "--campaign-definitions-csv",
        default="",
        help="Optional local CSV for deterministic campaign definitions (overrides sheet/cache)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Max events to process (0 = all)")
    parser.add_argument("--offset", type=int, default=0, help="Offset for batch slicing")
    parser.add_argument("--retag", action="store_true", help="Reprocess also already tagged events")
    parser.add_argument("--overwrite-existing", action="store_true", help="Allow replacing existing campaign_id")
    parser.add_argument("--dry-run", action="store_true", help="Do not write DB updates")
    parser.add_argument("--commit-every", type=int, default=50, help="Commit every N admitted events")
    parser.add_argument("--text-limit", type=int, default=5000, help="Max chars from event text sent to LLM")
    parser.add_argument("--openrouter-api-key", default=os.getenv("OPENROUTER_API_KEY", ""), help="OpenRouter API key override")
    parser.add_argument(
        "--require-llm",
        dest="require_llm",
        action="store_true",
        default=True,
        help="Fail fast if OpenAI SDK / OPENROUTER_API_KEY is unavailable (default: true)",
    )
    parser.add_argument(
        "--no-require-llm",
        dest="require_llm",
        action="store_false",
        help="Allow run without LLM runtime (events will not be admitted by two-agent flow)",
    )
    parser.add_argument("--proposals-out", default=DEFAULT_PROPOSALS_PATH, help="JSON output for proposed new campaigns")
    return parser.parse_args()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_env_file_fallback(path: str, overwrite: bool = False) -> None:
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
                if not key:
                    continue
                if overwrite or key not in os.environ:
                    os.environ[key] = val
    except Exception:
        return


def _safe_json_load(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _fetch_rows(conn: sqlite3.Connection, retag: bool, limit: int, offset: int) -> List[sqlite3.Row]:
    where = ["ai_analysis_status = 'COMPLETED'"]
    if not retag:
        where.append("(campaign_id IS NULL OR TRIM(campaign_id) = '')")
    where_sql = " AND ".join(where)

    sql = f"""
        SELECT
            event_id,
            last_seen_date,
            title,
            description,
            full_text_dossier,
            ai_report_json,
            titan_metrics,
            tie_score,
            kinetic_score,
            target_score,
            effect_score,
            campaign_id
        FROM unique_events
        WHERE {where_sql}
        ORDER BY last_seen_date DESC
    """
    params: List[Any] = []
    if limit and limit > 0:
        sql += " LIMIT ?"
        params.append(int(limit))
    if offset and offset > 0:
        if not (limit and limit > 0):
            sql += " LIMIT -1"
        sql += " OFFSET ?"
        params.append(int(offset))

    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.fetchall()


def _append_proposal(proposals: Dict[str, Dict[str, Any]], proposal: Any) -> None:
    if not isinstance(proposal, dict):
        return
    should_create = bool(proposal.get("should_create", False))
    campaign_id = str(proposal.get("campaign_id") or "").strip().lower()
    if not should_create or not campaign_id:
        return

    name = str(proposal.get("name") or campaign_id.replace("_", " ").title()).strip()
    target_types = proposal.get("target_types") if isinstance(proposal.get("target_types"), list) else []
    keywords = proposal.get("keywords") if isinstance(proposal.get("keywords"), list) else []
    color = str(proposal.get("color") or "#f59e0b").strip()
    reason = str(proposal.get("reason") or "").strip()

    target_types = [str(x).strip().lower() for x in target_types if str(x).strip()]
    keywords = [str(x).strip().lower() for x in keywords if str(x).strip()]
    if not target_types or not keywords:
        return

    proposals[campaign_id] = {
        "campaign_id": campaign_id,
        "name": name,
        "target_types": target_types,
        "keywords": keywords,
        "color": color,
        "reason": reason,
    }


def _persist_admission(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    admission: Dict[str, Any],
    overwrite_existing: bool,
    dry_run: bool,
) -> int:
    campaign_id = str(admission.get("campaign_id") or "").strip().lower()
    if not campaign_id:
        return 0

    tagged_at = _now_iso()
    ai_report_json = merge_campaign_into_ai_report(row["ai_report_json"], admission)

    payload = {
        "event_id": row["event_id"],
        "campaign_id": campaign_id,
        "campaign_tagged_at": tagged_at,
        "campaign_match_meta": json.dumps(
            {
                "pipeline": "two_agent_admission_v1",
                "tagged_at": tagged_at,
                "tie_gate": {
                    "passed": admission.get("tie_gate_passed"),
                    "reason": admission.get("tie_gate_reason"),
                    "kinetic_k": admission.get("kinetic_score"),
                    "effect_e": admission.get("effect_score"),
                },
                "agent1_raw": admission.get("agent1_raw"),
                "agent2_raw": admission.get("agent2_raw"),
                "keyword_candidates": admission.get("keyword_candidates"),
                "strategic_rationale": admission.get("strategic_rationale"),
            },
            ensure_ascii=False,
        ),
        "ai_report_json": ai_report_json,
        "overwrite_existing": 1 if overwrite_existing else 0,
    }

    if dry_run:
        return 1

    cur = conn.cursor()
    cur.execute(MASTER_UPDATE_SQL, payload)
    return int(cur.rowcount or 0)


def _write_proposals(path: str, proposals: Dict[str, Dict[str, Any]]) -> None:
    payload = {
        "generated_at": _now_iso(),
        "source": "run_backfill two_agent_admission_v1",
        "proposals": list(proposals.values()),
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def main() -> None:
    load_dotenv()
    load_dotenv(os.path.join(BASE_DIR, "..", ".env"))
    load_dotenv(os.path.join(BASE_DIR, "..", "war_tracker_v2", ".env"))
    _load_env_file_fallback(os.path.join(BASE_DIR, "..", ".env"), overwrite=False)
    _load_env_file_fallback(os.path.join(BASE_DIR, "..", "war_tracker_v2", ".env"), overwrite=True)
    args = parse_args()

    if not os.path.exists(args.db):
        raise FileNotFoundError(f"Database not found: {args.db}")

    runtime_status = get_llm_runtime_status(api_key=args.openrouter_api_key or None)
    if args.require_llm and not runtime_status.get("available"):
        raise RuntimeError(
            f"LLM runtime unavailable: {runtime_status.get('reason')}. "
            "Install dependencies (`pip install openai python-dotenv`) and set OPENROUTER_API_KEY."
        )
    if not runtime_status.get("available"):
        print(f"[ADMISSION][WARN] LLM runtime unavailable: {runtime_status.get('reason')}")

    campaign_csv = args.campaign_definitions_csv or ""
    if campaign_csv:
        campaigns = load_campaign_definitions_from_csv(campaign_csv)
    else:
        campaigns = load_campaign_definitions(
            sheet_url=args.sheet_url,
            cache_path=args.cache_path,
            tab_name=args.sheet_tab,
            timeout_seconds=15,
        )
        if not campaigns and os.path.exists(DEFAULT_CURATED_CSV_PATH):
            campaigns = load_campaign_definitions_from_csv(DEFAULT_CURATED_CSV_PATH)
    if not campaigns:
        raise RuntimeError("No campaign definitions available (sheet+cache both empty/unreachable).")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    ensure_campaign_columns(conn)

    rows = _fetch_rows(conn, retag=args.retag, limit=args.limit, offset=args.offset)
    total = len(rows)
    print(f"[ADMISSION] Loaded {total} candidate events from DB")
    print(f"[ADMISSION] Campaign definitions: {len(campaigns)}")
    if campaign_csv:
        print(f"[ADMISSION] Definitions source: csv={campaign_csv}")
    else:
        print(f"[ADMISSION] Definitions source: sheet/cache (sheet_url={args.sheet_url or 'N/A'})")

    stats = {
        "processed": 0,
        "tie_rejected": 0,
        "prefilter_rejected": 0,
        "agent1_rejected": 0,
        "agent2_rejected": 0,
        "llm_unavailable": 0,
        "admitted": 0,
        "db_updated": 0,
    }
    proposals: Dict[str, Dict[str, Any]] = {}
    pending_commits = 0

    for idx, row in enumerate(rows, start=1):
        admission = admit_event_two_agents(
            event_row=dict(row),
            campaigns=campaigns,
            api_key=args.openrouter_api_key or None,
            text_limit=args.text_limit,
        )
        stats["processed"] += 1

        if not admission.get("tie_gate_passed"):
            stats["tie_rejected"] += 1
        elif not admission.get("keyword_candidates"):
            stats["prefilter_rejected"] += 1
        elif (
            "missing" in str(admission.get("strategic_rationale") or "").lower()
            or "sdk" in str(admission.get("strategic_rationale") or "").lower()
        ) and not admission.get("passes_filter"):
            stats["llm_unavailable"] += 1
        elif not admission.get("passes_filter"):
            stats["agent1_rejected"] += 1
        elif not admission.get("admitted"):
            stats["agent2_rejected"] += 1
        else:
            stats["admitted"] += 1
            changed = _persist_admission(
                conn=conn,
                row=row,
                admission=admission,
                overwrite_existing=args.overwrite_existing,
                dry_run=args.dry_run,
            )
            stats["db_updated"] += changed
            pending_commits += changed

        _append_proposal(proposals, admission.get("proposed_campaign"))

        if not args.dry_run and pending_commits >= max(1, args.commit_every):
            conn.commit()
            pending_commits = 0

        if idx % 25 == 0 or idx == total:
            print(
                f"[ADMISSION] {idx}/{total} | admitted={stats['admitted']} "
                f"| updated={stats['db_updated']} | tie_rej={stats['tie_rejected']}"
            )

    if not args.dry_run:
        conn.commit()
    conn.close()

    _write_proposals(args.proposals_out, proposals)
    print("[ADMISSION] DONE")
    print(json.dumps(stats, indent=2))
    print(f"[ADMISSION] Proposed campaigns written to: {args.proposals_out}")
    print("[SQL] Master update query used (WAL-safe):")
    print(MASTER_UPDATE_SQL.strip())


if __name__ == "__main__":
    main()
