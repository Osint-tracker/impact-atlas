"""
=============================================================================
 THE CAMPAIGN STRATEGIST — Component_11
 Impact Atlas | Autonomous Campaign Assignment Engine
=============================================================================
 PURPOSE:  Semantic LLM-based classification of fused Master Events into
           strategic military campaigns, replacing fragile keyword matching.
 MODEL:    deepseek/deepseek-v4-flash (via OpenRouter)
 PIPELINE: Post-processing batch — runs AFTER Smart Fusion Engine
 AUTHOR:   Senior AI Architect — Impact Atlas
=============================================================================
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*a: Any, **kw: Any) -> bool:
        return False

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None  # type: ignore[assignment,misc]

# ============================================================================
# CONSTANTS
# ============================================================================

STRATEGIST_MODEL = "deepseek/deepseek-v4-flash"
CONFIDENCE_THRESHOLD = 0.75
DEFAULT_BATCH_LIMIT = 100
COMMIT_INTERVAL = 25
LLM_MAX_TOKENS = 512
LLM_TEMPERATURE = 0.05
LLM_TIMEOUT = 90.0
LLM_MAX_RETRIES = 2
TEXT_EXCERPT_LIMIT = 4000
RATE_LIMIT_DELAY = 0.3  # seconds between API calls

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.normpath(os.path.join(BASE_DIR, ".."))

DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, "war_tracker_v2", "data", "raw_events.db")
CURATED_CSV_PATH = os.path.join(PROJECT_ROOT, "bootstrap", "campaign_definitions.curated.csv")
FALLBACK_CSV_PATH = os.path.join(PROJECT_ROOT, "bootstrap", "campaign_definitions.csv")
CACHE_JSON_PATH = os.path.join(PROJECT_ROOT, "bootstrap", "campaign_definitions.cache.json")

# ============================================================================
# LOGGING
# ============================================================================

sys.stdout.reconfigure(encoding="utf-8")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)-18s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("CAMPAIGN_STRATEGIST")

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class CampaignDef:
    """Single campaign definition loaded from CSV."""
    campaign_id: str
    name: str
    target_types: List[str]
    keywords: List[str]

    def to_prompt_block(self) -> str:
        return (
            f"- ID: {self.campaign_id}\n"
            f"  Name: {self.name}\n"
            f"  Target Types: {', '.join(self.target_types)}\n"
            f"  Keywords: {', '.join(self.keywords)}"
        )


@dataclass
class LLMResult:
    """Parsed result from a single LLM campaign assignment call."""
    campaign_id: Optional[str] = None
    reasoning: str = ""
    confidence: float = 0.0
    raw_response: str = ""
    parse_success: bool = False
    error: str = ""


@dataclass
class RunMetrics:
    """Aggregated metrics for a full strategist run."""
    total_processed: int = 0
    assigned: int = 0
    skipped_low_confidence: int = 0
    skipped_null: int = 0
    llm_errors: int = 0
    db_updates: int = 0
    total_weighted_tie: float = 0.0
    total_api_time: float = 0.0
    campaigns_hit: Dict[str, int] = field(default_factory=dict)

    def summary(self) -> str:
        lines = [
            "=" * 60,
            " CAMPAIGN STRATEGIST — RUN SUMMARY",
            "=" * 60,
            f"  Events processed:        {self.total_processed}",
            f"  Campaigns assigned:      {self.assigned}",
            f"  Skipped (low conf):      {self.skipped_low_confidence}",
            f"  Skipped (null match):    {self.skipped_null}",
            f"  LLM errors:              {self.llm_errors}",
            f"  DB rows updated:         {self.db_updates}",
            f"  Cumulative weighted TIE: {self.total_weighted_tie:.1f}",
            f"  Total API time:          {self.total_api_time:.1f}s",
            "-" * 60,
            "  Campaign distribution:",
        ]
        for cid, count in sorted(self.campaigns_hit.items(), key=lambda x: -x[1]):
            lines.append(f"    {cid}: {count}")
        if not self.campaigns_hit:
            lines.append("    (none)")
        lines.append("=" * 60)
        return "\n".join(lines)


# ============================================================================
# CSV PARSER
# ============================================================================

def _normalize(value: Any) -> str:
    """Lowercase, strip, collapse whitespace."""
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _split_tokens(raw: Any, separators: str = r"[|;,]") -> List[str]:
    """Split a delimited string into cleaned, deduplicated tokens."""
    if not raw:
        return []
    parts = re.split(separators, str(raw))
    seen: set[str] = set()
    out: List[str] = []
    for p in parts:
        token = _normalize(p)
        if len(token) < 2 or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def load_campaigns_from_csv(path: str) -> List[CampaignDef]:
    """Parse a campaign definitions CSV file into CampaignDef objects."""
    if not path or not os.path.isfile(path):
        logger.warning("CSV not found: %s", path)
        return []

    campaigns: List[CampaignDef] = []
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                lowered = {str(k).strip().lower(): v for k, v in row.items()}
                cid = _normalize(lowered.get("campaign_id"))
                name = str(lowered.get("name") or "").strip()
                targets = _split_tokens(lowered.get("target_types"), r"[|]")
                keywords = _split_tokens(lowered.get("keywords"), r"[;]")

                if not cid or not name or not targets or not keywords:
                    continue

                campaigns.append(CampaignDef(
                    campaign_id=cid,
                    name=name,
                    target_types=targets,
                    keywords=keywords,
                ))
        logger.info("Loaded %d campaigns from %s", len(campaigns), os.path.basename(path))
    except Exception as exc:
        logger.error("Failed to parse CSV %s: %s", path, exc)
    return campaigns


def load_campaigns_from_cache(path: str) -> List[CampaignDef]:
    """Load campaign definitions from cached JSON."""
    if not path or not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        raw_list = data.get("campaigns", data if isinstance(data, list) else [])
        campaigns: List[CampaignDef] = []
        for item in raw_list:
            cid = _normalize(item.get("campaign_id"))
            name = str(item.get("name") or "").strip()
            targets = [_normalize(t) for t in (item.get("target_types") or []) if _normalize(t)]
            keywords = [_normalize(k) for k in (item.get("keywords") or []) if _normalize(k)]
            if cid and name and targets and keywords:
                campaigns.append(CampaignDef(cid, name, targets, keywords))
        logger.info("Loaded %d campaigns from cache %s", len(campaigns), os.path.basename(path))
        return campaigns
    except Exception as exc:
        logger.error("Failed to parse cache %s: %s", path, exc)
        return []


def load_campaign_definitions() -> List[CampaignDef]:
    """
    Load campaign definitions with priority chain:
    1. Curated CSV (authoritative)
    2. Fallback CSV
    3. Cached JSON
    Aborts if all sources fail.
    """
    campaigns = load_campaigns_from_csv(CURATED_CSV_PATH)
    if campaigns:
        return campaigns

    logger.warning("Curated CSV unavailable, trying fallback CSV...")
    campaigns = load_campaigns_from_csv(FALLBACK_CSV_PATH)
    if campaigns:
        return campaigns

    logger.warning("Fallback CSV unavailable, trying cache JSON...")
    campaigns = load_campaigns_from_cache(CACHE_JSON_PATH)
    if campaigns:
        return campaigns

    logger.critical("ALL campaign definition sources failed. Cannot proceed.")
    return []


# ============================================================================
# DATABASE
# ============================================================================

FETCH_SQL = """
    SELECT
        event_id,
        full_text_dossier,
        ai_report_json,
        tie_score,
        reliability,
        operational_sector,
        kinetic_score,
        target_score,
        effect_score,
        title,
        description,
        last_seen_date
    FROM unique_events
    WHERE ai_analysis_status = 'COMPLETED'
      AND (campaign_id IS NULL OR TRIM(campaign_id) = '')
    ORDER BY last_seen_date DESC
    LIMIT ?
"""

UPDATE_SQL = """
    UPDATE unique_events
    SET campaign_id         = :campaign_id,
        campaign_match_meta = :campaign_match_meta,
        campaign_tagged_at  = :campaign_tagged_at
    WHERE event_id = :event_id
      AND ai_analysis_status = 'COMPLETED'
      AND (campaign_id IS NULL OR TRIM(campaign_id) = '')
"""


def connect_db(db_path: str) -> sqlite3.Connection:
    """Open SQLite connection in WAL mode with Row factory."""
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"Database not found: {db_path}")
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=15000;")
    logger.info("Connected to DB: %s (WAL mode)", os.path.basename(db_path))
    return conn


def fetch_untagged_events(conn: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    """Fetch Master Events without campaign assignment."""
    cursor = conn.cursor()
    cursor.execute(FETCH_SQL, (limit,))
    rows = cursor.fetchall()
    logger.info("Fetched %d untagged COMPLETED events (limit=%d)", len(rows), limit)
    return rows


def ensure_campaign_columns(conn: sqlite3.Connection) -> None:
    """Ensure campaign columns exist (idempotent)."""
    for ddl in (
        "ALTER TABLE unique_events ADD COLUMN campaign_id TEXT",
        "ALTER TABLE unique_events ADD COLUMN campaign_match_meta TEXT",
        "ALTER TABLE unique_events ADD COLUMN campaign_tagged_at TEXT",
    ):
        try:
            conn.execute(ddl)
        except sqlite3.OperationalError:
            pass
    conn.commit()


# ============================================================================
# EVENT DATA EXTRACTION
# ============================================================================

def _safe_json(raw: Any) -> Dict[str, Any]:
    """Safely parse a JSON string or return empty dict."""
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def extract_event_context(row: sqlite3.Row) -> Dict[str, Any]:
    """
    Extract tactical context from a DB row for prompt construction.
    Pulls classification and target_type from ai_report_json.
    """
    ai_data = _safe_json(row["ai_report_json"])
    titan = ai_data.get("titan_metrics") or {}
    strategy = ai_data.get("strategy") or {}
    titan_assess = ai_data.get("titan_assessment") or strategy.get("titan_assessment") or {}

    # Classification extraction chain
    classification = (
        ai_data.get("classification")
        or ai_data.get("event_type")
        or strategy.get("classification")
        or "UNKNOWN"
    )

    # Target type extraction chain
    target_type = (
        ai_data.get("target_type")
        or titan.get("target_type_category")
        or titan_assess.get("target_type_category")
        or strategy.get("target_type")
        or "UNKNOWN"
    )

    # Build text excerpt
    text_parts = [
        str(row["title"] or ""),
        str(row["description"] or ""),
        str(row["full_text_dossier"] or ""),
    ]
    full_text = " ".join(p for p in text_parts if p.strip())
    excerpt = full_text[:TEXT_EXCERPT_LIMIT] if full_text else ""

    return {
        "event_id": row["event_id"],
        "text_excerpt": excerpt,
        "classification": str(classification).upper(),
        "target_type": _normalize(target_type),
        "sector_id": row["operational_sector"] or "UNKNOWN",
        "tie_score": float(row["tie_score"] or 0),
        "reliability": int(row["reliability"] or 0),
        "kinetic_score": float(row["kinetic_score"] or 0),
        "target_score": float(row["target_score"] or 0),
        "effect_score": float(row["effect_score"] or 0),
        "last_seen_date": row["last_seen_date"] or "",
    }


def calculate_weighted_tie(tie_score: float, reliability: int) -> float:
    """
    Weighted T.I.E. = tie_score * (reliability / 100).
    Example: TIE=80, reliability=50 → 40.0 points.
    """
    rel_factor = max(0, min(100, reliability)) / 100.0
    return round(tie_score * rel_factor, 2)


# ============================================================================
# LLM PROMPT BUILDER
# ============================================================================

SYSTEM_PROMPT = """You are a strategic military analyst in a C4ISR intelligence platform.
Your task is to assign military events to strategic campaigns.

RULES:
1. Evaluate STRATEGIC INTENT, not keyword overlap.
2. Reject false positives: isolated civilian incidents, routine patrols, or vague reports must return campaign_id=null.
3. Consider the operational sector for geographic coherence.
4. Be conservative: if uncertain, return campaign_id as null.

You MUST respond with a JSON object containing exactly these fields:
- "campaign_id": string (one of the campaign IDs listed) or null
- "reasoning": string (concise strategic explanation, max 100 words)
- "confidence": number between 0.0 and 1.0

Example: {"campaign_id": "energy_grid_degradation", "reasoning": "Strike on power substation aligns with systematic energy infrastructure targeting pattern", "confidence": 0.88}"""


def build_campaign_catalog(campaigns: List[CampaignDef]) -> str:
    """Build the campaign catalog section of the prompt."""
    lines = ["ACTIVE STRATEGIC CAMPAIGNS:"]
    for c in campaigns:
        lines.append(c.to_prompt_block())
    return "\n".join(lines)


def build_event_prompt(ctx: Dict[str, Any], catalog: str) -> str:
    """Build the per-event user prompt for LLM analysis."""
    return (
        f"{catalog}\n\n"
        f"---\n"
        f"ANALYZE THIS FUSED EVENT:\n"
        f"Text: \"{ctx['text_excerpt'][:2000]}\"\n"
        f"Classification: {ctx['classification']}\n"
        f"Target Type: {ctx['target_type']}\n"
        f"Operational Sector: {ctx['sector_id']}\n"
        f"T.I.E. Score: {ctx['tie_score']:.0f} "
        f"(K={ctx['kinetic_score']:.0f}, T={ctx['target_score']:.0f}, E={ctx['effect_score']:.0f})\n"
        f"Reliability: {ctx['reliability']}%\n"
        f"Date: {ctx['last_seen_date']}\n\n"
        f"Does this event belong to one of the active campaigns? "
        f"Evaluate strategic context and kinetic intent.\n"
        f"Return EXCLUSIVELY a JSON object:\n"
        f'{{"campaign_id": "string_id or null", '
        f'"reasoning": "concise strategic explanation", '
        f'"confidence": 0.0}}'
    )


# ============================================================================
# LLM CLIENT
# ============================================================================

def create_openrouter_client(api_key: str) -> Any:
    """Initialize OpenRouter client for deepseek/deepseek-v4-flash."""
    if OpenAI is None:
        raise RuntimeError("OpenAI SDK not installed. Run: pip install openai")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required.")

    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
        default_headers={
            "HTTP-Referer": "https://github.com/Osint-tracker/impact-atlas",
            "X-Title": "Impact Atlas Campaign Strategist",
        },
        timeout=LLM_TIMEOUT,
        max_retries=LLM_MAX_RETRIES,
    )
    logger.info("OpenRouter client initialized (model: %s)", STRATEGIST_MODEL)
    return client


def call_llm(client: Any, system: str, user_prompt: str) -> Tuple[str, float]:
    """
    Call deepseek/deepseek-v4-flash and return (raw_response, elapsed_seconds).
    Retries once on empty response. Raises on hard failure.
    """
    t0 = time.monotonic()
    for attempt in range(3):
        response = client.chat.completions.create(
            model=STRATEGIST_MODEL,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user_prompt},
            ],
            temperature=LLM_TEMPERATURE,
            max_tokens=LLM_MAX_TOKENS,
            response_format={"type": "json_object"},
        )
        raw = (response.choices[0].message.content or "").strip()
        if raw:
            break
        if attempt == 0:
            logger.warning("Empty LLM response, retrying after 1s...")
            time.sleep(1.0)
    elapsed = time.monotonic() - t0
    return raw, elapsed


# ============================================================================
# JSON RESPONSE PARSER (3-level fallback)
# ============================================================================

def parse_llm_response(raw: str, valid_ids: set[str]) -> LLMResult:
    """
    Parse LLM JSON response with 3-level fallback:
    1. Direct json.loads()
    2. Strip markdown fences and retry
    3. Regex extract {...} block
    """
    result = LLMResult(raw_response=raw)

    if not raw:
        result.error = "Empty LLM response"
        return result

    parsed: Optional[Dict[str, Any]] = None

    # Level 1: Direct parse
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        pass

    # Level 2: Strip markdown fences
    if parsed is None:
        cleaned = re.sub(r"```(?:json)?\s*", "", raw).strip().rstrip("`")
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            pass

    # Level 3: Regex extraction
    if parsed is None:
        match = re.search(r"\{[^{}]*\}", raw, flags=re.DOTALL)
        if match:
            try:
                parsed = json.loads(match.group(0))
            except json.JSONDecodeError:
                pass

    if not isinstance(parsed, dict):
        result.error = f"Failed to parse JSON from response: {raw[:200]}"
        return result

    result.parse_success = True

    # Extract fields
    cid_raw = parsed.get("campaign_id")
    if cid_raw is None or str(cid_raw).lower() in ("null", "none", ""):
        result.campaign_id = None
    else:
        result.campaign_id = _normalize(cid_raw)

    result.reasoning = str(parsed.get("reasoning") or "").strip()

    try:
        result.confidence = float(parsed.get("confidence", 0))
    except (TypeError, ValueError):
        result.confidence = 0.0

    # Validate campaign_id against known registry
    if result.campaign_id and result.campaign_id not in valid_ids:
        logger.warning(
            "LLM returned unknown campaign_id '%s' — rejecting",
            result.campaign_id,
        )
        result.campaign_id = None
        result.confidence = 0.0

    return result


# ============================================================================
# CORE ENGINE
# ============================================================================

class CampaignStrategist:
    """
    The Campaign Strategist — autonomous LLM-based campaign assignment engine.
    Processes fused Master Events in batch and assigns strategic campaign IDs.
    """

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        api_key: str = "",
        confidence_min: float = CONFIDENCE_THRESHOLD,
        dry_run: bool = False,
    ):
        self.db_path = db_path
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY", "")
        self.confidence_min = confidence_min
        self.dry_run = dry_run
        self.campaigns: List[CampaignDef] = []
        self.valid_ids: set[str] = set()
        self.catalog_prompt: str = ""
        self.conn: Optional[sqlite3.Connection] = None
        self.client: Any = None
        self.metrics = RunMetrics()

    def initialize(self) -> None:
        """Load campaigns, connect DB, init LLM client."""
        logger.info("=" * 60)
        logger.info(" THE CAMPAIGN STRATEGIST — Initializing")
        logger.info("=" * 60)

        # 1. Load campaign definitions
        self.campaigns = load_campaign_definitions()
        if not self.campaigns:
            raise RuntimeError("No campaign definitions available. Aborting.")
        self.valid_ids = {c.campaign_id for c in self.campaigns}
        self.catalog_prompt = build_campaign_catalog(self.campaigns)
        logger.info("Campaign registry: %d campaigns loaded", len(self.campaigns))

        # 2. Connect database
        self.conn = connect_db(self.db_path)
        ensure_campaign_columns(self.conn)

        # 3. Initialize LLM client
        self.client = create_openrouter_client(self.api_key)

        if self.dry_run:
            logger.warning("DRY RUN MODE — no DB writes will be performed")

    def _process_single_event(self, row: sqlite3.Row) -> None:
        """Process one event: extract context, call LLM, update DB."""
        ctx = extract_event_context(row)
        event_id = ctx["event_id"]
        short_id = event_id[:24]

        # Build prompt
        user_prompt = build_event_prompt(ctx, self.catalog_prompt)

        # Call LLM
        try:
            raw_response, elapsed = call_llm(self.client, SYSTEM_PROMPT, user_prompt)
            self.metrics.total_api_time += elapsed
        except Exception as exc:
            logger.error("[%s] LLM call failed: %s", short_id, exc)
            self.metrics.llm_errors += 1
            return

        # Parse response
        result = parse_llm_response(raw_response, self.valid_ids)

        if not result.parse_success:
            logger.error("[%s] JSON parse failed: %s", short_id, result.error)
            self.metrics.llm_errors += 1
            return

        # Decision gate
        if result.campaign_id is None:
            logger.debug("[%s] No campaign match (null)", short_id)
            self.metrics.skipped_null += 1
            return

        if result.confidence < self.confidence_min:
            logger.info(
                "[%s] Below threshold: %s (conf=%.2f < %.2f)",
                short_id, result.campaign_id, result.confidence, self.confidence_min,
            )
            self.metrics.skipped_low_confidence += 1
            return

        # Calculate weighted T.I.E.
        weighted_tie = calculate_weighted_tie(ctx["tie_score"], ctx["reliability"])

        # Build match metadata
        match_meta = json.dumps({
            "pipeline": "campaign_strategist_v1",
            "model": STRATEGIST_MODEL,
            "campaign_id": result.campaign_id,
            "confidence": result.confidence,
            "reasoning": result.reasoning,
            "weighted_tie": weighted_tie,
            "tie_score": ctx["tie_score"],
            "reliability": ctx["reliability"],
            "sector_id": ctx["sector_id"],
            "decided_at": datetime.now(timezone.utc).isoformat(),
        }, ensure_ascii=False)

        tagged_at = datetime.now(timezone.utc).isoformat()

        logger.info(
            "[%s] ASSIGNED -> %s (conf=%.2f, wTIE=%.1f)",
            short_id, result.campaign_id, result.confidence, weighted_tie,
        )

        # Update DB — commit immediately to release WAL writer lock
        # This allows concurrent scripts (refiner_fast.py) to write
        if not self.dry_run and self.conn:
            self.conn.execute(UPDATE_SQL, {
                "campaign_id": result.campaign_id,
                "campaign_match_meta": match_meta,
                "campaign_tagged_at": tagged_at,
                "event_id": event_id,
            })
            self.conn.commit()

        self.metrics.assigned += 1
        self.metrics.total_weighted_tie += weighted_tie
        self.metrics.db_updates += 1
        self.metrics.campaigns_hit[result.campaign_id] = (
            self.metrics.campaigns_hit.get(result.campaign_id, 0) + 1
        )

    def run(self, limit: int = DEFAULT_BATCH_LIMIT) -> RunMetrics:
        """Main execution loop: fetch events, process batch, commit."""
        if not self.conn:
            raise RuntimeError("Engine not initialized. Call initialize() first.")

        rows = fetch_untagged_events(self.conn, limit)
        if not rows:
            logger.info("No untagged events found. Nothing to process.")
            return self.metrics

        total = len(rows)
        logger.info("Starting batch processing: %d events", total)

        for idx, row in enumerate(rows, start=1):
            self.metrics.total_processed += 1
            self._process_single_event(row)

            # Periodic commit
            if not self.dry_run and idx % COMMIT_INTERVAL == 0:
                self.conn.commit()
                logger.info(
                    "Progress: %d/%d | assigned=%d | errors=%d",
                    idx, total, self.metrics.assigned, self.metrics.llm_errors,
                )

            # Rate limiting
            time.sleep(RATE_LIMIT_DELAY)

        # Final commit
        if not self.dry_run:
            self.conn.commit()

        logger.info("\n%s", self.metrics.summary())
        return self.metrics

    def close(self) -> None:
        """Clean up resources."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed.")


# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="The Campaign Strategist — LLM-based strategic campaign assignment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python campaign_strategist.py --limit 10 --dry-run\n"
            "  python campaign_strategist.py --limit 100 --confidence 0.8\n"
            "  python campaign_strategist.py --limit 50 --db path/to/raw_events.db\n"
        ),
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path")
    parser.add_argument("--limit", type=int, default=DEFAULT_BATCH_LIMIT,
                        help=f"Max events to process (default: {DEFAULT_BATCH_LIMIT})")
    parser.add_argument("--confidence", type=float, default=CONFIDENCE_THRESHOLD,
                        help=f"Min confidence for DB write (default: {CONFIDENCE_THRESHOLD})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run without writing to database")
    parser.add_argument("--api-key", default="",
                        help="OpenRouter API key override (default: from .env)")
    return parser.parse_args()


def main() -> None:
    """Entry point for CLI execution."""
    load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
    args = parse_args()

    strategist = CampaignStrategist(
        db_path=args.db,
        api_key=args.api_key,
        confidence_min=args.confidence,
        dry_run=args.dry_run,
    )

    try:
        strategist.initialize()
        strategist.run(limit=args.limit)
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    except Exception as exc:
        logger.critical("Fatal error: %s", exc, exc_info=True)
        raise
    finally:
        strategist.close()


if __name__ == "__main__":
    main()
