from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None

MINIMAX_MODEL = "minimax/minimax-m2.5:free"
JUDGE_MODEL = "deepseek/deepseek-v3.2"

MINIMAX_SYSTEM_PROMPT = """
You are AGENT-1 (Initial Tactical Filter) in a C4ISR pipeline.
Goal: reject weak or accidental keyword matches and allow only plausible strategic-intent events.

Rules:
1) Use ONLY provided event packet and campaign candidate hints.
2) Confirm whether the event likely represents an intentional tactical action aligned with at least one campaign.
3) Reject incidental mentions (e.g., drone debris in open field, unrelated civilian chatter, vague political statements).
4) Be conservative: if uncertain, return passes_filter=false.
5) Output STRICT JSON object ONLY:
{"passes_filter": boolean, "reason": string}

`reason` must be concise (<= 220 chars) and operational.
"""

JUDGE_SYSTEM_PROMPT = """
You are AGENT-2 (Supreme Strategic Judge) in a military intelligence admission pipeline.
You are independent from AGENT-1 and must make your own strategic assessment.

Task:
- Decide FINAL admission of an event into Strategic Campaigns.
- Choose exactly one campaign_id from `allowed_campaign_ids` when admitted=true.
- Use strategic impact logic, not keyword coincidence.

Decision standards:
1) Strategic relevance is mandatory (operational effect, infrastructure significance, force-multiplying consequence).
2) If event is tactical noise, deny admission.
3) If admitted=true, campaign_id must be in allowed list.
4) If no existing campaign fits but pattern is strategic, you may propose a new campaign object.

Output STRICT JSON ONLY:
{
  "admitted": boolean,
  "campaign_id": "string",
  "strategic_rationale": "string",
  "new_campaign_proposal": {
    "should_create": boolean,
    "campaign_id": "string",
    "name": "string",
    "target_types": ["string"],
    "keywords": ["string"],
    "color": "#hex",
    "reason": "string"
  }
}

If no proposal, set new_campaign_proposal.should_create=false and keep other fields empty.
"""


@dataclass
class TieGateResult:
    passed: bool
    kinetic_score: float
    effect_score: float
    reason: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


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


def _extract_json_object(raw: str) -> Dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}

    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def extract_target_type(event_row: Dict[str, Any]) -> Optional[str]:
    ai_data = _safe_json_load(event_row.get("ai_report_json"))
    titan_metrics = ai_data.get("titan_metrics") or _safe_json_load(event_row.get("titan_metrics"))
    strategy = ai_data.get("strategy") or {}
    strategy_verified = strategy.get("verified_data") or {}

    candidates = [
        ai_data.get("target_type"),
        titan_metrics.get("target_type_category"),
        (ai_data.get("titan_assessment") or {}).get("target_type_category"),
        (strategy.get("titan_assessment") or {}).get("target_type_category"),
        strategy.get("target_type"),
        strategy_verified.get("target_type"),
    ]

    for cand in candidates:
        normalized = _normalize_text(cand)
        if normalized and normalized not in {"unknown", "none", "null", "n/a", "na"}:
            return normalized
    return None


def evaluate_tie_gate(event_row: Dict[str, Any], min_k: float = 3.0, min_e: float = 3.0) -> TieGateResult:
    ai_data = _safe_json_load(event_row.get("ai_report_json"))
    titan_metrics = ai_data.get("titan_metrics") or _safe_json_load(event_row.get("titan_metrics"))

    kinetic = _safe_float(event_row.get("kinetic_score"), _safe_float(titan_metrics.get("kinetic_score"), 0.0))
    effect = _safe_float(event_row.get("effect_score"), _safe_float(titan_metrics.get("effect_score"), 0.0))

    if kinetic < min_k:
        return TieGateResult(
            passed=False,
            kinetic_score=kinetic,
            effect_score=effect,
            reason=f"K<{min_k:.0f} deterministic reject",
        )
    if effect < min_e:
        return TieGateResult(
            passed=False,
            kinetic_score=kinetic,
            effect_score=effect,
            reason=f"E<{min_e:.0f} deterministic reject",
        )
    return TieGateResult(
        passed=True,
        kinetic_score=kinetic,
        effect_score=effect,
        reason="passes deterministic TIE gate",
    )


def build_keyword_candidates(
    campaigns: List[Dict[str, Any]],
    target_type: Optional[str],
    event_text: str,
) -> List[Dict[str, Any]]:
    norm_target = _normalize_text(target_type)
    norm_text = _normalize_text(event_text)
    if not norm_target or not norm_text:
        return []

    matches: List[Dict[str, Any]] = []
    for campaign in campaigns:
        target_types = [str(x).strip().lower() for x in (campaign.get("target_types") or []) if str(x).strip()]
        keywords = [str(x).strip().lower() for x in (campaign.get("keywords") or []) if str(x).strip()]
        target_hits = [t for t in target_types if t == norm_target or t in norm_target or norm_target in t]
        if not target_hits:
            continue
        keyword_hits = [kw for kw in keywords if kw in norm_text]
        if not keyword_hits:
            continue
        score = len(keyword_hits) * 10 + max((len(k) for k in keyword_hits), default=0)
        matches.append(
            {
                "campaign_id": str(campaign.get("campaign_id") or "").strip().lower(),
                "name": campaign.get("name"),
                "color": campaign.get("color"),
                "target_hits": target_hits,
                "keyword_hits": keyword_hits,
                "score": score,
            }
        )

    matches.sort(key=lambda item: item.get("score", 0), reverse=True)
    return matches


def _openrouter_client(api_key: Optional[str] = None) -> Optional[Any]:
    key = api_key or os.getenv("OPENROUTER_API_KEY")
    if not key or OpenAI is None:
        return None
    return OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=key,
        default_headers={"X-Title": "Impact Atlas Strategic Admission"},
    )


def get_llm_runtime_status(api_key: Optional[str] = None) -> Dict[str, Any]:
    key = api_key or os.getenv("OPENROUTER_API_KEY")
    if OpenAI is None:
        return {"available": False, "reason": "OpenAI SDK not installed (pip install openai)"}
    if not key:
        return {"available": False, "reason": "OPENROUTER_API_KEY missing in runtime environment"}
    return {"available": True, "reason": "ok"}


def _chat_json(
    client: Any,
    model: str,
    system_prompt: str,
    payload: Dict[str, Any],
    max_tokens: int = 420,
) -> Tuple[Dict[str, Any], str]:
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt.strip()},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ],
        temperature=0.0,
        response_format={"type": "json_object"},
        max_tokens=max_tokens,
    )
    raw = (response.choices[0].message.content or "").strip()
    return _extract_json_object(raw), raw


def admit_event_two_agents(
    event_row: Dict[str, Any],
    campaigns: List[Dict[str, Any]],
    api_key: Optional[str] = None,
    text_limit: int = 5000,
) -> Dict[str, Any]:
    event_id = str(event_row.get("event_id") or "").strip()
    target_type = extract_target_type(event_row)
    full_text = " ".join(
        [
            str(event_row.get("title") or ""),
            str(event_row.get("description") or ""),
            str(event_row.get("full_text_dossier") or ""),
        ]
    ).strip()
    tie_score = _safe_float(event_row.get("tie_score"), 0.0)

    tie_gate = evaluate_tie_gate(event_row)
    keyword_matches = build_keyword_candidates(campaigns, target_type=target_type, event_text=full_text)

    base = {
        "event_id": event_id,
        "target_type": target_type,
        "tie_score": tie_score,
        "kinetic_score": tie_gate.kinetic_score,
        "effect_score": tie_gate.effect_score,
        "tie_gate_passed": tie_gate.passed,
        "tie_gate_reason": tie_gate.reason,
        "keyword_candidates": keyword_matches,
        "passes_filter": False,
        "admitted": False,
        "campaign_id": None,
        "strategic_rationale": "",
        "agent1_raw": "",
        "agent2_raw": "",
        "proposed_campaign": None,
    }

    if not tie_gate.passed:
        base["strategic_rationale"] = "Rejected by deterministic TIE gate"
        return base

    if not keyword_matches:
        base["strategic_rationale"] = "No target_type+keyword prefilter match"
        return base

    client = _openrouter_client(api_key=api_key)
    if client is None:
        base["strategic_rationale"] = "OPENROUTER_API_KEY missing or OpenAI SDK unavailable"
        return base

    event_packet = {
        "event_id": event_id,
        "date": event_row.get("last_seen_date"),
        "title": event_row.get("title"),
        "description": event_row.get("description"),
        "text_excerpt": full_text[:text_limit],
        "target_type": target_type,
        "scores": {
            "tie": tie_score,
            "kinetic_k": tie_gate.kinetic_score,
            "effect_e": tie_gate.effect_score,
        },
        "candidate_campaigns": keyword_matches[:8],
    }

    # Agent-1: Minimax tactical intent filter
    try:
        mini_obj, mini_raw = _chat_json(
            client=client,
            model=MINIMAX_MODEL,
            system_prompt=MINIMAX_SYSTEM_PROMPT,
            payload=event_packet,
            max_tokens=220,
        )
        base["agent1_raw"] = mini_raw
    except Exception as exc:
        base["strategic_rationale"] = f"Agent-1 error: {exc}"
        return base

    passes_filter = bool(mini_obj.get("passes_filter", False))
    base["passes_filter"] = passes_filter
    mini_reason = str(mini_obj.get("reason") or "").strip()

    if not passes_filter:
        base["strategic_rationale"] = mini_reason or "Rejected by Agent-1 tactical intent filter"
        return base

    # Agent-2: DeepSeek final strategic admission
    allowed_campaign_ids = [c["campaign_id"] for c in campaigns if c.get("campaign_id")]
    judge_payload = {
        "event_packet": event_packet,
        "agent1_result": {"passes_filter": passes_filter, "reason": mini_reason},
        "allowed_campaign_ids": allowed_campaign_ids,
    }
    try:
        judge_obj, judge_raw = _chat_json(
            client=client,
            model=JUDGE_MODEL,
            system_prompt=JUDGE_SYSTEM_PROMPT,
            payload=judge_payload,
            max_tokens=420,
        )
        base["agent2_raw"] = judge_raw
    except Exception as exc:
        base["strategic_rationale"] = f"Agent-2 error: {exc}"
        return base

    admitted = bool(judge_obj.get("admitted", False))
    campaign_id = _normalize_text(judge_obj.get("campaign_id"))
    strategic_rationale = str(judge_obj.get("strategic_rationale") or "").strip()
    proposal = judge_obj.get("new_campaign_proposal") if isinstance(judge_obj.get("new_campaign_proposal"), dict) else None

    if admitted and campaign_id not in allowed_campaign_ids:
        # Guardrail: force campaign to known registry or deny.
        if len(keyword_matches) == 1:
            campaign_id = keyword_matches[0]["campaign_id"]
        else:
            admitted = False
            strategic_rationale = (
                strategic_rationale
                or "Judge returned campaign_id outside allowed registry"
            )

    base["admitted"] = admitted
    base["campaign_id"] = campaign_id if admitted else None
    base["strategic_rationale"] = strategic_rationale or ("Admitted by Agent-2" if admitted else "Denied by Agent-2")
    base["proposed_campaign"] = proposal
    return base


def merge_campaign_into_ai_report(ai_report_json: Any, admission_result: Dict[str, Any]) -> str:
    ai_data = _safe_json_load(ai_report_json)
    strategy = ai_data.get("strategy")
    if not isinstance(strategy, dict):
        strategy = {}
        ai_data["strategy"] = strategy

    strategy["campaign"] = {
        "admitted": bool(admission_result.get("admitted")),
        "campaign_id": admission_result.get("campaign_id"),
        "strategic_rationale": admission_result.get("strategic_rationale"),
        "passes_filter": bool(admission_result.get("passes_filter")),
        "tie_gate_passed": bool(admission_result.get("tie_gate_passed")),
        "tie_gate_reason": admission_result.get("tie_gate_reason"),
        "rule": "two_agent_admission_v1",
        "agent_filter_model": MINIMAX_MODEL,
        "agent_judge_model": JUDGE_MODEL,
        "decided_at": _now_iso(),
    }

    strategy["campaign_admission_debug"] = {
        "agent1_raw": admission_result.get("agent1_raw", ""),
        "agent2_raw": admission_result.get("agent2_raw", ""),
        "keyword_candidates": admission_result.get("keyword_candidates") or [],
        "proposed_campaign": admission_result.get("proposed_campaign"),
    }
    return json.dumps(ai_data, ensure_ascii=False)
