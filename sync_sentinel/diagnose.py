"""
LLM diagnosis — explains WHY, over evidence the rules already gathered.

Division of labour, deliberately strict:

    rules  -> detection    (is 0 != 143? arithmetic, never the model)
    model  -> diagnosis    (what would explain this? which check next?)
    rules  -> verification (did the retry actually fix it?)

The model is never asked whether something is broken. It is handed a decided
verdict plus structured evidence and asked to interpret it. That keeps
detection deterministic and auditable, and confines the model to the part it
is genuinely better at than a rule tree.

Degrades to a rule-based explanation when no API key is configured, so the
agent still produces a useful alert without the LLM.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Optional

from .engine import Assessment, Verdict

MODEL = "claude-opus-5"

SYSTEM = """You diagnose POS-to-Supy data synchronisation failures.

You are given a verdict that has ALREADY been decided by deterministic rules,
plus the structured evidence behind it. Do not re-litigate the verdict — your
job is to explain the most likely cause and say what to check next.

Ground every statement in the evidence provided. If the evidence does not
support a conclusion, say so plainly rather than speculating. Never invent
log lines, error codes, timestamps or record counts that are not present.

Be specific about mechanism. "Sync failed" is useless; "the ingestion cursor
is still on 7 Sep while the POS has data for 8-10 Sep, and a manual fetch
returns nothing, which is consistent with a checkpoint that did not advance"
is what an engineer can act on.

Respond as JSON only."""

SCHEMA = {
    "type": "object",
    "properties": {
        "likely_cause": {
            "type": "string",
            "description": "One sentence naming the most probable mechanism.",
        },
        "confidence": {
            "type": "integer",
            "description": "0-100. Be honest; low confidence is useful information.",
        },
        "reasoning": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Numbered chain of inference, each step tied to evidence.",
        },
        "next_checks": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Concrete things an engineer should inspect, most useful first.",
        },
        "safe_to_auto_retry": {
            "type": "boolean",
            "description": "Whether re-requesting the affected dates is safe and likely to help.",
        },
        "auto_retry_rationale": {"type": "string"},
    },
    "required": ["likely_cause", "confidence", "reasoning", "next_checks",
                 "safe_to_auto_retry", "auto_retry_rationale"],
    "additionalProperties": False,
}


@dataclass
class Diagnosis:
    likely_cause: str
    confidence: int
    reasoning: list
    next_checks: list
    safe_to_auto_retry: bool
    auto_retry_rationale: str
    by: str = "llm"

    def render(self) -> str:
        lines = [f"Likely cause : {self.likely_cause}",
                 f"Confidence   : {self.confidence}%  (by {self.by})",
                 "", "Why:"]
        lines += [f"  {i}. {r}" for i, r in enumerate(self.reasoning, 1)]
        lines += ["", "Check next:"]
        lines += [f"  - {c}" for c in self.next_checks]
        lines += ["", f"Safe to auto-retry: {'yes' if self.safe_to_auto_retry else 'no'}"
                      f" — {self.auto_retry_rationale}"]
        return "\n".join(lines)


def _fallback(a: Assessment) -> Diagnosis:
    """Rule-based explanation when no API key is available."""
    ev = a.evidence
    pos, supy = ev.pos_sales, ev.supy_sales
    if a.verdict is Verdict.BROKEN:
        cause = ("Ingestion is not retrieving records the POS is reporting — "
                 "most likely a stuck sync cursor or an API/auth fault on the "
                 "ingestion side.")
        checks = [
            f"Ingestion checkpoint for {ev.location} — is it still on an older "
            f"business date?",
            "The raw POS API response for the affected dates (record count).",
            "Ingestion job logs around the last successful run.",
        ]
        retry, why = True, ("The POS has the data and Supy has none, so "
                            "re-requesting those dates cannot double-count.")
    elif a.verdict is Verdict.PARTIAL:
        cause = ("Ingestion started but did not finish — pagination, a timeout "
                 "or a filter dropping records.")
        checks = ["Whether the fetch paginates and stops early.",
                  "Ingestion timeouts for this location.",
                  "Any per-record rejections in the mapping step."]
        retry, why = False, ("Partial data is already loaded; re-running risks "
                             "duplicates unless the loader is idempotent.")
    elif a.verdict is Verdict.RECONCILE_FAIL:
        cause = ("Records arrived but money differs — likely tax, discount or "
                 "item-mapping handling rather than transport.")
        checks = ["Tax treatment on both sides (gross vs net).",
                  "Discount and promotion lines.",
                  "Items failing to map to a Supy item id."]
        retry, why = False, "Re-fetching will reproduce the same totals."
    elif a.verdict is Verdict.POS_UNAVAILABLE:
        cause = f"The POS could not be queried: {pos.error if pos else 'unknown'}."
        checks = ["POS credentials and session validity.",
                  "Portal or API reachability from the runner."]
        retry, why = False, "Nothing can be concluded until the POS answers."
    elif a.verdict is Verdict.UNKNOWN:
        cause = ("Supy-side figures are not wired up, so no comparison is "
                 "possible. This is a gap in the agent, not a client incident.")
        checks = ["Connect the Supy ingestion adapter (counts + totals per "
                  "location per business date)."]
        retry, why = False, "No comparison available."
    else:
        cause = "No fault detected."
        checks = []
        retry, why = False, "Nothing to recover."
    return Diagnosis(cause, 60 if a.needs_attention else 95, list(ev.reasons),
                     checks, retry, why, by="rules")


def diagnose(a: Assessment, history: Optional[list] = None) -> Diagnosis:
    """
    Ask Claude to interpret the evidence. Falls back to rules without a key.

    `history` is an optional list of recent assessments for the same location,
    which lets the model distinguish "broken since Tuesday" from "flapping".
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _fallback(a)

    try:
        import anthropic
    except ImportError:
        return _fallback(a)

    payload = {
        "verdict": a.verdict.value,
        "health_score": a.health,
        "evidence": a.evidence.as_dict(),
        "recent_history": history or [],
    }

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=MODEL,
            max_tokens=16000,
            system=SYSTEM,
            thinking={"type": "adaptive"},
            output_config={"format": {"type": "json_schema", "schema": SCHEMA}},
            messages=[{
                "role": "user",
                "content": (
                    "Diagnose this POS-to-Supy sync assessment.\n\n"
                    + json.dumps(payload, indent=2)
                ),
            }],
        )
        text = "".join(b.text for b in resp.content if b.type == "text")
        data = json.loads(text)
        return Diagnosis(
            likely_cause=data["likely_cause"],
            confidence=int(data["confidence"]),
            reasoning=list(data["reasoning"]),
            next_checks=list(data["next_checks"]),
            safe_to_auto_retry=bool(data["safe_to_auto_retry"]),
            auto_retry_rationale=data["auto_retry_rationale"],
            by="llm",
        )
    except Exception as exc:
        fb = _fallback(a)
        fb.reasoning = list(fb.reasoning) + [
            f"(LLM diagnosis unavailable: {type(exc).__name__}; "
            f"fell back to rules.)"]
        return fb
