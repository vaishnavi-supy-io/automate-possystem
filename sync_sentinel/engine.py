"""
Detection engine — deterministic rules only.

The classification below is arithmetic, not judgement, and it stays that way
deliberately. An LLM is excellent at reading logs and explaining a cause; it is
the wrong tool for deciding whether 0 != 143. Rules detect, the model explains.

The distinction that matters most is the last one: NO_ACTIVITY. A monitor that
alerts every time Supy shows zero gets muted within a week, and a muted monitor
is worse than none — it provides the feeling of coverage without the fact. So
"the POS also says zero" must be a first-class healthy state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional

from .adapters.base import ConnectionStatus, SalesSnapshot, SyncStatus


class Verdict(str, Enum):
    HEALTHY = "HEALTHY"                    # 🟢 both sides agree, data flowed
    NO_ACTIVITY = "NO_ACTIVITY"            # 🟢 both sides zero — the site was shut
    PARTIAL = "PARTIAL"                    # 🟡 Supy has some, not all
    RECONCILE_FAIL = "RECONCILE_FAIL"      # 🟡 counts match, money does not
    BROKEN = "BROKEN"                      # 🔴 POS has sales, Supy has none
    POS_UNAVAILABLE = "POS_UNAVAILABLE"    # ⚫ we could not ask the POS
    UNKNOWN = "UNKNOWN"                    # ⚫ Supy side not wired


SEVERITY = {
    Verdict.HEALTHY: 0,
    Verdict.NO_ACTIVITY: 0,
    Verdict.UNKNOWN: 1,
    Verdict.POS_UNAVAILABLE: 2,
    Verdict.RECONCILE_FAIL: 3,
    Verdict.PARTIAL: 4,
    Verdict.BROKEN: 5,
}


@dataclass
class Thresholds:
    # Below this share of POS checks, ingestion is partial rather than complete.
    partial_ratio: float = 0.98
    # Money may differ by rounding; beyond this it is a real discrepancy.
    money_tolerance_pct: float = 0.5
    # A sync that has not succeeded in this long is stale even if today is quiet.
    stale_sync_hours: int = 24


@dataclass
class Evidence:
    """
    Everything the verdict was based on.

    Kept as structured data rather than prose so it can be handed to the model
    for diagnosis, rendered in an alert, and stored as an audit trail of why
    the agent believed what it believed.
    """
    location: str
    business_date: date
    pos: str
    pos_sales: Optional[SalesSnapshot] = None
    supy_sales: Optional[SalesSnapshot] = None
    sync: Optional[SyncStatus] = None
    connection: Optional[ConnectionStatus] = None
    reasons: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        def snap(s):
            if s is None:
                return None
            return {"checks": s.checks, "gross": round(s.gross, 2),
                    "available": s.available, "source": s.source,
                    "error": s.error}
        return {
            "location": self.location,
            "business_date": self.business_date.isoformat(),
            "pos": self.pos,
            "pos_sales": snap(self.pos_sales),
            "supy_sales": snap(self.supy_sales),
            "sync": (None if not self.sync else {
                "last_success": self.sync.last_success,
                "last_attempt": self.sync.last_attempt,
                "status": self.sync.status,
                "cursor": self.sync.cursor,
                "message": self.sync.message}),
            "connection": (None if not self.connection else {
                "reachable": self.connection.reachable,
                "authenticated": self.connection.authenticated,
                "detail": self.connection.detail}),
            "reasons": self.reasons,
        }


@dataclass
class Assessment:
    verdict: Verdict
    health: int                 # 0-100
    evidence: Evidence
    checked_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))

    @property
    def needs_attention(self) -> bool:
        return SEVERITY[self.verdict] >= 2

    @property
    def icon(self) -> str:
        return {Verdict.HEALTHY: "🟢", Verdict.NO_ACTIVITY: "🟢",
                Verdict.PARTIAL: "🟡", Verdict.RECONCILE_FAIL: "🟡",
                Verdict.BROKEN: "🔴", Verdict.POS_UNAVAILABLE: "⚫",
                Verdict.UNKNOWN: "⚫"}[self.verdict]


def _hours_since(iso: Optional[str]) -> Optional[float]:
    if not iso:
        return None
    try:
        t = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t).total_seconds() / 3600
    except ValueError:
        return None


def assess(pos_sales: SalesSnapshot,
           supy_sales: Optional[SalesSnapshot],
           sync: Optional[SyncStatus] = None,
           connection: Optional[ConnectionStatus] = None,
           thresholds: Optional[Thresholds] = None) -> Assessment:
    """Compare the two sides and classify. Pure function — no I/O, easy to test."""
    th = thresholds or Thresholds()
    ev = Evidence(location=pos_sales.location, business_date=pos_sales.business_date,
                  pos=pos_sales.source, pos_sales=pos_sales, supy_sales=supy_sales,
                  sync=sync, connection=connection)

    # 1. Could we even ask the POS? Never let this masquerade as "no sales".
    if not pos_sales.available:
        ev.reasons.append(
            f"POS did not answer: {pos_sales.error or 'unknown error'}. "
            f"No conclusion can be drawn about whether sales exist.")
        return Assessment(Verdict.POS_UNAVAILABLE, 0, ev)

    # 2. Is the Supy side wired at all? Say so rather than inventing a verdict.
    if supy_sales is None or not supy_sales.available:
        ev.reasons.append(
            "Supy ingestion figures unavailable, so the two sides cannot be "
            "compared. Detection is blind until the Supy adapter is connected.")
        if pos_sales.has_sales:
            ev.reasons.append(
                f"POS reports {pos_sales.checks} check(s) / {pos_sales.gross:,.2f} "
                f"for this date.")
        return Assessment(Verdict.UNKNOWN, 50, ev)

    pos_c, supy_c = pos_sales.checks, supy_sales.checks
    pos_g, supy_g = pos_sales.gross, supy_sales.gross

    stale_h = _hours_since(sync.last_success) if sync else None
    if stale_h is not None and stale_h > th.stale_sync_hours:
        ev.reasons.append(
            f"Last successful ingestion was {stale_h:.0f}h ago "
            f"({sync.last_success}).")

    # 3. Neither side has anything — the site was closed. This is HEALTHY.
    if not pos_sales.has_sales and supy_c == 0 and supy_g == 0:
        ev.reasons.append(
            "Both POS and Supy report zero. The location did not trade; "
            "this is not a sync failure.")
        return Assessment(Verdict.NO_ACTIVITY, 100, ev)

    # 4. POS has sales, Supy has nothing — the TREAT'S case.
    if pos_sales.has_sales and supy_c == 0 and supy_g == 0:
        ev.reasons.append(
            f"POS reports {pos_c} check(s) totalling {pos_g:,.2f}; Supy has "
            f"received nothing for this date.")
        if sync and sync.status == "nothing_to_fetch":
            ev.reasons.append(
                "The integration reports 'nothing to fetch', which contradicts "
                "the POS. That points at ingestion state rather than absent sales.")
        health = 10 if not stale_h or stale_h < 72 else 0
        return Assessment(Verdict.BROKEN, health, ev)

    # 5. Some arrived, but not all.
    if pos_c > 0 and supy_c < pos_c * th.partial_ratio:
        pct = supy_c / pos_c * 100 if pos_c else 0
        ev.reasons.append(
            f"Supy has {supy_c} of {pos_c} check(s) ({pct:.0f}%). Ingestion "
            f"started but did not complete.")
        return Assessment(Verdict.PARTIAL, max(20, int(pct * 0.6)), ev)

    # 6. Counts line up, money does not.
    if pos_g > 0:
        drift = abs(pos_g - supy_g) / pos_g * 100
        if drift > th.money_tolerance_pct:
            ev.reasons.append(
                f"Check counts agree ({pos_c}), but totals differ by "
                f"{drift:.2f}% ({pos_g:,.2f} vs {supy_g:,.2f}). Suggests a "
                f"mapping, tax or discount handling difference rather than a "
                f"transport failure.")
            return Assessment(Verdict.RECONCILE_FAIL, 60, ev)

    ev.reasons.append(
        f"POS and Supy agree: {pos_c} check(s), {pos_g:,.2f}.")
    return Assessment(Verdict.HEALTHY, 100, ev)
