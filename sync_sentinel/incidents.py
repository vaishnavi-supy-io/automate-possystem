"""
Incident state machine.

A monitor that re-alerts on every cycle gets muted; one that never re-alerts
lets a broken feed rot. The state machine exists so an incident is announced
once, escalated if it persists, and closed automatically when the data
actually arrives.

    HEALTHY
       -> DETECTED        first failing check
       -> DIAGNOSED       cause identified (rules or model)
       -> RECOVERING      a safe replay was attempted
       -> VERIFYING       replay done, waiting to confirm
       -> RECOVERED       verified fixed -> back to HEALTHY
       -> ESCALATED       recovery unsafe, failed, or still broken after N cycles

Recovery is only ever ATTEMPTED, never assumed. The transition out of
RECOVERING goes through VERIFYING, and verification uses the same
deterministic comparison as detection — the agent does not get to mark its own
homework.
"""

from __future__ import annotations

import json
import pathlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class State(str, Enum):
    HEALTHY = "HEALTHY"
    DETECTED = "DETECTED"
    DIAGNOSED = "DIAGNOSED"
    RECOVERING = "RECOVERING"
    VERIFYING = "VERIFYING"
    RECOVERED = "RECOVERED"
    ESCALATED = "ESCALATED"


OPEN_STATES = {State.DETECTED, State.DIAGNOSED, State.RECOVERING,
               State.VERIFYING, State.ESCALATED}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class Incident:
    key: str                       # location|business_date
    location: str
    business_date: str
    state: str = State.DETECTED.value
    verdict: str = ""
    opened_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    closed_at: Optional[str] = None
    cycles: int = 0                # failing checks seen since opening
    alerts_sent: int = 0
    recovery_attempts: int = 0
    diagnosis: Optional[dict] = None
    timeline: list = field(default_factory=list)

    def log(self, event: str) -> None:
        self.timeline.append({"at": _now(), "event": event})
        self.updated_at = _now()

    def to(self, state: State, why: str = "") -> None:
        self.log(f"{self.state} -> {state.value}" + (f": {why}" if why else ""))
        self.state = state.value

    @property
    def is_open(self) -> bool:
        return State(self.state) in OPEN_STATES


class IncidentStore:
    """
    Flat JSON store. Deliberately boring — the value is in the state
    transitions, not the persistence layer, and a file that a human can read
    during an outage beats a database they cannot.
    """

    def __init__(self, path: pathlib.Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: dict = {}
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text())
            except (OSError, json.JSONDecodeError):
                self._data = {}

    def get(self, key: str) -> Optional[Incident]:
        raw = self._data.get(key)
        return Incident(**raw) if raw else None

    def put(self, inc: Incident) -> None:
        self._data[inc.key] = asdict(inc)
        self.save()

    def save(self) -> None:
        try:
            self.path.write_text(json.dumps(self._data, indent=2, sort_keys=True))
        except OSError:
            pass

    def open_incidents(self) -> list:
        out = []
        for raw in self._data.values():
            inc = Incident(**raw)
            if inc.is_open:
                out.append(inc)
        return sorted(out, key=lambda i: i.opened_at)

    def history_for(self, location: str, limit: int = 5) -> list:
        rows = [Incident(**r) for r in self._data.values()
                if r.get("location") == location]
        rows.sort(key=lambda i: i.opened_at, reverse=True)
        return [{"business_date": r.business_date, "state": r.state,
                 "verdict": r.verdict, "opened_at": r.opened_at,
                 "cycles": r.cycles} for r in rows[:limit]]


def advance(store: IncidentStore, key: str, location: str, business_date: str,
            verdict: str, healthy: bool, escalate_after: int = 3) -> Incident:
    """
    Move an incident along based on the latest assessment.

    Healthy closes an open incident (via RECOVERED) and is a no-op otherwise —
    so a feed that fixes itself resolves without anyone touching it, and the
    timeline records that it did.
    """
    inc = store.get(key)

    if healthy:
        if inc and inc.is_open:
            inc.to(State.RECOVERED, f"verified healthy ({verdict})")
            inc.state = State.HEALTHY.value
            inc.closed_at = _now()
            store.put(inc)
        return inc or Incident(key=key, location=location,
                               business_date=business_date,
                               state=State.HEALTHY.value, verdict=verdict)

    if inc is None or not inc.is_open:
        inc = Incident(key=key, location=location, business_date=business_date,
                       verdict=verdict)
        inc.log(f"opened: {verdict}")
        store.put(inc)
        return inc

    inc.cycles += 1
    inc.verdict = verdict
    if inc.cycles >= escalate_after and inc.state != State.ESCALATED.value:
        inc.to(State.ESCALATED,
               f"still failing after {inc.cycles} checks")
    else:
        inc.log(f"still failing ({verdict}), cycle {inc.cycles}")
    store.put(inc)
    return inc
