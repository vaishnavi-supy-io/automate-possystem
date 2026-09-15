"""
Sync Sentinel — run a check across locations and report.

    python -m sync_sentinel.cli --date 2026-09-10
    python -m sync_sentinel.cli --date 2026-09-10 --json
    python -m sync_sentinel.cli --demo        # the TREAT'S case, no credentials

Exit: 0 all healthy · 1 needs attention · 2 something broken
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
from datetime import date, datetime, timedelta

from .adapters.base import SalesSnapshot, SyncStatus
from .diagnose import diagnose
from .engine import Verdict, assess
from .incidents import IncidentStore, advance

BASE = pathlib.Path(__file__).parent.parent
STORE = BASE / "state" / "sync_sentinel" / "incidents.json"


def _demo_cases(d: date) -> list:
    """
    The four failure modes, using the real TREAT'S numbers.

    Exists so the engine can be exercised end to end before the Supy API is
    connected — and so the classification can be reviewed by someone who does
    not want to read the rule code.
    """
    return [
        ("TREAT'S-UM AL EMARAT",
         SalesSnapshot("TREAT'S-UM AL EMARAT", d, 143, 12450.20, True, "simphony"),
         SalesSnapshot("TREAT'S-UM AL EMARAT", d, 0, 0.0, True, "supy"),
         SyncStatus("TREAT'S-UM AL EMARAT", last_success="2026-09-07T02:14:00Z",
                    last_attempt="2026-09-11T14:30:00Z",
                    status="nothing_to_fetch",
                    cursor="last_processed_business_date=2026-09-07")),
        ("PARTIAL EXAMPLE",
         SalesSnapshot("PARTIAL EXAMPLE", d, 125, 9800.00, True, "simphony"),
         SalesSnapshot("PARTIAL EXAMPLE", d, 84, 6590.00, True, "supy"),
         SyncStatus("PARTIAL EXAMPLE", last_success=f"{d}T02:10:00Z", status="ok")),
        ("RECONCILE EXAMPLE",
         SalesSnapshot("RECONCILE EXAMPLE", d, 90, 12450.00, True, "simphony"),
         SalesSnapshot("RECONCILE EXAMPLE", d, 90, 11930.00, True, "supy"),
         SyncStatus("RECONCILE EXAMPLE", last_success=f"{d}T02:10:00Z", status="ok")),
        ("CLOSED MONDAY",
         SalesSnapshot("CLOSED MONDAY", d, 0, 0.0, True, "simphony"),
         SalesSnapshot("CLOSED MONDAY", d, 0, 0.0, True, "supy"),
         SyncStatus("CLOSED MONDAY", last_success=f"{d}T02:10:00Z", status="ok")),
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description="POS -> Supy sync health agent")
    ap.add_argument("--date", help="Business date (default: yesterday)")
    ap.add_argument("--demo", action="store_true",
                    help="Run the four failure modes with sample data")
    ap.add_argument("--json", action="store_true", help="Machine-readable output")
    ap.add_argument("--no-diagnose", action="store_true",
                    help="Skip the diagnosis step")
    args = ap.parse_args()

    d = (datetime.fromisoformat(args.date).date() if args.date
         else date.today() - timedelta(days=1))

    if not args.demo:
        print("Live mode needs the Supy adapter wired "
              "(see sync_sentinel/supy_client.py).\n"
              "Run with --demo to exercise the engine.", file=sys.stderr)
        return 1

    store = IncidentStore(STORE)
    results, worst = [], 0

    for name, pos, supy, sync in _demo_cases(d):
        a = assess(pos, supy, sync)
        key = f"{name}|{d.isoformat()}"
        healthy = a.verdict in (Verdict.HEALTHY, Verdict.NO_ACTIVITY)
        inc = advance(store, key, name, d.isoformat(), a.verdict.value, healthy)

        dx = None
        if a.needs_attention and not args.no_diagnose:
            dx = diagnose(a, history=store.history_for(name))
            inc.diagnosis = {"likely_cause": dx.likely_cause,
                             "confidence": dx.confidence, "by": dx.by}
            store.put(inc)

        worst = max(worst, 2 if a.verdict is Verdict.BROKEN
                    else 1 if a.needs_attention else 0)
        results.append((name, a, inc, dx))

    if args.json:
        print(json.dumps([{
            "location": n, "verdict": a.verdict.value, "health": a.health,
            "state": i.state, "evidence": a.evidence.as_dict(),
            "diagnosis": (None if not dx else {
                "likely_cause": dx.likely_cause, "confidence": dx.confidence,
                "next_checks": dx.next_checks, "by": dx.by}),
        } for n, a, i, dx in results], indent=2))
        return worst

    print(f"\n{'=' * 70}\n SYNC SENTINEL — business date {d}\n{'=' * 70}")
    tally = {}
    for _n, a, _i, _dx in results:
        tally[a.icon] = tally.get(a.icon, 0) + 1
    print("  " + "   ".join(f"{k} {v}" for k, v in sorted(tally.items())))

    for name, a, inc, dx in results:
        print(f"\n{'-' * 70}\n{a.icon} {name}   {a.verdict.value}   "
              f"health {a.health}/100   [{inc.state}]")
        pos, supy = a.evidence.pos_sales, a.evidence.supy_sales
        print(f"    POS  : {pos.checks:>5} checks  {pos.gross:>12,.2f}")
        if supy:
            print(f"    Supy : {supy.checks:>5} checks  {supy.gross:>12,.2f}")
        if a.evidence.sync:
            s = a.evidence.sync
            print(f"    sync : {s.status}   last success {s.last_success}")
            if s.cursor:
                print(f"    cursor: {s.cursor}")
        for r in a.evidence.reasons:
            print(f"    · {r}")
        if dx:
            print()
            for line in dx.render().splitlines():
                print(f"    {line}")

    print(f"\n{'=' * 70}")
    return worst


if __name__ == "__main__":
    sys.exit(main())
