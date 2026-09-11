"""
The Supy side of the comparison.

⚠️  NOT WIRED. This is the missing half, and it is the half that matters.

Sync Sentinel's entire value is comparing "what the POS says happened" against
"what Supy actually ingested". The POS half is real — the Simphony adapter
reads live figures today. The Supy half needs either:

    * a read API:  GET sales summary for (location, business_date)
                   -> {checks, gross, last_ingested_at}
    * or database read access to the ingestion tables
    * or, at minimum, the sync job's checkpoint/cursor per location

Without one of those, the agent runs in POS-ONLY mode: it can see that sales
exist, but not whether Supy received them, so every verdict is UNKNOWN.

This is stated loudly and fails loudly on purpose. A stub that returned zeros
would make every location look BROKEN, and a stub that returned the POS
figures back would make every location look HEALTHY. Both are worse than an
honest "not connected" — a monitoring system that lies about its own coverage
is the thing it is supposed to prevent.
"""

from __future__ import annotations

import os
from datetime import date
from typing import Optional

from .adapters.base import SalesSnapshot, SyncStatus


class SupyNotConfigured(RuntimeError):
    pass


class SupyClient:
    """
    Implement ONE of fetch_sales / fetch_sync_status against whatever access
    is granted, and the whole engine starts working.
    """

    def __init__(self, base_url: str = "", api_key: str = ""):
        self.base_url = base_url or os.environ.get("SUPY_API_URL", "")
        self.api_key = api_key or os.environ.get("SUPY_API_KEY", "")

    @property
    def configured(self) -> bool:
        return bool(self.base_url and self.api_key)

    def fetch_sales(self, location: str, business_date: date) -> SalesSnapshot:
        """
        What Supy holds for this location and business date.

        available=False (not zeros) when unconfigured — see the module note.
        """
        if not self.configured:
            return SalesSnapshot(
                location=location, business_date=business_date,
                available=False, source="supy",
                error="SUPY_API_URL / SUPY_API_KEY not set — Supy-side figures "
                      "unavailable, so no comparison can be made.")

        # ------------------------------------------------------------------
        # Expected shape once access exists:
        #
        #   GET {base_url}/sales/summary?location={location}&date={iso}
        #   -> {"checks": 143, "gross": 12450.20, "last_ingested_at": "..."}
        #
        # import requests
        # r = requests.get(f"{self.base_url}/sales/summary",
        #                  params={"location": location,
        #                          "date": business_date.isoformat()},
        #                  headers={"Authorization": f"Bearer {self.api_key}"},
        #                  timeout=30)
        # r.raise_for_status()
        # d = r.json()
        # return SalesSnapshot(location=location, business_date=business_date,
        #                      checks=int(d["checks"]), gross=float(d["gross"]),
        #                      available=True, source="supy")
        # ------------------------------------------------------------------
        raise SupyNotConfigured(
            "SupyClient.fetch_sales is not implemented. Wire it to the Supy "
            "sales-summary read API or the ingestion tables.")

    def fetch_sync_status(self, location: str) -> SyncStatus:
        if not self.configured:
            return SyncStatus(location=location, status="unknown",
                              message="Supy client not configured.")
        raise SupyNotConfigured(
            "SupyClient.fetch_sync_status is not implemented. Wire it to the "
            "ingestion checkpoint for this location.")

    def replay(self, location: str, business_date: date) -> bool:
        """
        Ask Supy to re-ingest one business date.

        This is the recovery action, and it is the one place the agent changes
        production state. It must be idempotent on the Supy side before it is
        ever enabled automatically — replaying into a loader that appends
        rather than upserts turns a missing-data incident into a
        double-counted-revenue incident, which is strictly worse.
        """
        raise SupyNotConfigured(
            "SupyClient.replay is not implemented. Do NOT enable auto-recovery "
            "until the ingestion endpoint is confirmed idempotent.")
