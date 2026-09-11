"""
POS adapter interface.

The whole point of Sync Sentinel is that it should not care which POS a
location runs. Simphony, Sapaad, Oracle BI, a file drop on SFTP — each one
answers the same four questions, and everything above this layer works on the
normalised answers.

Add a POS by writing an adapter, not by touching the engine.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import date
from typing import Optional


@dataclass
class SalesSnapshot:
    """
    What a POS says happened at one location on one business date.

    `checks` and `gross` are both carried because they fail differently:
    a partial ingestion usually shows fewer checks, while a mapping or
    rounding fault shows the same checks with a different total.

    `available` distinguishes "the POS told us there were no sales" from
    "we could not ask". Conflating those is how a broken connection gets
    reported as a quiet day — the exact mistake this system exists to stop.
    """
    location: str
    business_date: date
    checks: int = 0
    gross: float = 0.0
    available: bool = True
    source: str = ""
    error: Optional[str] = None

    @property
    def has_sales(self) -> bool:
        return self.available and (self.checks > 0 or self.gross > 0)


@dataclass
class SyncStatus:
    """What the integration itself claims about its own progress."""
    location: str
    last_success: Optional[str] = None      # ISO timestamp
    last_attempt: Optional[str] = None
    status: str = "unknown"                 # ok | nothing_to_fetch | error | unknown
    cursor: Optional[str] = None            # e.g. last_processed_business_date
    message: str = ""


@dataclass
class ConnectionStatus:
    location: str
    reachable: bool = False
    authenticated: bool = False
    detail: str = ""
    checked_at: Optional[str] = None


@dataclass
class Location:
    id: str
    name: str
    pos: str
    timezone: str = "UTC"
    # Some sites genuinely do not trade every day. Without this, a Monday
    # closure looks identical to a broken feed.
    trading_days: list = field(default_factory=lambda: [0, 1, 2, 3, 4, 5, 6])


class POSAdapter(abc.ABC):
    """
    One adapter per POS. Four questions, normalised answers.

    Implementations must NEVER return an empty SalesSnapshot to signal a
    failure — set available=False and populate `error`. A zero that means
    "couldn't ask" is indistinguishable from a zero that means "no trade",
    and that ambiguity is the entire bug class this project targets.
    """

    name: str = "unknown"

    @abc.abstractmethod
    def list_locations(self) -> list[Location]:
        ...

    @abc.abstractmethod
    def get_sales(self, location: str, business_date: date) -> SalesSnapshot:
        ...

    def get_sync_status(self, location: str) -> SyncStatus:
        """Optional: most POS systems expose nothing here."""
        return SyncStatus(location=location, status="unknown")

    def get_connection_status(self, location: str) -> ConnectionStatus:
        return ConnectionStatus(location=location, reachable=False,
                                detail="not implemented")

    def retry_sync(self, location: str, business_date: date) -> bool:
        """
        Ask the POS side to re-send a date. Returning False is fine and
        common — most POS systems have no such control, and recovery then
        has to happen on the Supy side instead.
        """
        return False
