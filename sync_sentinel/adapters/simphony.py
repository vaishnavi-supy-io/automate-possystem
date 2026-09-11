"""
Oracle MICROS Simphony adapter (Reporting & Analytics).

Reads "Menu Item Sales by Definition" straight from its report URL, skipping
the Favourites navigation — the report id is in the address bar, so there is
no menu to click and nothing to break when Oracle restyles.

WHAT THIS REPORT DOES AND DOES NOT GIVE YOU (measured 2026-09-11, IRM):

    Menu Item Name | Menu Item # | Qty Sold | Returns |
    Net VAT Before Disc. | Discounts VAT | Net VAT After Disc. |
    Gross Before Disc. | Discounts | Gross After Disc.

    Totals:  20,152 qty  ...  522,566.25 gross after disc.

  * There is NO transaction or check count. `Qty Sold` is items — 20,152
    items is not 143 checks. Anything per-transaction needs another report
    (the portal's "Guest Checks" section is the likely source).
  * There are no timestamps. This is a daily aggregate per business date, so
    "how old is the newest record" and hourly baselines are not available.
  * The figure that matters is GROSS AFTER DISC. — what actually transacted.
    Comparing against any of the other three money columns produces permanent
    reconciliation noise.

Login is the fragile part and has been made literal on purpose: it reproduces
the exact sequence that authenticates, because refactored variants of it
silently failed. See _login().
"""

from __future__ import annotations

import os
import pathlib
import re
from datetime import date
from typing import Optional

from .base import ConnectionStatus, Location, POSAdapter, SalesSnapshot

STATE_DIR = pathlib.Path(__file__).parent.parent.parent / "state" / "sync_sentinel"

_MONEY = r"\(?-?[\d,]+\.\d{2}\)?"


class SimphonyAdapter(POSAdapter):
    name = "simphony"

    def __init__(self, tenant: str, login_url: str, report_url: str,
                 username_env: str, enterprise_env: str, password_env: str,
                 locations: Optional[list] = None, headless: bool = True):
        self.tenant = tenant
        self.login_url = login_url
        self.report_url = report_url
        self.username = os.environ.get(username_env, "")
        self.enterprise = os.environ.get(enterprise_env, "")
        self.password = os.environ.get(password_env, "")
        self._locations = locations or []
        self.headless = headless
        STATE_DIR.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------- login

    def _login(self, page) -> bool:
        """
        The literal sequence that works. Do not "tidy" this.

        Three refactors of it failed in ways that looked like bad credentials:
        a wait_for_selector instead of the fixed pause, an early session-reuse
        check, and a cached storage_state. The last was the worst — a stale
        cookie puts the form into a different state, and the adapter then
        reported "sign-in did not complete" while a standalone script using
        this exact order logged in first time.

        No storage_state is loaded anywhere in this adapter for that reason.
        A fresh login every run costs ~25s and is reliable; the cached one was
        fast and wrong.
        """
        page.goto(self.login_url, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_timeout(6000)

        org = "input[name='org-name-input']"
        usr = "input[name='user-name-input']"
        pwd = "input[name='password-input']"

        if page.locator(org).count():
            page.fill(org, self.enterprise)
        if page.locator(usr).count():
            page.fill(usr, self.username)

        # Two-step form: enterprise + user, then Next reveals the password.
        nxt = page.locator("button:has-text('Next')")
        if nxt.count():
            nxt.first.click()
            page.wait_for_timeout(5000)

        if not page.locator(pwd).count():
            return False
        page.fill(pwd, self.password)
        page.click("button:has(span.oj-button-text:text-is('Sign In'))")
        page.wait_for_load_state("domcontentloaded", timeout=60000)
        page.wait_for_timeout(15000)
        return "oidc-ui" not in page.url

    # ------------------------------------------------------------ interface

    def list_locations(self) -> list:
        return [Location(id=l, name=l, pos=self.name) for l in self._locations]

    def get_connection_status(self, location: str) -> ConnectionStatus:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            b = p.chromium.launch(headless=self.headless)
            ctx = b.new_context(locale="en-GB",
                                viewport={"width": 1500, "height": 950})
            pg = ctx.new_page()
            try:
                ok = self._login(pg)
                return ConnectionStatus(location=location, reachable=True,
                                        authenticated=ok,
                                        detail="signed in" if ok
                                        else f"stalled at {pg.url[:70]}")
            except Exception as exc:
                return ConnectionStatus(location=location, reachable=False,
                                        detail=f"{type(exc).__name__}: {exc}"[:150])
            finally:
                ctx.close(); b.close()

    def get_sales(self, location: str, business_date: date) -> SalesSnapshot:
        """
        Read the report's Totals row.

        NOTE ON DATE: the report's Business Dates parameter defaults to
        "Yesterday" and is not settable from the URL. This returns whatever
        the report's current parameter yields — for a daily run that is
        yesterday, which is what a morning health check wants. Arbitrary dates
        need the Edit Parameters panel driven, which is not implemented; until
        it is, history is built by RECORDING each daily run rather than by
        asking for past dates.
        """
        from playwright.sync_api import sync_playwright
        snap = SalesSnapshot(location=location, business_date=business_date,
                             source=self.name)
        with sync_playwright() as p:
            b = p.chromium.launch(headless=self.headless)
            ctx = b.new_context(locale="en-GB", accept_downloads=True,
                                viewport={"width": 1500, "height": 950})
            pg = ctx.new_page()
            try:
                if not self._login(pg):
                    snap.available = False
                    snap.error = (f"Simphony sign-in did not complete "
                                  f"(stalled at {pg.url[:70]}) — cannot tell "
                                  f"whether sales exist.")
                    return snap

                pg.goto(self.report_url, timeout=60000,
                        wait_until="domcontentloaded")
                pg.wait_for_timeout(25000)

                if "oidc-ui" in pg.url:
                    snap.available = False
                    snap.error = "Redirected to sign-in when opening the report."
                    return snap

                text = pg.inner_text("body")
                qty, gross = self._parse_totals(text)
                if qty is None and gross is None:
                    snap.available = False
                    snap.error = (f"Report loaded ({len(text)} chars) but no "
                                  f"Totals row was found. Layout may have "
                                  f"changed, or the report returned no data.")
                    return snap

                # checks stays 0 — this report has no transaction count, and
                # inventing one from Qty Sold would be a lie the engine would
                # then reason over.
                snap.checks = 0
                snap.gross = gross or 0.0
                snap.error = None if qty is None else f"qty_sold={qty}"
                return snap
            except Exception as exc:
                snap.available = False
                snap.error = f"{type(exc).__name__}: {exc}"[:200]
                return snap
            finally:
                ctx.close(); b.close()

    # -------------------------------------------------------------- parsing

    @staticmethod
    def _parse_totals(text: str):
        """
        Read (qty_sold, gross_after_discount) from the Totals row.

        The row looks like:
            Totals:  20,152  0    516,910.48  (19,210.77)  497,699.71
                                  542,756.00  (20,189.75)  522,566.25

        Gross After Disc. is the LAST money figure on the line. Taking the
        first would give Net VAT Before Disc. — 516,910.48 against a true
        522,566.25, a 1% error that would read as a permanent reconciliation
        failure on every single location.

        Returns (None, None) when no Totals row is present; the caller turns
        that into available=False rather than a zero.
        """
        for line in text.splitlines():
            if not line.strip().lower().startswith("totals"):
                continue
            qty_m = re.search(r"Totals?:?\s*\|?\s*([\d,]+)\b", line)
            monies = re.findall(_MONEY, line)
            if not monies:
                continue

            def val(s):
                neg = s.startswith("(") and s.endswith(")")
                v = float(s.strip("()").replace(",", ""))
                return -v if neg else v

            qty = int(qty_m.group(1).replace(",", "")) if qty_m else None
            return qty, val(monies[-1])
        return None, None
