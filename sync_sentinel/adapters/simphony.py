"""
Oracle MICROS Simphony adapter (Reporting & Analytics).

Reads the "Menu Item Sales by Definition" report straight from its report URL,
skipping the Favourites navigation entirely — the report id is in the address
bar, so there is no menu to click and nothing to break when Oracle restyles.

Login is adaptive on purpose. The same Oracle sign-in renders as either a
single form (enterprise + user + password together) or a two-step flow
(enterprise + user, Next, then password), and which one you get varies between
environments and visits. Assuming one shape silently fails to log in and then
reports "no sales", which is precisely the false negative this project exists
to eliminate — so the adapter detects the shape at run time and raises rather
than returning zeros if it cannot get in.
"""

from __future__ import annotations

import os
import pathlib
import re
from datetime import date
from typing import Optional

from .base import ConnectionStatus, Location, POSAdapter, SalesSnapshot

STATE_DIR = pathlib.Path(__file__).parent.parent.parent / "state" / "sync_sentinel"


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
        self._storage = STATE_DIR / f"{tenant}_storage_state.json"
        STATE_DIR.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------- login

    def _login(self, page) -> bool:
        """
        Sign in, tolerating both the one-step and two-step Oracle forms.

        Returns False rather than raising so the caller can turn it into an
        explicit ConnectionStatus / unavailable snapshot.
        """
        page.goto(self.login_url, timeout=60000, wait_until="domcontentloaded")
        page.wait_for_timeout(5000)

        if "portal" in page.url and "oidc-ui" not in page.url:
            return True                       # cached session still valid

        org = "input[name='org-name-input']"
        usr = "input[name='user-name-input']"
        pwd = "input[name='password-input']"

        if page.locator(org).count():
            page.fill(org, self.enterprise)
        if page.locator(usr).count():
            page.fill(usr, self.username)

        # One-step form: the password box is already on screen.
        if page.locator(pwd).count() and page.locator(pwd).first.is_visible():
            page.fill(pwd, self.password)
        else:
            nxt = page.locator("button:has-text('Next')")
            if not nxt.count():
                return False
            nxt.first.click()
            # WAIT for the password step, do not sleep at it. A fixed delay
            # here returned "sign-in did not complete" whenever Oracle took
            # longer than the guess — a timing flake that reads exactly like
            # bad credentials, and would have been reported as a portal
            # outage rather than a slow page.
            try:
                page.wait_for_selector(pwd, state="visible", timeout=30000)
            except Exception:
                return False
            page.fill(pwd, self.password)

        # The submit button is NOT always the Oracle JET markup. On the
        # two-step form it is <button><span class="oj-button-text">Sign In</span>,
        # but on the one-step form it is a plain button — so a selector
        # requiring the span times out and the sign-in silently never happens.
        # Try the specific shape first, then fall back to text.
        for sel in ("button:has(span.oj-button-text:text-is('Sign In'))",
                    "button:has-text('Sign In')",
                    "button[type='submit']"):
            btn = page.locator(sel)
            if btn.count():
                btn.first.click()
                break
        else:
            return False
        page.wait_for_load_state("domcontentloaded", timeout=60000)
        page.wait_for_timeout(14000)
        return "oidc-ui" not in page.url

    # ------------------------------------------------------------ interface

    def list_locations(self) -> list:
        return [Location(id=l, name=l, pos=self.name) for l in self._locations]

    def get_connection_status(self, location: str) -> ConnectionStatus:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            b = p.chromium.launch(headless=self.headless)
            ctx = b.new_context(locale="en-GB",
                                viewport={"width": 1600, "height": 1000})
            pg = ctx.new_page()
            try:
                ok = self._login(pg)
                return ConnectionStatus(
                    location=location, reachable=True, authenticated=ok,
                    detail="signed in" if ok else "sign-in did not complete")
            except Exception as exc:
                return ConnectionStatus(location=location, reachable=False,
                                        detail=f"{type(exc).__name__}: {exc}"[:160])
            finally:
                ctx.close(); b.close()

    def get_sales(self, location: str, business_date: date) -> SalesSnapshot:
        from playwright.sync_api import sync_playwright
        snap = SalesSnapshot(location=location, business_date=business_date,
                             source=self.name)
        with sync_playwright() as p:
            b = p.chromium.launch(headless=self.headless)
            kw = {"locale": "en-GB", "viewport": {"width": 1600, "height": 1000}}
            if self._storage.exists():
                kw["storage_state"] = str(self._storage)
            ctx = b.new_context(**kw)
            pg = ctx.new_page()
            try:
                if not self._login(pg):
                    # Capture WHERE it stalled. "Sign-in did not complete" on
                    # its own sent me round several blind retries; the final
                    # URL plus which fields were on screen identifies the step
                    # immediately, and costs nothing.
                    try:
                        fields = {
                            n: pg.locator(s).count()
                            for n, s in (("org", "input[name='org-name-input']"),
                                         ("user", "input[name='user-name-input']"),
                                         ("pwd", "input[name='password-input']"))}
                        btns = pg.eval_on_selector_all(
                            "button", "e=>e.map(x=>(x.innerText||'').trim())"
                                      ".filter(Boolean).slice(0,4)")
                        head = pg.inner_text("body")[:120].replace("\n", " ")
                    except Exception:
                        fields, btns, head = {}, [], ""
                    snap.available = False
                    snap.error = (
                        f"Simphony sign-in did not complete — cannot tell "
                        f"whether sales exist. stalled_at={pg.url[:90]} "
                        f"fields={fields} buttons={btns} page={head!r}")
                    return snap
                try:
                    ctx.storage_state(path=str(self._storage))
                except Exception:
                    pass

                pg.goto(self.report_url, timeout=60000,
                        wait_until="domcontentloaded")
                pg.wait_for_timeout(20000)

                if "oidc-ui" in pg.url:
                    snap.available = False
                    snap.error = "Redirected back to sign-in when opening the report."
                    return snap

                text = pg.inner_text("body")
                checks, gross = self._parse_totals(text)
                if checks is None and gross is None:
                    snap.available = False
                    snap.error = ("Report rendered but no totals could be read "
                                  f"({len(text)} chars). Parameters may need "
                                  f"setting, or the layout has changed.")
                    return snap
                snap.checks = checks or 0
                snap.gross = gross or 0.0
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
        Pull check count and gross from the rendered report.

        Returns (None, None) when nothing matches — the caller turns that into
        available=False. Never guess a zero: a zero that means "could not read
        the page" is indistinguishable from a zero that means "no trade", and
        that ambiguity is the whole bug class.
        """
        def num(pattern):
            m = re.search(pattern, text, re.I)
            if not m:
                return None
            try:
                return float(m.group(1).replace(",", ""))
            except ValueError:
                return None

        gross = (num(r"(?:gross\s+sales|total\s+sales)[^\d\-]{0,20}([\d,]+\.\d{2})")
                 or num(r"Totals?:[^\d\-]{0,20}([\d,]+\.\d{2})"))
        checks = num(r"(?:check\s+count|checks|guest\s+count)[^\d\-]{0,20}([\d,]+)")
        return (int(checks) if checks is not None else None, gross)
