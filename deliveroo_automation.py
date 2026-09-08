"""
deliveroo_automation.py
-----------------------
Deliveroo Partner Hub Automation Pipeline — 4-Stage Orchestrator

Stage 1: Authentication   (Playwright — login + session caching)
Stage 2: Report + Download (Playwright — Items Sold wizard, async poll, download)
Stage 3: Transformation   (Pandas — raw → Supy upload format)
Stage 4: Email            (smtplib — attach .xlsx and send via Gmail)

Differs from the Oracle BI pipeline in two ways:
  * The report is generated asynchronously — Stage 2 submits a wizard, then
    polls the generated-reports list until a download link becomes active.
  * An empty report is a VALID outcome, not a failure: it means no sales in
    the period, so there is nothing to upload. Exits 0.

Usage:
    python deliveroo_automation.py                        # yesterday, headless, email
    python deliveroo_automation.py --debug                # headed browser, verbose
    python deliveroo_automation.py --date 2026-08-05       # single day
    python deliveroo_automation.py --from 2026-08-01 --to 2026-08-05
    python deliveroo_automation.py --no-email             # save locally only
    python deliveroo_automation.py --from-stage 3         # replay transform only
    python deliveroo_automation.py --discover-columns     # print raw headers and exit
    python deliveroo_automation.py --force-login          # ignore cached session

Exit codes:
    0  success (including "no sales")
    1  AuthError
    2  NavError
    3  TransformError
    4  EmailError
"""

import argparse
import functools
import json
import os
import re
import pathlib
import smtplib
import sys
import time
import traceback
import uuid
from datetime import datetime, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import pandas as pd
import yaml
from dotenv import load_dotenv
from playwright.sync_api import Page, sync_playwright

# ──────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ──────────────────────────────────────────────────────────────────────────────

load_dotenv()

BASE_DIR = pathlib.Path(__file__).parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
OUTPUT_DIR = BASE_DIR / "output"
STATE_DIR = BASE_DIR / "state" / "deliveroo"
LOGS_DIR = BASE_DIR / "logs"
SCREENSHOTS_DIR = BASE_DIR / "screenshots"

for d in (DOWNLOADS_DIR, OUTPUT_DIR, STATE_DIR, LOGS_DIR, SCREENSHOTS_DIR):
    d.mkdir(parents=True, exist_ok=True)

STORAGE_STATE_PATH = STATE_DIR / "storage_state.json"
CHECKPOINT_PATH = STATE_DIR / "checkpoint.json"

with open(BASE_DIR / "deliveroo_config.yaml") as _f:
    CONFIG = yaml.safe_load(_f)


# ──────────────────────────────────────────────────────────────────────────────
# Custom Exceptions
# ──────────────────────────────────────────────────────────────────────────────

class AuthError(Exception):
    """Wrong credentials or session invalid — do NOT retry."""


class NavError(Exception):
    """Wizard navigation or download failure — retryable."""


class ConfigError(Exception):
    """A required selector or column is unconfigured — do NOT retry."""


class TransformError(Exception):
    """Data transformation failure — raw file is preserved."""


class EmailError(Exception):
    """Email delivery failure — report was generated but not sent."""


# ──────────────────────────────────────────────────────────────────────────────
# Run ID + Structured Logger
# ──────────────────────────────────────────────────────────────────────────────

RUN_ID = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
_log_path: Optional[pathlib.Path] = None
_verbose = False


def _init_logger(verbose: bool) -> None:
    global _log_path, _verbose
    _verbose = verbose
    _log_path = LOGS_DIR / f"deliveroo_{RUN_ID}.jsonl"


def log(stage: str, step: str, outcome: str, duration_ms: int = 0, extra: dict = None) -> None:
    """Append one structured JSONL record for this run."""
    record = {
        "ts": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        "run_id": RUN_ID,
        "pipeline": "deliveroo",
        "stage": stage,
        "step": step,
        "outcome": outcome,
        "duration_ms": duration_ms,
    }
    if extra:
        record.update(extra)
    if _log_path:
        with open(_log_path, "a") as f:
            f.write(json.dumps(record) + "\n")
    if _verbose:
        print(f"  [log] {stage}/{step} → {outcome}"
              + (f" ({duration_ms} ms)" if duration_ms else ""))


def screenshot(page: Page, stage: str, label: str) -> None:
    """Best-effort screenshot — never let a screenshot failure break the run."""
    try:
        path = SCREENSHOTS_DIR / f"deliveroo_{RUN_ID}_{stage}_{label}.png"
        page.screenshot(path=str(path), full_page=True)
        if _verbose:
            print(f"  [📸] {path.name}")
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Checkpointing
# ──────────────────────────────────────────────────────────────────────────────

def read_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        try:
            return json.loads(CHECKPOINT_PATH.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def write_checkpoint(stage: int, extra: dict = None) -> None:
    data = read_checkpoint()
    data["stage"] = stage
    data["run_id"] = RUN_ID
    data["ts"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    if extra:
        data.update(extra)
    try:
        CHECKPOINT_PATH.write_text(json.dumps(data, indent=2))
    except OSError:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Retry decorator
# ──────────────────────────────────────────────────────────────────────────────

def retry(max_attempts: int = 3, base_delay: float = 1.5, exceptions=(NavError,)):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*a, **kw):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*a, **kw)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        break
                    delay = base_delay * (2 ** (attempt - 1))
                    print(f"  [!] {fn.__name__} attempt {attempt}/{max_attempts} failed: {exc}. "
                          f"Retrying in {delay:.1f}s...", file=sys.stderr)
                    time.sleep(delay)
            raise last_exc
        return wrapper
    return decorator


# ──────────────────────────────────────────────────────────────────────────────
# Config validation
# ──────────────────────────────────────────────────────────────────────────────

# Selectors required for each phase, so the error message names only what is
# actually missing for the work being attempted.
REQUIRED_AUTH_SELECTORS = ("username_field", "password_field", "login_button")
REQUIRED_WIZARD_SELECTORS = (
    "reports_tab", "create_report_button",
    # report_type_items_sold is deliberately NOT required: there is no separate
    # type picker — the report type is decided by which "Create <type> report"
    # button is pressed. It was required here until 2026-09-02, so a config
    # that correctly left it blank failed the whole run before the browser
    # reached the wizard.
    "time_period_toggle", "time_period_custom",
    "date_from_field", "date_to_field", "calendar_day_cell",
    "continue_button", "site_checkbox_template", "submit_report_button",
    "report_rows", "report_row_ready_link",
)


def _require_selectors(keys: tuple) -> None:
    sel = CONFIG.get("selectors", {})
    missing = [k for k in keys if not sel.get(k)]
    if missing:
        raise ConfigError(
            "deliveroo_config.yaml has unconfigured selectors:\n"
            + "".join(f"    selectors.{k}\n" for k in missing)
            + "\n  Discover them with:  python debug_selectors.py --portal deliveroo\n"
              "  (or run with --debug to step through the wizard in a headed browser)"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Stage 1 — Authentication
# ──────────────────────────────────────────────────────────────────────────────

def _reports_url() -> str:
    p = CONFIG["portal"]
    return p["reports_url"].format(org_id=p["org_id"], branch_id=p["branch_id"])


def _dismiss_cookie_banner(page: Page, configured: str = "") -> str:
    """Best-effort: click the cookie-accept control if a banner is showing."""
    candidates = ([configured] if configured else []) + [
        "#onetrust-accept-btn-handler",
        "#accept-recommended-btn-handler",
        "button:has-text('Accept all')",
    ]
    for selector in candidates:
        try:
            el = page.query_selector(selector)
            if el and el.is_visible():
                el.click(timeout=5_000)
                page.wait_for_timeout(1_000)
                if _verbose:
                    print(f"  [🍪] Dismissed cookie banner via {selector!r}")
                return selector
        except Exception:
            continue
    return ""


def _block_overlay_resources(context) -> None:
    """Refuse to load third-party widgets that cover the wizard.

    A Medallia feedback-survey iframe intercepted every click on the date step
    (verified 2026-09-02) and cannot be closed reliably from the parent page —
    it lives in a cross-origin frame and comes back. Blocking the request is
    both simpler and stable. None of these patterns serve sales data.
    """
    patterns = CONFIG["selectors"].get("blocked_url_patterns") or []
    for pattern in patterns:
        try:
            context.route(pattern, lambda route: route.abort())
        except Exception:
            continue
    if patterns and _verbose:
        print(f"  [x] Blocking {len(patterns)} overlay resource pattern(s)")


def _clear_overlays(page: Page) -> None:
    """Clear everything that swallows clicks, before each wizard step.

    Two overlays cover Partner Hub: the OneTrust cookie banner and the
    "Partner Hub app" promo (a ReactModalPortal / tcl__Modal). BOTH REAPPEAR
    AFTER NAVIGATION, which is why this runs before every step rather than
    once at login — clicking without it fails as
    "<div class=tcl__Modal…> intercepts pointer events" and retries until the
    step times out. Best-effort by design: an absent overlay is normal.
    """
    _dismiss_cookie_banner(page, CONFIG["selectors"].get("cookie_accept", ""))
    promo = CONFIG["selectors"].get("promo_dismiss", "")
    if not promo:
        return
    # Every matching overlay is dismissed, not just the first: Partner Hub can
    # show the app promo AND a survey card at once, and returning after one
    # left the other still swallowing clicks.
    for selector in [s.strip() for s in promo.split(",") if s.strip()]:
        try:
            el = page.query_selector(selector)
            if el and el.is_visible():
                el.click(timeout=5_000)
                page.wait_for_timeout(600)
                if _verbose:
                    print(f"  [x] Dismissed overlay via {selector!r}")
        except Exception:
            continue

    _remove_promo_modals(page)


def _remove_promo_modals(page: Page) -> int:
    """Delete promo overlays outright, matching on their copy.

    Clicking them away is not reliable: they mount ASYNCHRONOUSLY, so one
    cleared before a step reappears during it and intercepts the click —
    which is what stalled the wizard on 2026-09-02 ("<div class=tcl__Modal…>
    from <div class=ReactModalPortal> subtree intercepts pointer events",
    retried until timeout).

    Only portals whose text matches a configured marker are removed, because
    the wizard's own dialogs are ReactModalPortal nodes too and must survive.
    """
    markers = CONFIG["selectors"].get("promo_text_markers") or []
    if not markers:
        return 0
    try:
        return int(page.evaluate(
            """(markers) => {
                let n = 0;
                document.querySelectorAll('.ReactModalPortal').forEach((el) => {
                    const text = (el.innerText || '').toLowerCase();
                    if (!text) return;
                    if (markers.some((m) => text.includes(m))) { el.remove(); n++; }
                });
                return n;
            }""", [m.lower() for m in markers]))
    except Exception:
        return 0


def _session_is_valid(page: Page) -> bool:
    """Load cached storage state and verify we are actually logged in."""
    auth_el = CONFIG["portal"].get("authenticated_element", "")
    if not auth_el:
        return False
    try:
        page.goto(_reports_url(), wait_until="domcontentloaded", timeout=20_000)
        page.wait_for_selector(auth_el, timeout=5_000)
        return True
    except Exception:
        return False


def stage_auth(page: Page, context, force_login: bool) -> None:
    t0 = time.monotonic()
    _require_selectors(REQUIRED_AUTH_SELECTORS)
    sel = CONFIG["selectors"]

    # Attempt to reuse cached session
    if not force_login and STORAGE_STATE_PATH.exists():
        if _verbose:
            print("  [→] Checking cached session...")
        if _session_is_valid(page):
            log("auth", "session_cache_hit", "ok",
                duration_ms=int((time.monotonic() - t0) * 1000))
            return
        if _verbose:
            print("  [→] Cached session expired — re-authenticating...")

    username = os.environ.get("DELIVEROO_USERNAME", "")
    password = os.environ.get("DELIVEROO_PASSWORD", "")

    if not username or not password:
        raise AuthError(
            "DELIVEROO_USERNAME and DELIVEROO_PASSWORD are not set. "
            "Add them to your .env file (see .env.example)."
        )

    try:
        page.goto(CONFIG["portal"]["login_url"], wait_until="domcontentloaded", timeout=30_000)
        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass
        screenshot(page, "auth", "01_login_page")

        # Deliveroo shows a OneTrust consent overlay that swallows the click on
        # the Log in button. Dismiss it first.
        _dismiss_cookie_banner(page, sel.get("cookie_accept", ""))

        page.wait_for_selector(sel["username_field"], timeout=15_000)
        page.fill(sel["username_field"], username)
        page.fill(sel["password_field"], password)
        screenshot(page, "auth", "02_fields_filled")

        page.click(sel["login_button"])

        # Deliveroo is a SPA — a hard navigation may not fire. Wait for the
        # network to settle instead of expecting a page load.
        page.wait_for_load_state("networkidle", timeout=45_000)

        # Detect login failure before assuming success
        error_sel = sel.get("login_error", "")
        if error_sel:
            try:
                page.wait_for_selector(error_sel, timeout=3_000)
            except Exception:
                pass  # no error element → login succeeded
            else:
                screenshot(page, "auth", "03_login_error")
                raise AuthError("Login failed — error element detected on page.")

        # An OTP / 2FA challenge cannot be solved headlessly. Surface it clearly
        # rather than timing out on a missing selector later.
        if "otp" in page.url.lower() or "verify" in page.url.lower():
            screenshot(page, "auth", "03_otp_challenge")
            raise AuthError(
                f"Deliveroo presented a verification challenge at {page.url}. "
                "Re-run with --debug to complete it manually once; the session "
                "cache will then carry subsequent headless runs."
            )

        authenticated_el = CONFIG["portal"].get("authenticated_element", "")
        if authenticated_el:
            page.wait_for_selector(authenticated_el, timeout=20_000)

        screenshot(page, "auth", "03_logged_in")

    except AuthError:
        raise
    except Exception as exc:
        screenshot(page, "auth", "error")
        raise NavError(f"Login navigation failed: {exc}") from exc

    context.storage_state(path=str(STORAGE_STATE_PATH))
    log("auth", "login", "ok", duration_ms=int((time.monotonic() - t0) * 1000))
    write_checkpoint(1)


# ──────────────────────────────────────────────────────────────────────────────
# Stage 2 — Report wizard + async download
# ──────────────────────────────────────────────────────────────────────────────

def _open_reports_list(page: Page) -> None:
    """Navigate to the generated-reports list.

    Going straight to /reporting-platform renders NOTHING — verified
    2026-09-02: the list is only built when the in-app Reports tab is clicked
    from /analytics. So the path is replayed, overlays cleared at each hop.
    """
    sel = CONFIG["selectors"]
    page.goto(_reports_url(), wait_until="domcontentloaded", timeout=30_000)
    try:
        page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass
    _clear_overlays(page)
    page.click(sel["reports_tab"])
    try:
        page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass
    _clear_overlays(page)
    # The list's own search call is what supplies the auth header used for
    # polling, so give it a moment to fire rather than racing past it.
    _await_search_capture(page)


# The page's own reports/search call, captured so it can be replayed.
# The BODY holds only restaurant ids and paging, but the HEADERS carry the
# app's bearer token — cookies alone get HTTP 401, verified 2026-09-02. Header
# values are never logged.
_SEARCH_PAYLOAD: Optional[str] = None
_SEARCH_HEADERS: dict = {}

# Headers that must not be replayed: hop-by-hop or recomputed per request.
_SKIP_HEADERS = {"content-length", "host", "connection", "cookie"}


def _watch_search_requests(page: Page) -> None:
    """Remember the reports/search request so polling can reproduce it.

    Both parts matter: the body (restaurant ids, so they stay correct when
    sites change) and the headers (the authorization the endpoint requires).
    """
    path = CONFIG["api"]["search_path"]

    def handler(request):
        global _SEARCH_PAYLOAD, _SEARCH_HEADERS
        if path in request.url and request.method == "POST":
            try:
                if request.post_data:
                    _SEARCH_PAYLOAD = request.post_data
                _SEARCH_HEADERS = {k: v for k, v in request.headers.items()
                                   if k.lower() not in _SKIP_HEADERS}
            except Exception:
                pass

    page.on("request", handler)


def _await_search_capture(page: Page, timeout_seconds: int = 30) -> bool:
    """Wait for the app to issue reports/search.

    It fires SHORTLY AFTER the list finishes rendering, so returning from
    _open_reports_list and polling immediately saw nothing — the reason two
    600s polls found no report that was in fact sitting there available.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _SEARCH_HEADERS:
            return True
        page.wait_for_timeout(500)
    return False


def _search_payload() -> Optional[str]:
    """Body for reports/search.

    Built from configured site ids, because relying on the page's own request
    being observed was flaky (the listener never fired). A captured payload is
    still preferred when one happens to be seen, since it reflects whatever
    the app itself currently sends.
    """
    if _SEARCH_PAYLOAD:
        return _SEARCH_PAYLOAD
    api = CONFIG["api"]
    ids = api.get("restaurant_drn_ids") or []
    if not ids:
        return None
    return json.dumps({"restaurant_drn_ids": list(ids),
                       "starting_after": 0,
                       "limit": str(api.get("search_limit", "25"))})


def _search_reports(page: Page) -> list:
    """Current reports, newest first, straight from the API."""
    api = CONFIG["api"]
    payload = _search_payload()
    if not payload:
        return []
    url = CONFIG["portal"]["base_url"].rstrip("/") + api["search_path"] \
        if CONFIG["portal"].get("base_url") else \
        "https://partner-hub.deliveroo.com" + api["search_path"]
    headers = dict(_SEARCH_HEADERS) if _SEARCH_HEADERS else {}
    headers.setdefault("content-type", "application/json")
    headers.setdefault("accept", "application/json")
    try:
        resp = page.request.post(url, data=payload, headers=headers)
        if not resp.ok:
            return []
        reports = (resp.json() or {}).get("reports") or []
    except Exception:
        return []
    return sorted(reports, key=lambda r: r.get("requested_at") or "", reverse=True)


def _matches_request(report: dict, date_from: datetime, date_to: datetime) -> bool:
    api = CONFIG["api"]
    if (report.get("report_type") or "") != api["report_type"]:
        return False
    for key, want in (("start_date", date_from), ("end_date", date_to)):
        raw = (report.get(key) or "")[:10]
        if raw != want.strftime("%Y-%m-%d"):
            return False
    return True


def _wait_for_report_via_api(page: Page, date_from: datetime,
                             date_to: datetime) -> pathlib.Path:
    """Poll the API until our report is available, then fetch the file.

    Replaces a DOM poll that could not work: submitting leaves the browser on
    the wizard, the list only renders by re-entering the Reports tab, and
    promo modals kept intercepting that click. Meanwhile the reports
    themselves were generating fine — verified 2026-09-02, status "available"
    with the requested range and both sites.
    """
    api = CONFIG["api"]
    gen = CONFIG["report_generation"]
    interval = gen["poll_interval_seconds"]
    timeout = gen["timeout_seconds"]
    deadline = time.monotonic() + timeout
    attempt = 0

    while time.monotonic() < deadline:
        attempt += 1
        for report in _search_reports(page):
            if not _matches_request(report, date_from, date_to):
                continue
            if (report.get("status") or "") != api["ready_status"]:
                continue                      # still generating
            drn = report.get("drn_id")
            if not drn:
                continue
            base = "https://partner-hub.deliveroo.com"
            url = base + api["download_path"].format(drn_id=drn)
            resp = page.request.get(url)
            if not resp.ok:
                raise NavError(
                    f"Report {drn} is 'available' but its download returned "
                    f"HTTP {resp.status} ({url})")
            body = resp.body()
            suffix = ".csv" if "csv" in (report.get("s3_key") or "") else ".csv"
            dest = DOWNLOADS_DIR / f"deliveroo_{RUN_ID}_raw{suffix}"
            dest.write_bytes(body)
            log("nav", "report_ready", "ok",
                extra={"poll_attempts": attempt, "drn_id": drn,
                       "bytes": len(body), "sites": len(report.get("restaurant_drn_ids") or [])})
            if _verbose:
                print(f"  [→] Report {drn} downloaded ({len(body)} bytes)")
            return dest
        if _verbose:
            print(f"  [→] Report still generating (poll {attempt})...")
        time.sleep(interval)

    screenshot(page, "nav", "report_never_ready")
    raise NavError(
        f"No 'available' {api['report_type']} report for "
        f"{date_from:%Y-%m-%d}..{date_to:%Y-%m-%d} within {timeout}s. "
        f"Raise report_generation.timeout_seconds if Deliveroo is slow.")


def _wait_for_report_ready(page: Page) -> str:
    """
    Poll the generated-reports list until the target row exposes an active
    download link. Returns the selector of that link.

    Deliveroo builds the report server-side; the row appears immediately but
    its download link only materialises once generation finishes.
    """
    sel = CONFIG["selectors"]
    cfg = CONFIG["report_generation"]
    interval = cfg["poll_interval_seconds"]
    timeout = cfg["timeout_seconds"]
    row_index = cfg["download_row_index"]

    deadline = time.monotonic() + timeout
    attempt = 0

    while time.monotonic() < deadline:
        attempt += 1
        # RELOAD, don't just scroll. Submitting leaves the browser on the
        # wizard's confirmation view, and the reports list is only rendered by
        # re-entering it — so a poll that merely scrolled could never see the
        # new row and always timed out (verified 2026-09-02: the wizard
        # completed, then polled 600s for a report that had already been
        # generated).
        try:
            _open_reports_list(page)
        except Exception:
            pass
        try:
            page.mouse.wheel(0, 20_000)
        except Exception:
            pass

        rows = page.query_selector_all(sel["report_rows"])
        if len(rows) > row_index:
            row = rows[row_index]
            link = row.query_selector(sel["report_row_ready_link"])
            if link:
                elapsed = int(timeout - (deadline - time.monotonic()))
                log("nav", "report_ready", "ok",
                    extra={"poll_attempts": attempt, "waited_seconds": elapsed})
                if _verbose:
                    print(f"  [→] Report ready after {elapsed}s ({attempt} polls)")
                return link

        if _verbose:
            print(f"  [→] Report not ready yet (poll {attempt})... "
                  f"{int(deadline - time.monotonic())}s left")
        page.wait_for_timeout(interval * 1000)
        page.reload(wait_until="domcontentloaded")

    screenshot(page, "nav", "report_never_ready")
    raise NavError(
        f"Report was not ready within {timeout}s. "
        "Increase report_generation.timeout_seconds if Deliveroo is being slow."
    )


def _select_custom_period(page: Page) -> None:
    """Open the time-period control and choose "Custom".

    The control is a READONLY input — it cannot be typed into and must be
    clicked to reveal its list. The list holds exactly two options ("Last
    week", "Custom") which share one class, so the choice is made on exact
    text rather than position.
    """
    sel = CONFIG["selectors"]
    page.click(sel["time_period_toggle"])
    page.wait_for_selector(sel["time_period_custom"], state="visible",
                           timeout=20_000)
    seen = []
    for opt in page.query_selector_all(sel["time_period_custom"]):
        text = " ".join((opt.text_content() or "").split())
        seen.append(text)
        if text.casefold() == "custom":
            opt.click()
            page.wait_for_timeout(800)
            return
    screenshot(page, "nav", "02b_no_custom_period")
    raise NavError(
        "The time-period list has no 'Custom' option. Options seen: "
        + ", ".join(repr(t) for t in seen)
        + ". Correct selectors.time_period_custom in deliveroo_config.yaml."
    )


def _day_aria_label(when: datetime) -> str:
    """The aria-label of a day cell, e.g. "Wednesday, August 12, 2026".

    Weekday and month are full names and the day is NOT zero-padded, which is
    why the day is substituted rather than strftime-formatted (%-d is not
    portable).
    """
    fmt = CONFIG["selectors"]["calendar_day_aria_format"]
    return when.strftime(fmt.replace("{day}", str(when.day)))


def _visible_months(page: Page) -> list:
    """(year, month) of every month panel currently on screen.

    react-dates keeps several month panels in the DOM and hides all but the
    displayed one(s), so this reads only the visible captions.
    """
    sel = CONFIG["selectors"]
    out = []
    for cap in page.query_selector_all(f'{sel["calendar_month_caption"]} >> visible=true'):
        text = " ".join((cap.text_content() or "").split())
        try:
            parsed = datetime.strptime(text, "%B %Y")
        except ValueError:
            continue
        out.append((parsed.year, parsed.month))
    return sorted(out)


def _pick_date(page: Page, field_selector: str, when: datetime) -> None:
    """Set one wizard date by clicking its calendar cell.

    page.fill() TIMES OUT on these fields — they are react-dates controlled
    inputs whose value cannot be typed (documented in deliveroo_config.yaml).

    Two traps, both hit for real on 2026-09-02:

    1. The day-cell selector matches ~92 cells because react-dates keeps
       off-screen month panels mounted, and the FIRST match is invisible. A
       plain visibility wait therefore times out on a calendar that is
       perfectly fine — hence ">> visible=true" on every day-cell lookup.
    2. Which way to page cannot be derived from today's date: the picker may
       open on a different month than the one containing the target (it opened
       on August while the target was 1 September). Direction is taken from
       the VISIBLE month caption instead.
    """
    sel = CONFIG["selectors"]
    limit = int(sel.get("calendar_month_nav_limit", 18))
    cell = sel["calendar_day_cell"]
    page.click(field_selector)
    page.wait_for_selector(f"{cell} >> visible=true", state="visible",
                           timeout=20_000)

    label = _day_aria_label(when)
    # SUFFIX match, not exact. The aria-label carries a STATE PREFIX that
    # changes as the picker is used — verified 2026-09-02:
    #   "Friday, August 14, 2026"                     selectable
    #   "Selected. Tuesday, September 1, 2026"        already chosen
    #   "Not available. Wednesday, September 2, 2026" disabled
    # An exact match worked for the from-date and then failed for the to-date
    # on the same day, because clicking it rewrote its own label to
    # "Selected. …". Matching the tail is stable across all three states.
    cell_selector = f'{cell}[aria-label$="{label}"] >> visible=true'
    target = (when.year, when.month)

    for _ in range(limit):
        found = page.query_selector(cell_selector)
        if found:
            found.click()
            page.wait_for_timeout(600)
            return

        shown = _visible_months(page)
        if not shown:
            break
        direction = "prev" if target < shown[0] else "next"
        nav = page.query_selector(f'{sel[f"calendar_month_{direction}"]} >> visible=true')
        if not nav:
            break
        # A DISABLED arrow means Deliveroo will not let the picker go further
        # — Items Sold reports only reach back a limited window. Clicking it
        # anyway just burns the 30s actionability timeout on every retry
        # ("element is not enabled"), so stop and report the real reason.
        if not nav.is_enabled():
            shown = _visible_months(page)
            edge = f"{shown[0][0]}-{shown[0][1]:02d}" if shown else "unknown"
            screenshot(page, "nav", "03b_calendar_limit")
            raise NavError(
                f"{when:%Y-%m-%d} is outside the range Deliveroo allows for an "
                f"Items Sold report: the calendar will not page further "
                f"{direction} than {edge}. Request a later start date, or pull "
                f"older data another way.")
        nav.click()
        page.wait_for_timeout(700)

    # Nothing was clicked — say exactly what the calendar offered so the
    # selectors can be fixed from one run instead of a guessing game.
    labels = [c.get_attribute("aria-label")
              for c in page.query_selector_all(
                  f'{sel["calendar_day_cell"]} >> visible=true')]
    labels = [l for l in labels if l][:8]
    navs = sorted({(b.get_attribute("aria-label") or "")
                   for b in page.query_selector_all("button, [role='button']")
                   if (b.get_attribute("aria-label") or "")})
    screenshot(page, "nav", "03b_date_not_found")
    raise NavError(
        f"Could not find the calendar cell for {when:%Y-%m-%d} "
        f"(looked for aria-label {label!r}).\n"
        f"  Day labels visible: {labels}\n"
        f"  Button aria-labels: {navs[:20]}\n"
        f"  Fix selectors.calendar_day_aria_format / calendar_month_{direction} "
        f"in deliveroo_config.yaml."
    )


@retry(max_attempts=3, exceptions=(NavError,))
def stage_create_and_download(page: Page, date_from: datetime, date_to: datetime) -> pathlib.Path:
    t0 = time.monotonic()
    _require_selectors(REQUIRED_WIZARD_SELECTORS)
    sel = CONFIG["selectors"]

    try:
        _watch_search_requests(page)
        _open_reports_list(page)
        screenshot(page, "nav", "01_reports_tab")

        page.click(sel["create_report_button"])
        # No separate type picker: pressing "Create Items sold report" IS the
        # type choice. The selector is honoured only if Deliveroo brings one
        # back, rather than being required and failing when correctly blank.
        type_sel = sel.get("report_type_items_sold", "")
        if type_sel:
            page.wait_for_selector(type_sel, state="visible", timeout=20_000)
            page.click(type_sel)
        screenshot(page, "nav", "02_items_sold_selected")

        # Time period → Custom, then pick both dates from the calendar.
        _clear_overlays(page)
        _select_custom_period(page)
        page.wait_for_selector(sel["date_from_field"], state="visible",
                               timeout=20_000)
        _pick_date(page, sel["date_from_field"], date_from)
        _pick_date(page, sel["date_to_field"], date_to)
        screenshot(page, "nav", "03_dates_filled")

        # Continue
        _clear_overlays(page)
        page.click(sel["continue_button"])
        page.wait_for_load_state("networkidle", timeout=20_000)

        # Tick only the configured sites
        template = sel["site_checkbox_template"]
        for site in CONFIG["sites"]:
            label = site["label"]
            if not label:
                raise ConfigError(
                    "deliveroo_config.yaml sites[].label is empty — set the exact "
                    "visible label of the site's checkbox in the wizard."
                )
            site_sel = template.format(label=label)
            try:
                page.wait_for_selector(site_sel, state="visible", timeout=15_000)
                checkbox = page.query_selector(site_sel)
                if checkbox and not checkbox.is_checked():
                    checkbox.check()
            except ConfigError:
                raise
            except Exception as exc:
                screenshot(page, "nav", "04_site_select_error")
                raise NavError(
                    f"Could not tick site {label!r} using selector {site_sel!r}: {exc}"
                ) from exc
        screenshot(page, "nav", "04_sites_selected")

        # Submit — the report is now generated server-side
        _clear_overlays(page)
        page.click(sel["submit_report_button"])
        page.wait_for_load_state("networkidle", timeout=30_000)
        log("nav", "report_submitted", "ok",
            extra={"date_from": date_from.strftime("%Y-%m-%d"),
                   "date_to": date_to.strftime("%Y-%m-%d")})

        # Fetch the report from the API once it reports "available". This
        # replaces clicking a Download link in the DOM list — see
        # _wait_for_report_via_api for why that could not be made reliable.
        screenshot(page, "nav", "05_before_download")
        dest = _wait_for_report_via_api(page, date_from, date_to)

    except (NavError, ConfigError):
        raise
    except Exception as exc:
        screenshot(page, "nav", "error")
        raise NavError(f"Report wizard failed: {exc}") from exc

    if not dest.exists():
        raise NavError(f"Downloaded file is missing: {dest}")

    # NOTE: a zero-byte file is NOT an error here — Deliveroo returns an empty
    # report when there were no sales. Stage 3 handles that case.
    log("nav", "download", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"file": str(dest), "size_bytes": dest.stat().st_size})
    write_checkpoint(2, {
        "raw_file": str(dest),
        "date_from": date_from.strftime("%Y-%m-%d"),
        "date_to": date_to.strftime("%Y-%m-%d"),
    })
    return dest


# ──────────────────────────────────────────────────────────────────────────────
# Stage 3 — Transformation
# ──────────────────────────────────────────────────────────────────────────────

def _read_raw(raw_path: pathlib.Path) -> pd.DataFrame:
    """Load the raw export, auto-detecting the header row when configured to."""
    file_cfg = CONFIG.get("file", {})
    sheet = file_cfg.get("sheet_name")
    header_row = file_cfg.get("header_row")
    anchor = file_cfg.get("header_anchor_column", "")
    suffix = raw_path.suffix.lower()
    is_excel = suffix in (".xlsx", ".xls")

    def _load(header):
        if is_excel:
            return pd.read_excel(raw_path, header=header,
                                 sheet_name=sheet if sheet else 0)
        return pd.read_csv(raw_path, header=header, on_bad_lines="skip")

    if header_row is None:
        if not anchor:
            # No anchor to search for — assume the first row is the header.
            header_row = 0
        else:
            preview = _load(None).head(30)
            header_row = None
            for idx, row in preview.iterrows():
                if any(str(v).strip() == anchor for v in row.values):
                    header_row = idx
                    break
            if header_row is None:
                raise TransformError(
                    f"Could not locate the header row: no cell equal to "
                    f"{anchor!r} in the first 30 rows of {raw_path.name}. "
                    "Fix file.header_anchor_column or set file.header_row explicitly."
                )
            if _verbose:
                print(f"  [→] Header row auto-detected at index {header_row}")

    df = _load(header_row)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def discover_columns(raw_path: pathlib.Path) -> None:
    """Print the raw export's headers and a sample row, then exit."""
    print(f"\n{'='*64}")
    print(f" Deliveroo raw export — column discovery")
    print(f" File: {raw_path}")
    print(f"{'='*64}\n")

    suffix = raw_path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        raw = pd.read_excel(raw_path, header=None, nrows=30, sheet_name=0)
    else:
        raw = pd.read_csv(raw_path, header=None, nrows=30, on_bad_lines="skip")

    print("[FIRST 30 ROWS — find the header row index]\n")
    for idx, row in raw.iterrows():
        cells = [str(v) for v in row.values if str(v) not in ("nan", "")]
        if cells:
            print(f"  [row {idx}] {cells}")

    print(f"\n{'='*64}")
    print(" ACTION REQUIRED:")
    print("  1. Note the row index holding the column names.")
    print("     → set file.header_row (or file.header_anchor_column) in")
    print("       deliveroo_config.yaml")
    print("  2. Copy the exact header strings into the columns[].raw fields.")
    print(f"{'='*64}\n")


def _validate_raw_columns(df: pd.DataFrame) -> None:
    """
    Fail loudly if a configured raw column is absent. Silently producing an
    output file with missing sales figures is far worse than a failed run.
    """
    configured = [c["raw"] for c in CONFIG["columns"]
                  if c.get("raw") and not c.get("drop")]
    missing = [c for c in configured if c not in df.columns]
    if missing:
        raise TransformError(
            "The raw export is missing configured columns:\n"
            + "".join(f"    {c!r}\n" for c in missing)
            + "\n  Columns actually present:\n"
            + "".join(f"    {c!r}\n" for c in df.columns)
            + "\n  Fix the columns[].raw values in deliveroo_config.yaml "
              "(or run with --discover-columns)."
        )


def stage_transform(raw_path: pathlib.Path,
                    date_from: datetime,
                    date_to: datetime) -> tuple:
    """Returns (out_path, row_count, date_range_display)."""
    t0 = time.monotonic()
    out_fmt = CONFIG.get("output_date_format", "%d-%b-%Y")
    range_display = (
        date_from.strftime(out_fmt) if date_from.date() == date_to.date()
        else f"{date_from.strftime(out_fmt)} → {date_to.strftime(out_fmt)}"
    )

    # An empty file means "no sales in this period" — a valid, expected outcome.
    if raw_path.stat().st_size == 0:
        log("transform", "empty_report", "ok", extra={"reason": "zero_byte_file"})
        return None, 0, range_display

    try:
        df = _read_raw(raw_path)

        if df.empty:
            log("transform", "empty_report", "ok", extra={"reason": "no_data_rows"})
            return None, 0, range_display

        _validate_raw_columns(df)

        col_cfgs = CONFIG["columns"]

        # ── Drop + rename ─────────────────────────────────────────
        drop_cols = [c["raw"] for c in col_cfgs
                     if c.get("drop") and c.get("raw") in df.columns]
        rename_map = {c["raw"]: c["target"] for c in col_cfgs
                      if c.get("raw") and c.get("target") and not c.get("drop")}

        df.drop(columns=drop_cols, errors="ignore", inplace=True)
        df.rename(columns=rename_map, inplace=True)

        # Drop aggregate "Total" rows — a row with no item name is not an item.
        if "POS Item Name" in df.columns:
            df = df[df["POS Item Name"].notna()].copy()
            df = df[df["POS Item Name"].astype(str).str.strip() != ""].copy()
            df.reset_index(drop=True, inplace=True)

        if df.empty:
            log("transform", "empty_report", "ok", extra={"reason": "no_item_rows"})
            return None, 0, range_display

        # ── Type casting of mapped columns (before derived values) ─
        for col_cfg in col_cfgs:
            if col_cfg.get("drop") or col_cfg.get("inject"):
                continue
            target, dtype = col_cfg.get("target"), col_cfg.get("dtype")
            if not target or target not in df.columns:
                continue
            if dtype == "int":
                df[target] = pd.to_numeric(df[target], errors="coerce").fillna(0).astype(int)
            elif dtype == "float":
                df[target] = (
                    df[target].astype(str)
                    .str.replace(r"[^\d.\-]", "", regex=True)
                    .replace("", "0")
                )
                df[target] = pd.to_numeric(df[target], errors="coerce").fillna(0.0).round(2)
            elif dtype == "str":
                df[target] = df[target].astype(str).str.strip()

        # ── Inject columns ────────────────────────────────────────
        divisor = float(CONFIG["tax"]["divisor"])
        for col_cfg in col_cfgs:
            inject = col_cfg.get("inject")
            if not inject:
                continue
            target = col_cfg["target"]

            if inject == "zero":
                # Supy's template wants a number here, not a blank cell.
                df[target] = 0.0
            elif inject == "empty":
                df[target] = ""
            elif inject == "constant_1":
                df[target] = 1
            elif inject == "run_date_range":
                df[target] = date_from.strftime(out_fmt)
            elif inject.startswith("copy:"):
                source = inject.split(":", 1)[1]
                if source not in df.columns:
                    raise TransformError(
                        f"inject 'copy:{source}' for column {target!r}: "
                        f"source column {source!r} does not exist."
                    )
                df[target] = df[source]
            elif inject.startswith("derive_excl_tax:"):
                source = inject.split(":", 1)[1]
                if source not in df.columns:
                    raise TransformError(
                        f"inject 'derive_excl_tax:{source}' for column {target!r}: "
                        f"source column {source!r} does not exist."
                    )
                df[target] = (pd.to_numeric(df[source], errors="coerce")
                              .fillna(0.0) / divisor).round(2)
            else:
                raise TransformError(f"Unknown inject directive: {inject!r}")

        # ── Reorder to the Supy upload column order ───────────────
        # Columns outside the template are DROPPED, not appended. Appending
        # them (the behaviour until 2026-09-02) leaked Deliveroo's raw "Price"
        # column into the upload sheet, which Supy's importer does not expect.
        # They are named in the log rather than dropped silently, and
        # output_keep_unmapped: true restores the old behaviour if some other
        # consumer ever needs those columns.
        final_order = CONFIG.get("output_column_order", [])
        ordered = [c for c in final_order if c in df.columns]
        extras = [c for c in df.columns if c not in ordered]
        missing = [c for c in final_order if c not in df.columns]
        if missing:
            raise TransformError(
                f"Supy template columns missing from the transformed data: "
                f"{missing}. Check deliveroo_config.yaml columns[].target.")
        if extras:
            keep = bool(CONFIG.get("output_keep_unmapped", False))
            log("transform", "unmapped_columns", "warn",
                extra={"columns": extras, "kept": keep})
            print(f"  [!] Columns not in the Supy template "
                  f"{'kept' if keep else 'dropped'}: {extras}")
            df = df[ordered + extras] if keep else df[ordered]
        else:
            df = df[ordered]

        # ── Export ────────────────────────────────────────────────
        stamp = date_from.strftime("%Y-%m-%d")
        out_path = OUTPUT_DIR / f"deliveroo_sales_{stamp}_{RUN_ID[:8]}.xlsx"
        df.to_excel(str(out_path), index=False, engine="openpyxl")

    except TransformError:
        raise
    except (KeyError, ValueError, TypeError) as exc:
        raise TransformError(f"Transform failed: {exc}\n{traceback.format_exc()}") from exc
    except Exception as exc:
        raise TransformError(f"Unexpected transform error: {exc}\n{traceback.format_exc()}") from exc

    log("transform", "export", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"output": str(out_path), "rows": len(df)})
    write_checkpoint(3, {"output_file": str(out_path)})

    if _verbose:
        print(f"  [→] {len(df)} rows written → {out_path}")

    return out_path, len(df), range_display


# ──────────────────────────────────────────────────────────────────────────────
# Stage 4 — Email
# ──────────────────────────────────────────────────────────────────────────────

def resolve_recipients(override: Optional[list] = None) -> list:
    """Who gets the report email.

    --email-to wins over REPORT_RECIPIENT when supplied; otherwise the .env
    list is used. Both accept comma- or semicolon-separated addresses, and
    duplicates are dropped case-insensitively.

    Added 2026-09-02: this used to read REPORT_RECIPIENT only, so --email-to on
    run_all_partners.py reached the roll-up summary but never the per-partner
    reports — customer.care@ was on the summary and nothing else.
    """
    if override:
        raw = ",".join(str(a) for a in override if a)
    else:
        raw = os.environ.get("REPORT_RECIPIENT", "") or os.environ.get("GMAIL_USER", "")
    seen, out = set(), []
    for addr in re.split(r"[,;]", raw):
        addr = addr.strip()
        if addr and addr.casefold() not in seen:
            seen.add(addr.casefold())
            out.append(addr)
    return out


def stage_email(out_path: Optional[pathlib.Path], row_count: int, date_range: str,
                email_to: Optional[list] = None) -> None:
    t0 = time.monotonic()

    gmail_user = os.environ.get("GMAIL_USER", "")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD", "")
    recipients = resolve_recipients(email_to) or [gmail_user]
    recipient = ", ".join(recipients)
    entity = CONFIG.get("destination", {}).get("entity", "")

    if not gmail_user or not gmail_password:
        raise EmailError(
            "GMAIL_USER and GMAIL_APP_PASSWORD must be set in your .env / GitHub Secrets."
        )

    if row_count == 0:
        subject = f"Deliveroo Sales — {date_range} — NO SALES"
        body = (
            f"Hi,\n\n"
            f"The Deliveroo Items Sold report for {date_range} came back empty.\n"
            f"This means there were no sales in the period, so there is nothing "
            f"to upload to {entity}.\n\n"
            f"  • Run ID: {RUN_ID}\n\n"
            f"Regards,\nDeliveroo Automation"
        )
    else:
        subject = f"Deliveroo Sales Report — {date_range}"
        body = (
            f"Hi,\n\n"
            f"Please find attached the Deliveroo sales report for {date_range}, "
            f"formatted for upload to {entity}.\n\n"
            f"  • Rows: {row_count:,}\n"
            f"  • File: {out_path.name}\n"
            f"  • Run ID: {RUN_ID}\n\n"
            f"This report was generated automatically by the Deliveroo pipeline.\n\n"
            f"Regards,\nDeliveroo Automation"
        )

    msg = MIMEMultipart()
    msg["From"] = gmail_user
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    if out_path is not None:
        with open(out_path, "rb") as f:
            attachment = MIMEApplication(
                f.read(),
                _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            attachment.add_header("Content-Disposition", "attachment",
                                  filename=out_path.name)
            msg.attach(attachment)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, recipients, msg.as_string())
    except Exception as exc:
        raise EmailError(f"Failed to send email: {exc}") from exc

    log("email", "send", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"to": recipient, "subject": subject, "rows": row_count})

    print(f"[Stage 4] ✓ Email sent → {recipient}\n")


# ──────────────────────────────────────────────────────────────────────────────
# Date range resolution
# ──────────────────────────────────────────────────────────────────────────────

def _resolve_dates(args) -> tuple:
    """CLI dates → (date_from, date_to). Defaults to yesterday."""
    def parse(value, flag):
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise SystemExit(f"[✗] {flag} must be YYYY-MM-DD, got {value!r}")

    if args.date:
        d = parse(args.date, "--date")
        return d, d

    if args.date_from or args.date_to:
        if not (args.date_from and args.date_to):
            raise SystemExit("[✗] --from and --to must be given together.")
        df_, dt_ = parse(args.date_from, "--from"), parse(args.date_to, "--to")
        if df_ > dt_:
            raise SystemExit("[✗] --from must not be after --to.")
        return df_, dt_

    lookback = CONFIG["dates"].get("default_lookback_days", 1)
    d = datetime.now() - timedelta(days=lookback)
    return d, d


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Deliveroo Partner Hub Automation Pipeline")
    parser.add_argument("--debug", action="store_true",
                        help="Run with headed browser and verbose logging")
    parser.add_argument("--date", metavar="YYYY-MM-DD",
                        help="Single day to report on")
    parser.add_argument("--from", dest="date_from", metavar="YYYY-MM-DD",
                        help="Start of a custom date range (requires --to)")
    parser.add_argument("--to", dest="date_to", metavar="YYYY-MM-DD",
                        help="End of a custom date range (requires --from)")
    parser.add_argument("--from-stage", type=int, default=1, metavar="N",
                        help="Resume from stage N (1=auth, 2=report, 3=transform, 4=email)")
    parser.add_argument("--force-login", action="store_true",
                        help="Ignore cached session; always re-authenticate")
    parser.add_argument("--email-to", metavar="ADDR", action="append",
                        help="Send the report here instead of REPORT_RECIPIENT. "
                             "Repeat for several addresses.")
    parser.add_argument("--no-email", action="store_true",
                        help="Skip email — save output file locally only")
    parser.add_argument("--discover-columns", action="store_true",
                        help="Print the raw export's headers and exit (first-run setup)")
    args = parser.parse_args()

    _init_logger(verbose=args.debug)
    date_from, date_to = _resolve_dates(args)
    from_stage = args.from_stage

    print(f"\n[Deliveroo Pipeline] run_id={RUN_ID}  from_stage={from_stage}  "
          f"dates={date_from:%Y-%m-%d}→{date_to:%Y-%m-%d}\n")

    raw_file: Optional[pathlib.Path] = None

    # Resuming past the download stage requires an existing raw file
    if from_stage >= 3 or args.discover_columns:
        checkpoint = read_checkpoint()
        raw_file_str = checkpoint.get("raw_file")
        if raw_file_str:
            raw_file = pathlib.Path(raw_file_str)
        if not raw_file or not raw_file.exists():
            candidates = sorted(DOWNLOADS_DIR.glob("deliveroo_*_raw.*"),
                                key=lambda p: p.stat().st_mtime)
            raw_file = candidates[-1] if candidates else None
        if not raw_file or not raw_file.exists():
            print("[✗] No raw Deliveroo download found. Run stages 1-2 first.",
                  file=sys.stderr)
            return 3

    # --discover-columns is a setup helper, not part of the pipeline
    if args.discover_columns:
        discover_columns(raw_file)
        return 0

    # Stages 1 & 2 require a browser
    if from_stage <= 2:
        headless = not args.debug
        try:
            with sync_playwright() as p:
                browser_ctx_kwargs = {}
                if STORAGE_STATE_PATH.exists() and not args.force_login:
                    browser_ctx_kwargs["storage_state"] = str(STORAGE_STATE_PATH)

                browser = p.chromium.launch(headless=headless,
                                            slow_mo=200 if args.debug else 0)
                context = browser.new_context(accept_downloads=True, **browser_ctx_kwargs)
                _block_overlay_resources(context)
                page = context.new_page()

                # ── Stage 1: Auth ──────────────────────────────────
                if from_stage <= 1:
                    print("[Stage 1] Authentication...")
                    try:
                        stage_auth(page, context, force_login=args.force_login)
                        print("[Stage 1] ✓ Authenticated\n")
                    except (AuthError, ConfigError) as exc:
                        log("auth", "login", "error", extra={"error": str(exc)})
                        print(f"[✗] Auth error: {exc}", file=sys.stderr)
                        browser.close()
                        return 1

                # ── Stage 2: Create report & download ──────────────
                print("[Stage 2] Creating Items Sold report and downloading...")
                try:
                    raw_file = stage_create_and_download(page, date_from, date_to)
                    print(f"[Stage 2] ✓ Downloaded → {raw_file}\n")
                except ConfigError as exc:
                    log("nav", "config", "error", extra={"error": str(exc)})
                    print(f"[✗] Config error: {exc}", file=sys.stderr)
                    browser.close()
                    return 2
                except NavError as exc:
                    log("nav", "create_and_download", "error", extra={"error": str(exc)})
                    print(f"[✗] Nav error: {exc}", file=sys.stderr)
                    browser.close()
                    return 2

                browser.close()

        except Exception as exc:
            print(f"[✗] Unexpected browser error: {exc}", file=sys.stderr)
            log("browser", "unexpected", "error", extra={"error": str(exc)})
            return 2

    # ── Stage 3: Transform ─────────────────────────────────────────
    print("[Stage 3] Transforming raw data...")
    try:
        out_file, row_count, date_range = stage_transform(raw_file, date_from, date_to)
    except TransformError as exc:
        log("transform", "transform", "error", extra={"error": str(exc)})
        print(f"[✗] Transform error: {exc}", file=sys.stderr)
        return 3

    if row_count == 0:
        print(f"[Stage 3] ✓ Report is empty — no sales for {date_range}, "
              f"nothing to upload.\n")
    else:
        print(f"[Stage 3] ✓ Output → {out_file}  ({row_count} rows)\n")

    # ── Stage 4: Email ─────────────────────────────────────────────
    if not args.no_email:
        print("[Stage 4] Sending email...")
        try:
            stage_email(out_file, row_count, date_range, args.email_to)
        except EmailError as exc:
            log("email", "send", "error", extra={"error": str(exc)})
            print(f"[✗] Email error: {exc}", file=sys.stderr)
            return 4
    else:
        print("[Stage 4] — skipped (--no-email)\n")

    print(f"[Deliveroo Pipeline] ✓ Complete  run_id={RUN_ID}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
