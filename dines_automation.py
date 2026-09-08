#!/usr/bin/env python3
"""
dines_automation.py
-------------------
Dines Dashboard → Supy sales-upload pipeline (Black Bear Burger).

Stage 1: Authentication   (Playwright — per-branch login + session caching)
Stage 2: Report + Export  (Playwright — Reports → PIN → Sales By Product → Export)
Stage 3: Transformation   (Pandas — raw export → Supy upload format)
Stage 4: Email            (smtplib — attach .xlsx and send via Gmail)

Differs from every other pipeline here in one important way: each Dines branch
is a SEPARATE login, not a location filter on one account. So the run loops
branch → fresh browser context → login → PIN → export. One branch failing
never stops the others; a partial day beats no day.

VAT: Dines reports a VAT-INCLUSIVE figure, so excl. tax = incl. / 1.2. This is
the OPPOSITE of Deliveroo, where the export is net and VAT is added on top.

All nine Supy branches are configured, but a branch runs only when its three
.env keys exist. --all-branches reports the rest as "no_creds" and carries on,
so a day is never lost because a branch is not set up yet.

Credentials are read from .env only, never from config. Write them with:
    python set_credential.py DINES_CW_PASSWORD

Usage:
    python dines_automation.py --list-branches
    python dines_automation.py --branch canary_wharf --debug
    python dines_automation.py --all-branches
    python dines_automation.py --all-branches --date 2026-08-30
    python dines_automation.py --branch victoria --no-email
    python dines_automation.py --from-file downloads/dines_raw.csv --branch victoria
    python dines_automation.py --discover --branch canary_wharf   # dump selectors
    python dines_automation.py --all-branches --plu-file "PLU CODE.xlsx"

Exit codes:
    0  success (including "no sales" and branches skipped for no credentials)
    1  AuthError / ConfigError
    2  NavError
    3  TransformError
    4  EmailError
"""

import argparse
import functools
import re
import urllib.parse as urlparse
import json
import os
import pathlib
import smtplib
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd
import yaml
from dotenv import load_dotenv
from playwright.sync_api import Page, sync_playwright

load_dotenv()

BASE_DIR        = pathlib.Path(__file__).parent
DOWNLOADS_DIR   = BASE_DIR / "downloads"
OUTPUT_DIR      = BASE_DIR / "output" / "dines"
STATE_DIR       = BASE_DIR / "state" / "dines"
LOGS_DIR        = BASE_DIR / "logs"
SCREENSHOTS_DIR = BASE_DIR / "screenshots"

for _d in (DOWNLOADS_DIR, OUTPUT_DIR, STATE_DIR, LOGS_DIR, SCREENSHOTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

with open(BASE_DIR / "dines_config.yaml") as _f:
    CONFIG = yaml.safe_load(_f)

SUPY_COLUMNS = CONFIG["output_column_order"]
DATE_FMT = CONFIG["output_date_format"]


class ConfigError(Exception):
    pass


class AuthError(Exception):
    pass


class NavError(Exception):
    pass


class TransformError(Exception):
    pass


class EmailError(Exception):
    pass


EXIT = {ConfigError: 1, AuthError: 1, NavError: 2, TransformError: 3, EmailError: 4}

# ── Logging ─────────────────────────────────────────────────────────────
RUN_ID = (f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
          f"_{uuid.uuid4().hex[:8]}")
_log_path: Optional[pathlib.Path] = None
_verbose = False


def _init_logger(verbose: bool) -> None:
    global _log_path, _verbose
    _verbose = verbose
    _log_path = LOGS_DIR / f"dines_{RUN_ID}.jsonl"


def log(stage: str, step: str, outcome: str, duration_ms: int = 0,
        extra: dict = None) -> None:
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "run_id": RUN_ID,
        "pipeline": "dines",
        "stage": stage,
        "step": step,
        "outcome": outcome,
        "duration_ms": duration_ms,
        **(extra or {}),
    }
    if _log_path:
        with open(_log_path, "a") as fh:
            fh.write(json.dumps(entry) + "\n")
    if _verbose:
        icon = "✓" if outcome == "ok" else "✗" if outcome == "error" else "→"
        print(f"  [{icon}] [{stage}] {step}  ({duration_ms}ms)")
    elif outcome in ("error", "warning"):
        stream = sys.stderr if outcome == "error" else sys.stdout
        msg = (extra or {}).get("error", (extra or {}).get("message", ""))
        print(f"  [{'✗' if outcome == 'error' else '!'}] [{stage}] {step}: {msg}",
              file=stream)


def screenshot(page: Page, branch_key: str, label: str) -> None:
    run_dir = SCREENSHOTS_DIR / f"dines_{RUN_ID}"
    run_dir.mkdir(exist_ok=True)
    try:
        page.screenshot(path=str(run_dir / f"{branch_key}_{label}.png"))
    except Exception:
        pass          # a screenshot must never fail the run


def retry(max_attempts: int = 3, base_delay: float = 1.5, exceptions=(NavError,)):
    """Retry transient failures. Auth and transform errors are never retried."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_attempts:
                        raise
                    delay = base_delay * (2 ** (attempt - 1))
                    log("retry", fn.__name__, "warning",
                        extra={"attempt": attempt, "sleep_s": delay,
                               "message": str(exc)})
                    time.sleep(delay)
        return wrapper
    return decorator


# ── Branches & credentials ──────────────────────────────────────────────
def branches() -> list[dict]:
    return CONFIG.get("branches") or []


def find_branch(key: str) -> dict:
    for b in branches():
        if b["key"] == key or b["supy_branch"].casefold() == key.casefold():
            return b
    raise ConfigError(
        f"Unknown branch {key!r}. Known: {', '.join(b['key'] for b in branches())}")


def env_keys(branch: dict) -> tuple[str, str, str]:
    prefix = branch["env_prefix"]
    return (f"{prefix}_USERNAME", f"{prefix}_PASSWORD", f"{prefix}_PIN")


def missing_credentials(branch: dict) -> list[str]:
    """Which of the branch's three .env keys are absent or blank.

    Returns key NAMES only — never a value — so callers can report and skip an
    unconfigured branch without any risk of echoing a secret.
    """
    return [k for k in env_keys(branch) if not os.environ.get(k, "").strip()]


def credential_error(branch: dict, missing: list[str]) -> str:
    return (f"{branch['key']}: missing {', '.join(missing)} in .env. "
            f"Set them without echoing: python set_credential.py {missing[0]}")


def credentials(branch: dict) -> tuple[str, str, str]:
    """Read one branch's credentials from the environment.

    Never logged, never printed, never written to config — only the key NAMES
    appear in output so a missing value is diagnosable without exposing it.
    """
    missing = missing_credentials(branch)
    if missing:
        raise ConfigError(credential_error(branch, missing))
    return tuple(os.environ[k].strip() for k in env_keys(branch))


def target_date(arg: Optional[str]) -> datetime:
    if arg:
        try:
            return datetime.strptime(arg, "%Y-%m-%d")
        except ValueError:
            raise ConfigError(f"--date must be YYYY-MM-DD, got {arg!r}")
    return datetime.now() - timedelta(days=1)


# ── Stage 1: auth ───────────────────────────────────────────────────────
def _session_path(branch: dict) -> pathlib.Path:
    return STATE_DIR / f"storage_state_{branch['key']}.json"


def _logged_in(page: Page) -> bool:
    sel = CONFIG["portal"]["authenticated_element"]
    try:
        page.wait_for_selector(sel, timeout=8000)
        return "login" not in page.url.lower()
    except Exception:
        return False


def stage_auth(page: Page, branch: dict, force_login: bool) -> None:
    t0 = time.time()
    portal = CONFIG["portal"]
    sel = CONFIG["selectors"]

    page.goto(portal["login_url"], wait_until="domcontentloaded", timeout=60000)
    if not force_login and _logged_in(page):
        log("auth", "cached_session", "ok", int((time.time() - t0) * 1000),
            {"branch": branch["key"]})
        return

    username, password, _pin = credentials(branch)
    try:
        page.fill(sel["username_field"], username, timeout=20000)
        page.fill(sel["password_field"], password, timeout=20000)
        page.click(sel["login_button"], timeout=20000)
    except Exception as exc:
        screenshot(page, branch["key"], "auth_failed")
        raise AuthError(
            f"{branch['key']}: could not complete the login form — the selectors in "
            f"dines_config.yaml are unverified. Run --discover to correct them. ({exc})"
        ) from exc

    page.wait_for_timeout(3000)
    if not _logged_in(page):
        screenshot(page, branch["key"], "auth_rejected")
        err = ""
        try:
            if page.locator(sel["login_error"]).count():
                err = page.locator(sel["login_error"]).first.inner_text()[:200]
        except Exception:
            pass
        raise AuthError(f"{branch['key']}: login rejected. {err}".strip())

    screenshot(page, branch["key"], "auth_ok")
    log("auth", "login", "ok", int((time.time() - t0) * 1000),
        {"branch": branch["key"]})


def _enter_pin(page: Page, pin: str) -> None:
    """Enter the manager PIN.

    Dines shows a touch keypad (#pin-overlay with one button per digit), not a
    text field, so the PIN is clicked digit by digit. A text input is still
    handled in case the overlay changes. The PIN itself is never logged.
    """
    sel = CONFIG["selectors"]
    overlay = sel.get("pin_overlay", "#pin-overlay")

    try:
        page.wait_for_selector(overlay, timeout=20000)
    except Exception:
        # No overlay: maybe a plain field, maybe already authorised.
        if page.locator(sel["pin_field"]).count():
            page.fill(sel["pin_field"], pin)
            page.keyboard.press("Enter")
        return

    if page.locator(f"{overlay} input").count():
        page.fill(f"{overlay} input", pin)
        page.keyboard.press("Enter")
    else:
        for digit in pin.strip():
            if not digit.isdigit():
                raise NavError("manager PIN must be digits only for the keypad")
            key = page.locator(f"{overlay} button").filter(
                has_text=re.compile(rf"^\s*{digit}\s*$"))
            if not key.count():
                raise NavError(f"keypad has no button for one of the PIN digits")
            key.first.click(timeout=10000)
            page.wait_for_timeout(150)

    # The keypad usually submits itself on the last digit.
    try:
        page.wait_for_selector(overlay, state="hidden", timeout=15000)
    except Exception:
        for submit in (sel.get("pin_submit"), f"{overlay} button[type='submit']"):
            if submit and page.locator(submit).count():
                try:
                    page.locator(submit).first.click(timeout=5000)
                    page.wait_for_selector(overlay, state="hidden", timeout=10000)
                    return
                except Exception:
                    continue
        raise NavError("PIN entered but the overlay did not close — PIN rejected?")


def _ensure_pin(page: Page, pin: str) -> bool:
    """Clear the PIN overlay if it is up. Returns True if a PIN was entered.

    Dines re-prompts on report navigation even with a valid session, and the
    overlay intercepts pointer events page-wide, so every step has to check.
    """
    overlay = CONFIG["selectors"].get("pin_overlay", "#pin-overlay")
    if not page.locator(overlay).count():
        return False
    if not page.locator(overlay).first.is_visible():
        return False
    _enter_pin(page, pin)
    page.wait_for_timeout(1500)
    return True


def _day_bounds(when: datetime) -> tuple[str, str]:
    """Start/end of the venue's day as ISO stamps with the venue's UTC offset.

    The dashboard computes ranges in the BROWSER's timezone. Left alone on a
    machine set to +04:00, a London venue's 'yesterday' would start at 20:00 the
    previous evening, so the offset is pinned to the venue timezone instead.
    """
    tz = ZoneInfo(CONFIG.get("venue_timezone", "Europe/London"))
    start = datetime(when.year, when.month, when.day, 0, 0, 0, tzinfo=tz)
    end = datetime(when.year, when.month, when.day, 23, 59, 59, tzinfo=tz)
    return start.isoformat(), end.isoformat()


def _install_date_route(page: Page, when: datetime) -> list[str]:
    """Force the report fetch onto a single day.

    The on-screen date picker does not work under automation — clicking the
    'Yesterday' preset leaves the range untouched and the export byte-identical
    (verified 2026-08-31). The report data is fetched with explicit start_date /
    end_date parameters, so those are rewritten in flight. The request keeps the
    app's own auth headers, which a direct API call does not have (404).
    """
    glob = CONFIG.get("report_api_glob", "**/get_report/**")
    start, end = _day_bounds(when)
    rewritten: list[str] = []

    def handler(route, request):
        parsed = urlparse.urlparse(request.url)
        query = urlparse.parse_qs(parsed.query)
        if "start_date" not in query:
            route.continue_()
            return
        query["start_date"] = [start]
        query["end_date"] = [end]
        query["timespan"] = [""]
        new_url = parsed._replace(
            query=urlparse.urlencode(query, doseq=True)).geturl()
        rewritten.append(new_url)
        route.continue_(url=new_url)

    page.route(glob, handler)
    return rewritten


# ── Stage 2: navigate + export ──────────────────────────────────────────
REPORT_IDS_PATH = STATE_DIR / "report_ids.json"


def _cached_report_id(branch: dict) -> Optional[str]:
    if REPORT_IDS_PATH.exists():
        try:
            return json.loads(REPORT_IDS_PATH.read_text()).get(branch["key"])
        except Exception:
            return None
    return None


def _cache_report_id(branch: dict, report_id: str) -> None:
    data = {}
    if REPORT_IDS_PATH.exists():
        try:
            data = json.loads(REPORT_IDS_PATH.read_text())
        except Exception:
            data = {}
    data[branch["key"]] = report_id
    REPORT_IDS_PATH.write_text(json.dumps(data, indent=2))


def _find_report_id(page: Page, branch: dict, pin: str) -> str:
    """Resolve this venue's 'Sales by Product' report id.

    Every branch is a separate Dines account, so the report id differs per
    venue — it must never be hardcoded. Resolved from the reports list by link
    text and then cached, because the list page costs a PIN prompt to reach.
    """
    cached = _cached_report_id(branch)
    if cached:
        return cached

    urls = CONFIG["portal"]
    page.goto(urls["reports_list_url"], wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1500)
    _ensure_pin(page, pin)
    page.wait_for_timeout(1500)

    name = CONFIG.get("report_link_text", "Sales by Product")
    link = page.locator(f"a:has-text('{name}')")
    try:
        link.first.wait_for(state="attached", timeout=30000)
    except Exception as exc:
        raise NavError(f"{branch['key']}: no '{name}' report in the reports "
                       f"list ({exc})") from exc
    href = link.first.get_attribute("href") or ""
    m = re.search(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
                  href)
    if not m:
        raise NavError(f"{branch['key']}: '{name}' link has no report id: {href!r}")
    report_id = m.group(1)
    _cache_report_id(branch, report_id)
    log("export", "resolved_report_id", "ok",
        extra={"branch": branch["key"], "report_id": report_id})
    return report_id


@retry(max_attempts=3, exceptions=(NavError,))
def stage_export(page: Page, branch: dict, when: datetime) -> pathlib.Path:
    """Fetch one day's Sales by Product export.

    Deliberately short: the manager-PIN token expires quickly, and a long
    click-chain (Reports → Reporting → Sales by Product → Export) reliably ran
    out of time before reaching Export. Going straight to the report URL keeps
    the window small. The date comes from rewriting the report's own fetch,
    since the on-screen picker does nothing under automation.
    """
    _u, _p, pin = credentials(branch)
    urls = CONFIG["portal"]
    rewritten = _install_date_route(page, when)

    report_id = _find_report_id(page, branch, pin)
    page.goto(urls["report_url_template"].format(report_id=report_id),
              wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2000)
    _ensure_pin(page, pin)
    page.wait_for_timeout(int(CONFIG.get("render_wait_s", 6)) * 1000)

    step = {"click": CONFIG["selectors"]["export_button"],
            "poll_attempts": int(CONFIG.get("export_attempts", 3)),
            "poll_interval_s": int(CONFIG.get("export_interval_s", 8))}
    raw_path = _download(page, branch, step, when, pin)

    if not rewritten:
        raise NavError(
            "the report fetch was never intercepted, so the export covers the "
            f"report's default range, not {when:%Y-%m-%d}. Refusing to use it.")
    log("export", "downloaded", "ok",
        extra={"branch": branch["key"], "file": raw_path.name,
               "forced_range": rewritten[-1].split("start_date=")[-1][:40]})
    return raw_path


def _download(page: Page, branch: dict, step: dict, when: datetime,
              pin: str = "") -> pathlib.Path:
    attempts = int(step.get("poll_attempts", 3))
    interval = int(step.get("poll_interval_s", 10))
    for attempt in range(1, attempts + 1):
        try:
            # The Export control only appears once the async report has
            # rendered, so wait for it rather than clicking blind. A PIN
            # overlay can reappear here and hide it.
            if pin:
                _ensure_pin(page, pin)
            page.wait_for_selector(step["click"], state="visible", timeout=40000)
            page.wait_for_timeout(2000)
            with page.expect_download(timeout=60000) as dl:
                page.click(step["click"], timeout=30000)
            download = dl.value
            suffix = pathlib.Path(download.suggested_filename).suffix or ".csv"
            raw = (DOWNLOADS_DIR /
                   f"dines_{branch['key']}_{when:%Y%m%d}_{RUN_ID[:15]}_raw{suffix}")
            download.save_as(str(raw))
            screenshot(page, branch["key"], "export_ok")
            return raw
        except Exception as exc:
            if attempt == attempts:
                screenshot(page, branch["key"], "export_failed")
                raise NavError(
                    f"{branch['key']}: export did not download after {attempts} "
                    f"attempts: {exc}") from exc
            page.wait_for_timeout(interval * 1000)
            page.reload(wait_until="domcontentloaded")
            page.wait_for_timeout(2000)
            if pin:
                _ensure_pin(page, pin)
                page.wait_for_timeout(int(CONFIG.get("render_wait_s", 6)) * 1000)


# ── Stage 3: transform ──────────────────────────────────────────────────
def _read_raw(path: pathlib.Path) -> pd.DataFrame:
    if path.stat().st_size == 0:
        return pd.DataFrame()
    if path.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(path)
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
    raise TransformError(f"Could not decode {path}")


def _plu_lookup(plu_files: Optional[list[pathlib.Path]]) -> tuple[dict, list[str]]:
    """Reuse the Black Bear PLU loader so there is one implementation, not two."""
    notes: list[str] = []
    configured = CONFIG.get("plu", {}).get("file")
    if configured:
        # Config paths are repo-relative, not relative to the working directory,
        # so a cron job or a run from another folder resolves the same file.
        cfg_path = pathlib.Path(configured)
        if not cfg_path.is_absolute():
            cfg_path = BASE_DIR / cfg_path
    else:
        cfg_path = None
    files = plu_files or ([cfg_path] if cfg_path else None)
    if not files:
        notes.append("PLU source: NONE — 'POS Item ID *' falls back to the item name. "
                     "Dines PLU codes are unconfirmed (team said they differ from "
                     "Deliveroo's); pass --plu-file once settled.")
        return {}, notes
    try:
        import blackbear_convert as bb
        return bb.load_plu_codes(None, files, notes), notes
    except Exception as exc:
        raise TransformError(f"Could not load PLU codes: {exc}") from exc


def stage_transform(raw_path: pathlib.Path, branch: dict, when: datetime,
                    plu_files: Optional[list[pathlib.Path]] = None) -> tuple:
    t0 = time.time()
    df = _read_raw(raw_path)
    if df.empty:
        log("transform", "empty_export", "warning", extra={"branch": branch["key"],
            "message": "no rows — treated as no sales"})
        return None, 0, []

    df.columns = [str(c).strip() for c in df.columns]
    plu, notes = _plu_lookup(plu_files)

    src = {c["raw"]: c for c in CONFIG["columns"] if c.get("raw")}
    missing = [r for r in src if r not in df.columns]
    if missing:
        raise TransformError(
            f"{branch['key']}: export is missing expected column(s) {missing}. "
            f"Found: {list(df.columns)}")

    out = pd.DataFrame()
    for raw_name, spec in src.items():
        col = df[raw_name]
        # Dines writes thousands separators once a figure passes 999
        # ("1,201.90", "1,234"). Coercing those without stripping the comma
        # yields NaN, and defaulting NaN to 0 would silently report a
        # best-selling item as zero sold while its revenue stayed correct.
        if spec["dtype"] == "int":
            raw = col.astype(str).str.strip()
            num = pd.to_numeric(raw.str.replace(r"[^\d\-]", "", regex=True),
                                errors="coerce")
            unparsed = num.isna() & raw.ne("") & raw.ne("nan")
            if unparsed.any():
                bad = raw[unparsed].unique()[:5].tolist()
                raise TransformError(
                    f"{branch['key']}: could not read {unparsed.sum()} value(s) "
                    f"in '{raw_name}' as a whole number, e.g. {bad}. Refusing to "
                    f"guess — a wrong quantity is worse than a failed run.")
            col = num.fillna(0).round().astype(int)
        elif spec["dtype"] == "float":
            col = pd.to_numeric(
                col.astype(str).str.replace(r"[^\d.\-]", "", regex=True),
                errors="coerce").fillna(0.0)
        else:
            col = col.astype(str).str.strip()
        out[spec["target"]] = col

    # Drop blank product rows and any total/subtotal footer the export appends.
    name_col = "POS Item Name"
    out = out[out[name_col].notna()]
    out = out[out[name_col].str.len() > 0]
    out = out[~out[name_col].str.strip().str.casefold().isin(
        ("total", "totals", "grand total", "nan"))]

    vat = float(CONFIG.get("vat_rate", 0.20))
    incl = out["Total sales incl. tax *"].round(2)
    out["Total sales incl. tax *"] = incl
    # Dines figures INCLUDE VAT — divide. (Deliveroo is net; it multiplies.)
    out["Total sales excl. tax *"] = (incl / (1.0 + vat)).round(2)

    date_str = when.strftime(DATE_FMT)
    out["Sales Date *"] = date_str
    out["Total Discount Value"] = 0.0        # per the team's spec: 0, not blank
    for c in ("Order ID", "Sales Type Code", "Parent Item ID"):
        out[c] = ""

    unmatched: list[str] = []
    def code_for(name: str) -> str:
        if not plu:
            return name
        import blackbear_convert as bb
        hit = plu.get(bb.norm_item(name))
        if not hit:
            unmatched.append(name)
        return hit or name
    out["POS Item ID *"] = out[name_col].map(code_for)

    out = out[SUPY_COLUMNS].sort_values(name_col).reset_index(drop=True)

    stamp = when.strftime("%Y-%m-%d")
    safe = "".join(ch if ch.isalnum() else "_" for ch in branch["supy_branch"]).strip("_")
    out_dir = OUTPUT_DIR / stamp
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"dines_{safe}_{stamp}.xlsx"
    out.to_excel(path, index=False)

    log("transform", "done", "ok", int((time.time() - t0) * 1000),
        {"branch": branch["key"], "rows": len(out),
         "unmatched_plu": len(set(unmatched))})
    if unmatched:
        log("transform", "plu_unmatched", "warning",
            extra={"branch": branch["key"], "items": sorted(set(unmatched))[:20],
                   "message": f"{len(set(unmatched))} item(s) without a PLU code"})
    if unmatched:
        notes = notes + [f"items without a PLU code: {sorted(set(unmatched))}"]
    return path, len(out), notes


# ── Stage 4: email ──────────────────────────────────────────────────────
def recipients(override: Optional[list[str]]) -> list[str]:
    if override:
        raw = ",".join(override)
    else:
        raw = (os.environ.get("DINES_REPORT_RECIPIENT")
               or os.environ.get("REPORT_RECIPIENT") or "")
    seen, out = set(), []
    for part in raw.replace(";", ",").split(","):
        addr = part.strip()
        if addr and addr.casefold() not in seen:
            seen.add(addr.casefold())
            out.append(addr)
    if not out:
        raise EmailError(
            "No recipients. Set DINES_REPORT_RECIPIENT or REPORT_RECIPIENT in .env, "
            "or pass --email-to.")
    return out


def stage_email(path: pathlib.Path, branch: dict, when: datetime, rows: int,
                to: list[str], notes: list[str]) -> None:
    t0 = time.time()
    user = os.environ.get("GMAIL_USER", "")
    pw = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not user or not pw:
        raise EmailError("GMAIL_USER / GMAIL_APP_PASSWORD missing from .env")

    prefix = CONFIG.get("email", {}).get("subject_prefix", "Dines")
    body = [
        "Hi,",
        "",
        f"Attached is the Dines sales upload for {branch['supy_branch']}.",
        "",
        f"  * Sales date : {when.strftime(DATE_FMT)}",
        f"  * Branch     : {branch['supy_branch']}",
        f"  * Rows       : {rows:,}",
        f"  * File       : {path.name}",
        "",
        "Dines reports VAT-inclusive values, so 'Total sales excl. tax *' is "
        "derived by dividing by 1.20. 'Total Discount Value' is 0 per the "
        "documented process.",
    ]
    if notes:
        body += ["", "Notes:"] + [f"  - {n}" for n in notes]
    body += ["", "Regards,", "Supy POS Integration"]

    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = ", ".join(to)
    msg["Subject"] = (f"{prefix} — {branch['supy_branch']} "
                      f"({when.strftime(DATE_FMT)})")
    msg.attach(MIMEText("\n".join(body), "plain"))
    with open(path, "rb") as fh:
        part = MIMEApplication(
            fh.read(),
            _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    part.add_header("Content-Disposition", "attachment", filename=path.name)
    msg.attach(part)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(user, pw)
            server.sendmail(user, to, msg.as_string())
    except Exception as exc:
        raise EmailError(f"{branch['key']}: send failed: {exc}") from exc

    log("email", "sent", "ok", int((time.time() - t0) * 1000),
        {"branch": branch["key"], "to": to})
    print(f"  ✓ Email sent → {', '.join(to)}")


# ── Selector discovery ──────────────────────────────────────────────────
def _dump(page: Page, kinds=("input", "button", "a[href]", "form"),
          root: str = "", limit: int = 40) -> None:
    """Print interactive elements. Never reads `value`, so no secret is shown."""
    for kind in kinds:
        sel = f"{root} {kind}".strip()
        loc = page.locator(sel)
        total = loc.count()
        print(f"── {sel}  ({total} found)")
        for i in range(min(total, limit)):
            el = loc.nth(i)
            try:
                attrs = {a: el.get_attribute(a) for a in
                         ("id", "name", "type", "placeholder", "inputmode",
                          "data-test-id", "aria-label", "class", "href")}
                text = (el.inner_text() or "").strip().replace("\n", " ")[:36]
                shown = {k: (v[:60] if k == "class" else v)
                         for k, v in attrs.items() if v}
                print(f"   [{i}] {shown}  text={text!r}")
            except Exception:
                pass
        print()


def discover_deep(page: Page, branch: dict) -> None:
    """Walk the real export flow, dumping the page's controls at each stage.

    Everything past the login is invisible from outside, so this follows the
    same path stage_export takes and prints what each page offers. Stops at the
    first failure and dumps what IS present, which is what a selector needs to
    become. Credentials come from .env and are never printed.
    """
    _u, _p, pin = credentials(branch)
    urls = CONFIG["portal"]

    def controls() -> None:
        for kind in ("button", "a[href]", "select", "input"):
            loc = page.locator(kind)
            items = []
            for i in range(min(loc.count(), 40)):
                try:
                    el = loc.nth(i)
                    txt = (el.inner_text() or "").strip().replace("\n", " ")[:30]
                    ident = el.get_attribute("id") or el.get_attribute("data-test-id")
                    label = txt or el.get_attribute("placeholder") or ident or ""
                    if label:
                        items.append(repr(label))
                except Exception:
                    pass
            if items:
                print(f"    {kind:10s} {', '.join(dict.fromkeys(items))[:400]}")

    stage_auth(page, branch, force_login=False)
    print(f"\n═══ logged in → {page.url} ═══")
    controls()

    print(f"\n═══ reports list ═══")
    page.goto(urls["reports_list_url"], wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(1500)
    if _ensure_pin(page, pin):
        print("    (cleared PIN overlay)")
    page.wait_for_timeout(1500)
    print(f"    url: {page.url}")
    controls()

    try:
        report_id = _find_report_id(page, branch, pin)
        print(f"\n═══ report id: {report_id} ═══")
    except Exception as exc:
        print(f"    ✗ could not resolve the report id: {str(exc)[:160]}")
        screenshot(page, branch["key"], "deep_no_report_id")
        return

    when = datetime.now() - timedelta(days=1)
    rewritten = _install_date_route(page, when)
    page.goto(urls["report_url_template"].format(report_id=report_id),
              wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2000)
    if _ensure_pin(page, pin):
        print("    (cleared PIN overlay)")
    page.wait_for_timeout(int(CONFIG.get("render_wait_s", 6)) * 1000)
    print(f"\n═══ report page ({when:%Y-%m-%d} forced) ═══")
    print(f"    date rewrite fired: {len(rewritten)} request(s)")
    if rewritten:
        print(f"    {rewritten[-1][:150]}")
    export_sel = CONFIG["selectors"]["export_button"]
    print(f"    export button visible: "
          f"{page.locator(export_sel).first.is_visible() if page.locator(export_sel).count() else False}")
    controls()
    screenshot(page, branch["key"], "deep_report")
    print(f"\nScreenshots → screenshots/dines_{RUN_ID}/")


def discover(page: Page, branch: dict) -> None:
    """Print every interactive element on the login page.

    The selectors in dines_config.yaml are unverified guesses — this is how
    they get corrected without anyone reading a password out loud.
    """
    page.goto(CONFIG["portal"]["login_url"], wait_until="domcontentloaded",
              timeout=60000)
    page.wait_for_timeout(3000)
    print(f"\nURL   : {page.url}")
    print(f"Title : {page.title()}\n")
    _dump(page, limit=30)
    screenshot(page, branch["key"], "discover")
    print(f"Screenshot → screenshots/dines_{RUN_ID}/{branch['key']}_discover.png")


# ── Orchestration ───────────────────────────────────────────────────────
def run_branch(pw_ctx, branch: dict, args, when: datetime) -> tuple[str, int]:
    """Returns (outcome, rows). Never raises — failures are reported per branch."""
    browser = pw_ctx.chromium.launch(headless=not args.debug)
    state = _session_path(branch)
    ctx_args = {"accept_downloads": True,
                "timezone_id": CONFIG.get("venue_timezone", "Europe/London"),
                "locale": CONFIG.get("venue_locale", "en-GB")}
    if state.exists() and not args.force_login:
        ctx_args["storage_state"] = str(state)
    context = browser.new_context(**ctx_args)
    page = context.new_page()
    try:
        if args.discover:
            (discover_deep if args.deep else discover)(page, branch)
            return "discover", 0

        stage_auth(page, branch, args.force_login)
        context.storage_state(path=str(state))

        raw = stage_export(page, branch, when)
        path, rows, notes = stage_transform(raw, branch, when, args.plu_file)
        if path is None:
            print(f"  – no sales for {branch['supy_branch']}")
            return "no_sales", 0
        print(f"  ✓ {path.name}  {rows} rows")

        if not args.no_email:
            stage_email(path, branch, when, rows, recipients(args.email_to), notes)
        return "ok", rows

    except tuple(EXIT) as exc:
        kind = type(exc).__name__
        log("run", branch["key"], "error", extra={"error": str(exc), "kind": kind})
        print(f"  ✗ {branch['supy_branch']}: {kind}: {exc}", file=sys.stderr)
        return kind, 0
    finally:
        context.close()
        browser.close()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--branch", help="branch key (see --list-branches)")
    g.add_argument("--all-branches", action="store_true")
    ap.add_argument("--list-branches", action="store_true")
    ap.add_argument("--date", help="sales date YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--debug", action="store_true", help="headed browser + verbose")
    ap.add_argument("--no-email", action="store_true")
    ap.add_argument("--email-to", action="append", metavar="ADDR")
    ap.add_argument("--force-login", action="store_true")
    ap.add_argument("--plu-file", type=pathlib.Path, action="append", metavar="PATH",
                    help="PLU sheet mapping item name → POS code (repeatable)")
    ap.add_argument("--from-file", type=pathlib.Path,
                    help="skip the browser; transform an export already on disk")
    ap.add_argument("--deep", action="store_true",
                    help="With --discover: log in and dump the PIN overlay and "
                         "dashboard nav (needs credentials in .env)")
    ap.add_argument("--discover", action="store_true",
                    help="dump login-page selectors and exit (no credentials used "
                         "beyond opening the page)")
    args = ap.parse_args()

    _init_logger(args.debug)

    if args.list_branches:
        print(f"{'key':16s} {'supy branch':34s} {'env prefix':14s} credentials")
        for b in branches():
            missing = missing_credentials(b)
            state = "ready" if not missing else (
                "missing " + ", ".join(k.rsplit("_", 1)[1] for k in missing))
            print(f"{b['key']:16s} {b['supy_branch']:34s} "
                  f"{b['env_prefix'] + '_*':14s} {state}")
        return 0

    try:
        when = target_date(args.date)

        if args.from_file:
            branch = find_branch(args.branch or "")
            path, rows, notes = stage_transform(args.from_file, branch, when,
                                                args.plu_file)
            if path is None:
                print("no rows in that file")
                return 0
            print(f"  ✓ {path}  {rows} rows")
            if not args.no_email:
                stage_email(path, branch, when, rows,
                            recipients(args.email_to), notes)
            return 0

        targets = branches() if args.all_branches else [find_branch(args.branch)] \
            if args.branch else None
        if targets is None:
            ap.error("pass --branch <key>, --all-branches, or --list-branches")

    except ConfigError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    # Not every branch has credentials yet — all nine are configured, but only
    # the ones whose .env keys exist can run. Checked BEFORE any browser is
    # launched: an unconfigured branch is skipped, not driven to a login it
    # cannot complete. --discover needs no credentials (it only reads the
    # login page), so it is exempt.
    results: dict[str, tuple[str, int]] = {}
    notices: list[str] = []
    if not args.discover:
        runnable = []
        for branch in targets:
            missing = missing_credentials(branch)
            if not missing:
                runnable.append(branch)
            elif args.branch:
                # explicitly asked for by name — never silently skipped
                print(f"Error: {credential_error(branch, missing)}",
                      file=sys.stderr)
                return 1
            else:
                results[branch["key"]] = ("no_creds", 0)
                notices.append(f"  – {branch['supy_branch']}: no credentials "
                               f"in .env ({branch['env_prefix']}_*)")
                log("run", branch["key"], "skipped",
                    extra={"reason": "no_creds", "missing": missing})
        targets = runnable

    header = f"branches={len(targets)}"
    if results:
        header += f" ({len(results)} skipped, no credentials)"
    print(f"[Dines Pipeline] run_id={RUN_ID}  date={when:%Y-%m-%d}  {header}")
    for line in notices:
        print(line)

    # No browser is started when nothing is runnable — a run where every
    # branch is awaiting credentials should cost nothing and exit clean.
    if targets:
        with sync_playwright() as pw_ctx:
            for i, branch in enumerate(targets, start=1):
                print(f"\n[{i}/{len(targets)}] {branch['supy_branch']}")
                # run_branch only converts the EXIT exceptions into a result;
                # anything else (a Playwright nav timeout, most often) used to
                # propagate and abandon every branch still queued behind it.
                # One flaky portal response must not cost the whole run.
                try:
                    results[branch["key"]] = run_branch(pw_ctx, branch, args, when)
                except Exception as exc:
                    kind = type(exc).__name__
                    log("run", branch["key"], "error",
                        extra={"error": str(exc)[:500], "kind": kind,
                               "unexpected": True})
                    print(f"  ✗ {branch['supy_branch']}: {kind}: "
                          f"{str(exc).splitlines()[0][:200]}", file=sys.stderr)
                    results[branch["key"]] = (kind, 0)

    order = [b["key"] for b in branches() if b["key"] in results]
    print(f"\n{'branch':16s} {'result':16s} rows")
    for key in order:
        outcome, rows = results[key]
        print(f"{key:16s} {outcome:16s} {rows}")

    waiting = [k for k, (o, _) in results.items() if o == "no_creds"]
    if waiting:
        print(f"\n{len(waiting)} branch(es) not run for want of credentials: "
              f"{', '.join(waiting)}\n"
              f"Add each one with: python set_credential.py --partner dines_<key>"
              f"  (never paste credentials into a chat or a config file)")

    failed = [k for k, (o, _) in results.items()
              if o not in ("ok", "no_sales", "discover", "no_creds")]
    if failed:
        print(f"\n!! failed: {', '.join(failed)} — see logs/dines_{RUN_ID}.jsonl",
              file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
