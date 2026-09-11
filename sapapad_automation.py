"""
sapapad_automation.py
----------------------
Sapapad POS Automation Pipeline — 4-Stage Orchestrator

Stage 1: Authentication   (Playwright — login + session caching)
Stage 2: Navigation       (Playwright — report URL + CSV download)
Stage 3: Transformation   (Pandas — raw CSV → formatted .xlsx + item code matching)
Stage 4: Email            (smtplib — attach .xlsx and send via Gmail)

Usage:
    python sapapad_automation.py                 # headless, full pipeline + email
    python sapapad_automation.py --debug         # headed browser, verbose logging
    python sapapad_automation.py --no-email      # skip email, save locally only
    python sapapad_automation.py --from-stage 3  # replay transform only (raw file must exist)
    python sapapad_automation.py --force-login   # ignore cached session, always re-auth

Exit codes:
    0  success
    1  AuthError
    2  NavError
    3  TransformError
    4  EmailError
"""

import argparse
import functools
import json
import os
import pathlib
import re
import smtplib
import sys
import time
import traceback
import uuid
from datetime import datetime, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape as html_escape
from typing import Optional

import pandas as pd
import yaml
from dotenv import load_dotenv
from playwright.sync_api import Page, sync_playwright

# ──────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ──────────────────────────────────────────────────────────────────────────────

load_dotenv()

BASE_DIR      = pathlib.Path(__file__).parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
OUTPUT_DIR    = BASE_DIR / "output"
LOGS_DIR      = BASE_DIR / "logs"
SCREENSHOTS_DIR = BASE_DIR / "screenshots"
MAPPINGS_DIR  = BASE_DIR / "mappings"


def _config_path_from_argv() -> pathlib.Path:
    """
    Resolve --config before argparse runs.

    The config is read at import time (selectors and column maps are needed by
    module-level code), which is earlier than argparse. Rather than restructure
    that, --config is picked out of sys.argv here. Defaults to
    sapapad_config.yaml so BMD keeps working exactly as before.
    """
    argv = sys.argv[1:]
    for i, a in enumerate(argv):
        if a == "--config" and i + 1 < len(argv):
            return pathlib.Path(argv[i + 1])
        if a.startswith("--config="):
            return pathlib.Path(a.split("=", 1)[1])
    return BASE_DIR / "sapapad_config.yaml"


CONFIG_PATH = _config_path_from_argv()
if not CONFIG_PATH.is_absolute():
    CONFIG_PATH = BASE_DIR / CONFIG_PATH
if not CONFIG_PATH.exists():
    sys.exit(f"[x] No such config: {CONFIG_PATH}")

with open(CONFIG_PATH) as _f:
    CONFIG = yaml.safe_load(_f)

# Every tenant on this POS shares the scraping logic but must NOT share state:
# one storage_state per account, or logging into the second silently invalidates
# the first. tenant defaults to "sapapad" so BMD's existing state dir is reused.
TENANT = str((CONFIG.get("tenant") or {}).get("slug") or "sapapad").strip()
STATE_DIR = BASE_DIR / "state" / TENANT

for d in (DOWNLOADS_DIR, OUTPUT_DIR, STATE_DIR, LOGS_DIR, SCREENSHOTS_DIR, MAPPINGS_DIR):
    d.mkdir(parents=True, exist_ok=True)

STORAGE_STATE_PATH = STATE_DIR / "storage_state.json"
CHECKPOINT_PATH    = STATE_DIR / "checkpoint.json"


# ──────────────────────────────────────────────────────────────────────────────
# Custom Exceptions
# ──────────────────────────────────────────────────────────────────────────────

class AuthError(Exception):
    """Wrong credentials or session invalid — do NOT retry."""


class NavError(Exception):
    """Menu navigation or download failure — retryable."""


class TransformError(Exception):
    """Data transformation failure — raw file is preserved."""


class EmailError(Exception):
    """Email delivery failure — report was generated but not sent."""


# ──────────────────────────────────────────────────────────────────────────────
# Verification Result
# ──────────────────────────────────────────────────────────────────────────────

class VerificationResult:
    """Accumulates findings from the 3-layer verification stage."""

    def __init__(self):
        self.passed = True
        self.warnings = []
        self.errors = []

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def fail(self, msg: str) -> None:
        self.errors.append(msg)
        self.passed = False

    def status(self) -> str:
        if self.errors:
            return "FAIL"
        if self.warnings:
            return "WARN"
        return "PASS"

    def summary_lines(self) -> list:
        lines = []
        if not self.warnings and not self.errors:
            lines.append("All verification checks passed.")
            return lines
        if self.errors:
            lines.append(f"ERRORS ({len(self.errors)}):")
            for e in self.errors:
                lines.append(f"  [x] {e}")
        if self.warnings:
            lines.append(f"Warnings ({len(self.warnings)}):")
            for w in self.warnings:
                lines.append(f"  [!] {w}")
        return lines


# ──────────────────────────────────────────────────────────────────────────────
# Run ID + Structured Logger
# ──────────────────────────────────────────────────────────────────────────────

RUN_ID = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
_log_path: Optional[pathlib.Path] = None
_verbose = False


def _init_logger(verbose: bool) -> None:
    global _log_path, _verbose
    _verbose = verbose
    _log_path = LOGS_DIR / f"{TENANT}_{RUN_ID}.jsonl"


def log(stage: str, step: str, outcome: str, duration_ms: int = 0, extra: dict = None) -> None:
    entry = {
        "ts": datetime.utcnow().isoformat(),
        "run_id": RUN_ID,
        "pipeline": TENANT,
        "stage": stage,
        "step": step,
        "outcome": outcome,
        "duration_ms": duration_ms,
        **(extra or {}),
    }
    if _log_path:
        with open(_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
    if _verbose:
        icon = "✓" if outcome == "ok" else "✗" if outcome == "error" else "→"
        print(f"  [{icon}] [{stage}] {step}  ({duration_ms}ms)")
    elif outcome in ("error", "warning"):
        stream = sys.stderr if outcome == "error" else sys.stdout
        print(f"  [{'✗' if outcome == 'error' else '!'}] [{stage}] {step}: "
              f"{extra.get('error', extra.get('message', '')) if extra else ''}", file=stream)


# ──────────────────────────────────────────────────────────────────────────────
# Screenshot Helper
# ──────────────────────────────────────────────────────────────────────────────

def screenshot(page: Page, stage: str, label: str) -> None:
    run_dir = SCREENSHOTS_DIR / f"{TENANT}_{RUN_ID}"
    run_dir.mkdir(exist_ok=True)
    path = run_dir / f"{stage}_{label}.png"
    try:
        page.screenshot(path=str(path), full_page=False)
        if _verbose:
            print(f"       [📸] {path.name}")
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# Checkpoint Manager
# ──────────────────────────────────────────────────────────────────────────────

def read_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        with open(CHECKPOINT_PATH) as f:
            return json.load(f)
    return {}


def write_checkpoint(stage: int, extra: dict = None) -> None:
    data = {"run_id": RUN_ID, "completed_stage": stage, "ts": datetime.utcnow().isoformat()}
    if extra:
        data.update(extra)
    with open(CHECKPOINT_PATH, "w") as f:
        json.dump(data, f, indent=2)


# ──────────────────────────────────────────────────────────────────────────────
# Retry Decorator
# ──────────────────────────────────────────────────────────────────────────────

def retry(max_attempts: int = 3, base_delay: float = 1.5, exceptions=(NavError,)):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except AuthError:
                    raise
                except TransformError:
                    raise
                except exceptions as exc:
                    last_exc = exc
                    delay = base_delay * (2 ** (attempt - 1))
                    print(f"  [!] {fn.__name__} attempt {attempt}/{max_attempts} failed: {exc}. "
                          f"Retrying in {delay:.1f}s...", file=sys.stderr)
                    time.sleep(delay)
            raise last_exc
        return wrapper
    return decorator


# ──────────────────────────────────────────────────────────────────────────────
# Stage 1 — Authentication
# ──────────────────────────────────────────────────────────────────────────────

def _session_is_valid(page: Page) -> bool:
    auth_el = CONFIG["portal"].get("authenticated_element", "")
    if not auth_el:
        return False
    try:
        page.goto(CONFIG["portal"]["portal_url"], wait_until="domcontentloaded", timeout=20_000)
        page.wait_for_selector(auth_el, timeout=5_000)
        return True
    except Exception:
        return False


# The sales date this run covers. None = yesterday, i.e. the daily job.
TARGET_DATE: Optional[datetime] = None


def target_date() -> datetime:
    """The day being reported. Yesterday unless --date said otherwise.

    Every date decision goes through here. Before 2026-09-03 the date was
    derived independently in three places (the nav click, the Sales Date
    injector and the validator), which is why the pipeline could only ever
    produce yesterday and ten August days could not be recovered.
    """
    if TARGET_DATE is not None:
        return TARGET_DATE
    return datetime.now() - timedelta(days=1)


def stage_auth(page: Page, context, force_login: bool) -> None:
    t0 = time.monotonic()
    sel = CONFIG["selectors"]

    for key in ("username_field", "password_field", "login_button"):
        if not sel.get(key) or sel[key] == "FILL_IN":
            raise AuthError(
                f"sapapad_config.yaml selectors.{key} is not configured. "
                "Run with --debug and inspect the login page to find the correct selector."
            )

    if not force_login and STORAGE_STATE_PATH.exists():
        if _verbose:
            print("  [→] Checking cached session...")
        if _session_is_valid(page):
            log("auth", "session_cache_hit", "ok",
                duration_ms=int((time.monotonic() - t0) * 1000))
            return
        if _verbose:
            print("  [→] Cached session expired — re-authenticating...")

    # Which .env keys hold this tenant's credentials. Defaults keep BMD on
    # SAPAPAD_* so nothing changes for the existing client.
    _auth = CONFIG.get("auth") or {}
    user_key = _auth.get("username_env", "SAPAPAD_USERNAME")
    comp_key = _auth.get("company_env", "SAPAPAD_COMPANY")
    pass_key = _auth.get("password_env", "SAPAPAD_PASSWORD")

    username = os.environ.get(user_key, "")
    company  = os.environ.get(comp_key, "")
    password = os.environ.get(pass_key, "")

    if not username:
        raise AuthError(f"{user_key} is not set in your .env file.")
    if not password:
        raise AuthError(f"{pass_key} is not set in your .env file.")

    try:
        page.goto(CONFIG["portal"]["login_url"], wait_until="domcontentloaded", timeout=30_000)
        screenshot(page, "auth", "01_login_page")

        page.wait_for_selector(sel["username_field"], timeout=10_000)
        # Screenshot taken before filling so credentials never appear in an artifact
        screenshot(page, "auth", "02_before_fill")
        page.fill(sel["username_field"], username)

        if sel.get("company_field") and sel["company_field"] not in ("", "FILL_IN"):
            page.fill(sel["company_field"], company)

        page.fill(sel["password_field"], password)

        with page.expect_navigation(wait_until="domcontentloaded", timeout=45_000):
            page.click(sel["login_button"])

        error_sel = sel.get("login_error", "")
        if error_sel:
            try:
                page.wait_for_selector(error_sel, timeout=3_000)
                # Clear credential fields before screenshotting so they don't appear in artifacts
                try:
                    page.fill(sel["username_field"], "")
                    page.fill(sel["password_field"], "")
                except Exception:
                    pass
                screenshot(page, "auth", "03_login_error")
                raise AuthError("Login failed — error element detected on page.")
            except AuthError:
                raise
            except Exception:
                pass

        authenticated_el = CONFIG["portal"].get("authenticated_element", "")
        if authenticated_el:
            page.wait_for_selector(authenticated_el, timeout=15_000)

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
# Stage 2 — Navigation & Download
# ──────────────────────────────────────────────────────────────────────────────

@retry(max_attempts=3, exceptions=(NavError,))
def _select_report_date(page: Page) -> None:
    """Point the dashboard's date filter at target_date().

    Yesterday keeps the original preset click — the daily job is in production
    and its path is proven. Any other date goes through Custom, typing a
    range that mirrors the portal's own 04:00 -> 04:00 business day.
    """
    df = CONFIG.get("date_filter") or {}
    when = target_date()
    is_yesterday = when.date() == (datetime.now() - timedelta(days=1)).date()

    def option(kind: str) -> str:
        """Selector for a date option — id if configured, else its text."""
        by_id = df.get(f"{kind}_option")
        if by_id:
            return by_id
        return df["option_template"].format(label=df[f"{kind}_label"])

    # Open the dropdown only if it is not already open. Clicking the toggle
    # when it IS open closes it, leaving every option present-but-hidden.
    custom_sel = option("custom")
    already_open = False
    try:
        el = page.query_selector(custom_sel)
        already_open = bool(el and el.is_visible())
    except Exception:
        already_open = False
    if not already_open:
        page.click(df["toggle"])
    page.wait_for_selector(custom_sel, state="visible", timeout=20_000)

    if is_yesterday:
        label = df["yesterday_label"]
        page.click(option("yesterday"))
        page.wait_for_selector(f"{df['toggle']}:has-text('{label}')", timeout=20_000)
        log("nav", "select_date", "ok", extra={"mode": "preset", "label": label})
        return

    page.click(custom_sel)
    page.wait_for_selector(df["from_field"], state="visible", timeout=20_000)

    fmt = df.get("input_format", "%d/%m/%Y %H:%M:%S")
    # Reuse the day-boundary time the portal itself put in the field, so a
    # changed business-day start is picked up rather than assumed.
    day_start = df.get("default_day_start", "04:00:00")
    try:
        existing = page.input_value(df["from_field"]) or ""
        parsed = datetime.strptime(existing.strip(), fmt)
        day_start = parsed.strftime("%H:%M:%S")
    except Exception:
        pass

    start = datetime.strptime(f"{when:%d/%m/%Y} {day_start}", "%d/%m/%Y %H:%M:%S")
    end = start + timedelta(days=1)
    page.fill(df["from_field"], "")
    page.fill(df["from_field"], start.strftime(fmt))
    page.fill(df["to_field"], "")
    page.fill(df["to_field"], end.strftime(fmt))
    page.click(df["apply_button"])
    page.wait_for_timeout(3000)
    log("nav", "select_date", "ok",
        extra={"mode": "custom", "from": start.strftime(fmt),
               "to": end.strftime(fmt)})
    if _verbose:
        print(f"  [date] custom range {start.strftime(fmt)} -> {end.strftime(fmt)}")


def modifiers_config(args) -> Optional[dict]:
    """
    The modifiers block if this tenant has one and it was not disabled.

    Returns None for BMD and anyone else whose config predates the modifiers
    work, so those runs are untouched.
    """
    cfg = CONFIG.get("modifiers") or {}
    if not cfg.get("enabled"):
        return None
    if getattr(args, "no_modifiers", False):
        return None
    return cfg


def stage_navigate_and_download(
    page: Page,
    location_id: Optional[str] = None,
    branch_name: Optional[str] = None,
    section: Optional[dict] = None,
) -> pathlib.Path:
    """
    Drive one report's navigation chain and save the CSV it exports.

    `section` selects WHICH report. None means the Top Grossing Items report
    configured at the top level of the YAML — byte-for-byte the old behaviour.
    Passing CONFIG["modifiers"] runs that block's own report_url and navigation
    chain instead, so Marketing -> Top Paid Modifiers reuses this entire
    function (branch selection, export modal, Saved Reports polling) rather
    than duplicating it.
    """
    t0 = time.monotonic()
    section      = section or {}
    is_main      = not section
    nav_steps    = section.get("navigation") or CONFIG["navigation"]
    report_url   = section.get("report_url", CONFIG["portal"].get("report_url", ""))
    # Saved Reports lists one row per generated report, titled
    # "{label} for {branch}" — the label is what tells the two reports apart
    # when both have been queued for the same branch in the same run.
    report_label = section.get("saved_report_label", "Top Grossing Items")
    file_tag     = section.get("file_tag", "raw")
    dest: Optional[pathlib.Path] = None

    try:
        if report_url and report_url not in ("", "FILL_IN"):
            if _verbose:
                print(f"  [→] Navigating to report URL...")
            page.goto(report_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_load_state("networkidle", timeout=30_000)

        # Select a single branch if specified
        if location_id:
            if _verbose:
                print(f"  [→] Selecting branch: {branch_name or location_id}")
            select_location(page, location_id)
            log("nav", f"select_location:{branch_name}", "ok")

        for step_cfg in nav_steps:
            action     = step_cfg.get("action", "click")
            label      = step_cfg["step"]
            safe_label = label.replace(" ", "_")

            if _verbose:
                print(f"  [→] {label}")

            screenshot(page, "nav", f"before_{safe_label}")

            if action == "click":
                _nav_click(page, step_cfg)

            elif action == "select_date":
                _select_report_date(page)
            elif action == "wait_seconds":
                secs = int(step_cfg.get("seconds", 10))
                if _verbose:
                    print(f"       sleeping {secs}s for async export...")
                time.sleep(secs)

            elif action == "accept_modal":
                ok_sel = step_cfg.get("modal_ok_selector", "")
                if not ok_sel or ok_sel == "FILL_IN":
                    raise NavError("accept_modal requires modal_ok_selector in config.")
                page.wait_for_selector(ok_sel, state="visible", timeout=15_000)
                page.click(ok_sel)
                page.wait_for_timeout(1_000)

            elif action == "goto_url":
                url = step_cfg.get("url", "")
                if not url or url == "FILL_IN":
                    raise NavError(f"Step '{label}' has goto_url but url is not set.")
                page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                if step_cfg.get("wait"):
                    page.wait_for_selector(step_cfg["wait"], state="visible", timeout=30_000)

            elif action == "download_latest":
                # When running per-branch, find the row specific to this branch.
                # Otherwise fall back to the first download link on the page.
                # Saved Reports accumulates one row per queued export, so as
                # soon as a run fetches both reports "the first Download csv
                # link on the page" is ambiguous. Prefer the row whose title
                # matches THIS section's label.
                if branch_name:
                    # Row text: "{report_label} for {branch_name}"
                    candidates = [
                        f"tr:has-text('{report_label} for {branch_name}') "
                        f"a:has-text('Download csv')"
                    ]
                else:
                    candidates = [
                        f"tr:has-text('{report_label}') a:has-text('Download csv')"
                    ]

                # The configured bare selector stays as a fallback for the main
                # report only — it preserves the proven path for BMD. It is
                # deliberately NOT offered to the modifiers section: there the
                # grossing row is already on the page, so falling back would
                # quietly download it a second time and append a duplicate of
                # the grossing items instead of the modifiers.
                if is_main:
                    candidates.append(step_cfg["click"])

                poll_attempts = int(step_cfg.get("poll_attempts", 1))
                poll_interval = int(step_cfg.get("poll_interval_s", 15))

                # Poll: refresh the page until one of the candidates appears
                click_sel = None
                for attempt in range(poll_attempts):
                    click_sel = next(
                        (c for c in candidates if page.query_selector(c)), None
                    )
                    if click_sel:
                        break
                    if attempt < poll_attempts - 1:
                        if _verbose:
                            print(f"       download link not ready, "
                                  f"waiting {poll_interval}s "
                                  f"(attempt {attempt+1}/{poll_attempts})...")
                        time.sleep(poll_interval)
                        page.reload(wait_until="domcontentloaded", timeout=30_000)
                        page.wait_for_load_state("networkidle", timeout=20_000)
                        page.wait_for_timeout(2_000)
                if not click_sel:
                    raise NavError(
                        f"Download link not found after {poll_attempts} attempts. "
                        f"Selectors tried: {candidates}"
                    )

                page.wait_for_selector(click_sel, state="visible", timeout=15_000)
                screenshot(page, "nav", "before_download")

                branch_slug = re.sub(r"[^A-Za-z0-9_-]", "_", branch_name) if branch_name else "all"
                with page.expect_download(timeout=120_000) as dl_info:
                    page.click(click_sel)

                download = dl_info.value
                suffix = pathlib.Path(download.suggested_filename).suffix or ".csv"
                dest = DOWNLOADS_DIR / f"{TENANT}_{branch_slug}_{RUN_ID}_{file_tag}{suffix}"
                download.save_as(str(dest))

            else:
                raise NavError(f"Unknown navigation action '{action}' in step '{label}'.")

            log("nav", label, "ok")

    except NavError:
        raise
    except Exception as exc:
        screenshot(page, "nav", "error")
        raise NavError(f"Navigation/download failed: {exc}") from exc

    if dest is None:
        raise NavError("Navigation chain completed but no download was triggered.")
    if not dest.exists() or dest.stat().st_size == 0:
        raise NavError(f"Downloaded file is empty or missing: {dest}")

    log("nav", f"download:{file_tag}", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"file": str(dest), "size_bytes": dest.stat().st_size})
    # Only the main report advances the checkpoint: a modifiers download is a
    # supplement to that run, not a stage of its own.
    if is_main:
        write_checkpoint(2, {"raw_file": str(dest)})
    return dest


def _nav_click(page: Page, step_cfg: dict) -> None:
    sel = step_cfg["click"]
    page.wait_for_selector(sel, state="visible", timeout=30_000)
    page.click(sel)
    if step_cfg.get("wait"):
        page.wait_for_selector(step_cfg["wait"], state="visible", timeout=30_000)


# ──────────────────────────────────────────────────────────────────────────────
# Branch / Location Helpers
# ──────────────────────────────────────────────────────────────────────────────

def discover_locations(page: Page) -> list:
    """Return [{id, name}, ...] for every location in the dropdown."""
    return page.evaluate("""
        Array.from(document.querySelectorAll(
            'label.sapaadCheckboxSelection.checkboxContainer'))
            .map(el => ({
                name: el.innerText.trim(),
                id:   el.getAttribute('data-locationid')
            }))
    """)


def select_location(page: Page, location_id: str) -> None:
    """Open the location dropdown and select only one branch, then apply.

    The filter persists across page navigations, so we check the current
    state of the 'All Locations' checkbox before clicking it. If it is
    already unchecked (a previous branch was selected), we click it once
    to select all, then once more to deselect all — leaving a clean slate
    before checking just the target branch.
    """
    page.click(".multiLocationDropdown")
    page.wait_for_timeout(500)

    all_checked = page.evaluate(
        "!!document.querySelector('input.allLocationsCheckbox') && "
        "document.querySelector('input.allLocationsCheckbox').checked"
    )

    if all_checked:
        # All selected → one click deselects everything
        page.click("label.sapaadCheckboxSelection.allLocations")
        page.wait_for_timeout(300)
    else:
        # Some subset selected → select all first, then deselect all
        page.click("label.sapaadCheckboxSelection.allLocations")
        page.wait_for_timeout(300)
        page.click("label.sapaadCheckboxSelection.allLocations")
        page.wait_for_timeout(300)

    # Now select only the target branch
    page.click(f"label.sapaadCheckboxSelection[data-locationid='{location_id}']")
    page.wait_for_timeout(300)

    # Apply
    page.click("a.btn-success:has-text('APPLY')")
    page.wait_for_load_state("networkidle", timeout=20_000)
    page.wait_for_timeout(1_500)


# ──────────────────────────────────────────────────────────────────────────────
# Stage 3 — Transformation + Item Code Matching
# ──────────────────────────────────────────────────────────────────────────────

def _detect_header_row(raw_path: pathlib.Path) -> int:
    """
    Scan the first 30 rows for the header sentinel string in column 0.
    Returns 0 if sentinel is empty (CSV has clean header on row 0).
    """
    sentinel = CONFIG.get("header_sentinel", "")
    if not sentinel or sentinel == "FILL_IN":
        return 0

    preview = pd.read_csv(raw_path, header=None, nrows=30, on_bad_lines="skip")
    for i, row in preview.iterrows():
        if str(row.iloc[0]).strip() == sentinel:
            return int(i)
    return 0


def _match_item_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Join the Sapapad CSV against the local item code mapping file to add
    the Supy item ID (POS Item ID *).

    Resolution order (handles duplicate item names in the mapping file):
      1. Exact item name + exact category → unique match
      2. Normalized name + normalized category (strips trailing periods, lowercases)
         → prefer row whose raw category_name exactly matches Sapaad category
      3. Normalized name only → prefer first non-EVENT MENU row
    """
    icc = CONFIG.get("item_code_mapping", {})
    mapping_path_str  = icc.get("file", "")
    raw_key           = icc.get("raw_key", "")
    ref_key           = icc.get("ref_key", "")
    raw_cat_key       = icc.get("raw_category_key", "")
    ref_cat_key       = icc.get("ref_category_key", "")
    target_col        = icc.get("target_col", "")
    output_col        = icc.get("output_col", "POS Item ID *")

    if not all([mapping_path_str, raw_key, ref_key, target_col]) or "FILL_IN" in (
        mapping_path_str, raw_key, ref_key, target_col
    ):
        raise TransformError(
            "sapapad_config.yaml item_code_mapping is not fully configured. "
            "Fill in file, raw_key, ref_key, and target_col."
        )

    mapping_path = BASE_DIR / mapping_path_str
    if not mapping_path.exists():
        raise TransformError(f"Item code mapping file not found: {mapping_path}")

    if mapping_path.suffix.lower() in (".xlsx", ".xls"):
        ref_df = pd.read_excel(mapping_path)
    else:
        ref_df = pd.read_csv(mapping_path)

    ref_df.columns = [str(c).strip() for c in ref_df.columns]

    for col in (ref_key, target_col):
        if col not in ref_df.columns:
            raise TransformError(
                f"Column '{col}' not found in mapping file. "
                f"Available: {list(ref_df.columns)}"
            )
    if raw_key not in df.columns:
        raise TransformError(
            f"Column '{raw_key}' not found in Sapaad CSV. "
            f"Available: {list(df.columns)}"
        )

    def _norm(s: str) -> str:
        return str(s).strip().lower().rstrip(".")

    # Build normalised lookup keys on both sides
    ref_df["_norm_name"] = ref_df[ref_key].apply(_norm)
    ref_df["_norm_cat"]  = ref_df[ref_cat_key].apply(_norm) if ref_cat_key and ref_cat_key in ref_df.columns else ""
    df["_norm_name"]     = df[raw_key].apply(_norm)
    df["_norm_cat"]      = df[raw_cat_key].apply(_norm) if raw_cat_key and raw_cat_key in df.columns else ""

    result_ids = []
    for idx, row in df.iterrows():
        norm_name = row["_norm_name"]
        norm_cat  = row["_norm_cat"]
        raw_cat   = str(row.get(raw_cat_key, "")).strip() if raw_cat_key else ""

        # 1. Exact name + exact category
        candidates = ref_df[
            (ref_df[ref_key].str.strip() == str(row[raw_key]).strip()) &
            (ref_df[ref_cat_key].str.strip() == raw_cat if ref_cat_key and ref_cat_key in ref_df.columns else True)
        ]
        if len(candidates) == 1:
            result_ids.append(candidates.iloc[0][target_col])
            continue

        # 2. Normalised name + normalised category
        candidates = ref_df[
            (ref_df["_norm_name"] == norm_name) &
            (ref_df["_norm_cat"]  == norm_cat)
        ]
        if len(candidates) == 1:
            result_ids.append(candidates.iloc[0][target_col])
            continue
        if len(candidates) > 1:
            # Prefer exact category match within normalised candidates
            exact = candidates[candidates[ref_cat_key].str.strip() == raw_cat]
            if len(exact) >= 1:
                result_ids.append(exact.iloc[0][target_col])
                continue
            result_ids.append(candidates.iloc[0][target_col])
            continue

        # 3. Normalised name only — prefer non-EVENT MENU
        candidates = ref_df[ref_df["_norm_name"] == norm_name]
        if not candidates.empty:
            non_event = candidates[~candidates[ref_cat_key].str.strip().str.upper().str.startswith("EVENT")
                                   ] if ref_cat_key and ref_cat_key in candidates.columns else candidates
            best = non_event if not non_event.empty else candidates
            result_ids.append(best.iloc[0][target_col])
        else:
            result_ids.append(None)

    df[output_col] = result_ids

    # Clean up temp columns
    df.drop(columns=["_norm_name", "_norm_cat"], errors="ignore", inplace=True)

    unmatched = df[df[output_col].isna()]
    if not unmatched.empty:
        vals = unmatched[raw_key].unique().tolist()
        log("transform", "item_code_match", "warning",
            extra={"message": f"{len(unmatched)} rows unmatched", "unmatched_keys": vals[:20]})
        print(f"  [!] {len(unmatched)} rows had no item code match. "
              f"Sample: {vals[:5]}", file=sys.stderr)
    else:
        log("transform", "item_code_match", "ok", extra={"rows": len(df)})

    return df



# ──────────────────────────────────────────────────────────────────────────────
# Paid Modifiers (Marketing → Top Paid Modifiers)
# ──────────────────────────────────────────────────────────────────────────────

def _pick_column(df: pd.DataFrame, candidates, what: str,
                 required: bool = True) -> Optional[str]:
    """
    Return the first candidate header actually present, ignoring case/spacing.

    The modifiers export was specced from the written SOP rather than from a
    captured file, so every field lists several plausible headers. When none
    match, the error names both what was wanted and what the file really
    contains — the one fact needed to correct the config after a --debug run.
    """
    norm = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates or []:
        hit = norm.get(str(cand).strip().lower())
        if hit:
            return hit
    if required:
        raise TransformError(
            f"Modifiers CSV has no {what} column. Tried {list(candidates or [])}; "
            f"the file has {list(df.columns)}. Correct "
            f"modifiers.source_columns.{what} in {CONFIG_PATH.name}."
        )
    return None


def _read_modifiers_csv(raw_path: pathlib.Path) -> tuple:
    """Load the modifiers CSV and resolve its columns → (df, name, qty, incl, excl)."""
    mod_cfg = CONFIG.get("modifiers") or {}
    src     = mod_cfg.get("source_columns") or {}

    df = pd.read_csv(raw_path, header=_detect_header_row(raw_path), on_bad_lines="skip")
    df.columns = [str(c).strip() for c in df.columns]

    name_col = _pick_column(df, src.get("name"), "name")
    qty_col  = _pick_column(df, src.get("qty"),  "qty")
    incl_col = _pick_column(df, src.get("incl"), "incl")
    excl_col = _pick_column(df, src.get("excl"), "excl", required=False)

    df = df[
        df[name_col].notna() &
        (df[name_col].astype(str).str.strip() != "")
    ].copy()
    df.reset_index(drop=True, inplace=True)

    return df, name_col, qty_col, incl_col, excl_col


def _to_number(series: pd.Series) -> pd.Series:
    """Strip currency symbols and thousands separators, then coerce to float."""
    return pd.to_numeric(
        series.astype(str).str.replace(r"[^\d.\-]", "", regex=True).replace("", "0"),
        errors="coerce",
    ).fillna(0.0)


def _build_modifier_rows(raw_path: pathlib.Path, report_date: str) -> pd.DataFrame:
    """
    Turn the Top Paid Modifiers CSV into Supy-template rows.

    Two things differ from the grossing-items transform:

      * There is no VLOOKUP. Per the SOP the modifier NAME doubles as its POS
        code, so POS Item ID and POS Item Name both carry it. Modifiers are not
        in the item master, so joining against it would blank every row.

      * Sapaad reports paid modifiers at gross only. Excl. tax is therefore
        derived as incl / tax_divisor (1.05 for the UAE's 5% VAT). If the export
        ever does carry an excl-tax column, it is used verbatim instead —
        a stated value is never re-derived from its own gross.
    """
    mod_cfg = CONFIG.get("modifiers") or {}
    df, name_col, qty_col, incl_col, excl_col = _read_modifiers_csv(raw_path)

    names = df[name_col].astype(str).str.strip()
    qty   = _to_number(df[qty_col]).round(0).astype(int)
    incl  = _to_number(df[incl_col]).round(2)

    if excl_col:
        excl = _to_number(df[excl_col]).round(2)
        tax_note = f"column {excl_col!r}"
    else:
        divisor = float(mod_cfg.get("tax_divisor", 1.05))
        if divisor <= 0:
            raise TransformError(
                f"modifiers.tax_divisor must be greater than 0, got {divisor}."
            )
        excl = (incl / divisor).round(2)
        tax_note = f"incl / {divisor}"

    out = pd.DataFrame({
        "Sales Date *":            report_date,
        "POS Item ID *":           names,
        "POS Item Name":           names,
        "Sold QTY *":              qty,
        "Total Discount Value":    "",
        "Total sales excl. tax *": excl,
        "Total sales incl. tax *": incl,
        "Order ID":                "",
        "Sales Type Code":         "",
        "Parent Item ID":          "",
    })
    out.reset_index(drop=True, inplace=True)

    log("transform", "modifiers", "ok",
        extra={"rows": len(out), "excl_tax_source": tax_note,
               "incl_total": round(float(incl.sum()), 2)})
    if _verbose:
        print(f"  [→] {len(out)} modifier rows (excl. tax from {tax_note})")

    return out


def _modifier_raw_totals(raw_path: pathlib.Path) -> tuple:
    """(row_count, incl_tax_total) for the modifiers CSV — used by verification."""
    df, _name, _qty, incl_col, _excl = _read_modifiers_csv(raw_path)
    return len(df), float(_to_number(df[incl_col]).sum())


def stage_transform(
    raw_path: pathlib.Path,
    branch_name: Optional[str] = None,
    modifiers_raw: Optional[pathlib.Path] = None,
) -> tuple:
    """
    Returns (out_path, row_count, report_date_str).

    When `modifiers_raw` is given, the paid-modifier rows are appended beneath
    the last grossing-item row of the SAME sheet, which is what the SOP
    describes and what keeps Supy ingesting one file per branch per day.
    """
    t0 = time.monotonic()

    try:
        header_row = _detect_header_row(raw_path)

        if _verbose:
            print(f"  [→] Header row detected at index {header_row}")

        df = pd.read_csv(raw_path, header=header_row, on_bad_lines="skip")
        df.columns = [str(c).strip() for c in df.columns]

        # Drop rows with a non-numeric ID column (aggregate/total rows)
        id_col = CONFIG.get("id_column", "")
        if id_col and id_col not in ("", "FILL_IN") and id_col in df.columns:
            df = df[pd.to_numeric(df[id_col], errors="coerce").notna()].copy()

        # Always drop rows with no Item Name (blank/footer rows)
        if "Item Name" in df.columns:
            df = df[df["Item Name"].notna() & (df["Item Name"].astype(str).str.strip() != "")].copy()

        df.reset_index(drop=True, inplace=True)

        if _verbose:
            print(f"  [→] {len(df)} data rows after stripping blanks")

        # ── Item code matching ────────────────────────────────
        df = _match_item_codes(df)

        # ── Column mapping ────────────────────────────────────
        col_cfgs  = CONFIG.get("columns", [])
        rename_map = {}
        drop_cols  = []

        for col_cfg in col_cfgs:
            if col_cfg.get("drop"):
                raw_col = col_cfg.get("raw")
                if raw_col and raw_col in df.columns:
                    drop_cols.append(raw_col)
            elif col_cfg.get("inject"):
                pass
            elif col_cfg.get("raw") and col_cfg.get("target"):
                rename_map[col_cfg["raw"]] = col_cfg["target"]

        df.drop(columns=drop_cols, errors="ignore", inplace=True)
        df.rename(columns=rename_map, inplace=True)

        # ── Inject columns ────────────────────────────────────
        report_date = ""
        for col_cfg in col_cfgs:
            inject = col_cfg.get("inject")
            if not inject:
                continue
            target = col_cfg["target"]
            fmt = CONFIG.get("output_date_format", "%d-%b-%Y")

            if inject == "empty":
                df[target] = ""
            elif inject in ("date_yesterday", "date_target"):
                # The day this run covers — yesterday for the daily job, or
                # whatever --date asked for. The name "date_yesterday" is kept
                # so existing configs keep working.
                report_date = target_date().strftime(fmt)
                df[target] = report_date
            elif inject == "date_from_filename":
                match = re.search(r"(\d{4})(\d{2})(\d{2})", raw_path.name)
                if match:
                    y, m, d = match.groups()
                    report_date = datetime(int(y), int(m), int(d)).strftime(fmt)
                    df[target] = report_date
                else:
                    df[target] = ""
            elif inject == "business_dates_metadata":
                # Filename encodes the run date (YYYYMMDDTHHMMSS); report covers the previous day
                from datetime import timedelta
                m = re.search(r"(\d{4})(\d{2})(\d{2})T", raw_path.name)
                if m:
                    y, mo, d = m.groups()
                    run_date = datetime(int(y), int(mo), int(d))
                    report_date = (run_date - timedelta(days=1)).strftime(fmt)
                df[target] = report_date
            elif inject == "date_from_metadata":
                preview = pd.read_csv(raw_path, header=None, nrows=10, on_bad_lines="skip")
                raw_date_str = ""
                for _, row in preview.iterrows():
                    for cell in row:
                        cell_str = str(cell).strip()
                        try:
                            parsed = pd.to_datetime(
                                cell_str,
                                format=CONFIG.get("raw_date_format", "%d/%m/%Y"),
                                errors="raise",
                            )
                            raw_date_str = cell_str
                            report_date = parsed.strftime(fmt)
                            break
                        except Exception:
                            continue
                    if raw_date_str:
                        break
                df[target] = report_date

        # ── Type casting ──────────────────────────────────────
        for col_cfg in col_cfgs:
            if col_cfg.get("drop") or col_cfg.get("inject"):
                continue
            target = col_cfg.get("target")
            dtype  = col_cfg.get("dtype")
            if not target or target not in df.columns:
                continue
            if dtype == "int":
                df[target] = pd.to_numeric(df[target], errors="coerce").fillna(0).astype(int)
            elif dtype == "float":
                df[target] = (
                    df[target]
                    .astype(str)
                    .str.replace(r"[^\d.\-]", "", regex=True)
                    .replace("", "0")
                )
                df[target] = pd.to_numeric(df[target], errors="coerce").fillna(0.0).round(2)

        # ── Reorder columns ───────────────────────────────────
        final_order = CONFIG.get("output_column_order", [])
        ordered = [c for c in final_order if c in df.columns]
        extras  = [c for c in df.columns if c not in ordered]
        df = df[ordered + extras]

        # ── Append modifier rows ──────────────────────────────
        # Appended AFTER the reorder so they line up with the grossing columns
        # rather than widening the sheet.
        mod_rows = 0
        if modifiers_raw is not None:
            fmt = CONFIG.get("output_date_format", "%d-%b-%Y")
            mod_df = _build_modifier_rows(
                modifiers_raw, report_date or target_date().strftime(fmt)
            )
            mod_rows = len(mod_df)
            df = pd.concat(
                [df, mod_df.reindex(columns=df.columns, fill_value="")],
                ignore_index=True,
            )

        # ── Export ────────────────────────────────────────────
        # The date in the filename is the SALES date, not the run date. It was
        # datetime.now() until 2026-09-03, which meant a file named
        # 2026-08-26 actually held 25 Aug sales (the daily job runs the
        # morning after) — and a --date backfill would have been stamped with
        # today, overwriting the current day's report with historic data.
        sales_date = target_date().strftime("%Y-%m-%d")
        branch_slug = re.sub(r"[^A-Za-z0-9_-]", "_", branch_name) if branch_name else "all_locations"
        out_path   = OUTPUT_DIR / f"{TENANT}_{branch_slug}_{sales_date}_{RUN_ID[:8]}.xlsx"
        df.to_excel(str(out_path), index=False, engine="openpyxl")

    except (KeyError, ValueError, TypeError) as exc:
        raise TransformError(f"Transform failed: {exc}\n{traceback.format_exc()}") from exc
    except TransformError:
        raise
    except Exception as exc:
        raise TransformError(f"Unexpected transform error: {exc}\n{traceback.format_exc()}") from exc

    log("transform", "export", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"output": str(out_path), "rows": len(df),
               "grossing_rows": len(df) - mod_rows, "modifier_rows": mod_rows})
    write_checkpoint(3, {"output_file": str(out_path)})

    if _verbose:
        print(f"  [→] {len(df)} rows written → {out_path}")

    return out_path, len(df), report_date or today


# ──────────────────────────────────────────────────────────────────────────────
# Stage 3.5 — Verification
# ──────────────────────────────────────────────────────────────────────────────

def stage_verify(
    raw_path: pathlib.Path,
    out_path: pathlib.Path,
    page: Optional[Page] = None,
    branch_name: Optional[str] = None,
    modifiers_raw_path: Optional[pathlib.Path] = None,
) -> VerificationResult:
    """
    3-layer verification. Never raises — always returns a VerificationResult.

    Layer 1 — Raw → output integrity
        Row count and revenue sum must agree between the downloaded CSV
        and the transformed output (within a 5% / 1% tolerance respectively).
        When modifier rows were appended, both raw files are counted — the
        output legitimately holds more rows than the grossing CSV alone.

    Layer 2 — Business rules
        Sales Date = yesterday, no negative quantities, excl. tax <= incl. tax,
        item ID match rate >= 80%, no empty item names, grand total > 0.

    Layer 3 — Portal spot-check (only when page is provided)
        Navigate back to the report page (location filter persists), read the
        table row count and grand total shown in the browser, and compare
        against the output file.
    """
    t0 = time.monotonic()
    result = VerificationResult()

    # Tallied once, used by two layers: Layer 1 needs it for the row and
    # revenue totals, Layer 2 to exclude modifier rows from the item-ID match
    # rate (they carry their own name as their ID, so counting them would
    # dilute a completely broken item mapping into looking acceptable).
    mod_count = 0
    mod_total = 0.0
    if modifiers_raw_path is not None:
        try:
            mod_count, mod_total = _modifier_raw_totals(modifiers_raw_path)
        except Exception as exc:
            result.warn(f"Could not read modifiers CSV for verification: {exc}")

    # ── Layer 1: Raw → Output integrity ──────────────────────────────────────
    try:
        raw_df = pd.read_csv(raw_path, on_bad_lines="skip")
        raw_df.columns = [str(c).strip() for c in raw_df.columns]
        if "Item Name" in raw_df.columns:
            raw_df = raw_df[
                raw_df["Item Name"].notna() &
                (raw_df["Item Name"].astype(str).str.strip() != "")
            ].copy()

        out_df = pd.read_excel(out_path, engine="openpyxl")
        # The output is grossing + modifier rows. Comparing it against the
        # grossing CSV alone would blow the 5% row gate on every branch and
        # under-report the revenue sum by exactly the paid-modifier value.
        raw_count = len(raw_df) + mod_count
        out_count = len(out_df)

        if raw_count != out_count:
            diff = abs(raw_count - out_count)
            threshold = max(1, int(raw_count * 0.05))
            if diff > threshold:
                result.fail(
                    f"Row count mismatch: CSV had {raw_count} data rows, "
                    f"output has {out_count} (difference {diff} exceeds 5% tolerance)."
                )
            else:
                result.warn(
                    f"Row count differs by {diff}: raw={raw_count}, output={out_count}. "
                    f"Likely blank/aggregate rows stripped."
                )

        if "Total Amount" in raw_df.columns and "Total sales incl. tax *" in out_df.columns:
            raw_total = (
                pd.to_numeric(raw_df["Total Amount"], errors="coerce").sum()
                + mod_total
            )
            out_total = pd.to_numeric(out_df["Total sales incl. tax *"], errors="coerce").sum()
            if raw_total > 0:
                diff_pct = abs(raw_total - out_total) / raw_total * 100
                if diff_pct > 1.0:
                    result.fail(
                        f"Revenue mismatch: raw sum={raw_total:.2f}, "
                        f"output sum={out_total:.2f} ({diff_pct:.1f}% difference)."
                    )

        required_cols = [
            "Sales Date *", "POS Item ID *", "POS Item Name",
            "Sold QTY *", "Total sales excl. tax *", "Total sales incl. tax *",
            "Order ID", "Sales Type Code", "Parent Item ID",
        ]
        missing = [c for c in required_cols if c not in out_df.columns]
        if missing:
            result.fail(f"Output is missing required columns: {missing}")

    except Exception as exc:
        result.warn(f"Layer 1 integrity check could not run: {exc}")

    # ── Layer 2: Business rules ───────────────────────────────────────────────
    try:
        out_df = pd.read_excel(out_path, engine="openpyxl")

        if "Sales Date *" in out_df.columns and len(out_df) > 0:
            expected = target_date().strftime(
                CONFIG.get("output_date_format", "%d-%b-%Y")
            )
            bad_dates = out_df[out_df["Sales Date *"].astype(str) != expected]
            if not bad_dates.empty:
                result.warn(
                    f"{len(bad_dates)} rows have unexpected Sales Date "
                    f"(expected {expected!r}): "
                    f"{bad_dates['Sales Date *'].unique()[:3].tolist()}"
                )

        if "Sold QTY *" in out_df.columns:
            neg_qty = out_df[pd.to_numeric(out_df["Sold QTY *"], errors="coerce") < 0]
            if not neg_qty.empty:
                result.fail(f"{len(neg_qty)} rows have a negative Sold QTY.")

        if "Total sales excl. tax *" in out_df.columns and "Total sales incl. tax *" in out_df.columns:
            excl = pd.to_numeric(out_df["Total sales excl. tax *"], errors="coerce")
            incl = pd.to_numeric(out_df["Total sales incl. tax *"], errors="coerce")
            violations = ((excl - incl) > 0.01).sum()
            if violations > 0:
                result.fail(
                    f"{violations} rows have excl. tax > incl. tax — "
                    f"tax amount cannot be negative."
                )

        # Modifier rows sit at the tail of the sheet and always carry an ID.
        # The match rate only means anything for the grossing rows above them.
        grossing_df = out_df.iloc[:len(out_df) - mod_count] if mod_count else out_df
        mapping_file = ((CONFIG.get("item_code_mapping") or {})
                        .get("file", "the item code mapping"))

        if "POS Item ID *" in grossing_df.columns and len(grossing_df) > 0:
            matched = grossing_df["POS Item ID *"].notna().sum()
            total   = len(grossing_df)
            match_rate = matched / total * 100
            if match_rate < 80:
                result.fail(
                    f"Item ID match rate is {match_rate:.0f}% ({matched}/{total} "
                    f"grossing rows). Update {mapping_file}."
                )
            elif match_rate < 95:
                result.warn(
                    f"Item ID match rate is {match_rate:.0f}% ({matched}/{total} "
                    f"grossing rows). Some items may be missing from {mapping_file}."
                )

        if "Total sales incl. tax *" in out_df.columns and len(out_df) > 0:
            grand_total = pd.to_numeric(
                out_df["Total sales incl. tax *"], errors="coerce"
            ).sum()
            if grand_total <= 0:
                result.warn(
                    f"Grand total revenue is {grand_total:.2f} — "
                    f"unexpected for a branch with {len(out_df)} rows."
                )

        if "POS Item Name" in out_df.columns:
            empty_names = out_df[
                out_df["POS Item Name"].isna() |
                (out_df["POS Item Name"].astype(str).str.strip() == "")
            ]
            if not empty_names.empty:
                result.warn(f"{len(empty_names)} rows have an empty POS Item Name.")

    except Exception as exc:
        result.warn(f"Layer 2 business rule checks could not run: {exc}")

    # ── Layer 3: Portal spot-check ────────────────────────────────────────────
    # Skipped once modifiers are in the sheet: the portal's Top Grossing table
    # knows nothing about them, so its row count and total would never match.
    if page is not None and modifiers_raw_path is not None:
        result.warn(
            "Layer 3 portal spot-check skipped — the output includes modifier "
            "rows, which the Top Grossing table does not show."
        )
    elif page is not None:
        try:
            report_url = CONFIG["portal"].get("report_url", "")
            if report_url:
                page.goto(report_url, wait_until="domcontentloaded", timeout=30_000)
                page.wait_for_load_state("networkidle", timeout=20_000)
                page.wait_for_timeout(2_000)

                # Count visible data rows (exclude header and total rows)
                portal_row_count = page.evaluate("""
                    (() => {
                        var rows = Array.from(document.querySelectorAll('table tbody tr'));
                        return rows.filter(function(tr) {
                            var cells = tr.querySelectorAll('td');
                            if (cells.length < 2) return false;
                            var first = (cells[0].textContent || '').trim().toLowerCase();
                            return first !== '' && first !== 'total' && first !== 'grand total';
                        }).length;
                    })()
                """)

                # Try to read a grand total from a tfoot or "Total" row
                portal_total = page.evaluate("""
                    (() => {
                        var candidates = Array.from(
                            document.querySelectorAll('table tfoot tr, table tbody tr')
                        );
                        for (var i = candidates.length - 1; i >= 0; i--) {
                            var text = candidates[i].textContent.trim().toLowerCase();
                            if (text.includes('total')) {
                                var cells = candidates[i].querySelectorAll('td');
                                for (var j = cells.length - 1; j >= 0; j--) {
                                    var v = parseFloat(
                                        (cells[j].textContent || '').replace(/[^0-9.\\-]/g, '')
                                    );
                                    if (!isNaN(v) && v > 0) return v;
                                }
                            }
                        }
                        return null;
                    })()
                """)

                out_df = pd.read_excel(out_path, engine="openpyxl")
                out_count = len(out_df)

                if portal_row_count and portal_row_count > 0:
                    diff = abs(portal_row_count - out_count)
                    if diff > max(1, out_count * 0.05):
                        result.fail(
                            f"Portal shows {portal_row_count} rows but output has "
                            f"{out_count} rows."
                        )
                    else:
                        pass  # counts agree

                if portal_total is not None and portal_total > 0:
                    out_total = pd.to_numeric(
                        out_df.get("Total sales incl. tax *", pd.Series(dtype=float)),
                        errors="coerce",
                    ).sum()
                    if out_total > 0:
                        diff_pct = abs(portal_total - out_total) / out_total * 100
                        if diff_pct > 2.0:
                            result.warn(
                                f"Portal grand total ({portal_total:.2f}) differs from "
                                f"output grand total ({out_total:.2f}) by {diff_pct:.1f}%."
                            )

        except Exception as exc:
            result.warn(f"Layer 3 portal spot-check could not complete: {exc}")

    duration = int((time.monotonic() - t0) * 1000)
    log(
        "verify", "stage_verify",
        "ok" if result.passed else "error",
        duration_ms=duration,
        extra={
            "status": result.status(),
            "errors": len(result.errors),
            "warnings": len(result.warnings),
            "branch": branch_name,
        },
    )

    status = result.status()
    icon = "[✓]" if status == "PASS" else "[!]" if status == "WARN" else "[x]"
    print(
        f"[Stage 3.5] {icon} Verification {status} — "
        f"{len(result.errors)} error(s), {len(result.warnings)} warning(s)"
    )
    for line in result.summary_lines():
        print(f"    {line}")
    print()

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Stage 4 — Email
# ──────────────────────────────────────────────────────────────────────────────

def stage_email(
    out_path: pathlib.Path,
    row_count: int,
    report_date: str,
    branch_name: Optional[str] = None,
    verification: Optional[VerificationResult] = None,
) -> None:
    t0 = time.monotonic()

    gmail_user     = os.environ.get("GMAIL_USER", "")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD", "")
    recipients     = [
        r.strip() for r in
        os.environ.get("SAPAPAD_REPORT_RECIPIENT",
                       os.environ.get("REPORT_RECIPIENT", gmail_user)).split(",")
        if r.strip()
    ]

    if not gmail_user or not gmail_password:
        raise EmailError(
            "GMAIL_USER and GMAIL_APP_PASSWORD must be set in your .env / GitHub Secrets."
        )

    branch_label = f" — {branch_name}" if branch_name else ""
    vr_status = verification.status() if verification else "PASS"
    warn_tag = " [WARNINGS]" if vr_status in ("WARN", "FAIL") else ""
    # Strip CR/LF to prevent email header injection
    subject = (
        f"Sapapad POS Sales Report{branch_label} — {report_date}{warn_tag}"
    ).replace("\r", " ").replace("\n", " ")

    # ── Build verification block ──────────────────────────────────────────────
    if verification:
        if vr_status == "PASS":
            vr_color = "#16a34a"
            vr_bg    = "#f0fdf4"
            vr_badge = "PASS"
            vr_html  = "<p style='margin:0;color:#16a34a;'>All verification checks passed.</p>"
        elif vr_status == "WARN":
            vr_color = "#d97706"
            vr_bg    = "#fffbeb"
            vr_badge = "WARNINGS"
            items    = "".join(
                f"<li style='margin-bottom:4px;color:#92400e;'>{html_escape(w)}</li>"
                for w in (verification.warnings + verification.errors)
            )
            vr_html  = f"<ul style='margin:8px 0 0 0;padding-left:20px;'>{items}</ul>"
        else:
            vr_color = "#dc2626"
            vr_bg    = "#fef2f2"
            vr_badge = "FAILED"
            items    = "".join(
                f"<li style='margin-bottom:4px;color:#991b1b;'>{html_escape(e)}</li>"
                for e in (verification.errors + verification.warnings)
            )
            vr_html  = f"<ul style='margin:8px 0 0 0;padding-left:20px;'>{items}</ul>"

        verify_html_block = f"""
        <tr><td style="padding:24px 32px 0;">
          <div style="background:{vr_bg};border-left:4px solid {vr_color};
                      border-radius:6px;padding:14px 16px;">
            <p style="margin:0 0 6px;font-size:12px;font-weight:700;
                      letter-spacing:.05em;color:{vr_color};text-transform:uppercase;">
              Verification &nbsp;
              <span style="background:{vr_color};color:#fff;padding:2px 8px;
                           border-radius:12px;font-size:11px;">{vr_badge}</span>
            </p>
            {vr_html}
          </div>
        </td></tr>"""

        verify_plain = (
            "\n\nVerification: " + vr_badge + "\n"
            + "\n".join(verification.summary_lines())
        )
    else:
        verify_html_block = ""
        verify_plain = ""

    # ── Plain-text fallback ───────────────────────────────────────────────────
    plain_body = (
        f"Hi,\n\n"
        f"Please find attached the Sapapad POS Sales Report for "
        f"{branch_name or 'All Locations'} — {report_date}.\n\n"
        f"  Branch : {branch_name or 'All Locations'}\n"
        f"  Rows   : {row_count:,}\n"
        f"  File   : {out_path.name}\n"
        f"  Run ID : {RUN_ID}"
        f"{verify_plain}\n\n"
        f"Regards,\nOperations Team"
    )

    # ── HTML template ─────────────────────────────────────────────────────────
    html_body = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f5f3ff;font-family:'Segoe UI',Helvetica,Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f3ff;padding:32px 0;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0"
             style="background:#ffffff;border-radius:12px;
                    box-shadow:0 4px 24px rgba(91,30,153,.10);overflow:hidden;max-width:600px;">

        <!-- Header -->
        <tr>
          <td style="background:linear-gradient(135deg,#5b1e99 0%,#7c3aed 100%);
                     padding:28px 32px;text-align:left;">
            <p style="margin:0;font-size:22px;font-weight:700;color:#ffffff;
                      letter-spacing:-.3px;">supy</p>
            <p style="margin:4px 0 0;font-size:13px;color:#ddd6fe;font-weight:400;">
              POS Sales Report
            </p>
          </td>
        </tr>

        <!-- Title row -->
        <tr>
          <td style="padding:28px 32px 0;">
            <p style="margin:0;font-size:18px;font-weight:600;color:#1e1b4b;">
              {html_escape(branch_name or 'All Locations')}
            </p>
            <p style="margin:4px 0 0;font-size:14px;color:#6d28d9;">
              {html_escape(report_date)}
            </p>
          </td>
        </tr>

        <!-- Metrics -->
        <tr>
          <td style="padding:20px 32px 0;">
            <table cellpadding="0" cellspacing="0" width="100%"
                   style="border-collapse:separate;border-spacing:0;">
              <tr>
                <td width="50%" style="padding-right:8px;">
                  <div style="background:#f5f3ff;border:1px solid #ede9fe;
                              border-radius:8px;padding:14px 16px;">
                    <p style="margin:0;font-size:11px;font-weight:600;color:#7c3aed;
                               text-transform:uppercase;letter-spacing:.05em;">Items Sold</p>
                    <p style="margin:6px 0 0;font-size:26px;font-weight:700;
                               color:#1e1b4b;">{row_count:,}</p>
                    <p style="margin:2px 0 0;font-size:11px;color:#6b7280;">line items</p>
                  </div>
                </td>
                <td width="50%" style="padding-left:8px;">
                  <div style="background:#f5f3ff;border:1px solid #ede9fe;
                              border-radius:8px;padding:14px 16px;">
                    <p style="margin:0;font-size:11px;font-weight:600;color:#7c3aed;
                               text-transform:uppercase;letter-spacing:.05em;">Report Date</p>
                    <p style="margin:6px 0 0;font-size:18px;font-weight:700;
                               color:#1e1b4b;">{html_escape(report_date)}</p>
                    <p style="margin:2px 0 0;font-size:11px;color:#6b7280;">yesterday</p>
                  </div>
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- File details -->
        <tr>
          <td style="padding:20px 32px 0;">
            <table cellpadding="0" cellspacing="0" width="100%"
                   style="border-collapse:collapse;font-size:13px;">
              <tr style="border-bottom:1px solid #ede9fe;">
                <td style="padding:10px 0;color:#6b7280;width:110px;">Branch</td>
                <td style="padding:10px 0;color:#1e1b4b;font-weight:500;">
                  {html_escape(branch_name or 'All Locations')}
                </td>
              </tr>
              <tr style="border-bottom:1px solid #ede9fe;">
                <td style="padding:10px 0;color:#6b7280;">File</td>
                <td style="padding:10px 0;color:#1e1b4b;font-weight:500;word-break:break-all;">
                  {html_escape(out_path.name)}
                </td>
              </tr>
              <tr>
                <td style="padding:10px 0;color:#6b7280;">Run ID</td>
                <td style="padding:10px 0;color:#9ca3af;font-family:monospace;font-size:12px;">
                  {html_escape(RUN_ID)}
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- Verification block (injected) -->
        {verify_html_block}

        <!-- Footer -->
        <tr>
          <td style="padding:28px 32px;border-top:1px solid #ede9fe;margin-top:24px;">
            <p style="margin:0;font-size:13px;color:#374151;">Regards,</p>
            <p style="margin:4px 0 0;font-size:13px;font-weight:600;color:#5b1e99;">
              Operations Team
            </p>
          </td>
        </tr>

        <!-- Bottom bar -->
        <tr>
          <td style="background:linear-gradient(135deg,#5b1e99 0%,#7c3aed 100%);
                     padding:14px 32px;">
            <p style="margin:0;font-size:11px;color:#ddd6fe;text-align:center;">
              supy.io &nbsp;|&nbsp; Automated POS Reporting
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""

    # ── Assemble message ──────────────────────────────────────────────────────
    msg = MIMEMultipart("mixed")
    msg["From"]    = gmail_user
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(plain_body, "plain"))
    alt.attach(MIMEText(html_body, "html"))
    msg.attach(alt)

    with open(out_path, "rb") as f:
        attachment = MIMEApplication(
            f.read(),
            _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        attachment.add_header("Content-Disposition", "attachment", filename=out_path.name)
        msg.attach(attachment)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, recipients, msg.as_string())
    except Exception as exc:
        raise EmailError(f"Failed to send email: {exc}") from exc

    log("email", "send", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"to": recipients, "subject": subject, "attachment": out_path.name})

    print(f"[Stage 4] ✓ Email sent → {', '.join(recipients)}\n")


# ──────────────────────────────────────────────────────────────────────────────
# Resend Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def _main_resend(args) -> int:
    run_id = args.resend_run_id
    print(f"\n[Sapapad Resend] run_id={run_id}\n")

    # Tenant-scoped: the old "sapapad_*" glob matched nothing for Falafel,
    # Heal or Pinza, so --resend-run-id always reported "no raw files found".
    # The modifiers files end in _modifiers_raw.csv, so they are not picked up
    # here as reports in their own right — they are paired in below.
    raw_files = sorted(DOWNLOADS_DIR.glob(f"{TENANT}_*_{run_id}_raw.csv"))
    if not raw_files:
        print(f"[✗] No raw files found for run_id={run_id}", file=sys.stderr)
        return 1

    print(f"[Resend] Found {len(raw_files)} raw files to process\n")
    failed = []

    mod_cfg = modifiers_config(args)

    for raw_file in raw_files:
        stem = raw_file.stem  # e.g. sapapad_BMD_Business_Bay_20260613T040308_cd6f98df_raw
        # Strip leading "{tenant}_" and trailing "_{run_id}_raw"
        inner = stem.removeprefix(f"{TENANT}_")
        suffix = f"_{run_id}_raw"
        branch_slug = inner.removesuffix(suffix) if inner.endswith(suffix) else inner
        branch_name = branch_slug.replace("_", " ")

        print(f"{'─'*60}")
        print(f"[Branch] {branch_name}")

        # Pair each grossing download with the modifiers download from the
        # same run and branch, if one was captured.
        mod_file = None
        if mod_cfg:
            sibling = raw_file.with_name(
                raw_file.name.replace("_raw.csv", "_modifiers_raw.csv")
            )
            if sibling.exists():
                mod_file = sibling
            else:
                print(f"  [!] No modifiers file for this branch — "
                      f"grossing items only.")

        try:
            print(f"[Stage 3] Transforming...")
            out_file, row_count, report_date = stage_transform(
                raw_file, branch_name=branch_name, modifiers_raw=mod_file
            )
            print(f"[Stage 3] ✓ {row_count} rows → {out_file.name}\n")

            if row_count == 0:
                print(f"  [!] 0 rows — skipping email.\n")
                continue

            print(f"[Stage 3.5] Verifying...")
            vr = stage_verify(raw_file, out_file, page=None,
                              branch_name=branch_name,
                              modifiers_raw_path=mod_file)

            if not args.no_email:
                print(f"[Stage 4] Sending email...")
                stage_email(out_file, row_count, report_date, branch_name=branch_name, verification=vr)
            else:
                print(f"[Stage 4] Skipped (--no-email). File → {out_file}\n")

        except (TransformError, EmailError) as exc:
            print(f"[✗] {branch_name} failed: {exc}\n", file=sys.stderr)
            failed.append(branch_name)

    print(f"\n{'═'*60}")
    print(f"[✓] Resend complete.  run_id={run_id}")
    print(f"    Processed: {len(raw_files) - len(failed)}  Failed: {len(failed)}")
    if failed:
        print(f"    Failed branches: {failed}")
    print(f"{'═'*60}\n")

    return 1 if failed else 0


# ──────────────────────────────────────────────────────────────────────────────
# Per-Branch Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def _main_per_branch(args) -> int:
    headless = not args.debug
    mod_cfg  = modifiers_config(args)
    print(f"\n[Sapapad Per-Branch Pipeline] run_id={RUN_ID}\n")
    if mod_cfg:
        print("[Config] Paid modifiers enabled — output will carry grossing "
              "items with modifier rows appended.\n")

    failed_branches = []

    try:
        with sync_playwright() as p:
            browser_ctx_kwargs = {}
            if STORAGE_STATE_PATH.exists() and not args.force_login:
                browser_ctx_kwargs["storage_state"] = str(STORAGE_STATE_PATH)

            browser = p.chromium.launch(headless=headless, slow_mo=200 if args.debug else 0)
            context = browser.new_context(accept_downloads=True, **browser_ctx_kwargs)
            page    = context.new_page()

            # Stage 1 — Auth (once for all branches)
            print("[Stage 1] Authentication...")
            try:
                stage_auth(page, context, force_login=args.force_login)
                print("[Stage 1] ✓ Authenticated\n")
            except AuthError as exc:
                log("auth", "login", "error", extra={"error": str(exc)})
                print(f"[✗] Auth error: {exc}", file=sys.stderr)
                browser.close()
                return 1

            # Discover all branches
            report_url = CONFIG["portal"].get("report_url", "")
            page.goto(report_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_load_state("networkidle", timeout=20_000)
            page.wait_for_timeout(2_000)
            locations = discover_locations(page)
            if args.limit:
                locations = locations[:args.limit]
                print(f"[Branches] Found {len(locations)} branches (limited to {args.limit}): "
                      f"{[l['name'] for l in locations]}\n")
            else:
                print(f"[Branches] Found {len(locations)} branches: "
                      f"{[l['name'] for l in locations]}\n")

            # Loop over every branch
            for i, loc in enumerate(locations, 1):
                loc_id   = loc["id"]
                loc_name = loc["name"]
                print(f"{'─'*60}")
                print(f"[{i}/{len(locations)}] Branch: {loc_name}")
                print(f"{'─'*60}")

                try:
                    # Navigate back to report page before each branch
                    page.goto(report_url, wait_until="domcontentloaded", timeout=30_000)
                    page.wait_for_load_state("networkidle", timeout=20_000)
                    page.wait_for_timeout(1_500)

                    # Stage 2 — Download for this branch
                    print(f"[Stage 2] Downloading {loc_name}...")
                    raw_file = stage_navigate_and_download(
                        page, location_id=loc_id, branch_name=loc_name
                    )
                    print(f"[Stage 2] ✓ Downloaded → {raw_file.name}\n")

                    # Stage 2b — Paid modifiers (Marketing → Top Paid Modifiers).
                    # A failure here must not cost us the grossing report, which
                    # is the primary deliverable — warn and carry on without it.
                    mod_raw = None
                    if mod_cfg:
                        print(f"[Stage 2b] Downloading modifiers for {loc_name}...")
                        try:
                            page.goto(mod_cfg.get("report_url", ""),
                                      wait_until="domcontentloaded", timeout=30_000)
                            page.wait_for_load_state("networkidle", timeout=20_000)
                            page.wait_for_timeout(1_500)
                            mod_raw = stage_navigate_and_download(
                                page, location_id=loc_id, branch_name=loc_name,
                                section=mod_cfg,
                            )
                            print(f"[Stage 2b] ✓ Downloaded → {mod_raw.name}\n")
                        except Exception as exc:
                            log("nav", "modifiers", "warning",
                                extra={"branch": loc_name, "error": str(exc)})
                            print(f"[Stage 2b] [!] Modifiers unavailable for "
                                  f"{loc_name}: {exc}\n"
                                  f"           Continuing with grossing items only.\n",
                                  file=sys.stderr)
                            mod_raw = None

                    # Stage 3 — Transform
                    print(f"[Stage 3] Transforming...")
                    out_file, row_count, report_date = stage_transform(
                        raw_file, branch_name=loc_name, modifiers_raw=mod_raw
                    )
                    print(f"[Stage 3] ✓ {row_count} rows → {out_file.name}\n")

                    if row_count == 0:
                        print(f"  [!] 0 rows for {loc_name} — skipping email.\n")
                        continue

                    # Stage 3.5 — Verify (Layer 3 portal check skipped in per-branch mode:
                    # after download the browser is on Saved Reports and re-navigating
                    # back shows all-locations data, not just this branch)
                    print(f"[Stage 3.5] Verifying {loc_name}...")
                    vr = stage_verify(raw_file, out_file, page=None,
                                      branch_name=loc_name,
                                      modifiers_raw_path=mod_raw)

                    # Stage 4 — Email
                    if not args.no_email:
                        print(f"[Stage 4] Sending email for {loc_name}...")
                        stage_email(
                            out_file, row_count, report_date,
                            branch_name=loc_name, verification=vr,
                        )
                    else:
                        print(f"[Stage 4] Skipped (--no-email). File → {out_file}\n")

                except (NavError, TransformError, EmailError) as exc:
                    print(f"[✗] {loc_name} failed: {exc}\n", file=sys.stderr)
                    log("branch", loc_name, "error", extra={"error": str(exc)})
                    failed_branches.append(loc_name)
                    # Continue to next branch rather than aborting all
                    continue

            browser.close()

    except Exception as exc:
        print(f"[✗] Unexpected error: {exc}", file=sys.stderr)
        return 2

    print(f"\n{'═'*60}")
    print(f"[✓] Per-branch pipeline complete.  run_id={RUN_ID}")
    print(f"    Branches processed: {len(locations)}")
    if failed_branches:
        print(f"    Failed: {failed_branches}")
    print(f"{'═'*60}\n")

    return 1 if failed_branches else 0


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Sapapad POS Automation Pipeline")
    parser.add_argument("--config", metavar="PATH", default=None,
                        help="Tenant config (default: sapapad_config.yaml). "
                             "Read at import time, so it is also parsed from "
                             "sys.argv before argparse runs.")
    parser.add_argument("--debug", action="store_true",
                        help="Run with headed browser and verbose logging")
    parser.add_argument("--from-stage", type=int, default=1, metavar="N",
                        help="Resume from stage N (1=auth, 2=nav, 3=transform, 4=email)")
    parser.add_argument("--date", metavar="YYYY-MM-DD",
                        help="Sales date to report (default: yesterday). Uses "
                             "the dashboard's Custom date filter, honouring "
                             "Sapaad's 04:00 business-day boundary.")
    parser.add_argument("--force-login", action="store_true",
                        help="Ignore cached session; always re-authenticate")
    parser.add_argument("--no-email", action="store_true",
                        help="Skip email — save output file locally only")
    parser.add_argument("--per-branch", action="store_true",
                        help="Run pipeline for each branch individually")
    parser.add_argument("--limit", type=int, default=0, metavar="N",
                        help="Process only the first N branches (for test runs)")
    parser.add_argument("--no-modifiers", action="store_true",
                        help="Skip the Top Paid Modifiers report even if the "
                             "config enables it — grossing items only")
    parser.add_argument("--resend-run-id", metavar="RUN_ID",
                        help="Re-run transform+email for all raw files from a prior run (no browser needed)")
    args = parser.parse_args()

    # Pin the reporting date for the whole run before any stage looks at it.
    if getattr(args, "date", None):
        global TARGET_DATE
        try:
            TARGET_DATE = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"[✗] --date must be YYYY-MM-DD, got {args.date!r}",
                  file=sys.stderr)
            return 1
        if TARGET_DATE.date() >= datetime.now().date():
            print(f"[✗] --date {args.date} is today or later; the day is not "
                  f"closed off yet. Use yesterday or earlier.", file=sys.stderr)
            return 1

    _init_logger(verbose=args.debug)
    from_stage = args.from_stage

    if args.resend_run_id:
        return _main_resend(args)

    if args.per_branch:
        return _main_per_branch(args)

    mod_cfg = modifiers_config(args)
    print(f"\n[Sapapad Pipeline] run_id={RUN_ID}  from_stage={from_stage}\n")
    if mod_cfg:
        print("[Config] Paid modifiers enabled — output will carry grossing "
              "items with modifier rows appended.\n")

    raw_file: Optional[pathlib.Path] = None
    mod_file: Optional[pathlib.Path] = None

    if from_stage >= 3:
        checkpoint = read_checkpoint()
        raw_file_str = checkpoint.get("raw_file")
        if raw_file_str:
            raw_file = pathlib.Path(raw_file_str)
        else:
            # The glob was hardcoded to "sapapad_*" — it found nothing for any
            # tenant but BMD, silently falling through to the "run from stage 1"
            # error even when a perfectly good download was sitting there.
            candidates = sorted(
                DOWNLOADS_DIR.glob(f"{TENANT}_*_raw.*"), key=lambda p: p.stat().st_mtime
            )
            if candidates:
                raw_file = candidates[-1]

        mod_file_str = checkpoint.get("modifiers_raw")
        if mod_cfg and mod_file_str and pathlib.Path(mod_file_str).exists():
            mod_file = pathlib.Path(mod_file_str)
        if not raw_file or not raw_file.exists():
            print("[✗] --from-stage 3 requires an existing raw download. "
                  "Run from stage 1 or 2 first.", file=sys.stderr)
            return 3

    if from_stage <= 2:
        headless = not args.debug
        try:
            with sync_playwright() as p:
                browser_ctx_kwargs = {}
                if STORAGE_STATE_PATH.exists() and not args.force_login:
                    browser_ctx_kwargs["storage_state"] = str(STORAGE_STATE_PATH)

                browser = p.chromium.launch(headless=headless, slow_mo=200 if args.debug else 0)
                context = browser.new_context(
                    accept_downloads=True,
                    **browser_ctx_kwargs,
                )
                page = context.new_page()

                # ── Stage 1: Auth ──────────────────────────────────
                if from_stage <= 1:
                    print("[Stage 1] Authentication...")
                    try:
                        stage_auth(page, context, force_login=args.force_login)
                        print("[Stage 1] ✓ Authenticated\n")
                    except AuthError as exc:
                        log("auth", "login", "error", extra={"error": str(exc)})
                        print(f"[✗] Auth error: {exc}", file=sys.stderr)
                        browser.close()
                        return 1

                # ── Stage 2: Navigate & Download ───────────────────
                print("[Stage 2] Navigating to report and downloading...")
                try:
                    raw_file = stage_navigate_and_download(page)
                    print(f"[Stage 2] ✓ Downloaded → {raw_file}\n")
                except NavError as exc:
                    log("nav", "navigate_and_download", "error", extra={"error": str(exc)})
                    print(f"[✗] Nav error: {exc}", file=sys.stderr)
                    browser.close()
                    return 2

                # ── Stage 2b: Paid modifiers ───────────────────────
                if mod_cfg:
                    print("[Stage 2b] Navigating to modifiers report...")
                    try:
                        page.goto(mod_cfg.get("report_url", ""),
                                  wait_until="domcontentloaded", timeout=30_000)
                        page.wait_for_load_state("networkidle", timeout=20_000)
                        page.wait_for_timeout(1_500)
                        mod_file = stage_navigate_and_download(page, section=mod_cfg)
                        print(f"[Stage 2b] ✓ Downloaded → {mod_file}\n")
                        write_checkpoint(2, {"raw_file": str(raw_file),
                                             "modifiers_raw": str(mod_file)})
                    except Exception as exc:
                        # Same call as per-branch mode: the grossing report is
                        # the deliverable, so a missing modifiers export degrades
                        # the run rather than failing it.
                        log("nav", "modifiers", "warning", extra={"error": str(exc)})
                        print(f"[Stage 2b] [!] Modifiers unavailable: {exc}\n"
                              f"           Continuing with grossing items only.\n",
                              file=sys.stderr)
                        mod_file = None

                browser.close()

        except Exception as exc:
            print(f"[✗] Unexpected browser error: {exc}", file=sys.stderr)
            log("browser", "unexpected", "error", extra={"error": str(exc)})
            return 2

    # ── Stage 3: Transform ─────────────────────────────────────────
    print("[Stage 3] Transforming raw data + matching item codes...")
    try:
        out_file, row_count, report_date = stage_transform(
            raw_file, modifiers_raw=mod_file
        )
        print(f"[Stage 3] ✓ Output → {out_file}\n")
    except TransformError as exc:
        log("transform", "transform", "error", extra={"error": str(exc)})
        print(f"[✗] Transform error: {exc}", file=sys.stderr)
        return 3

    # ── Stage 3.5: Verify ──────────────────────────────────────────
    print("[Stage 3.5] Verifying output...")
    vr = stage_verify(raw_file, out_file, modifiers_raw_path=mod_file)

    # ── Stage 4: Email ─────────────────────────────────────────────
    if not args.no_email:
        print("[Stage 4] Sending email...")
        try:
            stage_email(out_file, row_count, report_date, verification=vr)
        except EmailError as exc:
            log("email", "send", "error", extra={"error": str(exc)})
            print(f"[✗] Email error: {exc}", file=sys.stderr)
            print(f"  ↳ Report was saved to: {out_file}", file=sys.stderr)
            return 4
    else:
        print(f"[Stage 4] Skipped (--no-email).  File saved → {out_file}\n")

    print(f"[✓] Pipeline complete.  run_id={RUN_ID}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
