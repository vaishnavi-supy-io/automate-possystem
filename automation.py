"""
automation.py
-------------
Oracle BI Automation Pipeline — 4-Stage Orchestrator

Stage 1: Authentication   (Playwright — login + session caching)
Stage 2: Navigation       (Playwright — report URL + Revenue Centers filter + Excel download)
Stage 3: Transformation   (Pandas — raw → formatted .xlsx)
Stage 4: Email            (smtplib — attach .xlsx and send via Gmail)

Usage:
    python automation.py                          # headless, single download (legacy mode)
    python automation.py --all-locations          # loop all active locations from config.yaml
    python automation.py --location "Parkers-Mirdiff"   # single named location
    python automation.py --list-locations         # print all active locations and exit
    python automation.py --debug                  # headed browser, verbose logging
    python automation.py --no-email               # skip email, save locally only
    python automation.py --from-stage 3           # replay transform only (raw file must exist)
    python automation.py --force-login            # ignore cached session, always re-auth

Exit codes:
    0  success (all locations processed, or single run succeeded)
    1  AuthError
    2  NavError  (single-location mode only; loop mode logs and continues)
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
from datetime import datetime
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
STATE_DIR = BASE_DIR / "state"
LOGS_DIR = BASE_DIR / "logs"
SCREENSHOTS_DIR = BASE_DIR / "screenshots"

for d in (DOWNLOADS_DIR, OUTPUT_DIR, STATE_DIR, LOGS_DIR, SCREENSHOTS_DIR):
    d.mkdir(exist_ok=True)

STORAGE_STATE_PATH = STATE_DIR / "storage_state.json"
CHECKPOINT_PATH = STATE_DIR / "checkpoint.json"

with open(BASE_DIR / "config.yaml") as _f:
    CONFIG = yaml.safe_load(_f)


# ──────────────────────────────────────────────────────────────────────────────
# Location Helpers
# ──────────────────────────────────────────────────────────────────────────────

# Supy names that indicate a location is inactive / not mappable.
_SKIP_SUPY_NAMES: set[str] = {
    "n/a",
    "not in use",
    "location closed",
    "no sales for now",
}


def _is_skip(supy_name: str) -> bool:
    """Return True when a supy_name should be excluded from processing."""
    if not supy_name or not supy_name.strip():
        return True
    lower = supy_name.strip().lower()
    if lower in _SKIP_SUPY_NAMES:
        return True
    if lower.startswith("location name not mentioned"):
        return True
    return False


def _get_active_locations() -> list[dict]:
    """
    Return the list of active {pos_name, supy_name} dicts from config.yaml,
    with all invalid supy_names filtered out.
    """
    all_locs = CONFIG.get("locations", [])
    return [
        loc for loc in all_locs
        if not _is_skip(loc.get("supy_name", ""))
    ]


def _sanitize_filename(name: str) -> str:
    """
    Convert a Supy location name into a safe filesystem component.
    Strips/replaces characters that are not allowed in filenames on any OS.
    """
    # Replace characters illegal on Windows/Linux/macOS with underscores
    safe = re.sub(r'[\\/*?:"<>|]', "_", name)
    # Collapse multiple spaces/underscores
    safe = re.sub(r"[\s_]+", "_", safe).strip("_")
    return safe or "unknown_location"


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
# Run ID + Structured Logger
# ──────────────────────────────────────────────────────────────────────────────

RUN_ID = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
_log_path: Optional[pathlib.Path] = None
_verbose = False


def _init_logger(verbose: bool) -> None:
    global _log_path, _verbose
    _verbose = verbose
    _log_path = LOGS_DIR / f"{RUN_ID}.jsonl"


def log(stage: str, step: str, outcome: str, duration_ms: int = 0, extra: dict = None) -> None:
    entry = {
        "ts": datetime.utcnow().isoformat(),
        "run_id": RUN_ID,
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
        status_icon = "✓" if outcome == "ok" else "✗" if outcome == "error" else "→"
        print(f"  [{status_icon}] [{stage}] {step}  ({duration_ms}ms)")
    elif outcome == "error":
        print(f"  [✗] [{stage}] {step}: {extra.get('error', '') if extra else ''}", file=sys.stderr)


# ──────────────────────────────────────────────────────────────────────────────
# Screenshot Helper
# ──────────────────────────────────────────────────────────────────────────────

def screenshot(page: Page, stage: str, label: str) -> None:
    run_dir = SCREENSHOTS_DIR / RUN_ID
    run_dir.mkdir(exist_ok=True)
    path = run_dir / f"{stage}_{label}.png"
    try:
        page.screenshot(path=str(path), full_page=False)
        if _verbose:
            print(f"       [📸] {path.name}")
    except Exception:
        pass  # never let screenshot failure kill the pipeline


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
    """Exponential backoff retry. AuthError and TransformError are never retried."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except AuthError:
                    raise  # never retry auth errors
                except TransformError:
                    raise  # never retry transform errors
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
    """Load cached storage state and verify we are actually logged in."""
    auth_el = CONFIG["portal"].get("authenticated_element", "")
    if not auth_el:
        return False
    try:
        page.goto(CONFIG["portal"]["portal_url"], wait_until="domcontentloaded", timeout=20_000)
        page.wait_for_selector(auth_el, timeout=5_000)
        return True
    except Exception:
        return False


def stage_auth(page: Page, context, force_login: bool) -> None:
    t0 = time.monotonic()
    sel = CONFIG["selectors"]

    # Validate required selectors are configured
    for key in ("username_field", "company_field", "password_field", "login_button"):
        if not sel.get(key):
            raise AuthError(
                f"config.yaml selectors.{key} is empty. "
                "Run debug_selectors.py first and fill in config.yaml."
            )

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

    # Perform login
    username = os.environ.get("PORTAL_USERNAME", CONFIG.get("portal", {}).get("username", ""))
    company = os.environ.get("PORTAL_COMPANY", CONFIG.get("portal", {}).get("company", ""))
    password = os.environ.get("PORTAL_PASSWORD", "")

    if not password:
        raise AuthError("PORTAL_PASSWORD is not set. Add it to your .env file.")

    try:
        page.goto(CONFIG["portal"]["login_url"], wait_until="domcontentloaded", timeout=30_000)
        screenshot(page, "auth", "01_login_page")

        page.wait_for_selector(sel["username_field"], timeout=10_000)
        page.fill(sel["username_field"], username)
        page.fill(sel["company_field"], company)
        page.fill(sel["password_field"], password)
        screenshot(page, "auth", "02_fields_filled")

        # Click login and wait for navigation — portal can be slow to redirect
        with page.expect_navigation(wait_until="domcontentloaded", timeout=45_000):
            page.click(sel["login_button"])

        # Detect login failure before assuming success
        error_sel = sel.get("login_error", "")
        if error_sel:
            try:
                page.wait_for_selector(error_sel, timeout=3_000)
                screenshot(page, "auth", "03_login_error")
                raise AuthError("Login failed — error element detected on page.")
            except AuthError:
                raise
            except Exception:
                pass  # no error element found → login succeeded

        # Confirm we landed on the portal
        authenticated_el = CONFIG["portal"].get("authenticated_element", "")
        if authenticated_el:
            page.wait_for_selector(authenticated_el, timeout=15_000)

        screenshot(page, "auth", "03_logged_in")

    except AuthError:
        raise
    except Exception as exc:
        screenshot(page, "auth", "error")
        raise NavError(f"Login navigation failed: {exc}") from exc

    # Persist session
    context.storage_state(path=str(STORAGE_STATE_PATH))
    log("auth", "login", "ok", duration_ms=int((time.monotonic() - t0) * 1000))
    write_checkpoint(1)


# ──────────────────────────────────────────────────────────────────────────────
# Stage 2a — Revenue Centers Location Filter
# ──────────────────────────────────────────────────────────────────────────────

def stage_set_location_filter(page: Page, pos_location_name: str) -> None:
    """
    Set the Revenue Centers filter on the Oracle BI report to the given
    ``pos_location_name`` and wait for the report to be ready.

    Confirmed portal structure (from debug_location_filter.py):
      - iframe name='revenueCenterFrame' contains a jsTree dual-listbox UI
      - Search input: #serachmaintree (portal typo intentional)
      - Search button: input[value='Search'] (first in frame)
      - Add button: input[value='Add']
      - Apply button: input#btnApplySelection
      - Tree results div: #loadlocationtreetd
      - Main frame also has select#revenueCenterData (117 options, used as fallback)

    Raises NavError if the filter cannot be applied.
    """
    t0 = time.monotonic()
    report_url = CONFIG["portal"]["report_url"]

    if _verbose:
        print(f"  [→] Setting Revenue Centers filter to: {pos_location_name!r}")

    # Navigate fresh each time so filter state is clean
    page.goto(report_url, wait_until="domcontentloaded", timeout=30_000)
    try:
        page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception:
        pass

    filter_applied = False

    # ── Primary: revenueCenterFrame jsTree search UI ──────────────────────────
    try:
        rc_frame = page.frame(name="revenueCenterFrame")
        if rc_frame is None:
            raise RuntimeError("revenueCenterFrame not found")

        # The iframe panels are collapsed by default — click the header button
        # in the main frame to expand the Revenue Centers section first.
        try:
            page.locator("#revenueCenterBtn").click(timeout=5_000)
            time.sleep(1.0)
        except Exception:
            pass

        # Wait for the search input to become interactive
        rc_frame.wait_for_selector("#serachmaintree", state="visible", timeout=15_000)

        # Clear any existing selection in the target tree
        try:
            clear_link = rc_frame.locator("a", has_text="Clear selection").first
            if clear_link.count():
                clear_link.click()
                time.sleep(0.5)
        except Exception:
            pass

        # Type location name into search box and click Search
        search_input = rc_frame.locator("#serachmaintree")
        search_input.fill("")
        search_input.fill(pos_location_name)

        rc_frame.locator("input[value='Search']").first.click(force=True)
        time.sleep(2.0)  # give jsTree time to filter results

        # Click the matching node in the source tree
        # jsTree renders nodes as <a> elements inside #loadlocationtreetd
        tree_div = rc_frame.locator("#loadlocationtreetd")
        node = tree_div.get_by_text(pos_location_name, exact=True).first
        node.click(timeout=10_000)
        time.sleep(0.5)

        # Click Add to move to selected side
        rc_frame.locator("input[value='Add']").click(force=True)
        time.sleep(0.5)

        # Apply selection
        rc_frame.locator("input#btnApplySelection").click(force=True)
        time.sleep(1.5)

        filter_applied = True
        if _verbose:
            print(f"  [✓] Revenue Centers filter applied via revenueCenterFrame jsTree")

    except Exception as e:
        if _verbose:
            print(f"  [!] revenueCenterFrame approach failed: {e}")

    # ── Fallback: select#revenueCenterData in main frame ─────────────────────
    if not filter_applied:
        try:
            # select#revenueCenterData has 117 options; do fuzzy match on portal names
            options_els = page.locator("#revenueCenterData option").all()
            matched = None
            pos_lower = pos_location_name.lower()
            for opt in options_els:
                text = opt.inner_text().strip()
                if text.lower() == pos_lower:
                    matched = text
                    break
            # Fuzzy: try stripping apostrophes/hyphens/spaces
            if matched is None:
                def _norm(s: str) -> str:
                    return s.lower().replace("'", "").replace("-", "").replace(" ", "")
                pos_norm = _norm(pos_location_name)
                for opt in options_els:
                    text = opt.inner_text().strip()
                    if _norm(text) == pos_norm:
                        matched = text
                        break

            if matched:
                page.evaluate(
                    """([val]) => {
                        const sel = document.querySelector('#revenueCenterData');
                        for (const opt of sel.options) { opt.selected = false; }
                        for (const opt of sel.options) {
                            if (opt.text === val) { opt.selected = true; break; }
                        }
                        sel.dispatchEvent(new Event('change', {bubbles: true}));
                    }""",
                    [matched]
                )
                filter_applied = True
                if _verbose:
                    print(f"  [✓] Revenue Centers filter applied via select#revenueCenterData "
                          f"(matched {matched!r})")
            else:
                if _verbose:
                    print(f"  [!] select#revenueCenterData: no option matched {pos_location_name!r}")
        except Exception as e:
            if _verbose:
                print(f"  [!] select#revenueCenterData fallback failed: {e}")

    if not filter_applied:
        if _verbose:
            print(f"  [!] Could not apply filter for {pos_location_name!r} — "
                  f"report will contain all locations for this entry")

    # Click "Run Report" to execute the report with the selected filter.
    # The Excel download button only appears AFTER the report has finished running.
    try:
        page.locator('[id="Run Report"]').click(timeout=15_000)
        if _verbose:
            print(f"  [→] Clicked 'Run Report'")
    except Exception as e:
        if _verbose:
            print(f"  [!] Could not click Run Report: {e}")

    # Wait for the Excel download button to appear (report finished rendering)
    try:
        final_step = CONFIG["navigation"][-1]
        page.wait_for_selector(final_step["click"], state="visible", timeout=120_000)
    except Exception as exc:
        screenshot(page, "nav", f"filter_error_{_sanitize_filename(pos_location_name)}")
        raise NavError(
            f"Report did not finish rendering after filter for {pos_location_name!r}: {exc}"
        ) from exc

    log("nav", "set_location_filter", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"location": pos_location_name, "filter_applied": filter_applied})

    screenshot(page, "nav", f"filter_set_{_sanitize_filename(pos_location_name)}")


# ──────────────────────────────────────────────────────────────────────────────
# Stage 2 — Navigation & Download
# ──────────────────────────────────────────────────────────────────────────────

@retry(max_attempts=3, exceptions=(NavError,))
def stage_navigate_and_download(
    page: Page,
    location_name: Optional[str] = None,
) -> pathlib.Path:
    """
    Navigate to the report, optionally set a Revenue Centers filter for
    ``location_name``, then download the Excel report.

    Args:
        page:          Active Playwright page.
        location_name: POS location name to filter by (from config.yaml locations[].pos_name).
                       If None, downloads the unfiltered report (legacy behaviour).

    Returns:
        Path to the downloaded raw file.
    """
    t0 = time.monotonic()
    nav_steps = CONFIG["navigation"]
    report_url = CONFIG["portal"].get("report_url", "")

    try:
        if location_name:
            # Filter mode: set_location_filter handles navigation to the report URL
            # and waits for the download button to appear.
            stage_set_location_filter(page, location_name)
        else:
            # Legacy mode: navigate directly to the report page
            if report_url:
                if _verbose:
                    print(f"  [→] Navigating directly to report URL...")
                page.goto(report_url, wait_until="domcontentloaded", timeout=30_000)
                # Wait for the report to fully render (reportsFrame populates asynchronously)
                page.wait_for_load_state("networkidle", timeout=30_000)

            # Walk any intermediate steps (currently just the Excel icon click)
            for step_cfg in nav_steps[:-1]:
                label = step_cfg["step"]
                page.wait_for_selector(step_cfg["click"], state="visible", timeout=30_000)
                screenshot(page, "nav", f"before_{label.replace(' ', '_')}")
                page.click(step_cfg["click"])
                page.wait_for_selector(step_cfg["wait"], state="visible", timeout=30_000)
                log("nav", label, "ok")

        # Final step — arm download handler before clicking Excel icon
        final = nav_steps[-1]
        page.wait_for_selector(final["click"], state="visible", timeout=60_000)
        screenshot(page, "nav", "before_download")

        with page.expect_download(timeout=120_000) as dl_info:
            page.click(final["click"])

        download = dl_info.value
        suffix = pathlib.Path(download.suggested_filename).suffix or ".xlsx"

        # Embed sanitized location name in the raw filename for traceability
        loc_tag = f"_{_sanitize_filename(location_name)}" if location_name else ""
        dest = DOWNLOADS_DIR / f"{RUN_ID}{loc_tag}_raw{suffix}"
        download.save_as(str(dest))

    except NavError:
        raise
    except Exception as exc:
        screenshot(page, "nav", "error")
        raise NavError(f"Navigation/download failed: {exc}") from exc

    # Validate file is non-empty
    if not dest.exists() or dest.stat().st_size == 0:
        raise NavError(f"Downloaded file is empty or missing: {dest}")

    log("nav", "download", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"file": str(dest), "size_bytes": dest.stat().st_size,
               "location": location_name or "all"})
    write_checkpoint(2, {"raw_file": str(dest)})
    return dest


# ──────────────────────────────────────────────────────────────────────────────
# Stage 3 — Transformation
# ──────────────────────────────────────────────────────────────────────────────

def _detect_header_row(raw_path: pathlib.Path) -> tuple[int, str]:
    """
    Scan the raw file to find the row index of the actual data header
    ("Menu Item Name") and extract the Business Dates value from metadata.
    Returns (header_row_index, business_date_string).
    """
    suffix = raw_path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        # Read without header to scan all rows
        preview = pd.read_excel(raw_path, header=None, nrows=20)
    else:
        preview = pd.read_csv(raw_path, header=None, nrows=20, on_bad_lines="skip")

    business_date = ""
    header_row = 0

    for i, row in preview.iterrows():
        first_cell = str(row.iloc[0]).strip()
        if first_cell == "Business Dates":
            business_date = str(row.iloc[1]).strip()
        if first_cell == "Menu Item Name":
            header_row = int(i)
            break

    return header_row, business_date


def stage_transform(
    raw_path: pathlib.Path,
    supy_name: Optional[str] = None,
) -> tuple:
    """
    Transform the raw downloaded Excel into the formatted output .xlsx.

    Args:
        raw_path:  Path to the raw downloaded file.
        supy_name: Supy location label (from config.yaml locations[].supy_name).
                   When provided, the output file is named
                   ``{sanitized_supy_name}_{date}.xlsx`` and the date is derived
                   from the report metadata.  Falls back to the legacy name when None.

    Returns:
        (out_path, row_count, business_date_display_str)
    """
    t0 = time.monotonic()

    try:
        header_row, business_date = _detect_header_row(raw_path)

        if _verbose:
            print(f"  [→] Header row detected at index {header_row}")
            print(f"  [→] Business Dates extracted: {business_date!r}")

        # Load the actual data
        suffix = raw_path.suffix.lower()
        if suffix in (".xlsx", ".xls"):
            df = pd.read_excel(raw_path, header=header_row)
        else:
            df = pd.read_csv(raw_path, header=header_row, on_bad_lines="skip")

        # Strip whitespace from column names
        df.columns = [str(c).strip() for c in df.columns]

        # Drop aggregate "Totals:" rows — identified by NaN in "Menu Item #"
        df = df[df["Menu Item #"].notna()].copy()

        # Drop rows where "Menu Item #" is not a valid number (extra sub-headers etc.)
        df = df[pd.to_numeric(df["Menu Item #"], errors="coerce").notna()].copy()
        df.reset_index(drop=True, inplace=True)

        # ── Column mapping from config ────────────────────────────
        col_cfgs = CONFIG["columns"]
        rename_map = {}
        drop_cols = []

        for col_cfg in col_cfgs:
            if col_cfg.get("drop"):
                raw_col = col_cfg["raw"]
                if raw_col in df.columns:
                    drop_cols.append(raw_col)
            elif col_cfg.get("inject"):
                pass  # handled below
            elif col_cfg.get("raw") and col_cfg.get("target"):
                rename_map[col_cfg["raw"]] = col_cfg["target"]

        df.drop(columns=drop_cols, errors="ignore", inplace=True)
        df.rename(columns=rename_map, inplace=True)

        # ── Inject columns ────────────────────────────────────────
        for col_cfg in col_cfgs:
            inject = col_cfg.get("inject")
            if not inject:
                continue
            target = col_cfg["target"]
            if inject == "business_dates_metadata":
                if business_date:
                    parsed = pd.to_datetime(business_date,
                                            format=CONFIG.get("raw_date_format", "%d/%m/%Y"),
                                            errors="coerce")
                    fmt = CONFIG.get("output_date_format", "%d-%b-%Y")
                    df[target] = parsed.strftime(fmt) if parsed is not pd.NaT else business_date
                else:
                    df[target] = ""
            elif inject == "empty":
                df[target] = ""

        # ── Type casting ──────────────────────────────────────────
        for col_cfg in col_cfgs:
            if col_cfg.get("drop") or col_cfg.get("inject"):
                continue
            target = col_cfg.get("target")
            dtype = col_cfg.get("dtype")
            if not target or target not in df.columns:
                continue
            if dtype == "int":
                df[target] = pd.to_numeric(df[target], errors="coerce").fillna(0).astype(int)
            elif dtype == "float":
                # Strip currency symbols, commas, whitespace then cast
                df[target] = (
                    df[target]
                    .astype(str)
                    .str.replace(r"[^\d.\-]", "", regex=True)
                    .replace("", "0")
                )
                df[target] = pd.to_numeric(df[target], errors="coerce").fillna(0.0).round(2)

        # ── Reorder to final column order ─────────────────────────
        final_order = CONFIG.get("output_column_order", [])
        # Keep only columns that exist in df, in the specified order
        ordered = [c for c in final_order if c in df.columns]
        # Append any unexpected extra columns at the end
        extras = [c for c in df.columns if c not in ordered]
        df = df[ordered + extras]

        # ── Export ────────────────────────────────────────────────
        today = datetime.now().strftime("%Y-%m-%d")

        if supy_name:
            safe_name = _sanitize_filename(supy_name)
            out_path = OUTPUT_DIR / f"{safe_name}_{today}.xlsx"
        else:
            out_path = OUTPUT_DIR / f"sales_report_{today}_{RUN_ID[:8]}.xlsx"

        df.to_excel(str(out_path), index=False, engine="openpyxl")

    except (KeyError, ValueError, TypeError) as exc:
        raise TransformError(f"Transform failed: {exc}\n{traceback.format_exc()}") from exc
    except Exception as exc:
        raise TransformError(f"Unexpected transform error: {exc}\n{traceback.format_exc()}") from exc

    log("transform", "export", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"output": str(out_path), "rows": len(df),
               "location": supy_name or "all"})
    write_checkpoint(3, {"output_file": str(out_path)})

    if _verbose:
        print(f"  [→] {len(df)} rows written → {out_path}")

    # Format business_date for the email subject (e.g. "16-Apr-2026")
    fmt = CONFIG.get("output_date_format", "%d-%b-%Y")
    try:
        bd_display = pd.to_datetime(business_date,
                                    format=CONFIG.get("raw_date_format", "%d/%m/%Y"),
                                    errors="coerce").strftime(fmt)
    except Exception:
        bd_display = business_date

    return out_path, len(df), bd_display


# ──────────────────────────────────────────────────────────────────────────────
# Stage 4 — Email
# ──────────────────────────────────────────────────────────────────────────────

def stage_email(
    out_path: pathlib.Path,
    row_count: int,
    business_date: str,
    location_label: Optional[str] = None,
) -> None:
    t0 = time.monotonic()

    gmail_user     = os.environ.get("GMAIL_USER", "")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD", "")
    recipient      = os.environ.get("REPORT_RECIPIENT", gmail_user)

    if not gmail_user or not gmail_password:
        raise EmailError(
            "GMAIL_USER and GMAIL_APP_PASSWORD must be set in your .env / GitHub Secrets."
        )

    if location_label:
        subject = f"POS Sales Report — {location_label} — {business_date}"
        body = (
            f"Hi,\n\n"
            f"Please find attached the daily POS Sales Report for:\n\n"
            f"  • Location : {location_label}\n"
            f"  • Date     : {business_date}\n"
            f"  • Rows     : {row_count:,}\n"
            f"  • File     : {out_path.name}\n"
            f"  • Run ID   : {RUN_ID}\n\n"
            f"This report was generated automatically by the Oracle BI pipeline.\n\n"
            f"Regards,\nOracle BI Automation"
        )
    else:
        subject = f"POS Sales Report — {business_date}"
        body = (
            f"Hi,\n\n"
            f"Please find attached the daily POS Sales Report for {business_date}.\n\n"
            f"  • Rows: {row_count:,}\n"
            f"  • File: {out_path.name}\n"
            f"  • Run ID: {RUN_ID}\n\n"
            f"This report was generated automatically by the Oracle BI pipeline.\n\n"
            f"Regards,\nOracle BI Automation"
        )

    msg = MIMEMultipart()
    msg["From"]    = gmail_user
    msg["To"]      = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with open(out_path, "rb") as f:
        attachment = MIMEApplication(f.read(),
                                     _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        attachment.add_header("Content-Disposition", "attachment", filename=out_path.name)
        msg.attach(attachment)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, recipient, msg.as_string())
    except Exception as exc:
        raise EmailError(f"Failed to send email: {exc}") from exc

    log("email", "send", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"to": recipient, "subject": subject, "attachment": out_path.name,
               "location": location_label or "all"})

    print(f"[Stage 4] ✓ Email sent → {recipient}  [{location_label or 'all locations'}]\n")


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Oracle BI Automation Pipeline")
    parser.add_argument("--debug", action="store_true",
                        help="Run with headed browser and verbose logging")
    parser.add_argument("--from-stage", type=int, default=1, metavar="N",
                        help="Resume from stage N (1=auth, 2=nav, 3=transform, 4=email)")
    parser.add_argument("--force-login", action="store_true",
                        help="Ignore cached session; always re-authenticate")
    parser.add_argument("--no-email", action="store_true",
                        help="Skip email — save output file locally only")

    # ── Location selection flags ───────────────────────────────────────────────
    loc_group = parser.add_mutually_exclusive_group()
    loc_group.add_argument(
        "--all-locations", action="store_true",
        help="Loop through ALL active locations in config.yaml and run one report per location",
    )
    loc_group.add_argument(
        "--location", metavar="POS_NAME",
        help="Run for a single location by its POS name (e.g. \"Parker's Mirdiff\")",
    )
    parser.add_argument(
        "--list-locations", action="store_true",
        help="Print all active locations from config.yaml and exit",
    )

    args = parser.parse_args()

    # ── --list-locations: print and exit ──────────────────────────────────────
    if args.list_locations:
        active = _get_active_locations()
        print(f"\nActive locations ({len(active)} total):\n")
        for i, loc in enumerate(active, start=1):
            print(f"  {i:3d}. POS: {loc['pos_name']}")
            print(f"       Supy: {loc['supy_name']}")
        print()
        return 0

    _init_logger(verbose=args.debug)
    from_stage = args.from_stage

    print(f"\n[Oracle BI Pipeline] run_id={RUN_ID}  from_stage={from_stage}\n")

    # ── Determine run mode ────────────────────────────────────────────────────
    # all_locations mode: loop all active config entries
    # single_location mode: specific POS name from --location flag
    # legacy mode: no location filter (original single-report behaviour)

    active_locations = _get_active_locations()

    if args.all_locations:
        if not active_locations:
            print("[!] No active locations found in config.yaml — running in legacy mode.")
            run_mode = "legacy"
        else:
            run_mode = "all_locations"
            print(f"[→] Running in ALL-LOCATIONS mode ({len(active_locations)} locations)\n")
    elif args.location:
        # Find matching location in config (match by pos_name, case-insensitive)
        matched = [
            loc for loc in active_locations
            if loc["pos_name"].lower() == args.location.lower()
        ]
        if not matched:
            print(f"[✗] Location {args.location!r} not found in config.yaml active locations.",
                  file=sys.stderr)
            print("    Run --list-locations to see all available locations.", file=sys.stderr)
            return 2
        run_mode = "single_location"
        single_location = matched[0]
        print(f"[→] Running for single location: {single_location['pos_name']!r} "
              f"→ {single_location['supy_name']!r}\n")
    else:
        run_mode = "legacy"

    # ── Stage 3-only resumption path ──────────────────────────────────────────
    raw_file: Optional[pathlib.Path] = None
    if from_stage >= 3:
        checkpoint = read_checkpoint()
        raw_file_str = checkpoint.get("raw_file")
        if raw_file_str:
            raw_file = pathlib.Path(raw_file_str)
        else:
            candidates = sorted(DOWNLOADS_DIR.glob("*_raw.*"), key=lambda p: p.stat().st_mtime)
            if candidates:
                raw_file = candidates[-1]
        if not raw_file or not raw_file.exists():
            print("[✗] --from-stage 3 requires an existing raw download. "
                  "Run from stage 1 or 2 first.", file=sys.stderr)
            return 3
        # Resume from stage 3 — legacy single-file mode
        print("[Stage 3] Transforming raw data (resumed from checkpoint)...")
        try:
            out_file, row_count, business_date = stage_transform(raw_file)
            print(f"[Stage 3] ✓ Output → {out_file}\n")
        except TransformError as exc:
            log("transform", "transform", "error", extra={"error": str(exc)})
            print(f"[✗] Transform error: {exc}", file=sys.stderr)
            return 3

        if not args.no_email:
            print("[Stage 4] Sending email...")
            try:
                stage_email(out_file, row_count, business_date)
            except EmailError as exc:
                log("email", "send", "error", extra={"error": str(exc)})
                print(f"[✗] Email error: {exc}", file=sys.stderr)
                print(f"  ↳ Report was saved to: {out_file}", file=sys.stderr)
                return 4
        else:
            print(f"[Stage 4] Skipped (--no-email).  File saved → {out_file}\n")

        print(f"[✓] Pipeline complete.  run_id={RUN_ID}\n")
        return 0

    # ── Stages 1 & 2 require a browser ────────────────────────────────────────
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

                # ── Stage 2 + 3 + 4: per-location loop ────────────
                if run_mode == "all_locations":
                    failed_locations = []
                    succeeded = 0

                    for idx, loc in enumerate(active_locations, start=1):
                        pos_name = loc["pos_name"]
                        supy_name = loc["supy_name"]
                        print(f"[Location {idx}/{len(active_locations)}] "
                              f"{pos_name!r} → {supy_name!r}")

                        # Stage 2
                        try:
                            raw_file = stage_navigate_and_download(page, location_name=pos_name)
                            print(f"  [Stage 2] ✓ Downloaded → {raw_file.name}")
                        except NavError as exc:
                            log("nav", "navigate_and_download", "error",
                                extra={"error": str(exc), "location": pos_name})
                            print(f"  [✗] Nav error for {pos_name!r}: {exc} — skipping",
                                  file=sys.stderr)
                            failed_locations.append({"location": pos_name, "stage": 2,
                                                      "error": str(exc)})
                            continue

                        # Stage 3
                        try:
                            out_file, row_count, business_date = stage_transform(
                                raw_file, supy_name=supy_name
                            )
                            print(f"  [Stage 3] ✓ Transformed → {out_file.name} ({row_count} rows)")
                        except TransformError as exc:
                            log("transform", "transform", "error",
                                extra={"error": str(exc), "location": supy_name})
                            print(f"  [✗] Transform error for {supy_name!r}: {exc} — skipping",
                                  file=sys.stderr)
                            failed_locations.append({"location": supy_name, "stage": 3,
                                                      "error": str(exc)})
                            continue

                        # Stage 4
                        if not args.no_email:
                            try:
                                stage_email(out_file, row_count, business_date,
                                            location_label=supy_name)
                            except EmailError as exc:
                                log("email", "send", "error",
                                    extra={"error": str(exc), "location": supy_name})
                                print(f"  [✗] Email error for {supy_name!r}: {exc}",
                                      file=sys.stderr)
                                print(f"       Report saved → {out_file}", file=sys.stderr)
                                failed_locations.append({"location": supy_name, "stage": 4,
                                                          "error": str(exc)})
                                # Do NOT continue — file was generated; count as partial success
                        else:
                            print(f"  [Stage 4] Skipped (--no-email). File → {out_file}\n")

                        succeeded += 1

                    browser.close()

                    # Summary
                    print(f"\n{'─'*60}")
                    print(f"[✓] All-locations run complete.  run_id={RUN_ID}")
                    print(f"    Succeeded: {succeeded}/{len(active_locations)}")
                    if failed_locations:
                        print(f"    Failed ({len(failed_locations)}):")
                        for fl in failed_locations:
                            print(f"      • [stage {fl['stage']}] {fl['location']}: {fl['error']}")
                    print(f"{'─'*60}\n")

                    # Return non-zero only if ALL locations failed
                    return 0 if succeeded > 0 else 2

                elif run_mode == "single_location":
                    pos_name = single_location["pos_name"]
                    supy_name = single_location["supy_name"]

                    # Stage 2
                    print(f"[Stage 2] Navigating and downloading for {pos_name!r}...")
                    try:
                        raw_file = stage_navigate_and_download(page, location_name=pos_name)
                        print(f"[Stage 2] ✓ Downloaded → {raw_file}\n")
                    except NavError as exc:
                        log("nav", "navigate_and_download", "error", extra={"error": str(exc)})
                        print(f"[✗] Nav error: {exc}", file=sys.stderr)
                        browser.close()
                        return 2

                    browser.close()

                    # Stage 3
                    print("[Stage 3] Transforming raw data...")
                    try:
                        out_file, row_count, business_date = stage_transform(
                            raw_file, supy_name=supy_name
                        )
                        print(f"[Stage 3] ✓ Output → {out_file}\n")
                    except TransformError as exc:
                        log("transform", "transform", "error", extra={"error": str(exc)})
                        print(f"[✗] Transform error: {exc}", file=sys.stderr)
                        return 3

                    # Stage 4
                    if not args.no_email:
                        print("[Stage 4] Sending email...")
                        try:
                            stage_email(out_file, row_count, business_date,
                                        location_label=supy_name)
                        except EmailError as exc:
                            log("email", "send", "error", extra={"error": str(exc)})
                            print(f"[✗] Email error: {exc}", file=sys.stderr)
                            print(f"  ↳ Report was saved to: {out_file}", file=sys.stderr)
                            return 4
                    else:
                        print(f"[Stage 4] Skipped (--no-email).  File saved → {out_file}\n")

                    print(f"[✓] Pipeline complete.  run_id={RUN_ID}\n")
                    return 0

                else:
                    # Legacy mode
                    print("[Stage 2] Navigating to report and downloading...")
                    try:
                        raw_file = stage_navigate_and_download(page)
                        print(f"[Stage 2] ✓ Downloaded → {raw_file}\n")
                    except NavError as exc:
                        log("nav", "navigate_and_download", "error", extra={"error": str(exc)})
                        print(f"[✗] Nav error: {exc}", file=sys.stderr)
                        browser.close()
                        return 2

                    browser.close()

        except Exception as exc:
            print(f"[✗] Unexpected browser error: {exc}", file=sys.stderr)
            log("browser", "unexpected", "error", extra={"error": str(exc)})
            return 2

    # ── Legacy mode: Stage 3 & 4 outside browser block ────────────────────────
    if run_mode == "legacy":
        print("[Stage 3] Transforming raw data...")
        try:
            out_file, row_count, business_date = stage_transform(raw_file)
            print(f"[Stage 3] ✓ Output → {out_file}\n")
        except TransformError as exc:
            log("transform", "transform", "error", extra={"error": str(exc)})
            print(f"[✗] Transform error: {exc}", file=sys.stderr)
            return 3

        if not args.no_email:
            print("[Stage 4] Sending email...")
            try:
                stage_email(out_file, row_count, business_date)
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
