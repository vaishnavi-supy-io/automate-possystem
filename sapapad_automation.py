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

BASE_DIR      = pathlib.Path(__file__).parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
OUTPUT_DIR    = BASE_DIR / "output"
STATE_DIR     = BASE_DIR / "state" / "sapapad"
LOGS_DIR      = BASE_DIR / "logs"
SCREENSHOTS_DIR = BASE_DIR / "screenshots"
MAPPINGS_DIR  = BASE_DIR / "mappings"

for d in (DOWNLOADS_DIR, OUTPUT_DIR, STATE_DIR, LOGS_DIR, SCREENSHOTS_DIR, MAPPINGS_DIR):
    d.mkdir(parents=True, exist_ok=True)

STORAGE_STATE_PATH = STATE_DIR / "storage_state.json"
CHECKPOINT_PATH    = STATE_DIR / "checkpoint.json"

with open(BASE_DIR / "sapapad_config.yaml") as _f:
    CONFIG = yaml.safe_load(_f)


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
    _log_path = LOGS_DIR / f"sapapad_{RUN_ID}.jsonl"


def log(stage: str, step: str, outcome: str, duration_ms: int = 0, extra: dict = None) -> None:
    entry = {
        "ts": datetime.utcnow().isoformat(),
        "run_id": RUN_ID,
        "pipeline": "sapapad",
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
    run_dir = SCREENSHOTS_DIR / f"sapapad_{RUN_ID}"
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

    username = os.environ.get("SAPAPAD_USERNAME", "")
    company  = os.environ.get("SAPAPAD_COMPANY", "")
    password = os.environ.get("SAPAPAD_PASSWORD", "")

    if not username:
        raise AuthError("SAPAPAD_USERNAME is not set in your .env file.")
    if not password:
        raise AuthError("SAPAPAD_PASSWORD is not set in your .env file.")

    try:
        page.goto(CONFIG["portal"]["login_url"], wait_until="domcontentloaded", timeout=30_000)
        screenshot(page, "auth", "01_login_page")

        page.wait_for_selector(sel["username_field"], timeout=10_000)
        page.fill(sel["username_field"], username)

        if sel.get("company_field") and sel["company_field"] not in ("", "FILL_IN"):
            page.fill(sel["company_field"], company)

        page.fill(sel["password_field"], password)
        screenshot(page, "auth", "02_fields_filled")

        with page.expect_navigation(wait_until="domcontentloaded", timeout=45_000):
            page.click(sel["login_button"])

        error_sel = sel.get("login_error", "")
        if error_sel:
            try:
                page.wait_for_selector(error_sel, timeout=3_000)
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
def stage_navigate_and_download(
    page: Page,
    location_id: Optional[str] = None,
    branch_name: Optional[str] = None,
) -> pathlib.Path:
    t0 = time.monotonic()
    nav_steps  = CONFIG["navigation"]
    report_url = CONFIG["portal"].get("report_url", "")
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
                if branch_name:
                    # Saved Reports row text: "Top Grossing Items for {branch_name}"
                    click_sel = (
                        f"tr:has-text('Top Grossing Items for {branch_name}') "
                        f"a:has-text('Download csv')"
                    )
                else:
                    click_sel = step_cfg["click"]

                poll_attempts = int(step_cfg.get("poll_attempts", 1))
                poll_interval = int(step_cfg.get("poll_interval_s", 15))

                # Poll: refresh the page until the download link appears
                for attempt in range(poll_attempts):
                    if page.query_selector(click_sel):
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
                else:
                    raise NavError(
                        f"Download link not found after {poll_attempts} attempts. "
                        f"Selector: {click_sel}"
                    )

                page.wait_for_selector(click_sel, state="visible", timeout=15_000)
                screenshot(page, "nav", "before_download")

                branch_slug = branch_name.replace(" ", "_") if branch_name else "all"
                with page.expect_download(timeout=120_000) as dl_info:
                    page.click(click_sel)

                download = dl_info.value
                suffix = pathlib.Path(download.suggested_filename).suffix or ".csv"
                dest = DOWNLOADS_DIR / f"sapapad_{branch_slug}_{RUN_ID}_raw{suffix}"
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

    log("nav", "download", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"file": str(dest), "size_bytes": dest.stat().st_size})
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


def stage_transform(raw_path: pathlib.Path, branch_name: Optional[str] = None) -> tuple:
    """Returns (out_path, row_count, report_date_str)."""
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
            elif inject == "date_yesterday":
                # Yesterday relative to today (the report always covers the previous day)
                from datetime import timedelta
                yesterday = datetime.now() - timedelta(days=1)
                report_date = yesterday.strftime(fmt)
                df[target] = report_date
            elif inject == "date_from_filename":
                import re
                match = re.search(r"(\d{4})(\d{2})(\d{2})", raw_path.name)
                if match:
                    y, m, d = match.groups()
                    report_date = datetime(int(y), int(m), int(d)).strftime(fmt)
                    df[target] = report_date
                else:
                    df[target] = ""
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

        # ── Export ────────────────────────────────────────────
        today      = datetime.now().strftime("%Y-%m-%d")
        branch_slug = branch_name.replace(" ", "_") if branch_name else "all_locations"
        out_path   = OUTPUT_DIR / f"sapapad_{branch_slug}_{today}_{RUN_ID[:8]}.xlsx"
        df.to_excel(str(out_path), index=False, engine="openpyxl")

    except (KeyError, ValueError, TypeError) as exc:
        raise TransformError(f"Transform failed: {exc}\n{traceback.format_exc()}") from exc
    except TransformError:
        raise
    except Exception as exc:
        raise TransformError(f"Unexpected transform error: {exc}\n{traceback.format_exc()}") from exc

    log("transform", "export", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"output": str(out_path), "rows": len(df)})
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
) -> VerificationResult:
    """
    3-layer verification. Never raises — always returns a VerificationResult.

    Layer 1 — Raw → output integrity
        Row count and revenue sum must agree between the downloaded CSV
        and the transformed output (within a 5% / 1% tolerance respectively).

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
        raw_count = len(raw_df)
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
            raw_total = pd.to_numeric(raw_df["Total Amount"], errors="coerce").sum()
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
            from datetime import timedelta
            yesterday = (datetime.now() - timedelta(days=1)).strftime(
                CONFIG.get("output_date_format", "%d-%b-%Y")
            )
            bad_dates = out_df[out_df["Sales Date *"].astype(str) != yesterday]
            if not bad_dates.empty:
                result.warn(
                    f"{len(bad_dates)} rows have unexpected Sales Date "
                    f"(expected {yesterday!r}): "
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

        if "POS Item ID *" in out_df.columns and len(out_df) > 0:
            matched = out_df["POS Item ID *"].notna().sum()
            match_rate = matched / len(out_df) * 100
            if match_rate < 80:
                result.fail(
                    f"Item ID match rate is {match_rate:.0f}% ({matched}/{len(out_df)} rows). "
                    f"Update mappings/sapapad_item_codes.csv."
                )
            elif match_rate < 95:
                result.warn(
                    f"Item ID match rate is {match_rate:.0f}% ({matched}/{len(out_df)} rows). "
                    f"Some items may be missing from the mapping file."
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
    if page is not None:
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
    subject = f"Sapapad POS Sales Report{branch_label} — {report_date}{warn_tag}"

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
                f"<li style='margin-bottom:4px;color:#92400e;'>{w}</li>"
                for w in (verification.warnings + verification.errors)
            )
            vr_html  = f"<ul style='margin:8px 0 0 0;padding-left:20px;'>{items}</ul>"
        else:
            vr_color = "#dc2626"
            vr_bg    = "#fef2f2"
            vr_badge = "FAILED"
            items    = "".join(
                f"<li style='margin-bottom:4px;color:#991b1b;'>{e}</li>"
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
              {branch_name or 'All Locations'}
            </p>
            <p style="margin:4px 0 0;font-size:14px;color:#6d28d9;">
              {report_date}
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
                               color:#1e1b4b;">{report_date}</p>
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
                  {branch_name or 'All Locations'}
                </td>
              </tr>
              <tr style="border-bottom:1px solid #ede9fe;">
                <td style="padding:10px 0;color:#6b7280;">File</td>
                <td style="padding:10px 0;color:#1e1b4b;font-weight:500;word-break:break-all;">
                  {out_path.name}
                </td>
              </tr>
              <tr>
                <td style="padding:10px 0;color:#6b7280;">Run ID</td>
                <td style="padding:10px 0;color:#9ca3af;font-family:monospace;font-size:12px;">
                  {RUN_ID}
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
# Per-Branch Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def _main_per_branch(args) -> int:
    headless = not args.debug
    print(f"\n[Sapapad Per-Branch Pipeline] run_id={RUN_ID}\n")

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

                    # Stage 3 — Transform
                    print(f"[Stage 3] Transforming...")
                    out_file, row_count, report_date = stage_transform(
                        raw_file, branch_name=loc_name
                    )
                    print(f"[Stage 3] ✓ {row_count} rows → {out_file.name}\n")

                    if row_count == 0:
                        print(f"  [!] 0 rows for {loc_name} — skipping email.\n")
                        continue

                    # Stage 3.5 — Verify (Layer 3 portal check skipped in per-branch mode:
                    # after download the browser is on Saved Reports and re-navigating
                    # back shows all-locations data, not just this branch)
                    print(f"[Stage 3.5] Verifying {loc_name}...")
                    vr = stage_verify(raw_file, out_file, page=None, branch_name=loc_name)

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
    parser.add_argument("--debug", action="store_true",
                        help="Run with headed browser and verbose logging")
    parser.add_argument("--from-stage", type=int, default=1, metavar="N",
                        help="Resume from stage N (1=auth, 2=nav, 3=transform, 4=email)")
    parser.add_argument("--force-login", action="store_true",
                        help="Ignore cached session; always re-authenticate")
    parser.add_argument("--no-email", action="store_true",
                        help="Skip email — save output file locally only")
    parser.add_argument("--per-branch", action="store_true",
                        help="Run pipeline for each branch individually")
    parser.add_argument("--limit", type=int, default=0, metavar="N",
                        help="Process only the first N branches (for test runs)")
    args = parser.parse_args()

    _init_logger(verbose=args.debug)
    from_stage = args.from_stage

    if args.per_branch:
        return _main_per_branch(args)

    print(f"\n[Sapapad Pipeline] run_id={RUN_ID}  from_stage={from_stage}\n")

    raw_file: Optional[pathlib.Path] = None

    if from_stage >= 3:
        checkpoint = read_checkpoint()
        raw_file_str = checkpoint.get("raw_file")
        if raw_file_str:
            raw_file = pathlib.Path(raw_file_str)
        else:
            candidates = sorted(
                DOWNLOADS_DIR.glob("sapapad_*_raw.*"), key=lambda p: p.stat().st_mtime
            )
            if candidates:
                raw_file = candidates[-1]
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

                browser.close()

        except Exception as exc:
            print(f"[✗] Unexpected browser error: {exc}", file=sys.stderr)
            log("browser", "unexpected", "error", extra={"error": str(exc)})
            return 2

    # ── Stage 3: Transform ─────────────────────────────────────────
    print("[Stage 3] Transforming raw data + matching item codes...")
    try:
        out_file, row_count, report_date = stage_transform(raw_file)
        print(f"[Stage 3] ✓ Output → {out_file}\n")
    except TransformError as exc:
        log("transform", "transform", "error", extra={"error": str(exc)})
        print(f"[✗] Transform error: {exc}", file=sys.stderr)
        return 3

    # ── Stage 3.5: Verify ──────────────────────────────────────────
    print("[Stage 3.5] Verifying output...")
    vr = stage_verify(raw_file, out_file)

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
