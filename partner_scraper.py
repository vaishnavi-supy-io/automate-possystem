"""
partner_scraper.py
------------------
Generic per-order scraping engine for delivery partners that provide NO
export (no CSV, no Excel, no API).

Seven partners share one shape:

    login → open order list → filter by date + status → open each order
          → scrape line items (name, qty, unit price) → Supy upload format

Everything that differs between partners lives in partners/<name>.yaml:
selectors, status filters, whether prices are scraped or looked up from a
menu-price sheet, and which Supy entity the sales upload to.

Partners driven by this engine:
    just_eat, justeat_business, ordit, feedr, anddine, homecook, uber_eats

Deliveroo is NOT here — it has a real report export, so it uses
deliveroo_automation.py instead.

Usage:
    python partner_scraper.py --partner feedr                  # yesterday
    python partner_scraper.py --partner feedr --debug          # headed browser
    python partner_scraper.py --partner just_eat --date 2026-08-05
    python partner_scraper.py --partner ordit --from 2026-08-01 --to 2026-08-05
    python partner_scraper.py --partner homecook --no-email
    python partner_scraper.py --partner feedr --dry-run        # scrape, don't write
    python partner_scraper.py --list-partners

Exit codes:
    0  success (including "no sales")
    1  AuthError / ConfigError
    2  ScrapeError
    3  TransformError
    4  EmailError
"""

import argparse
import collections
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
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse
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
PARTNERS_DIR = BASE_DIR / "partners"
MAPPINGS_DIR = BASE_DIR / "mappings"
OUTPUT_DIR = BASE_DIR / "output"
STATE_ROOT = BASE_DIR / "state"
LOGS_DIR = BASE_DIR / "logs"
SCREENSHOTS_DIR = BASE_DIR / "screenshots"

for d in (OUTPUT_DIR, STATE_ROOT, LOGS_DIR, SCREENSHOTS_DIR, MAPPINGS_DIR):
    d.mkdir(parents=True, exist_ok=True)


def available_partners() -> list:
    if not PARTNERS_DIR.exists():
        return []
    return sorted(p.stem for p in PARTNERS_DIR.glob("*.yaml"))


def load_partner_config(partner: str) -> dict:
    path = PARTNERS_DIR / f"{partner}.yaml"
    if not path.exists():
        raise ConfigError(
            f"No config for partner {partner!r} at {path}.\n"
            f"  Available: {', '.join(available_partners()) or '(none)'}"
        )
    with open(path) as f:
        return yaml.safe_load(f)


# ──────────────────────────────────────────────────────────────────────────────
# Custom Exceptions
# ──────────────────────────────────────────────────────────────────────────────

class AuthError(Exception):
    """Wrong credentials, or a 2FA challenge — do NOT retry."""


class ConfigError(Exception):
    """A required selector, env var, or mapping is missing — do NOT retry."""


class ScrapeError(Exception):
    """Order list / order detail scraping failure — retryable."""


class TransformError(Exception):
    """Data assembly failure — scraped rows are preserved to disk."""


class EmailError(Exception):
    """Email delivery failure — report was generated but not sent."""


# ──────────────────────────────────────────────────────────────────────────────
# Run ID + Structured Logger
# ──────────────────────────────────────────────────────────────────────────────

RUN_ID = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
_log_path: Optional[pathlib.Path] = None
_verbose = False
_partner_name = "partner"


def _init_logger(partner: str, verbose: bool) -> None:
    global _log_path, _verbose, _partner_name
    _verbose, _partner_name = verbose, partner
    _log_path = LOGS_DIR / f"{partner}_{RUN_ID}.jsonl"


def log(stage: str, step: str, outcome: str, duration_ms: int = 0, extra: dict = None) -> None:
    record = {
        "ts": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        "run_id": RUN_ID,
        "pipeline": _partner_name,
        "stage": stage,
        "step": step,
        "outcome": outcome,
        "duration_ms": duration_ms,
    }
    if extra:
        # Scrub in case a caller passes raw exception text containing a secret.
        record.update({k: (scrub(v) if isinstance(v, str) else v)
                       for k, v in extra.items()})
    if _log_path:
        with open(_log_path, "a") as f:
            f.write(json.dumps(record) + "\n")
    if _verbose:
        print(f"  [log] {stage}/{step} → {outcome}"
              + (f" ({duration_ms} ms)" if duration_ms else ""))


def screenshot(page: Page, stage: str, label: str) -> None:
    """Best-effort — a screenshot failure must never break the run."""
    try:
        path = SCREENSHOTS_DIR / f"{_partner_name}_{RUN_ID}_{stage}_{label}.png"
        page.screenshot(path=str(path), full_page=True)
        if _verbose:
            print(f"  [📸] {path.name}")
    except Exception:
        pass


def retry(max_attempts: int = 3, base_delay: float = 1.5, exceptions=(ScrapeError,)):
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
                    print(f"  [!] {fn.__name__} attempt {attempt}/{max_attempts} failed: "
                          f"{scrub(exc)}. Retrying in {delay:.1f}s...", file=sys.stderr)
                    time.sleep(delay)
            raise last_exc
        return wrapper
    return decorator


# ──────────────────────────────────────────────────────────────────────────────
# Config validation
# ──────────────────────────────────────────────────────────────────────────────

REQUIRED_AUTH_SELECTORS = ("username_field", "password_field", "login_button")
REQUIRED_LIST_SELECTORS = ("order_rows", "order_link")
REQUIRED_DETAIL_SELECTORS = ("line_item_rows", "item_name", "item_qty")


def require_selectors(cfg: dict, keys: tuple, partner: str) -> None:
    """Name only the selectors missing for the work being attempted."""
    sel = cfg.get("selectors", {}) or {}
    missing = [k for k in keys if not sel.get(k)]
    if missing:
        raise ConfigError(
            f"partners/{partner}.yaml has unconfigured selectors:\n"
            + "".join(f"    selectors.{k}\n" for k in missing)
            + f"\n  Discover them with:  python debug_selectors.py --partner {partner}\n"
              f"  Or step through the flow headed:  "
              f"python partner_scraper.py --partner {partner} --debug"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Pure helpers — parsing (unit-tested; no browser involved)
# ──────────────────────────────────────────────────────────────────────────────

_MONEY_RE = re.compile(r"-?\d[\d,]*(?:\.\d+)?")


def parse_money(value) -> float:
    """
    '£12.50' / '£1,250.00' / '£13.29 / unit Excl. VAT' → the first number.

    Takes the FIRST numeric run rather than stripping all non-digits: &Dine
    renders '£13.29 / unit Excl. VAT', and the full-strip approach picked up the
    dot from 'Excl.' too. Unparseable → 0.0.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    match = _MONEY_RE.search(str(value))
    if not match:
        return 0.0
    try:
        return round(float(match.group(0).replace(",", "")), 2)
    except ValueError:
        return 0.0


def parse_qty(value) -> int:
    """'3' / 'x3' / '3 pcs' / '3.0' → 3. Unparseable → 0."""
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    digits = "".join(ch for ch in str(value) if ch.isdigit() or ch == ".")
    if not digits:
        return 0
    try:
        return int(float(digits))
    except ValueError:
        return 0


# Day headers that name a day relatively: "Today (Wed 02 Sep)",
# "Yesterday (Wed 02 Sep)", or occasionally just "Today".
_RELATIVE_DAY_RE = re.compile(r"^\s*(today|yesterday)\b\s*(?:\((?P<inner>[^)]+)\))?",
                              re.I)


def _resolve_relative_day(text: str,
                          reference: Optional[datetime] = None) -> Optional[str]:
    """Turn a relative day header into something parseable, or None.

    Feedr labels the most recent days "Today (Wed 02 Sep)" / "Yesterday
    (Wed 02 Sep)" instead of "Wednesday 02 Sep". Those did not match the
    configured "%A %d %b" and fell through to pandas, which also failed — so
    every order under such a header was dropped as out-of-range and the run
    reported "no sales" while exiting 0. Found 2026-09-03 after a wrong
    "no sales for 02-Sep-2026" report was emailed.

    Prefers the date inside the brackets, which is absolute and therefore
    correct regardless of when the run happens; falls back to arithmetic from
    the reference date when the header carries no bracketed date.
    """
    match = _RELATIVE_DAY_RE.match(text or "")
    if not match:
        return None
    inner = (match.group("inner") or "").strip()
    if inner:
        return inner                      # e.g. "Wed 02 Sep"
    ref = reference or datetime.now()
    offset = 0 if match.group(1).lower() == "today" else 1
    # NOT an ISO string: parse_date runs pandas with dayfirst=True, which
    # reads "2026-09-03" as 3 March. "%d %b %Y" cannot be misread.
    return (ref - timedelta(days=offset)).strftime("%d %b %Y")


def parse_date(value, fmt: Optional[str] = None,
               reference: Optional[datetime] = None) -> Optional[datetime]:
    """
    Parse an order date. Tries the configured format first, then pandas.

    Handles year-less dates: Feedr's day headers read "Friday 31 Jul" with no
    year, which pandas resolves to year 0001. Left uncorrected that pushes every
    order outside the requested range and the run reports "no sales" instead of
    failing — so the year is inferred here.

    Also handles relative day headers ("Yesterday (Wed 02 Sep)") — see
    _resolve_relative_day.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    relative = _resolve_relative_day(text, reference)
    if relative is not None:
        text = relative
        fmt = None                        # the configured format no longer applies
    # Do NOT return straight from strptime: it would skip the year inference
    # below, and a yearless configured format yields 1900 — outside every
    # range, i.e. another silent "no sales".
    dt = None
    if fmt:
        try:
            dt = datetime.strptime(text, fmt)
        except ValueError:
            dt = None
    if dt is None:
        parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
        if pd.isna(parsed):
            return None
        dt = parsed.to_pydatetime()

    # <= 1900, not < 1900: pandas gives year 1 for a yearless date but
    # datetime.strptime gives exactly 1900, so a configured yearless
    # group_format (e.g. "%A %d %b") skipped the inference entirely and dated
    # every order to 1900 — outside any range, i.e. a silent "no sales".
    if dt.year <= 1900:
        ref = reference or datetime.now()
        dt = dt.replace(year=ref.year)
        # A date well into the future almost certainly belongs to the previous
        # year — e.g. reading a "31 Dec" header in early January.
        if (dt - ref).days > 7:
            dt = dt.replace(year=ref.year - 1)
    return dt


def status_matches(status: str, keep: list) -> bool:
    """Case-insensitive substring match. Empty keep list → accept everything."""
    if not keep:
        return True
    s = (status or "").strip().lower()
    return any(k.strip().lower() in s for k in keep)


def in_date_range(when: Optional[datetime], date_from: datetime, date_to: datetime) -> bool:
    """Inclusive on both ends, compared by calendar date."""
    if when is None:
        return False
    return date_from.date() <= when.date() <= date_to.date()


# ──────────────────────────────────────────────────────────────────────────────
# Menu-price lookup (for partners that expose no prices)
# ──────────────────────────────────────────────────────────────────────────────

def load_menu_prices(cfg: dict, partner: str) -> dict:
    """
    Load item_name → price_inc_tax from the configured CSV.
    Keys are lowercased and whitespace-collapsed for tolerant matching.
    """
    pricing = cfg.get("pricing", {}) or {}
    csv_rel = pricing.get("lookup_csv", "")
    if not csv_rel:
        raise ConfigError(
            f"partners/{partner}.yaml sets pricing.source: lookup but no "
            f"pricing.lookup_csv. Point it at a menu-price CSV in mappings/."
        )
    path = BASE_DIR / csv_rel
    if not path.exists():
        raise ConfigError(
            f"Menu-price sheet not found: {path}\n"
            f"  {partner} exposes no prices, so this file is required.\n"
            f"  Expected columns: item_name, price_inc_tax"
        )
    # comment="#" so the template's inline documentation is not read as items.
    df = pd.read_csv(path, comment="#", skip_blank_lines=True)
    df.columns = [str(c).strip().lower() for c in df.columns]
    for col in ("item_name", "price_inc_tax"):
        if col not in df.columns:
            raise ConfigError(
                f"{path} is missing the {col!r} column. "
                f"Found: {list(df.columns)}"
            )
    return {
        _norm_key(r["item_name"]): parse_money(r["price_inc_tax"])
        for _, r in df.iterrows()
        if str(r["item_name"]).strip()
    }


def _norm_key(name) -> str:
    return " ".join(str(name).strip().lower().split())


def resolve_prices(rows: list, cfg: dict, partner: str) -> list:
    """
    Fill each row's unit_price_inc_tax.

    pricing.source == "scraped" → already on the row.
    pricing.source == "lookup"  → resolve from the menu-price sheet, and FAIL
                                  on any unmatched item. A silently-missing
                                  price would flow into a customer's inventory
                                  as a zero, which is worse than a failed run.
    """
    pricing = cfg.get("pricing", {}) or {}
    source = pricing.get("source", "scraped")

    if source == "scraped":
        return rows

    if source != "lookup":
        raise ConfigError(
            f"partners/{partner}.yaml pricing.source must be "
            f"'scraped' or 'lookup', got {source!r}"
        )

    prices = load_menu_prices(cfg, partner)
    unmatched = set()

    for row in rows:
        key = _norm_key(row["item_name"])
        if key in prices:
            row["unit_price_inc_tax"] = prices[key]
        else:
            unmatched.add(row["item_name"])

    if unmatched:
        raise TransformError(
            f"{len(unmatched)} item(s) from {partner} have no price in the menu "
            f"sheet ({pricing.get('lookup_csv')}):\n"
            + "".join(f"    {n!r}\n" for n in sorted(unmatched))
            + "\n  Add them to the sheet and re-run with --from-scraped. "
              "Refusing to upload items priced at zero."
        )

    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Assemble scraped rows → Supy upload format
# ──────────────────────────────────────────────────────────────────────────────

SUPY_COLUMNS = [
    "Sales Date *",
    "POS Item ID *",
    "POS Item Name",
    "Sold QTY *",
    "Total Discount Value",
    "Total sales excl. tax *",
    "Total sales incl. tax *",
    "Order ID",
    "Sales Type Code",
]


def build_supy_frame(rows: list, cfg: dict, out_date_format: str = "%d-%b-%Y") -> pd.DataFrame:
    """
    Convert scraped line items into the Supy upload frame.

    Each scraped row is one order line:
        {order_id, order_date, item_name, qty, unit_price_inc_tax}

    One output row per order line — Order ID is preserved rather than
    aggregated away, matching how these are entered by hand today.
    """
    pricing = cfg.get("pricing", {}) or {}
    divisor = float(pricing.get("tax_divisor", 1.2))
    unit_is_per_item = pricing.get("unit_price_is_per_item", True)
    # Portals differ on which side of VAT they display:
    #   Feedr / Just Eat  → inclusive  → excl = incl / 1.2
    #   &Dine             → EXCLUSIVE  → incl = excl * 1.2
    # Getting this backwards misstates sales by 20%, so it is explicit per
    # partner rather than assumed.
    price_includes_tax = pricing.get("price_includes_tax", True)

    if divisor == 0:
        raise TransformError("pricing.tax_divisor must not be zero.")

    records = []
    for row in rows:
        qty = parse_qty(row.get("qty"))
        unit = parse_money(row.get("unit_price_inc_tax"))
        line_total = round(unit * qty, 2) if unit_is_per_item else unit
        # A row may override the partner default (order-level fallback lines
        # carry a VAT-inclusive total even when item prices are exclusive).
        row_incl = row.get("price_includes_tax", price_includes_tax)
        if row_incl:
            incl = line_total
            excl = round(incl / divisor, 2)
        else:
            excl = line_total
            incl = round(excl * divisor, 2)
        when = row.get("order_date")

        records.append({
            "Sales Date *": when.strftime(out_date_format) if when else "",
            # These partners expose no item code, so the name doubles as the ID.
            "POS Item ID *": row["item_name"],
            "POS Item Name": row["item_name"],
            "Sold QTY *": qty,
            "Total Discount Value": "",
            "Total sales excl. tax *": excl,
            "Total sales incl. tax *": incl,
            "Order ID": row.get("order_id", ""),
            "Sales Type Code": "",
        })

    return pd.DataFrame(records, columns=SUPY_COLUMNS)


def group_by_destination(rows: list, cfg: dict) -> dict:
    """
    Split scraped rows by the Supy entity they upload to.

    Most partners map to one entity. Just Eat is the exception: business
    153849 → Street Food Ltd., business 282355 → Thai Street Boxpark.
    """
    default = cfg.get("destination", "")
    out = {}
    for row in rows:
        dest = row.get("destination") or default
        if not dest:
            raise TransformError(
                "A scraped row has no destination entity and the partner config "
                "sets no top-level `destination:`."
            )
        out.setdefault(dest, []).append(row)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Stage 1 — Authentication
# ──────────────────────────────────────────────────────────────────────────────

def _storage_path(partner: str) -> pathlib.Path:
    d = STATE_ROOT / partner
    d.mkdir(parents=True, exist_ok=True)
    return d / "storage_state.json"


def _scraped_path(partner: str) -> pathlib.Path:
    d = STATE_ROOT / partner
    d.mkdir(parents=True, exist_ok=True)
    return d / "scraped_rows.json"


def _cdp_endpoint(cfg: dict) -> str:
    """Chrome DevTools endpoint to attach to, or "" to launch our own browser.

    Set browser.cdp_endpoint in the partner config, or override per-run with
    PARTNER_CDP_ENDPOINT. Used for portals whose bot protection blocks any
    Playwright-launched browser — see the attach branch in run_partner.
    """
    return (os.environ.get("PARTNER_CDP_ENDPOINT", "").strip()
            or str((cfg.get("browser") or {}).get("cdp_endpoint", "") or "").strip())


def _profile_path(partner: str) -> pathlib.Path:
    """
    Persistent browser-profile directory for a partner.

    `debug_selectors.py --partner <name> --profile` MUST seed this exact path,
    or clearing a Cloudflare challenge by hand does nothing for the real
    pipeline. Kept as one function so the two files cannot drift apart.
    """
    return STATE_ROOT / partner / "browser_profile"


CHALLENGE_MARKERS = (
    "performing security verification",
    "just a moment",
    "checking your browser",
    "verify you are human",
)


def wait_out_bot_challenge(page: Page, budget_seconds: int = 30) -> bool:
    """
    Some portals (JustEat Business) serve a Cloudflare interstitial before the
    real page. In a real browser it clears itself after a few seconds. Poll so
    we continue the moment it does, instead of sleeping blindly.

    Returns True if the page is usable, False if the challenge never cleared.
    """
    def challenged() -> str:
        try:
            body = (page.text_content("body") or "").lower()
        except Exception:
            return ""
        return next((m for m in CHALLENGE_MARKERS if m in body), "")

    marker = challenged()
    if not marker:
        return True

    if _verbose:
        print(f"  [🛡] Bot challenge ({marker!r}) — waiting up to {budget_seconds}s...")

    for _ in range(max(1, budget_seconds // 5)):
        page.wait_for_timeout(5_000)
        if not challenged():
            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass
            if _verbose:
                print("  [✓] Challenge cleared")
            return True
    return False


def dismiss_cookie_banner(page: Page, configured: str = "") -> str:
    """
    Click the cookie-accept control if one is present.

    Consent overlays intercept clicks on the login button — Deliveroo
    (OneTrust), Feedr, and Ordit all show one. Best-effort: a missing banner is
    normal, not an error.
    """
    candidates = ([configured] if configured else []) + [
        "#onetrust-accept-btn-handler",              # OneTrust — Deliveroo
        "button[aria-label='Accept all cookies']",   # Feedr
        ".cc-nb-okagree",                            # cookieconsent — Ordit
        "button:has-text('Accept all')",
        "button:has-text('I agree')",
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


# Every credential value read this run. Used to scrub error text before it is
# logged or printed. Playwright embeds the argument of a failed fill() in its
# error message — e.g. `fill("<the actual password>")` — so without this an
# ordinary timeout writes the password into logs/*.jsonl and onto the terminal.
_SECRETS: set = set()

# Orders that could not be scraped this run. A single unreadable order must
# not discard every other order's sales — they are collected and reported
# instead, so the rest still upload and the gaps are explicit.
_FAILED_ORDERS: list = []


def scrub(text, *extra: str) -> str:
    """Redact every known credential value from text."""
    out = str(text)
    for secret in list(_SECRETS) + [s for s in extra if s]:
        if secret and len(secret) >= 3:
            out = out.replace(secret, "***REDACTED***")
    return out


def _wait_for_button_enabled(page: Page, selector: str, timeout_ms: int = 15_000) -> None:
    """
    Wait until a submit button is actually clickable.

    Playwright's click() happily "succeeds" on a button the app has disabled via
    a CSS class, so check both the DOM disabled state and a `disabled` class.
    """
    deadline = time.monotonic() + (timeout_ms / 1000)
    while time.monotonic() < deadline:
        try:
            el = page.query_selector(selector)
            if el and el.is_enabled():
                classes = (el.get_attribute("class") or "").lower()
                if "disabled" not in classes.split():
                    return
        except Exception:
            pass
        page.wait_for_timeout(500)
    # Not fatal on its own — some buttons never expose an enabled state. Let the
    # click proceed; _confirm_left_login_page is the real gate.
    if _verbose:
        print(f"  [!] {selector} still looks disabled after "
              f"{timeout_ms // 1000}s — clicking anyway")


def _confirm_left_login_page(page: Page, url_before: str, partner: str,
                             cfg: dict, settle_ms: int = 8_000) -> None:
    """
    Confirm the login actually took effect.

    Without this, a portal that has no login_error selector and no
    authenticated_element reports success while sitting on the login form —
    which then fails confusingly three stages later.
    """
    deadline = time.monotonic() + (settle_ms / 1000)
    while time.monotonic() < deadline:
        if page.url.rstrip("/") != url_before.rstrip("/"):
            return
        page.wait_for_timeout(500)

    # Still on the same URL. If an authenticated element is configured and
    # present, this is a single-page app that logged in without navigating.
    auth_el = cfg["portal"].get("authenticated_element", "")
    if auth_el:
        try:
            page.wait_for_selector(auth_el, timeout=5_000)
            return
        except Exception:
            pass

    # Surface whatever the page is saying — usually the real reason.
    message = ""
    for sel in ("[class*='error']", "[class*='invalid']", "[role='alert']",
                ".alert", ".toast"):
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                text = " ".join((el.text_content() or "").split())
                if text:
                    message = text[:200]
                    break
        except Exception:
            continue

    screenshot(page, "auth", "04_still_on_login")
    raise AuthError(
        f"{partner}: still on the login page after submitting — the login did "
        f"not take effect.\n"
        f"  URL: {page.url}\n"
        + (f"  Page says: {message!r}\n" if message else "")
        + f"  Most likely: wrong credentials, or the submit needs another step.\n"
        f"  Re-run headed to watch it:  "
        f"python partner_scraper.py --partner {partner} --debug"
    )


def _get_credential(env_key: str, partner: str, label: str) -> str:
    value = os.environ.get(env_key, "")
    if value:
        # Register so any later error text can be scrubbed of it.
        _SECRETS.add(value)
    if not value:
        raise AuthError(
            f"{env_key} is not set — {partner} needs a {label}. "
            f"Add it to your .env file (see .env.example). "
            f"Never paste credentials into chat or a ticket."
        )
    return value


def stage_auth(page: Page, context, cfg: dict, partner: str, force_login: bool) -> None:
    t0 = time.monotonic()
    require_selectors(cfg, REQUIRED_AUTH_SELECTORS, partner)
    sel = cfg["selectors"]
    auth_cfg = cfg.get("auth", {}) or {}
    storage = _storage_path(partner)

    # Reuse a cached session when we can verify it
    auth_el = cfg["portal"].get("authenticated_element", "")
    if not force_login and storage.exists() and auth_el:
        try:
            page.goto(cfg["portal"]["orders_url"], wait_until="domcontentloaded", timeout=20_000)
            page.wait_for_selector(auth_el, timeout=5_000)
            log("auth", "session_cache_hit", "ok",
                duration_ms=int((time.monotonic() - t0) * 1000))
            return
        except Exception:
            if _verbose:
                print("  [→] Cached session expired — re-authenticating...")

    username = _get_credential(
        auth_cfg.get("username_env", ""), partner, "username")

    # Uber Eats authenticates with a PIN rather than a password.
    is_pin = auth_cfg.get("type", "password") == "pin"
    secret = _get_credential(
        auth_cfg.get("password_env", ""), partner, "PIN" if is_pin else "password")

    try:
        page.goto(cfg["portal"]["login_url"], wait_until="domcontentloaded", timeout=30_000)
        # Several portals redirect to an SSO host (Just Eat → Keycloak), so wait
        # for the real form rather than assuming we are already on it.
        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass
        screenshot(page, "auth", "01_login_page")

        # Cloudflare-style interstitial must clear before the form exists.
        budget = int((cfg.get("browser", {}) or {}).get("challenge_wait_seconds", 30))
        if not wait_out_bot_challenge(page, budget):
            screenshot(page, "auth", "01_bot_challenge")
            raise AuthError(
                f"{partner}: a bot challenge did not clear within {budget}s.\n"
                f"  This portal needs browser.persistent_profile: true in its "
                f"config, and the profile must be seeded once headed:\n"
                f"    python debug_selectors.py --partner {partner} --profile --no-prompt"
            )

        # A cookie-consent overlay will swallow the click on the login button,
        # so dismiss it before touching the form.
        dismiss_cookie_banner(page, sel.get("cookie_accept", ""))

        page.wait_for_selector(sel["username_field"], timeout=15_000)
        page.fill(sel["username_field"], username)

        # Some portals ask for the identifier first, then reveal the secret field
        # (JustEat Business: email → redirect to a Keycloak realm → password).
        if sel.get("username_submit_button"):
            page.click(sel["username_submit_button"])
            try:
                page.wait_for_load_state("networkidle", timeout=30_000)
            except Exception:
                pass
            # Step 2 often lives on a different host with its own challenge.
            if not wait_out_bot_challenge(page, budget):
                screenshot(page, "auth", "02_step2_challenge")
                raise AuthError(
                    f"{partner}: step-2 bot challenge did not clear within "
                    f"{budget}s at {page.url}"
                )
            page.wait_for_selector(sel["password_field"], state="visible", timeout=30_000)
            # Keycloak re-asks for the username on the second screen; it is not
            # always prefilled from the id_hint.
            step2_user = sel.get("username_field_step2", "")
            if step2_user:
                el = page.query_selector(step2_user)
                if el and not (el.input_value() or "").strip():
                    page.fill(step2_user, username)

        page.fill(sel["password_field"], secret)

        # "Remember me" extends the session, which means fewer logins and less
        # chance of tripping bot detection.
        remember = sel.get("remember_me", "")
        if remember:
            try:
                box = page.query_selector(remember)
                if box and not box.is_checked():
                    box.check()
            except Exception:
                pass

        screenshot(page, "auth", "02_fields_filled")

        # Many of these forms render the submit button disabled until client-side
        # validation passes (&Dine ships a literal `disabled` class). Clicking too
        # early is a silent no-op: the fields stay filled, the page never
        # navigates, and without a login_error selector it looks like success.
        _wait_for_button_enabled(page, sel["login_button"])

        url_before = page.url
        page.click(sel["login_button"])
        # These are SPAs — a hard navigation may never fire.
        try:
            page.wait_for_load_state("networkidle", timeout=45_000)
        except Exception:
            pass

        # Verify we actually left the login page. This is the backstop for
        # portals with no login_error / authenticated_element configured.
        _confirm_left_login_page(page, url_before, partner, cfg)

        error_sel = sel.get("login_error", "")
        if error_sel:
            try:
                page.wait_for_selector(error_sel, timeout=3_000)
            except Exception:
                pass  # no error element → login succeeded
            else:
                screenshot(page, "auth", "03_login_error")
                raise AuthError(f"{partner}: login failed — error element on page.")

        # A 2FA / OTP challenge cannot be solved headlessly. Say so plainly
        # instead of timing out on a missing selector three stages later.
        challenge_markers = ("otp", "verify", "two-factor", "2fa", "challenge")
        if any(m in page.url.lower() for m in challenge_markers):
            screenshot(page, "auth", "03_challenge")
            raise AuthError(
                f"{partner} presented a verification challenge at {page.url}.\n"
                f"  Run once headed to clear it manually:\n"
                f"    python partner_scraper.py --partner {partner} --debug\n"
                f"  The cached session then carries subsequent headless runs."
            )

        if auth_el:
            page.wait_for_selector(auth_el, timeout=20_000)

        screenshot(page, "auth", "03_logged_in")

    except AuthError:
        raise
    except Exception as exc:
        screenshot(page, "auth", "error")
        # Scrub before the message reaches logs or the terminal — Playwright
        # puts the filled value straight into its error text.
        raise ScrapeError(
            f"{partner}: login navigation failed: "
            f"{scrub(exc)}"
        ) from None

    context.storage_state(path=str(storage))
    log("auth", "login", "ok", duration_ms=int((time.monotonic() - t0) * 1000))


# ──────────────────────────────────────────────────────────────────────────────
# Stage 2 — Scrape order list + order details
# ──────────────────────────────────────────────────────────────────────────────

def _text_of(scope, selector: str) -> str:
    """Text content of the first match within scope, or '' if absent."""
    if not selector:
        return ""
    try:
        el = scope.query_selector(selector)
        return (el.text_content() or "").strip() if el else ""
    except Exception:
        return ""


def _open_order_list(page: Page, cfg: dict) -> None:
    """
    Navigate to the order list and put it into the state the scraper expects.

    Click-mode re-enters this for every order, so any list preparation (widening
    a default date window) MUST be repeated here — otherwise the list reverts to
    its default, the row indices no longer line up, and every order is skipped.
    """
    sel = cfg.get("selectors", {}) or {}
    nav_timeout = int((cfg.get("browser", {}) or {}).get("nav_timeout_seconds", 60)) * 1000

    page.goto(cfg["portal"]["orders_url"], wait_until="domcontentloaded",
              timeout=nav_timeout)
    try:
        page.wait_for_load_state("networkidle", timeout=nav_timeout)
    except Exception:
        pass

    pre_click = sel.get("list_pre_click", "")
    if pre_click:
        try:
            el = page.query_selector(pre_click)
            if el and el.is_visible():
                el.click()
                page.wait_for_timeout(2_000)
                try:
                    page.wait_for_load_state("networkidle", timeout=nav_timeout)
                except Exception:
                    pass
                if _verbose:
                    print(f"  [→] Widened list range via {pre_click!r}")
            elif _verbose:
                print(f"  [!] list_pre_click {pre_click!r} not found/visible")
        except Exception as exc:
            print(f"  [!] list_pre_click failed: {scrub(exc)[:120]}", file=sys.stderr)

    # Rows render asynchronously; without this the list can read as empty.
    try:
        page.wait_for_selector(sel["order_rows"], timeout=nav_timeout)
    except Exception:
        pass


def _group_date_text(row, group_selector: str, title_selector: str) -> str:
    """
    Read the date from the day-group header that applies to this row.

    Feedr renders one "Friday 31 Jul" heading per day and then the orders for
    that day, so the date is not on the row itself.

    Two DOM shapes exist and both must work:

    1. Each day is its own container holding the heading and its rows. Walking
       up with closest() finds it.
    2. Headings and rows are FLAT SIBLINGS with no per-day container at all
       (Feedr, verified 2026-08-27):

           H2  "Friday 31 Jul"
           P   "Last update: 14:27"
           DIV order row          <- belongs to 31 Jul
           DIV order row          <- also 31 Jul
           H2  "Thursday 30 Jul"
           DIV order row          <- 30 Jul

    Shape 2 is why the preceding-sibling scan comes FIRST. With a flat list,
    closest() resolves to the single wrapper around the whole list and
    querySelector returns its FIRST heading — so every order on the page gets
    stamped with the topmost date. That is silent and wrong: it does not fail,
    it produces confident bad dates (on 2026-08-06 every row was dated 31 Jul;
    on 2026-08-27 every row was dated 27 Aug and the range matched nothing).
    """
    try:
        return row.evaluate(
            """(el, args) => {
                const [groupSel, titleSel] = args;
                // Shape 2: nearest PRECEDING sibling heading wins.
                for (let n = el.previousElementSibling; n; n = n.previousElementSibling) {
                    if (n.matches(titleSel)) return n.textContent.trim();
                    const nested = n.querySelector(titleSel);
                    if (nested) return nested.textContent.trim();
                }
                // Shape 1: a real per-day container.
                const group = groupSel ? el.closest(groupSel) : el.parentElement;
                if (!group) return '';
                const t = group.querySelector(titleSel);
                return t ? t.textContent.trim() : '';
            }""",
            [group_selector, title_selector],
        ) or ""
    except Exception:
        return ""


def _switch_business(page: Page, cfg: dict, business: dict) -> None:
    """Switch tenant on partners hosting several businesses behind one login."""
    sel = cfg["selectors"]
    switcher = sel.get("business_switcher", "")
    template = sel.get("business_option_template", "")
    if not switcher or not template:
        raise ConfigError(
            f"Config lists multiple businesses but selectors.business_switcher / "
            f"selectors.business_option_template are unset — the scraper cannot "
            f"switch between them."
        )
    option = template.format(id=business.get("id", ""), name=business.get("name", ""))
    try:
        page.click(switcher)
        page.wait_for_selector(option, state="visible", timeout=15_000)
        page.click(option)
        page.wait_for_load_state("networkidle", timeout=30_000)
    except Exception as exc:
        screenshot(page, "scrape", "business_switch_error")
        raise ScrapeError(
            f"Could not switch to business {business.get('name') or business.get('id')!r}: {exc}"
        ) from exc


def _page_url(orders_url: str, param: str, number: int) -> str:
    """
    orders_url with ?<param>=<number>, replacing any value already there.

    Some portals paginate by URL alone and expose no clickable pager (JustEat
    Business: "?tab=Past&page=1"). Rewriting the param is safer than string
    surgery — it keeps every other query param (tab=Past) intact, and pinning
    page=1 in config no longer caps the walk at the first page.
    """
    parts = urlparse(orders_url)
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if k != param]
    query.append((param, str(number)))
    return urlunparse(parts._replace(query=urlencode(query)))


def _row_fingerprint(rows: list, sel: dict) -> str:
    """
    Identity of a list page, used to detect a portal that ignores a too-high
    page number and silently re-serves the last page — which would otherwise
    duplicate every order on it until max_pages.
    """
    refs = []
    for row in rows[:20]:
        ref = _text_of(row, sel.get("order_reference", "")) or ""
        if not ref:
            ref = " ".join((row.text_content() or "").split())[:60]
        refs.append(ref)
    return "|".join(refs)


def _collect_order_links(page: Page, cfg: dict,
                         date_from: datetime, date_to: datetime) -> list:
    """
    Walk the (paginated) order list and return the hrefs of orders that fall
    inside the date range and pass the status filter.
    """
    sel = cfg["selectors"]
    filters = cfg.get("filters", {}) or {}
    keep_status = filters.get("keep_status", []) or []
    list_date_fmt = cfg.get("dates", {}).get("list_format")
    max_pages = int(cfg.get("pagination", {}).get("max_pages", 20))
    # Feedr's rows have no href — they are click-to-open SPA rows.
    open_mode = (cfg.get("orders", {}) or {}).get("open_mode", "href")

    wanted, seen_pages, skipped = [], 0, {"status": 0, "date": 0, "duplicate": 0}
    # Identities already collected. A list that shifts between page loads (a
    # new order arriving, an unstable sort) shows the same order on the end of
    # one page and the start of the next — measured on &Dine 2026-09-11, where
    # SAT-32L8L came back twice and its 9 lines were written twice, booking
    # £1,230.58 against a £732.35 order. Pagination makes this reachable, so
    # the guard lives here rather than in the caller.
    seen_orders = set()
    # True only when the cap stopped us with a further page still available.
    # Reaching max_pages is NOT itself truncation: a portal with no pager
    # (Feedr — one list bounded by a date filter) legitimately reads one
    # "page" and is complete.
    truncated = False

    pager = cfg.get("pagination", {}) or {}
    url_param = pager.get("url_param", "")
    page_delay = float(pager.get("delay_seconds", 0) or 0)
    stop_when_older = bool(pager.get("stop_when_older", True))
    orders_url = (cfg.get("portal", {}) or {}).get("orders_url", "")
    last_fingerprint = ""

    while seen_pages < max_pages:
        seen_pages += 1
        try:
            page.wait_for_selector(
                sel["order_rows"],
                timeout=int((cfg.get("browser", {}) or {})
                            .get("nav_timeout_seconds", 60)) * 1000)
        except Exception:
            # No rows at all on page 1 is a legitimate "no orders" result.
            if seen_pages == 1:
                log("scrape", "order_list", "ok", extra={"orders_found": 0})
                return []
            break

        rows = page.query_selector_all(sel["order_rows"])

        # A portal asked for a page past the end may silently re-serve the
        # last one. Without this, every order on it would be collected again
        # on each remaining iteration.
        fingerprint = _row_fingerprint(rows, sel)
        if seen_pages > 1 and fingerprint and fingerprint == last_fingerprint:
            break
        last_fingerprint = fingerprint

        oldest_on_page = None
        for idx, row in enumerate(rows):
            status = _text_of(row, sel.get("status", ""))
            if not status_matches(status, keep_status):
                skipped["status"] += 1
                continue

            when = parse_date(_text_of(row, sel.get("order_date", "")), list_date_fmt)

            # Some portals show the date once per day-group header rather than
            # on each row (Feedr: "Friday 31 Jul"). Walk up to the group.
            group_title = sel.get("order_date_group_title", "")
            if when is None and group_title:
                raw = _group_date_text(row, sel.get("order_group", ""), group_title)
                when = parse_date(raw, cfg.get("dates", {}).get("group_format"))

            if when is not None and (oldest_on_page is None or when < oldest_on_page):
                oldest_on_page = when

            have_list_date = bool(sel.get("order_date") or group_title)
            # Only filter by date when the list actually shows one; otherwise
            # defer to the order-detail page.
            if have_list_date and not in_date_range(when, date_from, date_to):
                skipped["date"] += 1
                continue

            order_total_text = _text_of(row, sel.get("order_total", ""))
            reference_text = _text_of(row, sel.get("order_reference", ""))

            if open_mode == "click":
                # No href to follow — record the row's position so the detail
                # scrape can re-find and click it. Identity is the reference;
                # row_index repeats on every page and cannot stand in for it.
                key = reference_text or f"p{seen_pages}:{idx}"
                if key in seen_orders:
                    skipped["duplicate"] += 1
                    continue
                seen_orders.add(key)
                wanted.append({"row_index": idx, "order_date": when,
                               "status": status,
                               "order_total": order_total_text,
                               "reference": reference_text,
                               "row_text": " ".join((row.text_content() or "").split())[:80]})
                continue

            link_el = row.query_selector(sel["order_link"])
            href = link_el.get_attribute("href") if link_el else None
            if href:
                if href in seen_orders:
                    skipped["duplicate"] += 1
                    continue
                seen_orders.add(href)
                wanted.append({"href": href, "order_date": when, "status": status,
                               "order_total": order_total_text,
                               "reference": reference_text})

        # Stop once a whole page predates the window. These lists are
        # newest-first, so every later page is older still — without this a
        # backfill walks to max_pages skipping every row.
        if (stop_when_older and oldest_on_page is not None
                and oldest_on_page < date_from):
            break

        # Next page, if there is one
        next_sel = sel.get("next_page", "")
        nxt = page.query_selector(next_sel) if next_sel else None
        if next_sel:
            # is_visible() matters as much as is_enabled(): &Dine keeps its
            # pager buttons in the DOM and merely hides the one past the last
            # page, so an enabled-only check clicks an invisible element and
            # blocks for the full 30s click timeout.
            has_more = bool(nxt and nxt.is_enabled() and nxt.is_visible())
        elif url_param:
            # URL paging cannot be probed without loading the page; the
            # empty-page and fingerprint guards above end the walk instead.
            has_more = True
        else:
            has_more = False         # no pager at all — the list is complete
        if not has_more:
            break
        if seen_pages >= max_pages:
            truncated = True         # a next page exists but the cap stops us
            break

        try:
            if next_sel:
                nxt.click()
            else:
                page.goto(_page_url(orders_url, url_param, seen_pages + 1),
                          wait_until="domcontentloaded",
                          timeout=int((cfg.get("browser", {}) or {})
                                      .get("nav_timeout_seconds", 60)) * 1000)
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            break

        # Wait for the rows themselves to change, not just for the network to
        # go quiet. &Dine re-renders its table after networkidle: page 5 served
        # page 4's rows for over a second, which the repeat-guard above would
        # read as "past the last page" and end the walk three pages early.
        settle_deadline = time.monotonic() + 15
        while next_sel and time.monotonic() < settle_deadline:
            if _row_fingerprint(page.query_selector_all(sel["order_rows"]),
                                sel) != fingerprint:
                break
            page.wait_for_timeout(500)

        if page_delay:
            # Deliberate throttle: app.business.just-eat.co.uk began serving
            # 403 on 2026-09-08 after repeated rapid hits from one IP, and a
            # backfill loads far more pages than a daily run.
            time.sleep(page_delay)

    if truncated:
        # Never let a pagination cap silently truncate a month of sales.
        print(f"  [!] Stopped at pagination cap ({max_pages} pages) with more "
              f"pages still available. Raise pagination.max_pages — orders "
              f"HAVE been missed.", file=sys.stderr)
        log("scrape", "pagination_cap_hit", "warn", extra={"max_pages": max_pages})

    if skipped["duplicate"]:
        print(f"  [→] Ignored {skipped['duplicate']} order(s) already seen on an "
              f"earlier page.")

    log("scrape", "order_list", "ok",
        extra={"orders_found": len(wanted), "pages": seen_pages,
               "skipped_status": skipped["status"], "skipped_date": skipped["date"],
               "skipped_duplicate": skipped["duplicate"]})
    return wanted


def _scrape_order_detail(page: Page, cfg: dict, order: dict,
                         date_from: datetime, date_to: datetime) -> list:
    """Open one order and return its line items."""
    sel = cfg["selectors"]
    detail_date_fmt = cfg.get("dates", {}).get("detail_format")
    base = cfg["portal"].get("base_url", "")
    open_mode = (cfg.get("orders", {}) or {}).get("open_mode", "href")

    orders_cfg = cfg.get("orders", {}) or {}
    url_template = orders_cfg.get("detail_url_template", "")
    id_pattern = orders_cfg.get("detail_id_pattern", r"(\d+)$")

    if url_template and order.get("reference"):
        # Far cheaper than click-mode: one page load per order instead of
        # reloading (and re-widening) the whole list every time.
        m = re.search(id_pattern, order["reference"].strip())
        if m:
            path = url_template.format(order_num=m.group(1),
                                       reference=order["reference"].strip())
            url = path if path.startswith("http") else base.rstrip("/") + "/" + path.lstrip("/")
            nav_timeout = int((cfg.get("browser", {}) or {})
                              .get("nav_timeout_seconds", 60)) * 1000
            page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout)
            try:
                page.wait_for_load_state("networkidle", timeout=nav_timeout)
            except Exception:
                pass
            open_mode = "direct"

    if open_mode == "click":
        # Rows carry no href, so return to the list and click the row by
        # position. Element handles do not survive the navigation, which is why
        # the index is re-resolved here rather than cached.
        _open_order_list(page, cfg)
        rows = page.query_selector_all(sel["order_rows"])
        idx = order["row_index"]
        if idx >= len(rows):
            raise ScrapeError(
                f"Order row {idx} vanished from the list (now {len(rows)} rows) — "
                f"the list changed between collection and scraping."
            )
        rows[idx].click()
        try:
            page.wait_for_load_state("networkidle", timeout=25_000)
        except Exception:
            pass
        url = page.url
    elif open_mode != "direct":
        href = order["href"]
        url = href if href.startswith("http") else base.rstrip("/") + "/" + href.lstrip("/")
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass

    # Some portals put the line items behind a tab that is not active on load
    # (&Dine opens on "Details"; items live under "Order Summary"). The rows are
    # present in the DOM but hidden, so they must be revealed, not just read.
    tab = sel.get("detail_tab", "")
    if tab:
        try:
            el = page.query_selector(tab)
            if el and el.is_visible():
                el.click()
                page.wait_for_timeout(800)
        except Exception as exc:
            if _verbose:
                print(f"  [!] Could not click detail tab {tab!r}: {exc}")

    order_id = (_text_of(page, sel.get("order_id", ""))
                or url.split("?")[0].rstrip("/").split("/")[-1])

    when = order.get("order_date")
    if when is None:
        when = parse_date(_text_of(page, sel.get("order_date_detail", "")), detail_date_fmt)
        # The list had no date, so the range filter has to happen here.
        if not in_date_range(when, date_from, date_to):
            return []

    fallback_enabled = (cfg.get("orders", {}) or {}).get("fallback_single_line", False)
    try:
        page.wait_for_selector(sel["line_item_rows"], timeout=15_000)
    except Exception:
        screenshot(page, "scrape", f"no_line_items_{order_id}")
        if not fallback_enabled:
            raise ScrapeError(
                f"Order {order_id} has no rows matching selectors.line_item_rows "
                f"({sel['line_item_rows']!r}) at {url}"
            )
        # An order type with no item table at all (&Dine "Set" platters) — fall
        # through so it is booked as a single order-level line below.

    items = []
    for el in page.query_selector_all(sel["line_item_rows"]):
        name = _text_of(el, sel["item_name"])
        if not name:
            continue
        items.append({
            "order_id": order_id,
            "order_date": when,
            "item_name": " ".join(name.split()),
            "qty": parse_qty(_text_of(el, sel["item_qty"])),
            "unit_price_inc_tax": parse_money(_text_of(el, sel.get("item_unit_price", ""))),
        })

    if not items:
        # Some order types carry no per-item pricing at all (&Dine "Set"
        # platters show portions but no prices). Rather than drop the sale,
        # book the whole order as one line at its order total.
        fallback = (cfg.get("orders", {}) or {}).get("fallback_single_line", False)
        total = parse_money(order.get("order_total", ""))
        if fallback and total > 0:
            ref = (order.get("reference") or "").strip() or order_id
            template = (cfg.get("orders", {}) or {}).get(
                "fallback_name_template", "{partner} order {reference}")
            name = template.format(
                partner=cfg["portal"].get("name", ""), reference=ref,
                order_id=order_id)
            log("scrape", "order_level_fallback", "ok",
                extra={"order": str(order_id), "total": total})
            return [{
                "order_id": order_id,
                "order_date": when,
                "item_name": name,
                "qty": 1,
                "unit_price_inc_tax": total,
                # The list's Order Total is VAT-INCLUSIVE even where the
                # partner's item prices are exclusive, so override per row.
                "price_includes_tax": True,
            }]
        raise ScrapeError(f"Order {order_id} yielded zero line items at {url}")

    return items


@retry(max_attempts=3, exceptions=(ScrapeError,))
def stage_scrape(page: Page, cfg: dict, partner: str,
                 date_from: datetime, date_to: datetime) -> list:
    t0 = time.monotonic()
    sel = cfg.get("selectors", {}) or {}
    open_mode = (cfg.get("orders", {}) or {}).get("open_mode", "href")
    list_required = (("order_rows",) if open_mode == "click"
                     else REQUIRED_LIST_SELECTORS)
    require_selectors(cfg, list_required + REQUIRED_DETAIL_SELECTORS, partner)

    businesses = cfg.get("businesses") or [None]
    all_rows = []

    for business in businesses:
        if business is not None and len(businesses) > 1:
            if _verbose:
                print(f"  [→] Switching to business "
                      f"{business.get('name') or business.get('id')}...")
            _switch_business(page, cfg, business)

        _open_order_list(page, cfg)
        screenshot(page, "scrape", "01_order_list")

        orders = _collect_order_links(page, cfg, date_from, date_to)
        if _verbose:
            print(f"  [→] {len(orders)} order(s) in range")

        # Pause between order-detail loads. A daily run opens a handful; a
        # backfill opens hundreds, and app.business.just-eat.co.uk started
        # serving 403 on 2026-09-08 after exactly that kind of burst.
        order_delay = float((cfg.get("browser", {}) or {})
                            .get("request_delay_seconds", 0) or 0)

        for i, order in enumerate(orders, 1):
            if _verbose:
                print(f"  [→] Order {i}/{len(orders)}...")
            if order_delay and i > 1:
                time.sleep(order_delay)
            try:
                items = _scrape_order_detail(page, cfg, order, date_from, date_to)
            except ScrapeError as exc:
                # Record and continue: losing one order is bad, losing the whole
                # day's sales because of one order is worse.
                ref = (order.get("href") or f"row {order.get('row_index')}")
                _FAILED_ORDERS.append({"order": str(ref), "error": scrub(exc)[:300]})
                print(f"  [!] SKIPPED order {ref} — {scrub(exc)[:160]}", file=sys.stderr)
                log("scrape", "order_skipped", "error",
                    extra={"order": str(ref), "error": scrub(exc)[:300]})
                continue
            # Tag every line with its destination entity so multi-tenant
            # partners split correctly downstream.
            if business is not None:
                dest = business.get("destination", "")
                for item in items:
                    item["destination"] = dest
            all_rows.extend(items)

    log("scrape", "complete", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"line_items": len(all_rows)})

    # Persist raw scrape so --from-scraped can replay the transform for free.
    #
    # NEVER overwrite a good scrape with an empty one: a run that fails partway
    # (timeout, killed, every order skipped) would otherwise destroy the last
    # usable dataset and take --from-scraped down with it.
    path = _scraped_path(partner)
    if not all_rows and path.exists():
        print(f"  [!] Scrape produced 0 rows — keeping the previous "
              f"{path.name} rather than overwriting it.", file=sys.stderr)
        log("scrape", "preserved_previous_scrape", "warn",
            extra={"path": str(path)})
    else:
        try:
            serialisable = [
                {**r, "order_date": r["order_date"].isoformat() if r.get("order_date") else None}
                for r in all_rows
            ]
            path.write_text(json.dumps(serialisable, indent=2))
        except OSError:
            pass

    return all_rows


def load_scraped_rows(partner: str) -> list:
    """Reload the last scrape (for --from-scraped)."""
    path = _scraped_path(partner)
    if not path.exists():
        raise TransformError(
            f"No saved scrape for {partner} at {path}. "
            f"Run without --from-scraped first."
        )
    rows = json.loads(path.read_text())
    for r in rows:
        r["order_date"] = datetime.fromisoformat(r["order_date"]) if r.get("order_date") else None
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Stage 3 — Transform + write one file per destination entity
# ──────────────────────────────────────────────────────────────────────────────

def stage_transform(rows: list, cfg: dict, partner: str,
                    date_from: datetime, dry_run: bool = False) -> list:
    """Returns a list of {destination, path, rows} — one entry per entity."""
    t0 = time.monotonic()
    out_fmt = cfg.get("output_date_format", "%d-%b-%Y")

    if not rows:
        log("transform", "empty", "ok", extra={"reason": "no_line_items"})
        return []

    try:
        rows = resolve_prices(rows, cfg, partner)
        grouped = group_by_destination(rows, cfg)

        results = []
        for dest, dest_rows in sorted(grouped.items()):
            df = build_supy_frame(dest_rows, cfg, out_fmt)
            if df.empty:
                continue

            path = None
            if not dry_run:
                slug = "".join(ch if ch.isalnum() else "_" for ch in dest).strip("_")
                stamp = date_from.strftime("%Y-%m-%d")
                path = OUTPUT_DIR / f"{partner}_{slug}_{stamp}_{RUN_ID[:8]}.xlsx"
                df.to_excel(str(path), index=False, engine="openpyxl")

            results.append({"destination": dest, "path": path, "rows": len(df),
                            "qty": int(df["Sold QTY *"].sum()),
                            "total_incl": round(float(df["Total sales incl. tax *"].sum()), 2)})

    except (TransformError, ConfigError):
        raise
    except Exception as exc:
        raise TransformError(
            f"Transform failed: {exc}\n{traceback.format_exc()}") from exc

    log("transform", "export", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"files": len(results),
               "rows": sum(r["rows"] for r in results)})
    return results


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


def stage_email(results: list, partner: str, date_range: str,
                email_to: Optional[list] = None) -> None:
    t0 = time.monotonic()
    gmail_user = os.environ.get("GMAIL_USER", "")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD", "")
    recipients = resolve_recipients(email_to) or [gmail_user]
    recipient = ", ".join(recipients)

    if not gmail_user or not gmail_password:
        raise EmailError(
            "GMAIL_USER and GMAIL_APP_PASSWORD must be set in your .env / GitHub Secrets.")

    label = partner.replace("_", " ").title()

    if not results:
        subject = f"{label} Sales — {date_range} — NO SALES"
        body = (f"Hi,\n\nNo {label} sales found for {date_range}, so there is "
                f"nothing to upload.\n\n  • Run ID: {RUN_ID}\n\n"
                f"Regards,\n{label} Automation")
    else:
        subject = f"{label} Sales Report — {date_range}"
        if _FAILED_ORDERS:
            subject += f" — {len(_FAILED_ORDERS)} ORDER(S) MISSING"
        lines = [f"  • {r['destination']}: {r['rows']} rows, "
                 f"{r['qty']} items, £{r['total_incl']:,.2f} incl. tax"
                 for r in results]
        body = (f"Hi,\n\nPlease find attached the {label} sales report(s) for "
                f"{date_range}, formatted for Supy upload.\n\n"
                + "\n".join(lines)
                + f"\n\n  • Run ID: {RUN_ID}\n")
        if _FAILED_ORDERS:
            body += (f"\n!! {len(_FAILED_ORDERS)} order(s) could NOT be read and are "
                     f"NOT in the attached file.\n"
                     f"   Upload these by hand:\n"
                     + "".join(f"     - {f['order']}: {f['error'][:110]}\n"
                               for f in _FAILED_ORDERS))
        body += f"\nRegards,\n{label} Automation"

    msg = MIMEMultipart()
    msg["From"], msg["To"], msg["Subject"] = gmail_user, recipient, subject
    msg.attach(MIMEText(body, "plain"))

    for r in results:
        if r["path"] is None:
            continue
        with open(r["path"], "rb") as f:
            att = MIMEApplication(
                f.read(),
                _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            att.add_header("Content-Disposition", "attachment", filename=r["path"].name)
            msg.attach(att)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, recipients, msg.as_string())
    except Exception as exc:
        raise EmailError(f"Failed to send email: {exc}") from exc

    log("email", "send", "ok", duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"to": recipient,
               "attachments": len([r for r in results if r["path"]])})
    print(f"[Stage 4] ✓ Email sent → {recipient}\n")


# ──────────────────────────────────────────────────────────────────────────────
# Date resolution
# ──────────────────────────────────────────────────────────────────────────────

def resolve_dates(args, cfg: dict) -> tuple:
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
        a, b = parse(args.date_from, "--from"), parse(args.date_to, "--to")
        if a > b:
            raise SystemExit("[✗] --from must not be after --to.")
        return a, b
    lookback = int(cfg.get("dates", {}).get("default_lookback_days", 1))
    d = datetime.now() - timedelta(days=lookback)
    return d, d


def format_range(date_from: datetime, date_to: datetime, fmt: str) -> str:
    if date_from.date() == date_to.date():
        return date_from.strftime(fmt)
    return f"{date_from.strftime(fmt)} → {date_to.strftime(fmt)}"


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def fetch_rows_via_capture(cfg: dict, partner: str, date_from, date_to) -> list:
    """
    Read the JSON the SPA itself receives, rather than replaying its query.

    Added 2026-09-08 for HomeCook, which is backed by Supabase. Replaying with
    credentials lifted from its requests returns 0 rows — row-level security
    evaluates the session identity, and a replayed header is not it. Reading
    the response the app already got sidesteps that entirely.

    Prices are NOT in po_lines; only the PO's total_amount is. For these
    single-item POs the unit price is total_amount / quantity, verified
    against a pair that differ only in size: Tofu Thai Green Curry is 521.00
    at x100 and 1042.00 at x200 — exactly double, so there is no fixed
    delivery component folded into the total.
    """
    api = cfg.get("api") or {}
    for key in ("seed_url", "match"):
        if not api.get(key):
            raise ConfigError(
                f"partners/{partner}.yaml uses api.mode: capture_response "
                f"but sets no api.{key}")

    statuses = {s.upper() for s in (api.get("statuses") or [])}
    date_field = api.get("date_field", "delivery_date")
    lines_key = api.get("lines_key", "po_lines")
    total_key = api.get("total_key", "total_amount")
    qty_key = api.get("qty_key", "quantity")
    name_key = api.get("name_key", "sku_name")

    best: list = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx_kwargs = {"locale": "en-GB", "timezone_id": "Europe/London"}
        storage = _storage_path(partner)
        if storage.exists():
            ctx_kwargs["storage_state"] = str(storage)
        context = browser.new_context(**ctx_kwargs)
        page = context.new_page()

        def _capture(resp):
            if api["match"] not in resp.url:
                return
            if total_key not in resp.url:      # the widest projection wins
                return
            try:
                data = resp.json()
            except Exception:
                return
            if isinstance(data, list) and len(data) > len(best):
                best.clear()
                best.extend(data)

        page.on("response", _capture)
        try:
            page.goto(api["seed_url"], timeout=60000, wait_until="domcontentloaded")
            page.wait_for_timeout(8000)

            sel = cfg.get("selectors", {}) or {}
            if page.locator(sel.get("password_field", "input[type=password]")).count():
                auth_cfg = cfg.get("auth", {}) or {}
                user = _get_credential(
                    auth_cfg.get("username_env", ""), partner, "username")
                pw = _get_credential(
                    auth_cfg.get("password_env", ""), partner, "password")
                page.fill(sel.get("username_field", "input[type=email]"), user)
                page.fill(sel.get("password_field", "input[type=password]"), pw)
                page.click(sel.get("login_button", "button[type=submit]"))
                page.wait_for_timeout(12000)
                best.clear()
                page.goto(api["seed_url"], timeout=60000, wait_until="networkidle")
            page.wait_for_timeout(int(api.get("token_wait_ms", 15000)))

            if not best:
                raise ScrapeError(
                    f"{partner}: no {api['match']} payload seen. The session "
                    f"may have lapsed — re-run with --force-login.")
            log("nav", "api_captured", "ok", extra={"records": len(best)})
            print(f"  [api] {len(best)} record(s) captured")
        finally:
            context.close()
            browser.close()

    rows, skipped = [], collections.Counter()
    for po in best:
        status = str(po.get("status") or "").upper()
        if statuses and status not in statuses:
            skipped[f"status={status}"] += 1
            continue
        raw_date = po.get(date_field)
        if not raw_date:
            skipped["no-date"] += 1
            continue
        when = datetime.fromisoformat(str(raw_date)[:10])
        if not (date_from.date() <= when.date() <= date_to.date()):
            skipped["out-of-range"] += 1
            continue

        lines = po.get(lines_key) or []
        total = po.get(total_key)
        total_qty = sum((ln.get(qty_key) or 0) for ln in lines)
        if not lines or not total_qty or total is None:
            skipped["no-lines-or-total"] += 1
            continue
        # Only the PO total is priced. Splitting it across DIFFERENT products
        # would invent per-item prices, so refuse rather than guess.
        names = {ln.get(name_key) for ln in lines}
        if len(names) > 1:
            log("transform", "multi_item_po", "warn",
                extra={"po": po.get("po_number"), "items": sorted(map(str, names))})
            print(f"  [!] {po.get('po_number')} has {len(names)} different items "
                  f"but only one total — cannot price it; skipped.",
                  file=sys.stderr)
            skipped["multi-item"] += 1
            continue

        unit = float(total) / total_qty
        for ln in lines:
            rows.append({
                "order_id": po.get("po_number"),
                "order_date": when,
                "item_name": str(ln.get(name_key) or "").strip(),
                "qty": ln.get(qty_key) or 0,
                "unit_price_inc_tax": round(unit, 4),
            })

    if skipped:
        print(f"  [api] skipped: {dict(skipped)}")

    path = _scraped_path(partner)
    if not rows and path.exists():
        print(f"  [!] API returned 0 usable rows — keeping the previous "
              f"{path.name}.", file=sys.stderr)
        log("scrape", "preserved_previous_scrape", "warn", extra={"path": str(path)})
    else:
        try:
            path.write_text(json.dumps(
                [{**r, "order_date": r["order_date"].isoformat()
                  if r.get("order_date") else None} for r in rows], indent=2))
        except OSError:
            pass
    return rows


def fetch_rows_via_api(cfg: dict, partner: str, date_from, date_to) -> list:
    """
    Read line items from the portal's own JSON API instead of its DOM.

    A browser is still needed — but only to capture the Bearer token the SPA
    sends; nothing is read off the page, so there are no selectors to rot.

    Added 2026-09-08 for Ordit, whose order detail is unreachable any other
    way: the detail route renders nothing in a headless context and the CSV
    export is order-level only. The API returns items in one call per order.
    """
    api = cfg.get("api") or {}
    for key in ("seed_url", "list_url", "detail_url", "host"):
        if not api.get(key):
            raise ConfigError(
                f"partners/{partner}.yaml sets api.enabled but no api.{key}")

    items_key = api.get("items_key", "meals")
    # priceWithMealOptions, NOT price: it folds in PAID modifiers. Verified
    # 2026-09-08 that sum(priceWithMealOptions * qty) reconciles to the order's
    # priceSumItems to the penny on three orders (44.40 / 18.45 / 272.40),
    # while sum(price * qty) under-reports by exactly the paid modifiers.
    price_key = api.get("price_key", "priceWithMealOptions")

    captured: dict = {}
    rows: list = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx_kwargs = {"locale": "en-GB", "timezone_id": "Europe/London"}
        storage = _storage_path(partner)
        if storage.exists():
            ctx_kwargs["storage_state"] = str(storage)
        context = browser.new_context(**ctx_kwargs)
        page = context.new_page()

        def _grab(req):
            if api["host"] in req.url and not captured:
                auth = req.headers.get("authorization")
                if auth:
                    captured["authorization"] = auth

        page.on("request", _grab)
        try:
            page.goto(api["seed_url"], timeout=60000,
                      wait_until="domcontentloaded")
            page.wait_for_timeout(int(api.get("token_wait_ms", 12000)))
            if not captured:
                raise AuthError(
                    f"{partner}: no Authorization header seen on "
                    f"{api['host']}. The session under state/{partner}/ has "
                    f"probably expired — re-run with --force-login.")
            log("auth", "api_token_captured", "ok", extra={"scheme": "Bearer"})

            list_url = api["list_url"].format(
                after=quote(date_from.strftime("%Y-%m-%dT00:00:00+01:00"), safe=""),
                before=quote(date_to.strftime("%Y-%m-%dT23:59:59+01:00"), safe=""))
            resp = context.request.get(list_url, headers=captured)
            if resp.status != 200:
                raise ScrapeError(
                    f"{partner}: order list returned {resp.status} from "
                    f"{api['host']}")
            payload = resp.json()
            orders = payload.get("hydra:member", payload.get("member", []))
            log("nav", "api_list", "ok",
                extra={"orders": len(orders),
                       "total": payload.get("hydra:totalItems")})
            print(f"  [api] {len(orders)} order(s) in range")

            for o in orders:
                oid = o.get("id")
                detail = context.request.get(
                    api["detail_url"].format(id=oid), headers=captured)
                if detail.status != 200:
                    log("nav", "api_detail", "warn",
                        extra={"order": oid, "status": detail.status})
                    continue
                d = detail.json()
                ident = d.get("identifier") or str(oid)
                when = str(d.get("requiredDeliveryTime") or "")[:10]
                for it in d.get(items_key, []) or []:
                    meal = it.get("meal") or {}
                    name = meal.get("name") or it.get("name")
                    qty = it.get("quantity") or 0
                    unit = it.get(price_key)
                    if unit is None:
                        unit = it.get("price")
                    if not name or not qty:
                        continue
                    # `children` are modifiers. Free ones cost nothing and paid
                    # ones are ALREADY inside priceWithMealOptions, so emitting
                    # them as their own rows would double-count the order.
                    rows.append({
                        "order_id": ident,
                        # datetime in memory, ISO on disk — the shape
                        # load_scraped_rows() and stage_transform() expect.
                        "order_date": (datetime.fromisoformat(when)
                                       if when else None),
                        "item_name": str(name).strip(),
                        "qty": qty,
                        "unit_price_inc_tax": float(unit or 0),
                    })

            # Per-order reconciliation: the API states the item subtotal, so a
            # mismatch means the price field or modifier handling is wrong.
            for o in orders:
                want = o.get("priceSumItems")
                if want is None:
                    continue
                ident = o.get("identifier") or str(o.get("id"))
                got = sum(r["qty"] * r["unit_price_inc_tax"]
                          for r in rows if r["order_id"] == ident)
                if abs(got - float(want)) > 0.01:
                    log("transform", "api_reconcile", "warn",
                        extra={"order": ident, "expected": float(want),
                               "got": round(got, 2)})
                    print(f"  [!] {ident}: items sum to {got:.2f} but the API "
                          f"reports {float(want):.2f}", file=sys.stderr)
        finally:
            context.close()
            browser.close()

    # Same rule as stage_scrape: never replace a good dataset with an empty one.
    path = _scraped_path(partner)
    if not rows and path.exists():
        print(f"  [!] API returned 0 rows — keeping the previous {path.name} "
              f"rather than overwriting it.", file=sys.stderr)
        log("scrape", "preserved_previous_scrape", "warn",
            extra={"path": str(path)})
    else:
        try:
            path.write_text(json.dumps(
                [{**r, "order_date": r["order_date"].isoformat()
                  if r.get("order_date") else None} for r in rows], indent=2))
        except OSError:
            pass

    return rows


def run_partner(partner: str, args) -> int:
    cfg = load_partner_config(partner)
    _init_logger(partner, args.debug)
    date_from, date_to = resolve_dates(args, cfg)
    out_fmt = cfg.get("output_date_format", "%d-%b-%Y")
    date_range = format_range(date_from, date_to, out_fmt)

    print(f"\n[{partner}] run_id={RUN_ID}  "
          f"dates={date_from:%Y-%m-%d}→{date_to:%Y-%m-%d}\n")

    rows = []

    if args.from_scraped:
        print("[Stage 1-2] — skipped (--from-scraped)")
        try:
            rows = load_scraped_rows(partner)
            print(f"[Stage 1-2] ✓ Reloaded {len(rows)} saved line items\n")
        except TransformError as exc:
            print(f"[✗] {exc}", file=sys.stderr)
            return 3
    elif (cfg.get("api") or {}).get("enabled"):
        # The portal's own JSON API, not its DOM. No selectors involved.
        print("[Stage 1-2] Reading the portal API...")
        try:
            mode = (cfg["api"].get("mode") or "replay").lower()
            fetch = (fetch_rows_via_capture if mode == "capture_response"
                     else fetch_rows_via_api)
            rows = fetch(cfg, partner, date_from, date_to)
            print(f"[Stage 1-2] ✓ {len(rows)} line item(s) from the API\n")
        except (AuthError, ConfigError) as exc:
            print(f"[✗] {exc}", file=sys.stderr)
            return 1
        except ScrapeError as exc:
            print(f"[✗] {exc}", file=sys.stderr)
            return 2
    else:
        browser_cfg = cfg.get("browser", {}) or {}
        use_profile = bool(browser_cfg.get("persistent_profile"))
        # Cloudflare fingerprints Playwright's own launch flags, not the browser
        # itself. Suppressing them lets a REAL Chrome through unaided — measured
        # 2026-09-08 on partner-hub.just-eat.co.uk, which had defeated every
        # previous approach including CDP attach.
        suppress_flags = bool(browser_cfg.get("suppress_automation_flags"))
        headless = not args.debug
        if suppress_flags and headless:
            # HEADED IS PART OF THE FIX, not a debugging convenience. Same
            # profile, same flags, same host, measured 2026-09-08:
            #   headless -> "a bot challenge did not clear within 30s"
            #   headed   -> cleared, then logged in (Stage 1 OK)
            # A real Chrome in headless mode is still fingerprintable, so the
            # flags alone are not enough. Forced here rather than left to
            # --debug, because a headless run looks like a portal outage.
            #
            # CONSEQUENCE: a partner with this set needs a display. It cannot
            # join a GitHub Actions workflow without a virtual one (xvfb).
            print("  [browser] forcing headed — suppress_automation_flags is set "
                  "and headless does not clear the challenge")
            headless = False
        try:
            with sync_playwright() as p:
                storage = _storage_path(partner)
                base_kwargs = {
                    "accept_downloads": True,
                    "viewport": {"width": 1400, "height": 900},
                    "locale": "en-GB",
                    "timezone_id": "Europe/London",
                }
                if not suppress_flags:
                    # A realistic UA matters on bot-protected portals — the
                    # default headless Chromium string is an instant tell.
                    base_kwargs["user_agent"] = (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/121.0.0.0 Safari/537.36")
                # With a real Chrome, pinning that UA is itself a tell: the
                # binary is Chrome 152 and the string claims 121. Left unset so
                # Chrome reports the truth — that is what passed the challenge.

                cdp = _cdp_endpoint(cfg)
                attached = False
                if cdp:
                    # ATTACH to a Chrome a person already cleared Cloudflare
                    # in, instead of launching our own. Playwright-driven
                    # Chromium is detected by Cloudflare whether headless or
                    # headed — proven 2026-09-02 on partner-hub.just-eat.co.uk,
                    # which held "Verifying you are human" for 90s headless and
                    # 60s headed and never presented a checkbox. A real Chrome
                    # the user logged into carries a genuine fingerprint and a
                    # live clearance cookie, so it is left alone.
                    #
                    # The browser belongs to the USER: reuse its existing
                    # context (a new one would not share the clearance) and
                    # never close it on the way out.
                    browser = p.chromium.connect_over_cdp(cdp)
                    if not browser.contexts:
                        raise ScrapeError(
                            f"{partner}: attached to Chrome at {cdp} but it has "
                            f"no browser context. Open a normal window first.")
                    context = browser.contexts[0]
                    page = context.pages[0] if context.pages else context.new_page()
                    attached = True
                    print(f"  [cdp] attached to Chrome at {cdp} "
                          f"({len(context.pages)} tab(s) open)")
                elif use_profile:
                    # Cloudflare's clearance cookie has to survive between runs,
                    # which a fresh context throws away.
                    # A portal may need the SAME profile a human cleared
                    # Cloudflare in, which is not necessarily this partner's own
                    # state/ dir. browser.profile_dir overrides it.
                    override = str(browser_cfg.get("profile_dir", "") or "").strip()
                    profile_dir = (pathlib.Path(os.path.expanduser(override))
                                   if override else _profile_path(partner))
                    profile_dir.mkdir(parents=True, exist_ok=True)
                    launch_kwargs = dict(base_kwargs)
                    if suppress_flags:
                        # channel=chrome uses the real installed browser rather
                        # than bundled Chromium; the genuine build is half the
                        # fingerprint, the flags are the other half.
                        launch_kwargs["channel"] = "chrome"
                        launch_kwargs["ignore_default_args"] = ["--enable-automation"]
                        launch_kwargs["args"] = [
                            "--disable-blink-features=AutomationControlled",
                            "--no-first-run", "--no-default-browser-check"]
                    if _verbose:
                        print(f"  [profile] {profile_dir}"
                              f"{'  [automation flags suppressed]' if suppress_flags else ''}")
                    context = p.chromium.launch_persistent_context(
                        str(profile_dir), headless=headless,
                        slow_mo=200 if args.debug else 0, **launch_kwargs)
                    browser = None
                    page = context.pages[0] if context.pages else context.new_page()
                else:
                    ctx_kwargs = dict(base_kwargs)
                    if storage.exists() and not args.force_login:
                        ctx_kwargs["storage_state"] = str(storage)
                    browser = p.chromium.launch(headless=headless,
                                                slow_mo=200 if args.debug else 0)
                    context = browser.new_context(**ctx_kwargs)
                    page = context.new_page()

                def close_browser():
                    if attached:
                        # Detach only. Closing would kill the user's own Chrome
                        # window and, with it, the Cloudflare clearance that
                        # makes the next run possible.
                        try:
                            browser.close()      # closes the CDP connection
                        except Exception:
                            pass
                        return
                    try:
                        context.close()
                    except Exception:
                        pass
                    if browser is not None:
                        try:
                            browser.close()
                        except Exception:
                            pass

                print("[Stage 1] Authentication...")
                try:
                    stage_auth(page, context, cfg, partner, args.force_login)
                    print("[Stage 1] ✓ Authenticated\n")
                except (AuthError, ConfigError) as exc:
                    log("auth", "login", "error", extra={"error": str(exc)})
                    print(f"[✗] Auth error: {scrub(exc)}", file=sys.stderr)
                    close_browser()
                    return 1

                print("[Stage 2] Scraping orders...")
                try:
                    rows = stage_scrape(page, cfg, partner, date_from, date_to)
                    print(f"[Stage 2] ✓ Scraped {len(rows)} line items\n")
                except ConfigError as exc:
                    print(f"[✗] Config error: {exc}", file=sys.stderr)
                    close_browser()
                    return 1
                except ScrapeError as exc:
                    log("scrape", "scrape", "error", extra={"error": str(exc)})
                    print(f"[✗] Scrape error: {scrub(exc)}", file=sys.stderr)
                    close_browser()
                    return 2

                close_browser()
        except Exception as exc:
            print(f"[✗] Unexpected browser error: {scrub(exc)}", file=sys.stderr)
            log("browser", "unexpected", "error", extra={"error": str(exc)})
            return 2

    print("[Stage 3] Building Supy upload file(s)...")
    try:
        results = stage_transform(rows, cfg, partner, date_from, args.dry_run)
    except (TransformError, ConfigError) as exc:
        log("transform", "transform", "error", extra={"error": str(exc)})
        print(f"[✗] Transform error: {exc}", file=sys.stderr)
        return 3

    if not results:
        print(f"[Stage 3] ✓ No sales for {date_range} — nothing to upload.\n")
    else:
        for r in results:
            where = r["path"].name if r["path"] else "(dry run — not written)"
            print(f"[Stage 3] ✓ {r['destination']}: {r['rows']} rows → {where}")
        print()

    if _FAILED_ORDERS:
        print(f"[!] {len(_FAILED_ORDERS)} order(s) could not be read and are NOT "
              f"in the output:", file=sys.stderr)
        for f in _FAILED_ORDERS:
            print(f"      {f['order']}: {f['error'][:140]}", file=sys.stderr)
        print("    Upload those by hand.\n", file=sys.stderr)

    if args.dry_run:
        print("[Stage 4] — skipped (--dry-run)\n")
    elif args.no_email:
        print("[Stage 4] — skipped (--no-email)\n")
    else:
        print("[Stage 4] Sending email...")
        try:
            stage_email(results, partner, date_range, args.email_to)
        except EmailError as exc:
            log("email", "send", "error", extra={"error": str(exc)})
            print(f"[✗] Email error: {exc}", file=sys.stderr)
            return 4

    if _FAILED_ORDERS:
        print(f"[{partner}] ⚠ Completed with {len(_FAILED_ORDERS)} unreadable "
              f"order(s)  run_id={RUN_ID}\n")
        return 2
    print(f"[{partner}] ✓ Complete  run_id={RUN_ID}\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generic per-order scraper for delivery partners")
    parser.add_argument("--partner", help="Which partner config to run")
    parser.add_argument("--list-partners", action="store_true",
                        help="List configured partners and exit")
    parser.add_argument("--debug", action="store_true",
                        help="Headed browser and verbose logging")
    parser.add_argument("--date", metavar="YYYY-MM-DD", help="Single day")
    parser.add_argument("--from", dest="date_from", metavar="YYYY-MM-DD",
                        help="Start of a custom range (requires --to)")
    parser.add_argument("--to", dest="date_to", metavar="YYYY-MM-DD",
                        help="End of a custom range (requires --from)")
    parser.add_argument("--force-login", action="store_true",
                        help="Ignore the cached session")
    parser.add_argument("--email-to", metavar="ADDR", action="append",
                        help="Send the report here instead of REPORT_RECIPIENT. "
                             "Repeat for several addresses.")
    parser.add_argument("--no-email", action="store_true",
                        help="Save output locally only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scrape and summarise without writing or emailing")
    parser.add_argument("--from-scraped", action="store_true",
                        help="Replay the transform from the last saved scrape")
    args = parser.parse_args()

    if args.list_partners:
        partners = available_partners()
        print("\nConfigured partners:\n")
        for p in partners:
            cfg = load_partner_config(p)
            dest = cfg.get("destination", "")
            n_biz = len(cfg.get("businesses") or [])
            extra = f"{n_biz} businesses" if n_biz > 1 else dest
            print(f"  {p:<20} {extra}")
        print(f"\n  (Deliveroo has a real export — use deliveroo_automation.py)\n")
        return 0

    if not args.partner:
        parser.error("--partner is required (or use --list-partners)")

    try:
        return run_partner(args.partner, args)
    except ConfigError as exc:
        print(f"[✗] Config error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
