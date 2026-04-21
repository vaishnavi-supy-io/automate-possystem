"""
joejuice_automation.py
----------------------
Daily POS report automation for Joe & The Juice (KSA) via wp2.joejuice.com API.

Flow:
  1. Login via Playwright (headless) → capture JWT token
  2. Use JWT + requests to call API per location (no browser per location)
  3. Flatten JSON response → Excel: {code}_YYYY-MM-DD.xlsx
  4. Email to REPORT_RECIPIENT

Usage:
  python joejuice_automation.py                      # all active locations, yesterday
  python joejuice_automation.py --date 2026-04-19    # specific date
  python joejuice_automation.py --location 50001     # single location by code
  python joejuice_automation.py --list-locations     # list all mapped locations
  python joejuice_automation.py --no-email           # skip email (save file only)
"""

import argparse
import json
import os
import pathlib
import re
import smtplib
import sys
import time
from datetime import date, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = pathlib.Path(__file__).parent
STATE_DIR  = BASE_DIR / "state";  STATE_DIR.mkdir(exist_ok=True)
OUTPUT_DIR = BASE_DIR / "output"; OUTPUT_DIR.mkdir(exist_ok=True)
TOKEN_FILE = STATE_DIR / "jj_token.json"

# ── Config ────────────────────────────────────────────────────────────────────
JJ_LOGIN_URL   = "https://wp2.joejuice.com/reports/pos-reports"
JJ_API_BASE    = "https://api2.joejuice.com/latest"
JJ_USERNAME    = os.environ.get("JJ_USERNAME", "31662")
JJ_PASSWORD    = os.environ.get("JJ_PASSWORD", "Azeem@123999")
REPORT_RECIPIENT = os.environ.get("REPORT_RECIPIENT", "vaishnavi@supy.io")
GMAIL_USER     = os.environ.get("GMAIL_USER", "")
GMAIL_PASS     = os.environ.get("GMAIL_APP_PASSWORD", "")

# reporting_currency=15 → SAR (id confirmed below; 1=DKK)
SAR_CURRENCY_ID = 15

# ── Location mapping: code → (supy_label, pos_name) ──────────────────────────
# pos_name must match the workplace name in the portal exactly (stripped).
LOCATIONS = {
    "50001": ("50001 - J & J - Al Bahar",       "Khobar Al Bahar"),
    "50002": ("50002 - J & J - Bujairi",         "Bujairi Terrace [Diriyah]"),
    "50003": ("50003 - J & J - DQ",              "Diplomatic Quarter"),
    "50004": ("50004 - J & J - Granada Cloud",   "Ghirnatah Cloud Campus"),
    "50005": ("50005 - J & J - KAFD",            "KAFD"),
    "50006": ("50006 - J & J - Olaya Khobar",    "Khobar Olaya"),
    "50007": ("50007 - J & J - Khoja Jeddah",    "Khojah Street"),
    "50008": ("50008 - J & J - Kingdom Tower",   "Kingdom Tower"),
    "50010": ("50010 - J & J - Muhammadia",      "Al Muhammadiyah"),
    "50011": ("50011 - J & J - Narjis",          "Al Narjes"),
    "50012": ("50012 - J & J - Saad Square",     "Saad Square"),
    "50013": ("50013 - J & J - Solitaire",       "Solitaire Mall"),
    "50015": ("50015 - J & J - La Strada Yard",  "La Strada Yard"),
    "50016": ("50016 - J & J - Al Sulimaniyah",  "Al Sulimaniyah"),
    "50017": ("50017 - J & J - Aqiq Cloud",      "Aqiq Cloud"),
    "50020": ("50020 - J & J - Hessa Square",    "Hessa Square"),
    "50023": ("50023 - J & J - Sports Blvd",     "Sports Boulevard"),
    "50025": ("50025 - J & J - Laysen Valley",   "Laysen Valley"),
    "JJ-E1": ("J & J - Event Bar 1",             "Joe Event Bar 1"),
}


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_token() -> str:
    """Login via Playwright (headless) and return Bearer token string."""
    token = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        def on_req(req):
            nonlocal token
            if "api2.joejuice.com" in req.url:
                auth = req.headers.get("authorization", "")
                if auth and "undefined" not in auth and token is None:
                    token = auth

        page.on("request", on_req)
        page.goto(JJ_LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
        time.sleep(2)
        page.fill("input[name='username']", JJ_USERNAME)
        page.fill("input[name='password']", JJ_PASSWORD)
        page.click("button:has-text('LOG IN')")
        try:
            page.wait_for_url("**/wp2.joejuice.com/**", timeout=20_000)
            page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        time.sleep(4)
        browser.close()

    if not token:
        raise RuntimeError("Failed to capture JWT token from Joe Juice portal.")

    TOKEN_FILE.write_text(json.dumps({"token": token, "ts": time.time()}))
    return token


def get_token() -> str:
    """Return cached token if <6h old, otherwise re-login."""
    if TOKEN_FILE.exists():
        data = json.loads(TOKEN_FILE.read_text())
        if time.time() - data.get("ts", 0) < 6 * 3600:
            return data["token"]
    return _get_token()


# ── Workplaces ────────────────────────────────────────────────────────────────

def get_workplace_map(token: str) -> dict[str, int]:
    """Return {stripped_name: workplace_id} for all KSA workplaces."""
    r = requests.get(
        f"{JJ_API_BASE}/shiftplanning/workplaces?filter=%3Amarket.id%3D%3D%2738%27&limit=200&sort=%3Asort_order%2B",
        headers={"Authorization": token},
        timeout=15,
    )
    r.raise_for_status()
    return {w["name"].strip(): w["id"] for w in r.json()["data"]}


def resolve_workplace_id(pos_name: str, workplace_map: dict) -> int | None:
    """Match pos_name to a workplace ID (exact then fuzzy)."""
    if pos_name in workplace_map:
        return workplace_map[pos_name]
    # Fuzzy: strip punctuation/spaces
    def norm(s): return re.sub(r"[\s\-'\[\]]", "", s).lower()
    pn = norm(pos_name)
    for name, wid in workplace_map.items():
        if norm(name) == pn:
            return wid
    return None


# ── Report fetch & flatten ────────────────────────────────────────────────────

def fetch_report(token: str, workplace_id: int, report_date: str) -> dict:
    """Call API and return the single Entry data dict for this workplace."""
    url = (
        f"{JJ_API_BASE}/reporting/new_pos_reports"
        f"?filter=%3Aworkplace.id%3D%3D%27{workplace_id}%27"
        f"&from={report_date}&to={report_date}"
        f"&reporting_currency={SAR_CURRENCY_ID}"
    )
    r = requests.get(url, headers={"Authorization": token}, timeout=30)
    r.raise_for_status()
    payload = r.json()

    # Drill: data[0].tables[0].content[0].content[0].content[0]  →  Entry
    try:
        entry = (
            payload["data"][0]["tables"][0]
            ["content"][0]["content"][0]["content"][0]
        )
        return entry["data"]
    except (KeyError, IndexError):
        return {}


CATEGORIES = [
    "juice", "sandwich", "shake", "coffee",
    "loyalty_card", "breakfast", "salad",
    "counter_product", "topping", "misc",
]

def flatten_entry(code: str, label: str, pos_name: str, report_date: str, data: dict) -> dict:
    """Flatten one entry dict into a single-row dict."""
    total = data.get("total", {})
    avg   = data.get("average", {})
    cats  = data.get("categories", {})
    disc  = data.get("discount_group", {})
    emeal = data.get("employee_meal", {})

    row = {
        "Date":          report_date,
        "Code":          code,
        "Location":      label,
        "POS Name":      pos_name,
        "Turnover":      total.get("turnover", 0),
        "Products":      total.get("products", 0),
        "Employees":     total.get("employees", 0),
        "PHP Max":       total.get("php_max", 0),
        "Avg Turnover":  avg.get("turnover", 0),
        "Avg Products":  avg.get("products", 0),
    }

    for cat in CATEGORIES:
        c = cats.get(cat, {})
        label_cat = cat.replace("_", " ").title()
        row[f"{label_cat} Turnover"]   = c.get("turnover", 0)
        row[f"{label_cat} Products"]   = c.get("products", 0)
        row[f"{label_cat} TO %"]       = c.get("turnover_percentage", 0)
        row[f"{label_cat} Prod %"]     = c.get("product_percentage", 0)

    for dk, dlabel in [
        ("neighbour", "Disc Neighbour"),
        ("employee", "Disc Employee"),
        ("loyalty", "Disc Loyalty"),
        ("employee_meal", "Disc Emp Meal"),
    ]:
        d = disc.get(dk, {})
        row[f"{dlabel} Products"] = d.get("products", 0)
        row[f"{dlabel} %"]        = d.get("percentage", 0)

    row["Emp Meal Products"] = emeal.get("products", 0)
    row["Emp Meal %"]        = emeal.get("percentage", 0)
    return row


# ── Email ─────────────────────────────────────────────────────────────────────

def send_email(to: str, subject: str, body: str, attachment_path: pathlib.Path) -> None:
    if not GMAIL_USER or not GMAIL_PASS:
        print(f"  [!] Email skipped — GMAIL_USER / GMAIL_APP_PASSWORD not set.")
        return

    msg = MIMEMultipart()
    msg["From"]    = GMAIL_USER
    msg["To"]      = to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with open(attachment_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=attachment_path.name)
    part["Content-Disposition"] = f'attachment; filename="{attachment_path.name}"'
    msg.attach(part)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(GMAIL_USER, GMAIL_PASS)
        smtp.sendmail(GMAIL_USER, to, msg.as_string())

    print(f"  [✓] Email sent → {to}")


# ── Per-location runner ───────────────────────────────────────────────────────

def run_location(code: str, token: str, workplace_map: dict, report_date: str, skip_email: bool) -> bool:
    label, pos_name = LOCATIONS[code]
    print(f"\n[{code}] {pos_name}")

    workplace_id = resolve_workplace_id(pos_name, workplace_map)
    if workplace_id is None:
        print(f"  [!] Workplace not found in portal for pos_name={pos_name!r} — skipping")
        return False

    try:
        data = fetch_report(token, workplace_id, report_date)
        if not data:
            print(f"  [!] Empty report data — skipping")
            return False

        row = flatten_entry(code, label, pos_name, report_date, data)
        df  = pd.DataFrame([row])

        out_path = OUTPUT_DIR / f"{code}_{report_date}.xlsx"
        df.to_excel(out_path, index=False)
        print(f"  [✓] Saved → {out_path.name}  ({len(df.columns)} cols)")

        if not skip_email:
            send_email(
                to=REPORT_RECIPIENT,
                subject=f"JJ POS Report — {label} — {report_date}",
                body=f"Daily POS report for {label} ({pos_name})\nDate: {report_date}\n\nTurnover: {row['Turnover']:.2f} SAR\nProducts: {row['Products']}\n",
                attachment_path=out_path,
            )
        return True

    except Exception as e:
        print(f"  [✗] Error: {e}")
        return False


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Joe Juice POS daily report automation")
    parser.add_argument("--date",           default=None,  help="Report date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--location",       default=None,  help="Run for single location code (e.g. 50001)")
    parser.add_argument("--list-locations", action="store_true", help="Print all mapped locations and exit")
    parser.add_argument("--no-email",       action="store_true", help="Skip email, save files only")
    args = parser.parse_args()

    if args.list_locations:
        print(f"Joe Juice KSA locations ({len(LOCATIONS)} total):\n")
        for code, (label, pos_name) in LOCATIONS.items():
            print(f"  {code:8s}  POS: {pos_name!r:35s}  Label: {label}")
        return

    report_date = args.date or (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[Joe Juice Pipeline]  date={report_date}")

    print("[Auth] Logging in...")
    token = get_token()
    print("[Auth] Token OK")

    print("[Workplaces] Fetching KSA workplace list...")
    workplace_map = get_workplace_map(token)
    print(f"[Workplaces] {len(workplace_map)} workplaces found")

    codes = [args.location] if args.location else list(LOCATIONS.keys())

    ok, fail = 0, 0
    for code in codes:
        if code not in LOCATIONS:
            print(f"[!] Unknown code: {code}")
            fail += 1
            continue
        success = run_location(code, token, workplace_map, report_date, args.no_email)
        if success:
            ok += 1
        else:
            fail += 1

    print(f"\n[Done] {ok} succeeded, {fail} failed  |  date={report_date}")


if __name__ == "__main__":
    main()
