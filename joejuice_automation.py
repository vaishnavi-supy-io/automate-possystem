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
    # ── J & J (50xxx) ────────────────────────────────────────────────────────
    "50001": ("50001 - J & J - Al Bahar, Riyadh - Kitchen",          "Khobar Al Bahar"),
    "50002": ("50002 - J & J - Bujairi, Riyadh - Kitchen",           "Bujairi Terrace [Diriyah]"),
    "50003": ("50003 - J & J - DQ, Riyadh - Kitchen",                "Diplomatic Quarter"),
    "50004": ("50004 - J & J - Granada Cloud, Riyadh - Kitchen",     "Ghirnatah Cloud Campus"),
    "50005": ("50005 - J & J - KAFD, Riyadh - Kitchen",              "KAFD"),
    "50006": ("50006 - J & J - Olaya, Al Khobar - Kitchen",          "Khobar Olaya"),
    "50007": ("50007 - J & J - Khoja, Jeddah - Kitchen",             "Khojah Street"),
    "50008": ("50008 - J & J - Kingdom Tower, Riyadh - Kitchen",     "Kingdom Tower"),
    # 50009 LOCATION CLOSED
    "50010": ("50010 - J & J - Muhammadia, Riyadh - Kitchen",        "Al Muhammadiyah"),
    "50011": ("50011 - J & J - Narjis, Riyadh - Kitchen",            "Al Narjes"),
    "50012": ("50012 - J & J - Saad Square, Riyadh - Kitchen",       "Saad Square"),
    "50013": ("50013 - J & J - Solitaire, Riyadh - Kitchen",         "Solitaire Mall"),
    # 50014 N/A
    "50015": ("50015 - J & J - La Strada Yard, Riyadh - Kitchen",    "La Strada Yard"),
    "50016": ("50016 - J & J - King Abdul Aziz, Riyadh - Kitchen",   "Al Sulimaniyah"),
    "50017": ("50017 - J & J - Cloud Aqiq, Riyadh - Kitchen",        "Aqiq Cloud"),
    # 50018/50019/50021/50022 N/A
    "50020": ("50020 - J & J - Al Hassa Squar, Al Khobar - Kitchen", "Hessa Square"),
    "50023": ("50023 - J & J promenade Sports Boulevard, Riyadh",    "Sports Boulevard"),
    "50025": ("50025 - J & J - Laysen valley, Riyadh",               "Laysen Valley"),
    "JJ-E1": ("J & J - Event Al Hasa - Kitchen",                     "Joe Event Bar 1"),

    # ── Parker's KSA (20xxx) ─────────────────────────────────────────────────
    "20001": ("20001 - Parker's - DQ, Riyadh",                       "Parkers-DQ"),
    "20002": ("20002 - Parker's - Olaya, Al Khobar",                 "Parkers-Khobar"),
    # 20003 Location Name not mentioned in POS
    "20004": ("20004 - Parker's - Solitaire, Riyadh",                "Parkers Solitaire"),
    "20007": ("20007 - Parker's - Al Bahar - Al Bahar",              "PARKER'S AL BAHAR"),

    # ── Public KSA (40xxx) ───────────────────────────────────────────────────
    "40001": ("40001 - Public - Dabab, Riyadh",                      "Public-Dabbab"),
    "40002": ("40002 - Public - Al Khobar",                          "Public-Khobar"),
    "40003": ("40003 - Public - DQ, Riyadh",                         "Public DQ"),
    # 40004 Location Name not mentioned in POS

    # ── Somewhere KSA (30xxx) ────────────────────────────────────────────────
    "30001": ("30001 - SMW - Al Ula",                                 "Somewhere-Alula 2"),
    "30002": ("30002 - SMW - Bujairi, Riyadh",                       "Somewhere-Bujairi"),
    "30003": ("30003 - SMW - Riyadh Front, KKIA",                    "Somewhere-Riyadh Front"),
    # 30004 Location Name not mentioned in POS
    "30005": ("30005 - SMW - Solitaire, Riyadh",                     "Somewhere Solitaire"),
    "SMW-KX": ("SMW Khobar, Khobar X Mall",                          "Somewhere-ALKhobar X"),

    # ── Salt KSA (10xxx) ─────────────────────────────────────────────────────
    "10001": ("10001 - Salt - Al Bahar, Al Khobar",                  "Find Salt-Salt Albahar"),
    "10003": ("10003 - Salt - Elephant Rock, Al Ula",                "Find Salt-Alula"),
    "10004": ("10004 - Salt - Cloud Kitchen - Ghirnatah",            "Find Salt-CK Gharnatha"),
    "10005": ("10005 - Salt - Olaya, Al Khobar",                     "Find Salt-Khobar Park"),
    "10006": ("10006 - Salt - Nakheel Mall, Dammam",                 "Find Salt-Nakheel Mall"),
    "10007": ("10007 - Salt - Riyadh Park Mall",                     "Find Salt-Riyad Park"),
    "10008": ("10008 - Salt - Saad Square, Riyadh",                  "Find Salt-Saad Square"),
    "10009": ("10009 - Salt - UWalk, Riyadh",                        "Find Salt-Uwalk"),
    "10011": ("10011 - Salt - Events Truck 1",                       "Find Salt-Food Truck KSA-1"),
    "10012": ("10012 - Salt - Events Truck 2",                       "Find Salt-Food Truck KSA-2"),
    "10013": ("10013 - Salt - Events Truck 3",                       "Find Salt-Food Truck KSA-3"),
    "10014": ("10014 - Salt - Events Truck 4",                       "Find Salt- Food Truck KSA-4"),
    "10015": ("10015 - Salt - Events Truck 5",                       "Find Salt- Food Truck KSA-5"),
    "10016": ("10016 - Salt - Events Ksa 3",                         "Find Salt-Events KSA-3"),
    # 10017 N/A

    # ── Play ─────────────────────────────────────────────────────────────────
    "Play-KX": ("Play - Khobar X",                                   "Play Khobar x"),
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
    """Return {stripped_name: workplace_id} for ALL workplaces (all brands/markets)."""
    all_workplaces = {}
    limit  = 500
    offset = 0
    while True:
        r = requests.get(
            f"{JJ_API_BASE}/shiftplanning/workplaces",
            params={"limit": limit, "offset": offset, "sort": ":sort_order+"},
            headers={"Authorization": token},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        for w in data:
            all_workplaces[w["name"].strip()] = w["id"]
        if len(data) < limit:
            break   # last page
        offset += limit
    return all_workplaces


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
    parser.add_argument("--limit",          type=int, default=None, metavar="N",
                        help="Cap number of locations processed (for testing, e.g. --limit 5)")
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
    if args.limit:
        codes = codes[:args.limit]

    ok, fail = 0, 0
    results = []
    for code in codes:
        if code not in LOCATIONS:
            print(f"[!] Unknown code: {code}")
            fail += 1
            continue
        success = run_location(code, token, workplace_map, report_date, args.no_email)
        results.append({"code": code, "ok": success})
        if success:
            ok += 1
        else:
            fail += 1

    print(f"\n[Done] {ok} succeeded, {fail} failed  |  date={report_date}")

    # ── Digest summary email ──────────────────────────────────────────────────
    if not args.no_email and not args.location and GMAIL_USER and GMAIL_PASS:
        lines = [
            f"Joe Juice KSA Daily Run Summary — {report_date}",
            f"  ✅ Succeeded : {ok}",
            f"  ❌ Failed    : {fail}",
            f"  Total       : {ok + fail}",
            "",
            "─" * 50,
        ]
        for r in results:
            code = r["code"]
            label, pos_name = LOCATIONS.get(code, (code, code))
            status = "✅" if r["ok"] else "❌"
            lines.append(f"{status}  {code:<8}  {label}")

        body = "\n".join(lines)
        msg = MIMEMultipart()
        msg["From"]    = GMAIL_USER
        msg["To"]      = REPORT_RECIPIENT
        msg["Subject"] = f"[Summary] Joe Juice KSA Run — {report_date} — {ok}/{ok+fail} OK"
        msg.attach(MIMEText(body, "plain"))
        try:
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
                smtp.login(GMAIL_USER, GMAIL_PASS)
                smtp.sendmail(GMAIL_USER, REPORT_RECIPIENT, msg.as_string())
            print(f"[Digest] Summary email sent → {REPORT_RECIPIENT}")
        except Exception as e:
            print(f"[Digest] Failed to send summary: {e}")


if __name__ == "__main__":
    main()
