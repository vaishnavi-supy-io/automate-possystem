"""
debug_location_filter.py
------------------------
Discovery script — run this ONCE (headed browser) to find the Revenue Centers
filter selectors on the Oracle BI portal.

Usage:
    python debug_location_filter.py

It will:
  1. Log in (or reuse cached session)
  2. Navigate to the report URL
  3. Print all iframes and interactive elements related to location/revenue filters
  4. Take a screenshot: screenshots/debug_location_filter.png
  5. Keep browser open 60s for manual inspection

Copy the selectors you see into config.yaml under portal.location_filter.
"""

import os
import pathlib
import time

import yaml
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

BASE_DIR        = pathlib.Path(__file__).parent
STATE_PATH      = BASE_DIR / "state" / "storage_state.json"
SCREENSHOTS_DIR = BASE_DIR / "screenshots"
SCREENSHOTS_DIR.mkdir(exist_ok=True)

with open(BASE_DIR / "config.yaml") as f:
    CONFIG = yaml.safe_load(f)

PORTAL    = CONFIG["portal"]
SELECTORS = CONFIG["selectors"]


def login(page, context):
    print("  → Logging in...")
    page.goto(PORTAL["login_url"], wait_until="domcontentloaded", timeout=30_000)
    page.fill(SELECTORS["username_field"], os.environ["PORTAL_USERNAME"])
    page.fill(SELECTORS["company_field"],  os.environ["PORTAL_COMPANY"])
    page.fill(SELECTORS["password_field"], os.environ["PORTAL_PASSWORD"])
    with page.expect_navigation(wait_until="domcontentloaded", timeout=45_000):
        page.click(SELECTORS["login_button"])
    context.storage_state(path=str(STATE_PATH))
    print("  ✓ Logged in")


def inspect_frame(frame, depth=0):
    indent = "  " * depth
    print(f"{indent}[FRAME] name={frame.name!r}  url={frame.url[:80]!r}")
    for sel in ["select", "input[type='text']", "[role='listbox']", "[role='combobox']",
                "[class*='prompt']", "[class*='filter']"]:
        try:
            els = frame.query_selector_all(sel)
            if els:
                print(f"{indent}  [{sel}] → {len(els)} found")
                for el in els[:2]:
                    try:
                        attrs = {"id": el.get_attribute("id"), "name": el.get_attribute("name"),
                                 "class": (el.get_attribute("class") or "")[:60]}
                        print(f"{indent}    {attrs}  text={el.inner_text()[:50]!r}")
                    except Exception:
                        pass
        except Exception:
            pass
    for child in frame.child_frames:
        inspect_frame(child, depth + 1)


def main():
    with sync_playwright() as p:
        kwargs = {}
        if STATE_PATH.exists():
            kwargs["storage_state"] = str(STATE_PATH)
            print("  → Reusing cached session")

        browser = p.chromium.launch(headless=False, slow_mo=300)
        context = browser.new_context(accept_downloads=True, **kwargs)
        page    = context.new_page()

        if not STATE_PATH.exists():
            login(page, context)
        else:
            page.goto(PORTAL["portal_url"], wait_until="domcontentloaded", timeout=30_000)
            if "login" in page.url.lower():
                login(page, context)

        print(f"\n  → Navigating to report URL...")
        page.goto(PORTAL["report_url"], wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=30_000)
        time.sleep(3)

        shot = SCREENSHOTS_DIR / "debug_location_filter.png"
        page.screenshot(path=str(shot), full_page=True)
        print(f"  → Screenshot: {shot}")

        print("\n═══ FRAME TREE ═══════════════════════════════════════")
        inspect_frame(page.main_frame)

        print("\n═══ SEARCHING FOR REVENUE/LOCATION FILTER ════════════")
        for frame in [page.main_frame] + page.frames:
            try:
                html = frame.content()
                if "revenue" in html.lower():
                    print(f"\n  ✓ FOUND in frame={frame.name!r} url={frame.url[:60]!r}")
                    for sel in ["table.PromptTable", "[class*='PromptColumn']",
                                "select", "input[title*='Revenue']",
                                "td:has-text('Revenue Centers')", "td:has-text('Locations')"]:
                        try:
                            els = frame.query_selector_all(sel)
                            if els:
                                print(f"    [{sel}] → {len(els)}")
                                for el in els[:2]:
                                    print(f"      {el.evaluate('e => e.outerHTML')[:250]}")
                        except Exception:
                            pass
            except Exception:
                pass

        print("\n  Browser open for 60s — inspect manually. Ctrl+C to exit early.")
        time.sleep(60)
        browser.close()


if __name__ == "__main__":
    main()
