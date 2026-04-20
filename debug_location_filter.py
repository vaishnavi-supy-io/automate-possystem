"""
debug_location_filter.py
------------------------
Diagnostic tool for discovering Oracle BI Revenue Centers filter selectors.

Run this BEFORE implementing/updating stage_set_location_filter() in automation.py
to find the exact selectors for the Revenue Centers (location) filter widget.

Usage:
    python debug_location_filter.py             # headed, uses cached session
    python debug_location_filter.py --force-login  # re-authenticate first

What it does:
    1. Logs into the portal (reuses state/storage_state.json if present)
    2. Navigates to the report URL
    3. Waits for all frames to load
    4. Prints all iframe names/URLs found on the page
    5. For each frame (main + iframes), looks for:
       - <select> elements near "Revenue Centers" or "Location" labels
       - Any input, dropdown, or listbox that could be a location filter
       - Form elements that accept text input
    6. Takes a full-page screenshot → screenshots/debug_location_filter.png
    7. Prints all interactive elements (inputs, selects, buttons, links)
"""

import os
import pathlib
import sys
import time

import yaml
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

BASE_DIR = pathlib.Path(__file__).parent
STATE_DIR = BASE_DIR / "state"
SCREENSHOTS_DIR = BASE_DIR / "screenshots"
SCREENSHOTS_DIR.mkdir(exist_ok=True)

with open(BASE_DIR / "config.yaml") as _f:
    CONFIG = yaml.safe_load(_f)

STORAGE_STATE_PATH = STATE_DIR / "storage_state.json"


def _login(page, context):
    """Authenticate using credentials from .env."""
    sel = CONFIG["selectors"]
    username = os.environ.get("PORTAL_USERNAME", "")
    company = os.environ.get("PORTAL_COMPANY", "")
    password = os.environ.get("PORTAL_PASSWORD", "")

    if not password:
        print("[✗] PORTAL_PASSWORD not set in .env", file=sys.stderr)
        sys.exit(1)

    print(f"  [→] Navigating to login page...")
    page.goto(CONFIG["portal"]["login_url"], wait_until="domcontentloaded", timeout=30_000)
    page.wait_for_selector(sel["username_field"], timeout=10_000)
    page.fill(sel["username_field"], username)
    page.fill(sel["company_field"], company)
    page.fill(sel["password_field"], password)

    with page.expect_navigation(wait_until="domcontentloaded", timeout=45_000):
        page.click(sel["login_button"])

    context.storage_state(path=str(STORAGE_STATE_PATH))
    print("  [✓] Logged in and session cached\n")


def _inspect_frame(frame, frame_label: str):
    """Inspect a single frame for filter-related elements."""
    print(f"\n{'='*60}")
    print(f"  FRAME: {frame_label}")
    print(f"{'='*60}")

    # --- All <select> elements ---
    try:
        selects = frame.query_selector_all("select")
        if selects:
            print(f"\n  [SELECT elements: {len(selects)} found]")
            for i, sel_el in enumerate(selects):
                try:
                    name = sel_el.get_attribute("name") or ""
                    id_attr = sel_el.get_attribute("id") or ""
                    cls = sel_el.get_attribute("class") or ""
                    label_text = ""
                    # Try to find associated label
                    if id_attr:
                        try:
                            lbl = frame.query_selector(f"label[for='{id_attr}']")
                            if lbl:
                                label_text = lbl.inner_text().strip()[:60]
                        except Exception:
                            pass
                    options = sel_el.query_selector_all("option")
                    option_count = len(options)
                    first_options = [o.inner_text().strip()[:40] for o in options[:5]]
                    print(f"    [{i}] id={id_attr!r} name={name!r} class={cls[:40]!r}")
                    print(f"         label={label_text!r}  options={option_count}  first={first_options}")
                except Exception as e:
                    print(f"    [{i}] (error reading select: {e})")
        else:
            print("  [SELECT elements: none]")
    except Exception as e:
        print(f"  [SELECT scan error: {e}]")

    # --- All <input> elements ---
    try:
        inputs = frame.query_selector_all("input:not([type='hidden'])")
        if inputs:
            print(f"\n  [INPUT elements: {len(inputs)} found]")
            for i, inp in enumerate(inputs[:20]):  # cap at 20
                try:
                    itype = inp.get_attribute("type") or "text"
                    iname = inp.get_attribute("name") or ""
                    iid = inp.get_attribute("id") or ""
                    iplaceholder = inp.get_attribute("placeholder") or ""
                    ivalue = inp.get_attribute("value") or ""
                    print(f"    [{i}] type={itype!r} id={iid!r} name={iname!r} "
                          f"placeholder={iplaceholder!r} value={ivalue[:30]!r}")
                except Exception as e:
                    print(f"    [{i}] (error: {e})")
        else:
            print("  [INPUT elements: none]")
    except Exception as e:
        print(f"  [INPUT scan error: {e}]")

    # --- Elements with "Revenue" or "Location" in text/attributes ---
    try:
        print(f"\n  [Revenue/Location keyword search]")
        for kw in ["Revenue", "revenue", "Location", "location", "Center", "center", "Filter", "filter"]:
            found = frame.query_selector_all(f"*[id*='{kw}'], *[name*='{kw}'], *[class*='{kw}']")
            if found:
                print(f"    keyword={kw!r}: {len(found)} element(s)")
                for el in found[:3]:
                    try:
                        tag = el.evaluate("el => el.tagName.toLowerCase()")
                        eid = el.get_attribute("id") or ""
                        ename = el.get_attribute("name") or ""
                        ecls = (el.get_attribute("class") or "")[:50]
                        etext = el.inner_text().strip()[:60] if tag not in ("input", "select") else ""
                        print(f"      <{tag}> id={eid!r} name={ename!r} class={ecls!r} text={etext!r}")
                    except Exception:
                        pass
    except Exception as e:
        print(f"  [keyword search error: {e}]")

    # --- All listbox / combobox (ARIA roles) ---
    try:
        listboxes = frame.query_selector_all("[role='listbox'], [role='combobox'], [role='option']")
        if listboxes:
            print(f"\n  [ARIA listbox/combobox/option: {len(listboxes)} found]")
            for i, el in enumerate(listboxes[:10]):
                try:
                    role = el.get_attribute("role") or ""
                    eid = el.get_attribute("id") or ""
                    etext = el.inner_text().strip()[:60]
                    print(f"    [{i}] role={role!r} id={eid!r} text={etext!r}")
                except Exception:
                    pass
        else:
            print("  [ARIA listbox/combobox: none]")
    except Exception as e:
        print(f"  [ARIA scan error: {e}]")

    # --- All <button> and <a> elements (interactive) ---
    try:
        buttons = frame.query_selector_all("button, input[type='button'], input[type='submit']")
        if buttons:
            print(f"\n  [BUTTON elements: {len(buttons)} found]")
            for i, btn in enumerate(buttons[:15]):
                try:
                    btext = btn.inner_text().strip()[:50]
                    bid = btn.get_attribute("id") or ""
                    bname = btn.get_attribute("name") or ""
                    bval = btn.get_attribute("value") or ""
                    print(f"    [{i}] id={bid!r} name={bname!r} value={bval!r} text={btext!r}")
                except Exception:
                    pass
    except Exception as e:
        print(f"  [BUTTON scan error: {e}]")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Debug Oracle BI location filter selectors")
    parser.add_argument("--force-login", action="store_true", help="Re-authenticate even if session cached")
    args = parser.parse_args()

    report_url = CONFIG["portal"]["report_url"]
    print(f"\n[Debug] Oracle BI Location Filter Inspector")
    print(f"[Debug] Report URL: {report_url}\n")

    with sync_playwright() as p:
        browser_ctx_kwargs = {}
        if STORAGE_STATE_PATH.exists() and not args.force_login:
            browser_ctx_kwargs["storage_state"] = str(STORAGE_STATE_PATH)
            print("  [→] Reusing cached session state\n")

        browser = p.chromium.launch(headless=False, slow_mo=300)
        context = browser.new_context(
            accept_downloads=True,
            viewport={"width": 1400, "height": 900},
            **browser_ctx_kwargs,
        )
        page = context.new_page()

        # Login if no session cached or forced
        if not STORAGE_STATE_PATH.exists() or args.force_login:
            _login(page, context)

        # Navigate to report
        print(f"  [→] Navigating to report URL...")
        page.goto(report_url, wait_until="domcontentloaded", timeout=30_000)
        print(f"  [→] Waiting for network idle...")
        try:
            page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            print("  [!] networkidle timeout — proceeding anyway")
        time.sleep(3)

        # Screenshot
        ss_path = SCREENSHOTS_DIR / "debug_location_filter.png"
        page.screenshot(path=str(ss_path), full_page=True)
        print(f"\n  [📸] Screenshot saved → {ss_path}")

        # --- Print all iframes ---
        print(f"\n{'='*60}")
        print("  ALL FRAMES ON PAGE")
        print(f"{'='*60}")
        all_frames = page.frames
        print(f"  Total frames: {len(all_frames)}")
        for i, frame in enumerate(all_frames):
            print(f"  [{i}] name={frame.name!r}  url={frame.url[:100]!r}")

        # --- Inspect main frame ---
        _inspect_frame(page.main_frame, "MAIN FRAME")

        # --- Inspect each iframe ---
        for i, frame in enumerate(all_frames[1:], start=1):
            frame_label = f"IFRAME [{i}] name={frame.name!r} url={frame.url[:80]!r}"
            try:
                _inspect_frame(frame, frame_label)
            except Exception as e:
                print(f"\n  [!] Could not inspect frame {i}: {e}")

        print(f"\n{'='*60}")
        print("  INSPECTION COMPLETE")
        print(f"  Screenshot: {ss_path}")
        print(f"  Use the selectors above to update stage_set_location_filter()")
        print(f"  in automation.py (look for the TODO comment).")
        print(f"{'='*60}\n")

        input("  [Press ENTER to close browser and exit]")
        browser.close()


if __name__ == "__main__":
    main()

