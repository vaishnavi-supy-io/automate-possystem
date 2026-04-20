"""
debug_selectors.py
------------------
One-time DOM inspection utility.

Run this BEFORE first use to discover the exact HTML field
selectors for the Oracle BI login page. Output is printed to
stdout; copy the values into config.yaml [selectors].

Usage:
    python debug_selectors.py
"""

import os
import pathlib
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv
import yaml

load_dotenv()

BASE_DIR = pathlib.Path(__file__).parent
SCREENSHOT_DIR = BASE_DIR / "screenshots"
SCREENSHOT_DIR.mkdir(exist_ok=True)

with open(BASE_DIR / "config.yaml") as f:
    CONFIG = yaml.safe_load(f)

LOGIN_URL = CONFIG["portal"]["login_url"]


def _fmt(attrs: dict) -> str:
    parts = []
    for k in ("id", "name", "type", "placeholder", "aria-label", "value", "class"):
        v = attrs.get(k, "")
        if v:
            parts.append(f"{k}={repr(v)}")
    return "  " + ", ".join(parts) if parts else "  (no relevant attrs)"


def inspect_login_page() -> None:
    print(f"\n{'='*60}")
    print(f" Oracle BI — DOM Inspector")
    print(f" Target: {LOGIN_URL}")
    print(f"{'='*60}\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=300)
        context = browser.new_context()
        page = context.new_page()

        print(f"[→] Navigating to {LOGIN_URL} ...")
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_load_state("networkidle", timeout=15_000)

        # Screenshot for visual reference
        ref_path = SCREENSHOT_DIR / "login_page_reference.png"
        page.screenshot(path=str(ref_path), full_page=True)
        print(f"[📸] Screenshot saved → {ref_path}\n")

        # ── INPUT FIELDS ───────────────────────────────────────────
        inputs = page.query_selector_all("input")
        print(f"[INPUT FIELDS] Found {len(inputs)} <input> element(s):\n")
        for i, el in enumerate(inputs):
            attrs = {
                k: el.get_attribute(k)
                for k in ("id", "name", "type", "placeholder", "aria-label", "value", "class")
                if el.get_attribute(k) is not None
            }
            tag_type = attrs.get("type", "text")
            print(f"  [{i}] <input type={repr(tag_type)}>")
            print(_fmt(attrs))

            # Suggest a reliable CSS selector
            if attrs.get("id"):
                suggestion = f"#{attrs['id']}"
            elif attrs.get("name"):
                suggestion = f"input[name='{attrs['name']}']"
            elif attrs.get("placeholder"):
                suggestion = f"input[placeholder='{attrs['placeholder']}']"
            elif attrs.get("aria-label"):
                suggestion = f"input[aria-label='{attrs['aria-label']}']"
            else:
                suggestion = f"input:nth-of-type({i + 1})"
            print(f"  → Suggested selector: {suggestion}\n")

        # ── BUTTONS ────────────────────────────────────────────────
        buttons = page.query_selector_all("button, input[type='submit'], input[type='button']")
        print(f"\n[BUTTONS / SUBMIT] Found {len(buttons)} element(s):\n")
        for i, el in enumerate(buttons):
            attrs = {
                k: el.get_attribute(k)
                for k in ("id", "name", "type", "value", "class", "aria-label")
                if el.get_attribute(k) is not None
            }
            text = (el.text_content() or "").strip()
            print(f"  [{i}] tag={el.evaluate('el => el.tagName').lower()}"
                  f"  text={repr(text)}")
            print(_fmt(attrs))

            if attrs.get("id"):
                suggestion = f"#{attrs['id']}"
            elif attrs.get("name"):
                suggestion = f"[name='{attrs['name']}']"
            elif text:
                suggestion = f"text={repr(text)}"
            else:
                suggestion = f"button:nth-of-type({i + 1})"
            print(f"  → Suggested selector: {suggestion}\n")

        # ── FORM ACTION ────────────────────────────────────────────
        forms = page.query_selector_all("form")
        print(f"\n[FORMS] Found {len(forms)} <form> element(s):")
        for i, form in enumerate(forms):
            action = page.evaluate("(el) => el.getAttribute('action')", form) or "(none)"
            method = page.evaluate("(el) => el.getAttribute('method')", form) or "GET"
            print(f"  [{i}] action={action!r}  method={method!r}")

        print(f"\n{'='*60}")
        print(" ACTION REQUIRED:")
        print(f"  1. Review the selectors printed above.")
        print(f"  2. Open config.yaml and fill in the [selectors] section.")
        print(f"  3. Close this browser window when done.")
        print(f"{'='*60}\n")

        # Keep window open so engineer can inspect it manually
        input("  Press ENTER here to close the browser...")
        browser.close()


if __name__ == "__main__":
    inspect_login_page()
