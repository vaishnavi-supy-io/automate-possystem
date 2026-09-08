"""
debug_selectors.py
------------------
DOM inspection utility for portal login pages.

Login pages are public, so this needs NO credentials — it navigates to a
portal's login_url, dumps every input/button with a suggested CSS selector,
and classifies the likely username / password / submit fields so the result
can be pasted straight into that portal's config.

Usage:
    python debug_selectors.py                          # Oracle BI (default), headed
    python debug_selectors.py --portal deliveroo        # a report-export portal
    python debug_selectors.py --partner feedr           # a partners/<name>.yaml
    python debug_selectors.py --all-partners --headless # sweep all eight, no prompts
    python debug_selectors.py --list                    # show every target

Headed mode pauses so you can inspect the page by hand. --headless skips both
the browser window and the prompt, which is what you want for a sweep or CI.

NOTE: this only discovers LOGIN selectors. The order-list and line-item
selectors must be captured while logged in:
    python partner_scraper.py --partner <name> --debug
"""

import argparse
import pathlib
import sys

import yaml
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

BASE_DIR = pathlib.Path(__file__).parent
SCREENSHOT_DIR = BASE_DIR / "screenshots"
SCREENSHOT_DIR.mkdir(exist_ok=True)
PARTNERS_DIR = BASE_DIR / "partners"

# portal key → (config filename, fallback label)
PORTALS = {
    "oracle": ("config.yaml", "Oracle BI"),
    "sapapad": ("sapapad_config.yaml", "Sapapad"),
    "deliveroo": ("deliveroo_config.yaml", "Deliveroo Partner Hub"),
    "symphony": ("symphony_config.yaml", "Micros Symphony"),
}

ATTRS = ("id", "name", "type", "placeholder", "aria-label", "autocomplete",
         "value", "class")


def partner_configs() -> list:
    if not PARTNERS_DIR.exists():
        return []
    return sorted(p.stem for p in PARTNERS_DIR.glob("*.yaml"))


def load_config(config_file: str) -> dict:
    with open(BASE_DIR / config_file) as f:
        return yaml.safe_load(f)


# ── Selector suggestion ───────────────────────────────────────────────────────

def suggest_selector(attrs: dict, tag: str, index: int, text: str = "") -> str:
    """Most stable CSS selector we can build from the attributes present."""
    if attrs.get("id"):
        return f"#{attrs['id']}"
    if attrs.get("name"):
        return f"{tag}[name='{attrs['name']}']"
    if attrs.get("autocomplete"):
        return f"{tag}[autocomplete='{attrs['autocomplete']}']"
    if attrs.get("placeholder"):
        return f"{tag}[placeholder='{attrs['placeholder']}']"
    if attrs.get("aria-label"):
        return f"{tag}[aria-label='{attrs['aria-label']}']"
    if text:
        return f"{tag}:has-text('{text}')"
    return f"{tag}:nth-of-type({index + 1})"


def classify(inputs: list, buttons: list) -> dict:
    """
    Guess which discovered element is the username / password / submit field.

    Ranked by how reliable the signal is: input type and autocomplete are
    strong, name/id substrings are decent, position is a last resort.
    """
    guess = {"username_field": "", "password_field": "", "login_button": ""}

    # Password: type="password" is unambiguous.
    for el in inputs:
        if el["attrs"].get("type") == "password":
            guess["password_field"] = el["selector"]
            break

    # Username: prefer email/text inputs with a telling name or autocomplete.
    user_hints = ("email", "user", "login", "identifier", "username")
    for el in inputs:
        a = el["attrs"]
        if a.get("type") in ("hidden", "password", "checkbox", "submit", "button"):
            continue
        haystack = " ".join(str(a.get(k, "")).lower()
                            for k in ("name", "id", "autocomplete", "placeholder",
                                      "aria-label", "type"))
        if any(h in haystack for h in user_hints):
            guess["username_field"] = el["selector"]
            break
    if not guess["username_field"]:
        for el in inputs:
            if el["attrs"].get("type") in (None, "text", "email"):
                guess["username_field"] = el["selector"]
                break

    # Submit: a button whose text or type says so.
    submit_hints = ("log in", "login", "sign in", "signin", "continue",
                    "submit", "next")
    for el in buttons:
        text = el["text"].lower()
        if any(h in text for h in submit_hints):
            guess["login_button"] = el["selector"]
            break
    if not guess["login_button"]:
        for el in buttons:
            if el["attrs"].get("type") == "submit":
                guess["login_button"] = el["selector"]
                break

    return guess


# ── Inspection ────────────────────────────────────────────────────────────────

# Cookie-consent overlays intercept clicks on the login button, and some
# (Just Eat) gate the login form out of the DOM entirely until consent is
# handled. Dismiss the common ones before inspecting.
COOKIE_ACCEPT_SELECTORS = [
    "#onetrust-accept-btn-handler",              # OneTrust — Deliveroo
    "button[aria-label='Accept all cookies']",   # Feedr
    "[data-test-id='cookie-banner-accept']",
    "button:has-text('Accept all')",
    "button:has-text('Accept All')",
    "button:has-text('Accept cookies')",
    "button:has-text('I accept')",
    "#ccc-recommended-settings",
]


def dismiss_cookie_banner(page) -> str:
    """Click the first cookie-accept control that is actually visible."""
    for selector in COOKIE_ACCEPT_SELECTORS:
        try:
            el = page.query_selector(selector)
            if el and el.is_visible():
                el.click(timeout=5_000)
                page.wait_for_timeout(1_500)
                return selector
        except Exception:
            continue
    return ""


def collect_elements(page) -> tuple:
    inputs = []
    for i, el in enumerate(page.query_selector_all("input, textarea")):
        attrs = {k: el.get_attribute(k) for k in ATTRS
                 if el.get_attribute(k) is not None}
        tag = el.evaluate("e => e.tagName").lower()
        inputs.append({"attrs": attrs, "tag": tag,
                       "selector": suggest_selector(attrs, tag, i)})

    buttons = []
    for i, el in enumerate(page.query_selector_all(
            "button, input[type='submit'], input[type='button'], "
            "a[role='button']")):
        attrs = {k: el.get_attribute(k) for k in ATTRS
                 if el.get_attribute(k) is not None}
        tag = el.evaluate("e => e.tagName").lower()
        text = " ".join((el.text_content() or "").split())[:60]
        buttons.append({"attrs": attrs, "tag": tag, "text": text,
                        "selector": suggest_selector(attrs, tag, i, text)})

    return inputs, buttons


def fmt_attrs(attrs: dict) -> str:
    parts = [f"{k}={attrs[k]!r}" for k in ATTRS if attrs.get(k)]
    return "      " + ", ".join(parts) if parts else "      (no relevant attrs)"


CHALLENGE_MARKERS = (
    "performing security verification",
    "just a moment",
    "checking your browser",
    "verify you are human",
    "enable javascript and cookies",
)


def looks_like_bot_challenge(page) -> str:
    """Return the marker text if the page is an interstitial, else ''."""
    try:
        body = (page.text_content("body") or "").lower()
    except Exception:
        return ""
    for marker in CHALLENGE_MARKERS:
        if marker in body:
            return marker
    return ""


def inspect(page, target: str, label: str, config_file: str,
            login_url: str, wait_seconds: int = 0) -> dict:
    print(f"\n{'='*70}")
    print(f" {label}  ({target})")
    print(f" {login_url}")
    print(f"{'='*70}")

    try:
        page.goto(login_url, wait_until="domcontentloaded", timeout=45_000)
    except Exception as exc:
        print(f"\n  [✗] Could not load the page: {exc}")
        return {"target": target, "error": str(exc)}

    try:
        page.wait_for_load_state("networkidle", timeout=20_000)
    except Exception:
        pass  # SPAs may never go fully idle — inspect whatever rendered

    # A bot challenge resolves itself after a few seconds in a real browser.
    # Poll rather than sleeping blindly so we stop as soon as it clears.
    marker = looks_like_bot_challenge(page)
    if marker:
        budget = max(wait_seconds, 25)
        print(f"\n  [🛡] Bot challenge detected ({marker!r}) — "
              f"waiting up to {budget}s for it to clear...")
        for _ in range(budget // 5):
            page.wait_for_timeout(5_000)
            marker = looks_like_bot_challenge(page)
            if not marker:
                print(f"  [✓] Challenge cleared — now at {page.url}")
                try:
                    page.wait_for_load_state("networkidle", timeout=15_000)
                except Exception:
                    pass
                break
        else:
            print(f"  [✗] Challenge did not clear within {budget}s.")
    elif wait_seconds:
        page.wait_for_timeout(wait_seconds * 1_000)

    shot = SCREENSHOT_DIR / f"login_{target}.png"
    try:
        page.screenshot(path=str(shot), full_page=True)
    except Exception:
        shot = None

    final_url = page.url
    if final_url.rstrip("/") != login_url.rstrip("/"):
        print(f"\n  [!] Redirected to: {final_url}")

    cookie_sel = dismiss_cookie_banner(page)
    if cookie_sel:
        print(f"\n  [🍪] Dismissed cookie banner via {cookie_sel!r}")
        print(f"       → set selectors.cookie_accept to this in the config")

    inputs, buttons = collect_elements(page)

    print(f"\n  [INPUTS] {len(inputs)} found")
    for i, el in enumerate(inputs):
        print(f"    [{i}] <{el['tag']} type={el['attrs'].get('type', 'text')!r}>"
              f"  → {el['selector']}")
        print(fmt_attrs(el["attrs"]))

    print(f"\n  [BUTTONS] {len(buttons)} found")
    for i, el in enumerate(buttons):
        print(f"    [{i}] <{el['tag']}> text={el['text']!r}  → {el['selector']}")
        print(fmt_attrs(el["attrs"]))

    guess = classify(inputs, buttons)

    print(f"\n  {'-'*66}")
    print(f"  SUGGESTED — paste into {config_file} under `selectors:`")
    print(f"  {'-'*66}")
    for key in ("username_field", "password_field", "login_button"):
        value = guess[key]
        mark = " " if value else "?"
        print(f"  {mark} {key}: {value!r}")
    if not all(guess.values()):
        print("\n  [!] Blank entries mean the page did not expose an obvious "
              "match —\n      it may be multi-step, JS-gated, or bot-protected. "
              "Re-run headed\n      for that portal and read the DOM by hand.")
    if shot:
        print(f"\n  [📸] {shot}")

    return {"target": target, "guess": guess, "inputs": len(inputs),
            "buttons": len(buttons), "final_url": final_url,
            "cookie_accept": cookie_sel}


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="DOM login-selector inspector")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--portal", choices=sorted(PORTALS),
                       help="A report-export portal (default: oracle)")
    group.add_argument("--partner", choices=partner_configs(),
                       help="A per-order scraping partner from partners/")
    group.add_argument("--all-partners", action="store_true",
                       help="Sweep every partners/*.yaml plus Deliveroo")
    parser.add_argument("--headless", action="store_true",
                        help="No browser window and no prompt (for sweeps/CI)")
    parser.add_argument("--no-prompt", action="store_true",
                        help="Headed browser but do not wait for ENTER")
    parser.add_argument("--profile", action="store_true",
                        help="Use a persistent browser profile under state/. "
                             "Needed for Cloudflare-protected portals, whose "
                             "clearance cookie must survive between runs.")
    parser.add_argument("--wait", type=int, default=0, metavar="SECONDS",
                        help="Pause after load to let a bot challenge resolve")
    parser.add_argument("--list", action="store_true",
                        help="List every available target and exit")
    args = parser.parse_args()

    if args.list:
        print("\n  Report-export portals (--portal):")
        for k, (_, label) in sorted(PORTALS.items()):
            print(f"    {k:<14} {label}")
        print("\n  Per-order scraping partners (--partner):")
        for p in partner_configs():
            print(f"    {p}")
        print()
        return 0

    # Build the list of (target, config_file) to inspect
    if args.all_partners:
        targets = [("deliveroo", "deliveroo_config.yaml")]
        targets += [(p, f"partners/{p}.yaml") for p in partner_configs()]
    elif args.partner:
        targets = [(args.partner, f"partners/{args.partner}.yaml")]
    else:
        key = args.portal or "oracle"
        targets = [(key, PORTALS[key][0])]

    headless = args.headless or args.all_partners
    results = []

    # A realistic UA matters for bot-protected portals — the default headless
    # Chromium string is an instant tell.
    UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    ctx_kwargs = {"user_agent": UA, "viewport": {"width": 1400, "height": 900},
                  "locale": "en-GB", "timezone_id": "Europe/London"}

    with sync_playwright() as p:
        if args.profile:
            # Persistent profile: Cloudflare issues a clearance cookie that must
            # survive between runs, which a fresh context throws away.
            #
            # This path MUST match what partner_scraper.py uses
            # (STATE_ROOT/<partner>/browser_profile), otherwise seeding the
            # profile here does nothing for the real pipeline.
            scope = args.partner or args.portal or "shared"
            profile_dir = BASE_DIR / "state" / scope / "browser_profile"
            profile_dir.mkdir(parents=True, exist_ok=True)
            print(f"  [profile] {profile_dir}")
            context = p.chromium.launch_persistent_context(
                str(profile_dir), headless=headless,
                slow_mo=0 if headless else 300, **ctx_kwargs)
            browser = None
            page = context.pages[0] if context.pages else context.new_page()
        else:
            browser = p.chromium.launch(headless=headless,
                                        slow_mo=0 if headless else 300)
            context = browser.new_context(**ctx_kwargs)
            page = context.new_page()

        for target, config_file in targets:
            try:
                cfg = load_config(config_file)
            except Exception as exc:
                print(f"\n[✗] {target}: cannot read {config_file}: {exc}")
                results.append({"target": target, "error": str(exc)})
                continue

            label = cfg.get("portal", {}).get("name") or target
            login_url = cfg.get("portal", {}).get("login_url", "")
            if not login_url:
                print(f"\n[✗] {target}: no portal.login_url in {config_file}")
                results.append({"target": target, "error": "no login_url"})
                continue

            results.append(inspect(page, target, label, config_file, login_url,
                                   wait_seconds=args.wait))

        if not headless and not args.no_prompt:
            print(f"\n{'='*70}")
            print(" ACTION REQUIRED:")
            print(f"  1. Paste the suggested selectors into the config.")
            print(f"  2. Capture order-list selectors while logged in:")
            print(f"       python partner_scraper.py --partner <name> --debug")
            print(f"{'='*70}\n")
            input("  Press ENTER to close the browser...")

        context.close()
        if browser is not None:
            browser.close()

    # ── Sweep summary ─────────────────────────────────────────────────
    if len(results) > 1:
        print(f"\n{'='*70}")
        print(" SWEEP SUMMARY")
        print(f"{'='*70}\n")
        print(f"  {'Target':<20} {'user':<6} {'pass':<6} {'button':<7} Notes")
        print(f"  {'-'*20} {'-'*6} {'-'*6} {'-'*7} {'-'*20}")
        for r in results:
            if r.get("error"):
                print(f"  {r['target']:<20} {'—':<6} {'—':<6} {'—':<7} "
                      f"{r['error'][:40]}")
                continue
            g = r["guess"]
            tick = lambda v: "✓" if v else "✗"  # noqa: E731
            print(f"  {r['target']:<20} {tick(g['username_field']):<6} "
                  f"{tick(g['password_field']):<6} "
                  f"{tick(g['login_button']):<7} "
                  f"{r['inputs']} inputs, {r['buttons']} buttons")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
