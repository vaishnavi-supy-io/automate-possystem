#!/bin/bash
# Start a real Chrome that partner_scraper.py can attach to.
#
# WHY: Cloudflare blocks every Playwright-LAUNCHED browser on
# partner-hub.just-eat.co.uk — headless and headed alike (measured
# 2026-09-02: 90s and 60s respectively, challenge never cleared, checkbox
# never even offered). A real Chrome you log into yourself has a genuine
# fingerprint and a live clearance cookie, so it is left alone; the scraper
# then drives that browser over the DevTools protocol.
#
#   1. ./start_chrome_cdp.sh
#   2. In the window that opens, go to partner-hub.just-eat.co.uk and log in.
#      Clear the Cloudflare check like a normal user.
#   3. LEAVE THE WINDOW OPEN. Then run:
#        .venv/bin/python partner_scraper.py --partner just_eat
#
# The profile below is persistent, so the clearance and session usually
# survive for days — only the first login needs anyone present. When the
# session lapses, log in again in the same window.
#
# The scraper NEVER closes this browser; it only detaches.

PORT="${CDP_PORT:-9222}"
PROFILE="${CDP_PROFILE:-$HOME/.chrome-partner-hub}"
CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

if [ ! -x "$CHROME" ]; then
  echo "Chrome not found at: $CHROME" >&2
  echo "Install Chrome, or edit CHROME in this script." >&2
  exit 1
fi

if curl -s --max-time 2 "http://localhost:$PORT/json/version" >/dev/null 2>&1; then
  echo "Chrome is already listening on port $PORT — reusing it."
  echo "Attach with: browser.cdp_endpoint: http://localhost:$PORT"
  exit 0
fi

mkdir -p "$PROFILE"
echo "Starting Chrome with remote debugging on port $PORT"
echo "  profile: $PROFILE   (persistent — keeps the Cloudflare clearance)"
echo ""
echo "Log into partner-hub.just-eat.co.uk in the window that opens, then"
echo "leave it running and start the scraper."

"$CHROME" \
  --remote-debugging-port="$PORT" \
  --user-data-dir="$PROFILE" \
  --no-first-run \
  --no-default-browser-check \
  >/dev/null 2>&1 &

sleep 3
if curl -s --max-time 5 "http://localhost:$PORT/json/version" >/dev/null 2>&1; then
  echo ""
  echo "✓ Chrome is listening on http://localhost:$PORT"
else
  echo "" >&2
  echo "!! Chrome did not open a debugging port. If Chrome was ALREADY running" >&2
  echo "   with your normal profile, quit it fully and re-run this script —" >&2
  echo "   Chrome ignores --remote-debugging-port when an instance is live." >&2
  exit 1
fi
