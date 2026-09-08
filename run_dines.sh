#!/bin/bash
# Black Bear Burger (Dines) daily POS report — for local cron.
#
# WHY THIS EXISTS ALONGSIDE .github/workflows/dines_daily.yml:
# every scheduled workflow on this repo is currently `disabled_inactivity`,
# because GitHub measures activity on what is PUSHED and origin/main has not
# moved since 2026-06-18. Until the commits are pushed and the workflows
# re-enabled, GitHub Actions will not fire — this script is the automation
# that actually runs today.
#
# Install (07:00 London, daily):
#   crontab -e
#   0 7 * * *  /Users/macbook/supy/supy-ai-agents/automate-possystem/run_dines.sh
#
# Credentials come from .env (never arguments, never this file). Branches with
# no credentials are skipped as `no_creds`, so this stays green while only
# 4 of the 9 configured branches have logins.

cd /Users/macbook/supy/supy-ai-agents/automate-possystem || exit 1

LOG="logs/cron_dines_$(date +%Y%m%d).log"

{
  echo "======================================"
  echo "Started: $(date)"
  echo "Args:    $*"
  echo "======================================"
} >> "$LOG"

.venv/bin/python dines_automation.py --all-branches "$@" >> "$LOG" 2>&1
EXIT=$?

{
  echo ""
  echo "Finished: $(date)  exit=$EXIT"
  echo ""
} >> "$LOG"

# Exit codes: 0 ok (including no-sales and no-credential skips), 2 a branch
# failed — see logs/dines_*.jsonl and screenshots/dines_*.
exit $EXIT
