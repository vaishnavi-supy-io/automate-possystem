#!/bin/bash
# Street Food Ltd. — all delivery-partner pipelines in one run.
#
# Exists so the recipient list is not retyped from memory. Three addresses go
# on the roll-up summary; customer care was added 2026-09-01 on request.
#
#   ./run_streetfood.sh                  # yesterday
#   ./run_streetfood.sh --date 2026-08-31
#   ./run_streetfood.sh --only feedr,ordit
#
# These addresses now apply to EVERY email this run sends — each per-partner
# report and the roll-up summary. (Until 2026-09-02 --email-to reached only the
# summary, because run_all_partners.py did not forward it and the engines had
# no such flag; customer care was on the summary alone.) REPORT_RECIPIENT in
# .env is left alone, so no other client's reports are affected.

cd /Users/macbook/supy/supy-ai-agents/automate-possystem || exit 1

RECIPIENTS=(
  --email-to vaishnavi@supy.io
  --email-to charlotte@supy.io
  --email-to customer.care@supy.io
)

LOG="logs/streetfood_$(date +%Y%m%d).log"

{
  echo "======================================"
  echo "Started: $(date)"
  echo "Args:    $*"
  echo "======================================"
} >> "$LOG"

.venv/bin/python run_all_partners.py "${RECIPIENTS[@]}" "$@" >> "$LOG" 2>&1
EXIT=$?

{
  echo ""
  echo "Finished: $(date)  exit=$EXIT"
  echo ""
} >> "$LOG"

exit $EXIT
