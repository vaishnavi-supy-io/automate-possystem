#!/bin/bash
# Daily Sapapad POS report runner — called by cron at 8am Dubai time

cd /Users/macbook/supy/supy-ai-agents/automate-possystem

LOG=logs/cron_$(date +%Y%m%d).log

echo "======================================" >> "$LOG"
echo "Started: $(date)" >> "$LOG"
echo "======================================" >> "$LOG"

.venv/bin/python sapapad_automation.py --per-branch >> "$LOG" 2>&1
EXIT=$?

echo "" >> "$LOG"
echo "Finished: $(date)  exit=$EXIT" >> "$LOG"
echo "" >> "$LOG"

exit $EXIT
