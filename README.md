# POS Automation Pipeline

Automated daily sales report pipeline for Supy. Logs into POS portals using a headless browser, downloads raw sales data, transforms it into the Supy upload format, and emails the final `.xlsx` file — one per branch — every day. No manual browser interaction required.

Two independent pipelines are included:

| Pipeline | Portal | Client | Script |
|---|---|---|---|
| Oracle BI | `reports.indpt.com` | Independent (indpt) | `automation.py` |
| Sapapad | `pos.sapaad.com` | Bake My Day | `sapapad_automation.py` |

---

## How it works

Every pipeline runs through four stages in sequence:

```
Stage 1: Auth        Login to the portal (reuses cached session when valid)
Stage 2: Download    Navigate to the report page, trigger export, download the file
Stage 3: Transform   Clean + reshape raw data into Supy's column format
Stage 4: Email       Attach the .xlsx to an email and send via Gmail SMTP
```

Each stage is retried on transient failures (up to 3 attempts, exponential backoff: 1.5 s → 3 s → 6 s). Auth and transform errors are never retried — they surface immediately. A checkpoint file is written after each stage so a partial run can be resumed with `--from-stage N`.

---

## Project structure

```
automate-possystem/
├── automation.py              # Oracle BI pipeline
├── config.yaml                # Oracle BI config (selectors, columns, nav chain)
├── sapapad_automation.py      # Sapapad pipeline
├── sapapad_config.yaml        # Sapapad config (selectors, columns, nav chain)
├── mappings/
│   └── sapapad_item_codes.csv # Bake My Day item master (item_id lookup table)
├── debug_selectors.py         # One-shot headed browser to inspect portal selectors
├── requirements.txt
├── .env                       # Credentials (never committed)
├── .env.example               # Template for .env
├── downloads/                 # Raw files from the portal (gitignored)
├── output/                    # Final .xlsx reports (gitignored)
├── state/                     # Browser session cache + checkpoint files (gitignored)
│   └── sapapad/               # Sapapad-specific session state
├── logs/                      # JSONL structured logs, one file per run (gitignored)
├── screenshots/               # Per-run browser screenshots for debugging (gitignored)
└── tests/
    └── test_transform.py      # Offline unit tests for Stage 3
```

---

## Setup

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure credentials

Copy `.env.example` to `.env` and fill in every value:

```bash
cp .env.example .env
```

```ini
# Oracle BI portal
PORTAL_USERNAME=your_username
PORTAL_COMPANY=your_company
PORTAL_PASSWORD=your_password

# Sapapad portal
SAPAPAD_USERNAME=supy@bakemyday.me
SAPAPAD_PASSWORD=your_password

# Gmail — must be an App Password, not your account password
# Create one at: myaccount.google.com/apppasswords
GMAIL_USER=you@supy.io
GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx

# Report recipients
REPORT_RECIPIENT=recipient@supy.io          # Oracle BI reports
SAPAPAD_REPORT_RECIPIENT=recipient@supy.io  # Sapapad reports
```

---

## Running the pipelines

### Oracle BI (single combined report)

```bash
python automation.py                  # headless, full pipeline + email
python automation.py --debug          # headed browser, verbose output
python automation.py --no-email       # download + transform only, no email
python automation.py --from-stage 3   # re-run transform on existing raw file
python automation.py --force-login    # ignore cached session, re-authenticate
```

### Sapapad — per-branch (recommended daily run)

```bash
python sapapad_automation.py --per-branch
```

This logs in **once**, auto-discovers all branches from the location dropdown, then for each branch independently:

1. Navigates to the Top Grossing Items report page
2. Selects only that branch in the multi-location filter
3. Sets the date filter to **Yesterday**
4. Clicks the export icon and dismisses the confirmation modal
5. Waits ~25 seconds for the server-side async export to complete
6. Polls the Saved Reports page (up to 6 × 15 s) until the download link appears
7. Downloads the CSV, transforms it, and emails the `.xlsx`

Branches with zero rows (no sales yesterday) are skipped — no email sent, no error raised.

**Example output:**

```
[Sapapad Per-Branch Pipeline] run_id=20260612T115954_df0df837

[Stage 1] Authentication...
[Stage 1] ✓ Authenticated

[Branches] Found 9 branches: ['BMD Al Danah Abu Dhabi', 'BMD Business Bay', ...]

────────────────────────────────────────────────────────────
[1/9] Branch: BMD Al Danah Abu Dhabi
────────────────────────────────────────────────────────────
[Stage 2] Downloading BMD Al Danah Abu Dhabi...
[Stage 2] ✓ Downloaded → sapapad_BMD_Al_Danah_Abu_Dhabi_..._raw.csv

[Stage 3] Transforming...
[Stage 3] ✓ 29 rows → sapapad_BMD_Al_Danah_Abu_Dhabi_2026-06-12_20260612.xlsx

[Stage 4] Sending email for BMD Al Danah Abu Dhabi...
[Stage 4] ✓ Email sent → recipient@supy.io
```

### Sapapad — all branches combined (single report)

```bash
python sapapad_automation.py          # all locations, one combined report
python sapapad_automation.py --debug  # headed browser
python sapapad_automation.py --no-email
python sapapad_automation.py --from-stage 3
python sapapad_automation.py --force-login
```

---

## Bake My Day branches (Sapapad)

The pipeline auto-discovers these from the portal at runtime:

| Branch | Notes |
|---|---|
| BMD Al Danah Abu Dhabi | Active |
| BMD Business Bay | Active |
| BMD Ck | Often 0 rows |
| BMD DIC | Active |
| BMD Events | Often 0 rows |
| BMD JVC | Active |
| BMD Khalifa City Abu Dhabi | Active |
| BMD Mirdif | Active |
| Bmd Sharjah | Active |

---

## Output file format

Every report is an `.xlsx` file written to `output/` with the filename:

```
sapapad_{branch_name}_{YYYY-MM-DD}_{run_id_prefix}.xlsx
```

It contains the following columns in Supy's required upload order:

| Column | Source |
|---|---|
| `Sales Date *` | Yesterday's date (computed at runtime, format: `15-Jun-2026`) |
| `POS Item ID *` | Resolved from item master via item name matching |
| `POS Item Name` | `Item Name` from Sapapad CSV |
| `Sold QTY *` | `Total Sold` from Sapapad CSV |
| `Total Discount Value` | Empty (Sapapad has no discount column) |
| `Total sales excl. tax *` | `Total Amount Excluding Tax` |
| `Total sales incl. tax *` | `Total Amount` |
| `Order ID` | Empty |
| `Sales Type Code` | Empty |

For Oracle BI, `POS Item ID *` comes directly from `Menu Item #` in the raw export (no lookup needed).

---

## Item code matching (Sapapad only)

Sapapad's CSV does not include Supy item IDs. The pipeline resolves them by joining on **item name** against `mappings/sapapad_item_codes.csv` (the Bake My Day item master exported from Supy).

Some items share the same name across different menu categories (e.g. "The OG Cookie" exists in both `THE LEGENDS` and `EVENT MENU`). The matcher handles this with three-tier resolution:

| Tier | Rule |
|---|---|
| 1 | Exact `item_name` + exact `category_name` → unique match → use it |
| 2 | Normalised name + normalised category (lowercase, trailing `.` stripped) → prefer exact category match among candidates |
| 3 | Normalised name only → prefer the first row that is **not** in EVENT MENU |

Unmatched items are logged as warnings in the JSONL log and printed to stderr. The report is still produced — unmatched rows will have an empty `POS Item ID *`.

### Updating the item master

1. Export the latest item list from Supy
2. Save as `mappings/sapapad_item_codes.csv`
3. Required columns: `item_name`, `item_id`, `category_name`

---

## Configuration files

All portal-specific settings live in YAML — no Python changes needed for routine updates.

### Key sections

```yaml
portal:
  login_url: "https://pos.sapaad.com/"
  report_url: "https://pos.sapaad.com/reports/top_grossing_items?order_by=totalamount&sort_by=DESC"
  authenticated_element: ".LoginDetRight"  # only present when logged in

selectors:
  username_field: "#user_email"
  password_field: "#user_password"
  login_button: "button.mt-5"

navigation:          # steps executed in order each time a report is triggered
  - step: "Open date filter"
    action: click
    click: ".dateFilterToggle"
    wait: "a.customDateSelection"

  - step: "Select Yesterday"
    action: click
    click: "a.customDateSelection:has-text('Yesterday')"
    wait: ".dateFilterToggle:has-text('Yesterday')"

  - step: "Wait for export to process"
    action: wait_seconds
    seconds: 25

  - step: "Download CSV"
    action: download_latest
    poll_attempts: 6
    poll_interval_s: 15

columns:             # raw CSV column → Supy output column
  - raw: "Item Name"
    target: "POS Item Name"
  - raw: null
    target: "Sales Date *"
    inject: date_yesterday
```

**Supported navigation action types:**

| Action | Description |
|---|---|
| `click` | Wait for selector, click it, optionally wait for a follow-up element |
| `wait_seconds` | Sleep N seconds (used after triggering async server-side exports) |
| `accept_modal` | Click an OK/close button inside an HTML modal dialog |
| `goto_url` | Navigate to a hardcoded URL |
| `download_latest` | Poll the page (with page reloads) until a download link appears, then download |

**Supported inject strategies:**

| Value | Behaviour |
|---|---|
| `date_yesterday` | Yesterday's date in `output_date_format` |
| `date_from_filename` | Date extracted from the raw filename (YYYYMMDD pattern) |
| `date_from_metadata` | Date scanned from the first 10 rows of the raw file |
| `business_dates_metadata` | Oracle BI-specific: reads the `Business Dates` metadata row |
| `empty` | Empty string |

---

## Session caching

After a successful login, the browser session (cookies + local storage) is saved to `state/storage_state.json` (or `state/sapapad/storage_state.json` for Sapapad). On the next run, the pipeline reloads the session and checks whether it is still valid by navigating to the portal and looking for `authenticated_element`. If valid, Stage 1 completes instantly without touching the login form.

To force fresh authentication: add `--force-login`.

---

## Logging

Each run writes a structured JSONL log to `logs/sapapad_<run_id>.jsonl`. Every entry:

```json
{
  "ts": "2026-06-12T11:59:54.123456",
  "run_id": "20260612T115954_df0df837",
  "pipeline": "sapapad",
  "stage": "transform",
  "step": "item_code_match",
  "outcome": "warning",
  "duration_ms": 142,
  "message": "3 rows unmatched",
  "unmatched_keys": ["Seasonal Special"]
}
```

Screenshots are saved per-run under `screenshots/sapapad_<run_id>/` at every stage boundary — useful for debugging failed runs without re-running the browser.

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | All branches succeeded |
| `1` | Auth error — bad credentials or session could not be established |
| `2` | Navigation/download error — after 3 retries |
| `3` | Transform error — raw file exists but could not be processed |
| `4` | Email error — report generated but not delivered |

In `--per-branch` mode, a single branch failure does not abort the run. Other branches continue and the final exit code is `1` if any branch failed.

---

## Running tests

Unit tests cover Stage 3 (transform) entirely offline — no browser, no credentials, no network:

```bash
python -m pytest tests/ -v
```

---

## Scheduling (daily automation)

To run every day at 08:00 AM via cron:

```bash
crontab -e
```

```cron
0 8 * * * cd /path/to/automate-possystem && .venv/bin/python sapapad_automation.py --per-branch >> logs/cron.log 2>&1
```

For CI/CD, use a GitHub Actions `schedule` trigger and store `.env` values as repository secrets.

---

## Troubleshooting

**Login fails / selectors not found**
Run `python debug_selectors.py` — it opens a headed browser on the login page, prints every input selector it finds, and takes a screenshot.

**"Download link not found after 6 attempts"**
The server-side export took longer than 6 × 15 s = 90 s. Increase `poll_attempts` or `poll_interval_s` in `sapapad_config.yaml`. This can happen when the Sapapad server is under load.

**Item codes all NaN / empty in output**
The item master at `mappings/sapapad_item_codes.csv` may be outdated or have a column name mismatch. Verify that `item_name` in the CSV matches what Sapapad exports in the `Item Name` column.

**Gmail "Username and Password not accepted"**
`GMAIL_APP_PASSWORD` must be a 16-character Google App Password, not your regular Gmail password. 2-Step Verification must be enabled on the sending account.

**Branch report contains data from multiple branches**
The Sapapad location filter persists across page navigations. The `select_location` function in `sapapad_automation.py` handles this by reading the current checkbox state before acting — if "All Locations" is already unchecked (a previous single branch is still selected), it first selects all, then deselects all, before selecting the target branch. If you see cross-contamination, add `--force-login` to force a clean session.
