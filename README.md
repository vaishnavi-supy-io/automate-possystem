# Oracle BI POS Sales Report Automation

Automates the full end-to-end process of downloading a daily sales report from an Oracle BI portal and transforming it into a clean, Supy-ready Excel file — no manual browser interaction required.

---

## What It Does

Every run executes a 3-stage pipeline:

```
Stage 1 — Auth        Login to Oracle BI portal (with session caching)
Stage 2 — Navigate    Click through the report menu and trigger the file download
Stage 3 — Transform   Clean & reformat the raw file → formatted .xlsx for Supy upload
```

The output is a formatted `.xlsx` file written to `output/` that matches exactly the column schema Supy's POS upload expects.

---

## Project Structure

```
automate-possystem/
├── automation.py          # Main pipeline — all 3 stages
├── config.yaml            # Single source of truth: URLs, selectors, nav chain, column mapping
├── debug_selectors.py     # One-time DOM inspector — run this before first use
├── requirements.txt       # Python dependencies
├── .env.example           # Template for credentials
│
├── downloads/             # Raw files downloaded from Oracle BI (auto-created)
├── output/                # Transformed .xlsx files ready for Supy (auto-created)
├── state/                 # Session cache + last checkpoint (auto-created)
├── logs/                  # Structured JSONL logs per run (auto-created)
├── screenshots/           # Browser screenshots per stage per run (auto-created)
└── tests/
    └── test_transform.py  # Offline unit tests for Stage 3 (no browser/credentials needed)
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

### 2. Create your `.env` file

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
PORTAL_USERNAME=your_username
PORTAL_COMPANY=your_company
PORTAL_PASSWORD=your_password
```

### 3. Discover login selectors (first-time only)

The login form selectors are specific to your Oracle BI deployment. Run the DOM inspector once to find them:

```bash
python debug_selectors.py
```

This opens a **headed browser** on the login page, prints every `<input>` field and button with suggested CSS selectors, and takes a screenshot. Copy the suggested selectors into `config.yaml` under `selectors:`.

### 4. Configure `config.yaml`

All configuration lives in one file — no Python changes needed:

| Section | What to configure |
|---|---|
| `portal.login_url` | URL of the Oracle BI login page |
| `portal.portal_url` | URL to verify a valid session |
| `portal.authenticated_element` | CSS selector of an element that only exists when logged in |
| `selectors.*` | CSS selectors for login fields (from `debug_selectors.py`) |
| `navigation` | The sequence of clicks to reach the Sales Repo download |
| `columns` | Raw → target column name mapping, types, drops, and injected columns |
| `output_column_order` | Final column order in the output file |
| `raw_date_format` | Date format in the raw file (e.g. `"%d/%m/%Y"`) |
| `output_date_format` | Date format to write in the output (e.g. `"%d-%b-%Y"`) |

---

## Running the Pipeline

### Full pipeline (headless)

```bash
python automation.py
```

### Debug mode — headed browser + verbose logging

```bash
python automation.py --debug
```

### Replay transform only (skip login/download)

Useful when you already have the raw file and only want to re-run the transformation:

```bash
python automation.py --from-stage 3
```

### Force re-authentication (ignore cached session)

```bash
python automation.py --force-login
```

---

## CLI Reference

| Flag | Description |
|---|---|
| *(none)* | Run full pipeline headlessly |
| `--debug` | Headed browser + verbose console output + screenshots at every step |
| `--from-stage N` | Start from stage N (1=auth, 2=nav, 3=transform) |
| `--force-login` | Bypass the session cache and always re-authenticate |

### Exit Codes

| Code | Meaning |
|---|---|
| `0` | Pipeline completed successfully |
| `1` | `AuthError` — wrong credentials or login blocked; do not retry automatically |
| `2` | `NavError` — menu navigation or download failed (auto-retried up to 3 times with backoff) |
| `3` | `TransformError` — data transformation failed; raw file is preserved in `downloads/` |

---

## Output File

The formatted `.xlsx` is written to `output/` with the filename:

```
sales_report_YYYY-MM-DD_<run_id>.xlsx
```

It contains the following columns in order:

| Column | Source |
|---|---|
| `Sales Date *` | Extracted from the `Business Dates` metadata row in the raw file |
| `POS Item ID *` | `Menu Item #` |
| `POS Item Name` | `Menu Item Name` |
| `Sold QTY *` | `Qty Sold` |
| `Total Discount Value` | `Discounts` |
| `Total sales excl. tax *` | `Net VAT after Disc.` |
| `Total sales incl. tax *` | `Gross after Disc.` |
| `Order ID` | *(empty — injected)* |
| `Sales Type Code` | *(empty — injected)* |

Aggregate "Totals:" rows and all raw columns not in the mapping are automatically stripped.

---

## Session Caching

After a successful login, the browser session is saved to `state/storage_state.json`. On subsequent runs the pipeline reloads this session and checks whether it's still valid (by loading `portal.portal_url` and looking for `authenticated_element`). If the session is valid, Stage 1 completes instantly without touching the login form.

To force a fresh login: `python automation.py --force-login`

---

## Logging

Each run produces a structured JSONL log file at `logs/<run_id>.jsonl`. Every log line is a JSON object:

```json
{
  "ts": "2026-04-15T10:23:01.123456",
  "run_id": "20260415T102300_a1b2c3d4",
  "stage": "nav",
  "step": "download",
  "outcome": "ok",
  "duration_ms": 4521,
  "file": "downloads/20260415T102300_a1b2c3d4_raw.xlsx",
  "size_bytes": 48320
}
```

Screenshots are saved per run under `screenshots/<run_id>/`.

---

## Retry Logic

Stage 2 (navigation + download) retries up to **3 times** with exponential backoff (1.5 s → 3 s → 6 s) on any `NavError`. Auth errors and transform errors are **never retried** automatically.

---

## Running Tests

Unit tests cover Stage 3 (transform) entirely offline — no browser, no credentials, no network:

```bash
# Activate your venv first
python -m pytest tests/ -v
```

The test suite covers:

- Header-row detection in `.xlsx` and `.csv` raw files
- Business Dates extraction from metadata rows
- Totals-row stripping
- Correct column schema and order
- Data value and type correctness (int IDs, float financials)
- Date format conversion (`15/04/2026` → `15-Apr-2026`)
- Injected empty columns (`Order ID`, `Sales Type Code`)
- `config.yaml` structural integrity

---

## Modifying the Pipeline

**Change login selectors or navigation steps** → edit `config.yaml` only.  
**Change column mapping or output format** → edit `config.yaml` only.  
**No Python code changes needed for routine updates.**

To add a new injected column, add an entry to `columns` with `inject: empty` (or implement a new inject strategy in `stage_transform`) and add the target name to `output_column_order`.
