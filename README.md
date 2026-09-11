# POS Automation Pipeline

Automated daily sales report pipeline for Supy. Logs into POS portals using a headless browser, downloads raw sales data, transforms it into the Supy upload format, and emails the final `.xlsx` file — one per branch — every day. No manual browser interaction required.

Two independent pipelines are included:

| Pipeline | Portal | Client | Script |
|---|---|---|---|
| Oracle BI | `reports.indpt.com` | Independent (indpt) | `automation.py` |
| Sapapad | `pos.sapaad.com` | Bake My Day | `sapapad_automation.py` |
| Sapaad | `pos.sapaad.com` | Falafel Frayha | `sapapad_automation.py --config sapaad_falafel_config.yaml` |
| Sapaad | `pos.sapaad.com` | Heal Restaurant | `sapapad_automation.py --config sapaad_heal_config.yaml` |
| Sapaad | `pos.sapaad.com` | Pinza | `sapapad_automation.py --config sapaad_pinza_config.yaml` |

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
REPORT_RECIPIENT=vaishnavi@supy.io,csm@supy.io   # comma-separated list is supported
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

### Sapaad — the other three tenants

Falafel Frayha, Heal and Pinza are the same portal, so they run the same script
with a different config. Each config sets `tenant.slug`, which keeps that
account's cached session in its own `state/<slug>/` — sharing a `storage_state`
between two Sapaad accounts logs the first one out when the second runs.

```bash
python sapapad_automation.py --config sapaad_falafel_config.yaml --per-branch
python sapapad_automation.py --config sapaad_heal_config.yaml    --per-branch
python sapapad_automation.py --config sapaad_pinza_config.yaml   --per-branch
```

Branches are not listed in any config — `discover_locations()` reads them off
the portal's location dropdown at run time.

#### Before the first run

Each tenant needs two things that cannot be derived from the portal alone:

1. **Credentials**, set via the prompt so they never land in shell history:

   ```bash
   python set_credential.py SAPAAD_FALAFEL_FRAYHA_USERNAME
   python set_credential.py SAPAAD_FALAFEL_FRAYHA_PASSWORD
   ```

   Repeat for `SAPAAD_HEAL_RESTAURANT_*` and `SAPAAD_PINZA_SAPAAD_*`.

2. **The item master**, which is what resolves `POS Item ID`. In Sapaad:
   ☰ → Setup → Menu Setup → Upload Menus → *Download all items as CSV file*,
   then save it over the matching file in `mappings/`:

   | Tenant | Mapping file | Sapaad download |
   |---|---|---|
   | Falafel Frayha | `mappings/sapaad_falafel_item_codes.csv` | `falafel_all items.csv` |
   | Heal Restaurant | `mappings/sapaad_heal_item_codes.csv` | `HEAL_items.csv` |
   | Pinza | `mappings/sapaad_pinza_item_codes.csv` | `Pinza Restaurant LLC _items.csv` |

   The file must keep the `item_name`, `category_name` and `item_id` headers.
   Until it is supplied, every `POS Item ID` comes out blank and verification
   fails the run with a 0% match rate — deliberately, rather than emailing an
   unusable sheet.

### Paid modifiers (Marketing → Top Paid Modifiers)

The three Sapaad tenants fetch a **second** report in the same run and append
it beneath the grossing items in the same sheet, so Supy still ingests one file
per branch per day:

```
Sales Date | POS Item ID | POS Item Name | Sold QTY | ... ← grossing items
...
Sales Date | Extra Cheese | Extra Cheese | 8        | ... ← modifiers continue here
```

Two things differ from the grossing half:

- **No item-master lookup.** Modifiers are not in the item master, so the
  modifier *name* doubles as its POS code — it fills both `POS Item ID *` and
  `POS Item Name`.
- **Excl. tax is derived**, as `incl / 1.05` (UAE VAT 5%), because Sapaad
  reports paid modifiers at gross only. If the export ever gains an excl-tax
  column, name it under `modifiers.source_columns.excl` and it is used verbatim
  instead — a value the source states is never re-derived from its own gross.

Pass `--no-modifiers` to fetch grossing items only. BMD has no `modifiers`
block in its config, so it is unaffected.

A missing or failed modifiers export **degrades** the run rather than failing
it: the grossing report is still transformed and emailed, with the reason
logged and printed. Verification counts both raw CSVs, and the item-ID match
rate is measured on the grossing rows alone — counting modifiers, which always
carry an ID, would dilute a completely broken item mapping into looking fine.

### Sapapad — all branches combined (single report)

```bash
python sapapad_automation.py          # all locations, one combined report
python sapapad_automation.py --debug  # headed browser
python sapapad_automation.py --no-email
python sapapad_automation.py --from-stage 3
python sapapad_automation.py --force-login
python sapapad_automation.py --no-modifiers   # grossing items only
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

## All partners in one run (`run_all_partners.py`)

Runs every delivery partner end to end — login, download, convert to Supy
format, email — and finishes with one roll-up summary email.

```bash
python run_all_partners.py                             # yesterday, all partners
python run_all_partners.py --date 2026-08-18
python run_all_partners.py --only feedr,ordit
python run_all_partners.py --email-to vaishnavi@supy.io --email-to csm@supy.io
python run_all_partners.py --no-summary-email          # per-partner mails only
```

Each partner runs in its own subprocess, so one failure never stops the rest —
a partial day beats no day. Exit codes: `0` ok, `1` auth/config, `2` scrape/nav,
`3` transform, `4` email.

**Recipients.** `REPORT_RECIPIENT` accepts a comma- or semicolon-separated list,
and duplicates are dropped.

`--email-to` overrides `REPORT_RECIPIENT` for **both** the roll-up summary and
every per-partner report — fixed 2026-09-02. It used to reach the summary only:
`build_command` did not forward it and the engines had no such flag, so
per-partner reports always went to `REPORT_RECIPIENT` however it was invoked.
Both engines now accept `--email-to` (repeatable, and each value may itself be
a comma-separated list), and the runner passes every address through.

Use `./run_streetfood.sh` rather than retyping the recipient list — it carries
the three Street Food Ltd. addresses (vaishnavi@, charlotte@, and
customer.care@supy.io, added 2026-09-01) and forwards any other flags:

```bash
./run_streetfood.sh --date 2026-08-31
./run_streetfood.sh --only feedr,ordit
```

Because the flag now covers the per-partner reports too, `run_streetfood.sh`
puts customer care on every Street Food Ltd. email without touching
`REPORT_RECIPIENT` — so Oracle BI, Talabat, Dines and Black Bear reports are
unaffected and no client sees another client's data.

**The summary email** carries the run table in the body, attaches every `.xlsx`
written during the run, and names any partner that produced no file so it is
obvious what still has to be uploaded by hand:

```
  Partner              Result             Time
  -------------------- -------------- --------
  deliveroo            OK ok               41s
  feedr                OK ok               88s
  ordit                !! scrape/nav       12s

!! These partners did NOT produce a file and must be uploaded by hand:
     - ordit (scrape/nav)
```

**Credentials** live in `.env` only — never in a config file, a spreadsheet or a
chat message. Use `set_credential.py` to write them without echoing them to the
terminal. Each partner reads `<PARTNER>_USERNAME` / `<PARTNER>_PASSWORD`
(`UBER_EATS_PIN` for Uber Eats).

---

## Black Bear Burger — Deliveroo file converter (`blackbear_convert.py`)

Black Bear Burger emails a single multi-tab `.xlsx` exported from the Deliveroo
Looker report to customer care. There is no portal to scrape — this is a
file-in / file-out converter, not a browser pipeline.

```bash
python blackbear_convert.py "downloads/Items Sold 17th Aug.xlsx"
python blackbear_convert.py in.xlsx --out-dir output/bbb --vat-rate 0.20
python blackbear_convert.py in.xlsx --plu-file "New Deliveroo - BBB.xlsx" \
                                    --plu-file "New Deliveroo 20ft .xlsx"
python blackbear_convert.py in.xlsx --one-file-per-date
python blackbear_convert.py in.xlsx --year 2026     # override tab-name year inference
```

**Source layout.** One tab per sales date (`Items Sold 17th Aug`). Row 1 is a
merged Deliveroo restaurant name spanning three metric columns; row 2 is the
metric labels; the final row is Looker's grand total.

| Deliveroo metric | Supy column |
|---|---|
| `Count Orders (incl Undelivered)` | `Sold QTY *` |
| `Item Value Sum (before discounts)` − `(after item discounts)` | `Total Discount Value` |
| `Item Value Sum (after item discounts)` | `Total sales excl. tax *` |
| derived: excl. tax × (1 + VAT) | `Total sales incl. tax *` |
| `Menu Item Name` | `POS Item Name` |
| PLU CODE sheet, matched on item name | `POS Item ID *` |

Deliveroo's `Item Value Sum` figures are **net of VAT** — confirmed with the team
on 2026-08-26 — so the after-discount value is the excl.-tax figure and VAT is
added on top, never divided out.

**PLU codes.** `POS Item ID *` comes from the team's PLU masters, matched on item
name. The authoritative codes are the `sku-` / `mod-` scheme held on the
`Standard Plus` and `Modifiers Plus` tabs of two workbooks in Drive (Black Bear
Burger → POS/Sales → New Folder - Sales Upload BBB):

| Workbook | Brand |
|---|---|
| `New Deliveroo - BBB.xlsx` | Black Bear Burger |
| `New Deliveroo 20ft .xlsx` | 20Ft Fried Chicken |

Both are needed — the two brands merge into the same Supy branches. Pass
`--plu-file` once per workbook; the first code seen for an item wins and any
disagreement between masters is reported rather than silently overwritten.

The loader takes a tab named `PLU CODE` / `Standard Plus` / `Modifiers Plus`, or
falls back to the only sheet in a single-sheet file. Header positions are
sniffed, blank names and blank codes are skipped (doc step 6), numeric codes lose
Excel's trailing `.0`, matching is case- and whitespace-insensitive, and tabs
carrying a `Sales Date` column are skipped so a completed upload is never
mistaken for a master. An item with no PLU code keeps the item name as its
`POS Item ID *` and is listed in `_report.txt` — nothing is silently dropped.

> **Not the Kobas numbers.** `BBB PLUs.xlsx` in the same Drive folder holds a
> different scheme (numeric Kobas EPoS ids: `827` Black Bear, `1476` Fries).
> That is the Kobas till export, not the Deliveroo upload scheme — confirmed
> 2026-08-31. Do not mix the two.

**Branch merging.** Several Deliveroo restaurants feed one Supy branch — the
20Ft Fried Chicken virtual brand shares a kitchen with the Black Bear Burger
site. Rows are summed by item name per date. The mapping lives in
`mappings/blackbear_branches.csv`:

| Deliveroo restaurants | Supy branch |
|---|---|
| 20Ft Fried Chicken - Boxpark + Black Bear Burger - Boxpark | Black Bear Burger Shoreditch |
| 20Ft Fried Chicken - Brixton + Black Bear Burger - Brixton | Black Bear Burger Brixton |
| 20Ft Fried Chicken - Camden + Black Bear Burger Camden High Street | Black Bear Burger Camden |
| 20Ft Fried Chicken - Oxford Street + Black Bear Burger - Oxford Street | 20ft Chicken Oxford St |
| 20Ft Fried Chicken - Paddington + Black Bear Burger - Paddington | Black Bear Burger Paddington |
| 20Ft Fried Chicken - Westfield + Black Bear Burger - Westfield | Black Bear Burger Westfield ⚠ 20Ft leg assumed |
| Black Bear Burger - Canary Wharf | Black Bear Burger Canary Wharf |
| Black Bear Burger - Exmouth Market | Black Bear Burger Exmouth Market |
| Black Bear Burger - Victoria | Black Bear Burger Victoria |

An unmapped restaurant is **excluded** from the output and listed in the report
— it never silently lands in the wrong branch.

**Outputs** (`output/blackbear/<run-date>/` by default):

- `BlackBearBurger_Supy_POS_Upload_<from>_to_<to>.xlsx` — **the full POS sheet**:
  a `Read Me` tab, a `Summary` tab, and one tab per Supy branch
- `<Supy Branch>_<from>_to_<to>.xlsx` — the same data split one file per branch
- `_summary.csv` / `_summary.xlsx` — rows, qty and value per branch per date
- `_report.txt` — assumptions, skipped tabs, merges, reconciliation

**Emailing.** `--email` sends the combined workbook plus `_report.txt` from
`GMAIL_USER` via Gmail SMTP. Repeat the flag for more recipients:

```bash
python blackbear_convert.py in.xlsx --email vaishnavi@supy.io --email malak@supy.io
```

**Reconciliation.** Every per-restaurant gross and net total is checked against
Looker's own grand-total row. Quantities are deliberately *not* checked: the
total row reports a distinct order count, so it is smaller than the sum of the
per-item counts by design.

**Known data limits** (all repeated in `_report.txt`):

- `Count Orders` counts orders *containing* an item, not units sold, and
  includes undelivered orders. A quantity measure added to the Looker look
  would fix this at source.
- Deliveroo exports no item IDs. `POS Item ID *` comes from the client's PLU CODE
  sheet; without one, it falls back to the item name.
- Tabs that carry only counts and no `Item Value Sum` columns are skipped —
  they cannot fill the required sales columns.

---

## Dines — dashboard pipeline (`dines_automation.py`)

Black Bear Burger's dine-in sales come from the Dines dashboard. Unlike every
other pipeline here, **each branch is a separate login**, not a location filter
on one account, so the run loops branch → fresh browser context → login → PIN →
export. One branch failing never stops the others.

```bash
python dines_automation.py --list-branches
python dines_automation.py --discover --branch canary_wharf   # dump selectors
python dines_automation.py --branch canary_wharf --debug      # headed browser
python dines_automation.py --all-branches                     # yesterday, every configured branch
python dines_automation.py --all-branches --date 2026-08-30
python dines_automation.py --from-file downloads/raw.csv --branch victoria
```

**The manual process it replaces:** `Reports → enter manager PIN → Reporting →
Sales By Product → date: Yesterday → Export`. That chain lives in
`dines_config.yaml` under `navigation`, using the same action vocabulary as the
Sapapad config plus one addition, `enter_pin`.

**Branches** — all nine Supy branches are configured. A branch runs only when
its three `.env` keys are present; `--all-branches` reports the rest as
`no_creds` and carries on, so the daily run never fails over a branch that is
not set up yet and no browser is launched for one.

| Key | Supy branch | `.env` keys | Credentials |
|---|---|---|---|
| `canary_wharf` | Black Bear Burger Canary Wharf | `DINES_CW_USERNAME` / `_PASSWORD` / `_PIN` | ✅ on file |
| `victoria` | Black Bear Burger Victoria | `DINES_VIC_*` | ✅ on file |
| `oxford` | 20ft Chicken Oxford St | `DINES_OS_*` | ✅ on file |
| `paddington` | Black Bear Burger Paddington | `DINES_PD_*` | ✅ on file |
| `shoreditch` | Black Bear Burger Shoreditch | `DINES_SH_*` | ⏳ needed |
| `brixton` | Black Bear Burger Brixton | `DINES_BX_*` | ⏳ needed |
| `camden` | Black Bear Burger Camden | `DINES_CM_*` | ⏳ needed |
| `exmouth_market` | Black Bear Burger Exmouth Market | `DINES_EM_*` | ⏳ needed |
| `westfield` | Black Bear Burger Westfield | `DINES_WF_*` | ⏳ needed |

`python dines_automation.py --list-branches` prints that last column live, and
`python set_credential.py --list` shows it per key — key names only, never a
value.

> The five branches awaiting credentials were added on 2026-09-01 to cover all
> nine. They were previously believed **Deliveroo-only**; listing a branch here
> asserts nothing about whether it is actually on Dines. Confirm that with the
> team before chasing a login for it — and note the Deliveroo converter already
> covers all nine branches independently, so a branch that is not on Dines is
> not a gap in coverage.

Bring one online with:

```bash
python set_credential.py --partner dines_sh    # prompts for user, password, PIN
```

Write credentials with `set_credential.py` so they are never echoed to the
terminal, shell history or a transcript. They are read from `.env` only — never
put a username, password or PIN in a config file, a spreadsheet or a chat
message. Bulk `--dines-table` loading only recognises the four Market Hall
branches (identified by their `mh**` username token); the other five must be
set one at a time until a real username for each has been seen, because
guessing that token would file one branch's password under another.

### Daily automation (added 2026-09-02)

Two runners, because one of them cannot fire yet:

| | File | Schedule | State |
|---|---|---|---|
| GitHub Actions | `.github/workflows/dines_daily.yml` | `0 6 * * *` (07:00 London BST) | ⛔ blocked — see below |
| Local cron | `run_dines.sh` | whatever you put in `crontab` | ✅ works today |

**The GitHub Actions route is blocked and it is not the workflow's fault.**
Every scheduled workflow on this repo currently reports
`disabled_inactivity`, because GitHub measures activity on what is **pushed**
and `origin/main` has not moved since **2026-06-18**. All of the Talabat,
Black Bear, Dines and Deliveroo work sits in unpushed local commits. So:

```bash
gh workflow list --all          # confirm the disabled_inactivity state
gh workflow enable <id>         # re-enable each one you want
git push origin main            # required, or they are disabled again in 60 days
```

That is also what silently killed the BMD/Sapapad daily reports after 18 Aug.

**Secrets the workflow needs** (Settings → Secrets → Actions). Twelve for the
four branches with logins, plus three shared:

```
DINES_CW_USERNAME   DINES_CW_PASSWORD   DINES_CW_PIN
DINES_VIC_USERNAME  DINES_VIC_PASSWORD  DINES_VIC_PIN
DINES_OS_USERNAME   DINES_OS_PASSWORD   DINES_OS_PIN
DINES_PD_USERNAME   DINES_PD_PASSWORD   DINES_PD_PIN
GMAIL_USER          GMAIL_APP_PASSWORD  DINES_REPORT_RECIPIENT
```

The five branches awaiting credentials need **no** workflow change — absent
keys are reported as `no_creds` and skipped, so the run stays green. Add a
branch's three secrets and it joins the next run by itself.

**Until the push happens**, install the local runner instead:

```bash
crontab -e
0 7 * * *  /Users/macbook/supy/supy-ai-agents/automate-possystem/run_dines.sh
```

Note the machine has to be awake at 07:00 for cron to fire — which is the
reason to prefer Actions once the repo is pushed.

**Both routes** retry once with `--force-login` before giving up, and on a
double failure email an alert saying the sales must be uploaded by hand. The
Actions run also attaches the run log, screenshots and raw CSVs as a
`dines-failure-diagnostics` artifact, kept 14 days.

**Field mapping** (from the client's documented process):

| Dines column | Supy column |
|---|---|
| `Product` | `POS Item Name` |
| `Qty` | `Sold QTY *` |
| `Gross Product Sales` | `Total sales incl. tax *` |
| derived: incl. tax ÷ 1.2 | `Total sales excl. tax *` |
| fixed `0` | `Total Discount Value` |

> **VAT runs the opposite way to Deliveroo.** Dines reports a VAT-**inclusive**
> figure, so excl. tax is obtained by **dividing** by 1.2. Deliveroo reports net
> and **multiplies**. Swapping them changes every money column while quantities
> and discounts look untouched, so the two configs are kept separate and each
> report states its direction.

**PLU codes are settled** (2026-09-01). Dines has its **own numeric** scheme —
not Deliveroo's `sku-` / `mod-` codes — taken from the `PLU` tab of the
"MarketHall - BBB Sales" sheet and held in `mappings/dines_plu.csv` (167
mappings, 135 distinct codes; 25 codes carry name aliases). `plu.file` points
there. An item with no match keeps its name as `POS Item ID *` and is listed in
the run log — visible rather than silently wrong. Earlier runs used the
Deliveroo PLU sheet here; those `POS Item ID`s were wrong and must not be
uploaded.

**Selectors are verified** (2026-08-31, live login page): `<form id="login-form">`
with `#username` / `#password` and `<button id="submit">`. The manager PIN is a
touch **keypad** (`#pin-overlay`), not a text field, so the PIN is clicked digit
by digit. The dashboard's date picker does nothing under automation, so the date
is set by rewriting `start_date` / `end_date` on the report's own fetch — see
`_install_date_route`. `--discover` re-dumps every input, button, link and form
if the page changes, without anyone reading a password aloud.

---

## Daily automation — every client, 08:00 Dubai

All scheduled reports run in **GitHub Actions**, not on anyone's laptop, so
they fire whether or not a machine is awake. Every workflow is on
`cron: "0 4 * * *"` (04:00 UTC = 08:00 Dubai) and reports **yesterday**.

| Client | Workflow | Covers |
|---|---|---|
| BMD | `sapapad_daily.yml` | 9 UAE branches |
| Black Bear Burger | `dines_daily.yml` | 4 Dines branches (of 9 configured) |
| Street Food Ltd. | `streetfood_daily.yml` | `deliveroo`, `anddine`, `feedr`, `homecook` |
| FLARE | `talabat_daily.yml` | Talabat UAE |

Each one retries once with `--force-login`, then emails a failure alert saying
the sales must be uploaded by hand, and attaches logs, screenshots and raw
CSVs as a 14-day artifact. All support `workflow_dispatch`, so any day can be
re-run by hand from the Actions tab; `streetfood_daily.yml` also takes optional
`date` and `only` inputs for a targeted re-run.

### ⚠️ Three things stop this working today

**1. Every workflow is `disabled_inactivity`.** GitHub disables scheduled
workflows after 60 days without repository activity, measured on what is
**pushed** — and `origin/main` has not moved since **2026-06-18**. This is
what silently killed BMD's reports after 18 Aug.

```bash
gh workflow list --all      # confirm the state
gh workflow enable <id>     # for each workflow you want
git push origin main        # REQUIRED, or they are disabled again in 60 days
```

**2. Secrets must exist in the repo** (Settings → Secrets → Actions). Shared:
`GMAIL_USER`, `GMAIL_APP_PASSWORD`, `REPORT_RECIPIENT`. Then per client:
Dines needs 12 (`DINES_CW/VIC/OS/PD_USERNAME|PASSWORD|PIN`); Street Food needs
8 (`DELIVEROO_*`, `ANDDINE_*`, `FEEDR_*`, `HOMECOOK_*` username/password);
BMD and Talabat already have theirs.

**3. There is a duplicate BMD workflow.** `daily_sapapad_report.yml` and
`sapapad_daily.yml` share the same cron; the first has failed every day in
~30s while the second succeeds in ~6m30s. Delete the first.

### What cannot be automated in the cloud, and why

| Partner | Reason |
|---|---|
| `just_eat`, `justeat_business` | Need `browser.cdp_endpoint` — attaching to a REAL Chrome that a human cleared Cloudflare in. A runner has no such browser, so these are laptop-bound (`start_chrome_cdp.sh`) until Just Eat's CSV Integration replaces them. |
| `ordit` | Authenticates, but its order detail never renders under automation (verified headless **and** headed). No item-level data exists to collect. |
| `uber_eats` | No login or scrape selectors configured yet. |
| Black Bear Deliveroo | Arrives as an emailed Looker export; `blackbear_convert.py` is file-in/file-out. Needs an IMAP fetch (like Talabat's) before it can be scheduled. |

`homecook` and `justeat_business` also stop at Stage 3 until wholesale prices
are in `mappings/menu_prices.csv` — they scrape correctly, they just refuse to
upload rows priced at zero.

### Local fallbacks

`run_dines.sh` and `run_streetfood.sh` exist for running from the laptop, and
are the only option for the CDP-bound partners. They need the machine awake,
which is exactly why Actions is the primary route.

## VAT treatment — the rule and every pipeline's direction

**Confirmed by Charlotte, 2026-09-03:**

> "That's the standard calculation for UK accounts if you only have tax
> inclusive. So yes please use that to attain tax exclusive sales."

Read the condition, not just the number. The rule is **not** "always divide by
1.2". It has three branches:

| What the source gives you | What we do |
|---|---|
| **Gross only** | `excl = incl / 1.2` |
| **Net** | `incl = excl * 1.2` |
| **Both figures** | derive nothing — copy both |

That third branch matters: deriving a number you were already given is a
chance to introduce an error for no benefit.

### Every pipeline, audited 2026-09-03

| Pipeline | Source | Direction | Rate | Evidence |
|---|---|---|---|---|
| Dines (Black Bear) | gross only | **÷** | 1.2 | ratio exactly 1.2000 across all 4 branches shipped 02-09 |
| Deliveroo Partner Hub (Street Food) | gross | **÷** | 1.2 | 13.95 → 11.62 in the file emailed 02-09 |
| Deliveroo Looker email (`blackbear_convert.py`) | **net** | **×** | 1.2 | team-confirmed 2026-08-26 |
| &Dine | **net** ("Excl. VAT" on screen) | **×** | 1.2 | order SAT-IVB2M: 13.29 × 1.2 = 15.95 = order total |
| Feedr | gross | **÷** | 1.2 | 7.47 ÷ 1.2 = 6.22 in the file emailed 02-09 |
| HomeCook | lookup `price_inc_tax` | **÷** | 1.2 | column is inc-tax by definition |
| JustEat Business | lookup `price_inc_tax` | **÷** | 1.2 | same lookup sheet |
| Just Eat | gross (customer prices) | **÷** | 1.2 | config; pipeline not yet running |
| Ordit | ⚠️ **assumed** gross | **÷** | 1.2 | **UNVERIFIED — see below** |
| Talabat (UAE) | gross | **÷** | **1.05** | UAE VAT is 5%, not 20% |
| Sapapad / BMD (UAE) | **both** | none | n/a | CSV has `Total Amount Excluding Tax` and `Total Amount` |

### Two traps this audit exposed

**1. The two Deliveroo pipelines run OPPOSITE directions — both correctly.**
`blackbear_convert.py` reads a Looker email export whose values are net and
**multiplies**; `deliveroo_automation.py` reads Partner Hub's Items Sold report
whose prices are gross and **divides**. Same brand, two reports. `dines_config.yaml`
used to state flatly that "Deliveroo reports NET and we MULTIPLY", which is now
corrected — believing it would have flipped the Partner Hub pipeline.

**2. Ordit's direction has never been verified.** It sets no
`price_includes_tax`, so the code default (gross → divide) applies. But &Dine —
the other B2B catering marketplace here — displays prices **excluding** VAT. If
Ordit does the same, we divide a net figure and understate both money columns.
Flagged in `partners/ordit.yaml`; settle it by opening one Ordit order and
checking whether its line prices sum to the order total as-is.

### Locked by tests

`tests/test_vat_direction.py` (17 tests) pins each pipeline's direction, the
UAE rate, the fact that Sapapad derives nothing, and that the two Deliveroo
pipelines disagree on purpose — so "tidying" one to match the other fails
loudly. It also asserts the arithmetic against rows actually shipped on
2026-09-02.

## Attaching to a real Chrome (`browser.cdp_endpoint`)

Some portals block **any** Playwright-launched browser. Measured against
`partner-hub.just-eat.co.uk` on 2026-09-02:

| Route | Result |
|---|---|
| headless + saved `storage_state` | held at the challenge |
| headless + persistent profile | 90s, never cleared |
| **headed** + persistent profile | 60s, never cleared |

Cloudflare is detecting the automation driver, not headlessness — a visible
real-window Chrome driven by Playwright is blocked identically, and it never
presents the checkbox, so "have a human tick it once" is not available either.

The workaround is to **attach to a Chrome a person logged into**, which has a
genuine fingerprint and a live clearance cookie:

```bash
./start_chrome_cdp.sh                    # opens Chrome with remote debugging
# log into the portal in that window, then leave it open
.venv/bin/python partner_scraper.py --partner just_eat
```

Set `browser.cdp_endpoint: "http://localhost:9222"` in the partner config, or
`PARTNER_CDP_ENDPOINT` for one run. When set, the scraper connects over CDP,
**reuses the browser's existing context** (a fresh one would not share the
clearance) and **only detaches** at the end — it never closes a browser it did
not launch. Smoke-tested: attach, drive a tab, detach, Chrome still alive.

**How much human involvement this removes.** Runs after the first need nobody
present: the persistent profile keeps clearance and session for days. But the
FIRST login is manual, and someone re-logs in when the session lapses. It also
cannot run on GitHub Actions — there is no Chrome to attach to — so this
partner is tied to a machine with a real browser.

The durable fix remains Just Eat's own **CSV Integration / JET Connect** (see
the research notes): no browser, nothing to detect, nothing to re-log-in.

## Street Food Ltd. portal status (2026-09-08)

Re-measured 2026-09-08. The previous table was stale in two places: Deliveroo
and JustEat Business were both listed as blocked on selectors, and both were
already working.

| Partner | State | Blocker |
|---|---|---|
| `anddine` | ✅ working | — |
| `feedr` | ✅ working | — (see the pagination note below) |
| `deliveroo` | ✅ working | — (verified end to end 2026-09-04, 5 rows emailed) |
| `justeat_business` | ✅ **scrapes unattended** | Needs 21 prices in `mappings/justeat_business_prices.csv` |
| `homecook` | ✅ scrapes correctly | Needs 4 wholesale prices in `mappings/homecook_prices.csv` |
| `ordit` | ✅ **working, via the portal's JSON API** | — (see below) |
| `just_eat` | ❌ | Cloudflare; needs a human-seeded profile — see below |
| `uber_eats` | ❌ | SMS one-time code; no unattended LOGIN exists |

### Ordit — solved by reading the API, not the DOM (2026-09-08)

The "no item-level data exists" conclusion above was wrong. It is unreachable
through the **DOM** — but `coreapi.ordit.co.uk` serves it freely:

```
GET /api/v1/orders/v2?requiredDeliveryTime[after]=…&[before]=…   → order list
GET /api/v1/orders/{id}                                          → meals[] with items
```

`/orders/v2/{id}`, `/orders/{id}/items` and `/order-items?order={id}` all 404 —
`/orders/{id}` is the only detail route. A browser still runs, but only to
capture the Bearer token the SPA sends; nothing is read off the page, so there
are **no selectors to rot**. Driven by `api.enabled` in `partners/ordit.yaml`.

Two further claims in that config were also wrong: August was not empty
(06-Aug OC-MTYZ-1291 is £44.40), and `status=new` is not a limitation —
arbitrary date ranges work, and the SPA itself uses them.

Verified end to end: 14 orders → 37 line items → £495.30 gross / £412.75 net
across 01-Jul–08-Sep, emailed 2026-09-08.

**Use `priceWithMealOptions`, not `price`.** `price` and `supplierPrice`
exclude PAID modifiers; `priceWithMealOptions` folds them in. Summing the
former under-reports by exactly the modifier value. The engine re-checks every
order against the API's own `priceSumItems` and warns if it stops reconciling.
`children` must never become their own rows — free ones are £0 and paid ones
are already counted, so emitting them double-counts the order.

⚠️  VAT direction is still ASSUMED gross. The reconciliation proves internal
consistency only. Circumstantial support: Ordit and Deliveroo list identical
prices for identical items (Stir Fry Chilli & Basil, 12.95 on both) and
Deliveroo is documented gross. One invoice showing a VAT line would settle it;
if Ordit reports net, every money column is 20% light.

### ⚠️ Automated access degrades these portals — space runs out

Both `just_eat` and `justeat_business` were hit many times from one IP on
2026-09-08 while diagnosing. Consequences, both self-inflicted:

* `partner-hub.just-eat.co.uk` — a working human-seeded Cloudflare clearance
  was destroyed by a `clear_cookies()` call and could not be re-earned.
* `app.business.just-eat.co.uk` — authenticated and scraped 30 line items in
  the morning, then began returning **403** after repeated probing. The host
  serves Cloudflare RUM (`/cdn-cgi/rum`), so it is protected after all.

If a partner that worked starts failing, back off for hours rather than
retrying in a loop. Retrying is what caused this.

### The two hosts do NOT share Cloudflare protection (measured 2026-09-08)

`justeat_business.yaml` assumed `app.business.just-eat.co.uk` was protected
like `partner-hub.just-eat.co.uk`, and routed it over CDP for that reason. It
is not. Probed with a Playwright-launched persistent context:

```
app.business.just-eat.co.uk/menus/vendor/orders  -> 302 /login?forward=%2Flogin   NO challenge
partner-hub.just-eat.co.uk/home                  -> "Performing security verification"
```

JustEat Business had simply lost its session; the expiry looked like a block
because the CDP path failed first. With `cdp_endpoint` commented out and
`persistent_profile: true` kept, a `--force-login` run authenticated and
scraped 30 line items for 01-07 Sep with nobody present.

### CDP attach is broken on this machine — affects `just_eat`

Playwright 1.59.0 against Chrome 152 fails every `connect_over_cdp`:

```
Protocol error (Browser.setDownloadBehavior): Browser context management is not supported.
```

Reproducible with a bare three-line connect, so it is environmental, not this
repo. `just_eat` has no other route — its Cloudflare challenge genuinely needs
a real human-cleared Chrome — so pinning a compatible Playwright/Chrome pair is
the prerequisite before any further work on it.

### Prices are now per-partner

`pricing.lookup_csv` was always per-partner config, but JustEat Business and
HomeCook both pointed at the same `menu_prices.csv`. They must not: Street Food
charges different prices per channel, with &Dine running ~1.6x Feedr on every
item observed on both (Pad Thai (Chicken) 11.95 vs 7.47, Vegetable Dumplings
6.95 vs 4.34, Bento Boxes 15.95 vs 9.97). One shared sheet silently misprices
whichever channel it was not derived from. Split into
`mappings/justeat_business_prices.csv` and `mappings/homecook_prices.csv`; rows
are commented out until confirmed, so an unpriced item still fails the run.

### Feedr — `max_pages: 1` is correct, not a truncation (verified 2026-09-02)

An earlier note here claimed `pagination.max_pages: 1` under-reported Feedr
backfills. **That was wrong.** Feedr has no pager: `selectors.next_page` is
empty and the list is bounded by an on-page date filter, widened from the
7-day default to 60 days by `selectors.list_pre_click` before collecting.

Verified against the live list:

```
rows BEFORE widening: 1
widened via "button:has-text('60 days')"
rows AFTER widening : 11
rows after scrolling: 11  -> no change   (so no lazy-loaded remainder)
day groups: Today (Wed 02 Sep), Thu 27 Aug, Thu 20 Aug, Thu 13 Aug, Wed 12 Aug,
            Fri 31 Jul, Thu 30 Jul, Wed 29 Jul, Tue 28 Jul, Thu 09 Jul
```

This supplier receives roughly one order a week, on Thursdays. For a
27 Aug–1 Sep run the only day group in range is 27 Aug, so **one line item was
the complete and correct answer.**

What *was* broken is the alarm. `pagination_cap_hit` fired whenever
`seen_pages >= max_pages`, which is true on every single run of a pager-less
portal — so a correct result looked truncated and a real report got called
incomplete. It now fires only when a next-page control genuinely exists and
the cap stopped us from following it, i.e. only when data really was missed.
Covered by `test_no_cap_warning_when_the_portal_has_no_pager` and
`test_cap_warning_when_a_further_page_really_exists`.

### Backfills reached only page one, silently (fixed 2026-09-11)

Asked for 1 Jul–31 Aug, both `anddine` and `justeat_business` returned the
**most recent page** and exited `OK`. `_collect_order_links` treated an empty
`selectors.next_page` as "this list has no more pages" — true for Feedr above,
false for these two, which page but expose no *clickable* Next. So &Dine read
its 20 newest orders and JustEat Business its 10, whatever date range was
requested, and no alarm fired because the cap warning only covers a pager that
exists.

The damage is visible in `output/`: the file named `..._2026-07-01_20260807.xlsx`
starts on **9 Jul**, and `..._2026-08-31_20260904.xlsx` contains **3 Sep** rows.

Three config keys now drive the walk:

| Key | Meaning |
|---|---|
| `pagination.url_param` | Page by URL (`?page=N`) when there is no control to click |
| `pagination.stop_when_older` | Stop once a page is wholly older than `--from`. Default **true** — safe only on a newest-first list |
| `pagination.delay_seconds` | Pause between list pages |
| `browser.request_delay_seconds` | Pause between order-detail loads |

**JustEat Business** pages by URL only; `orders_url` pins `page=1`, so
`url_param: page` rewrites it while keeping `tab=Past`. 10 pages, 63 orders for
Jul–Aug.

**&Dine** is the opposite: `?page=2` is **ignored** — it re-serves page 1
verbatim (measured) — and the pager is numbered `1 2 3 … 7`, not a Next button.
A bare `.table__pagination--button` would re-click "1" forever, so the selector
is the adjacent sibling of the selected button:

```yaml
next_page: ".table__pagination--button.button-selected + .table__pagination--button"
```

Two things that bite on this portal, both now handled in the engine:

* **The button past the last page is hidden, not removed.** `is_enabled()`
  alone returns true for it, so the run blocked for the full 30s click timeout.
  Visibility is checked too.
* **The table re-renders after `networkidle`.** Page 5 served page 4's rows for
  over a second; the repeat-guard read that as the end of the list and stopped
  three pages early. The engine now waits for the rows themselves to change.

`stop_when_older` **must stay false for &Dine.** `order_date` is the *delivery*
date while rows are ordered by when the order was placed, so dates jump around:
page 1 ran 18/09/26 down to 28/07/26 and ended 28/07 *then* 30/07, and page 4
(Jan–May 2026) sat between pages of 2026 and 2024 orders.

### One order, booked twice — what pagination exposed (2026-09-11)

The first paged &Dine backfill returned 373 line items where the truth was 364.
`SAT-32L8L` (order 15131) sat at the end of page 1 *and* the start of page 2 —
the list shifts between page loads — and the engine collected it twice, writing
its 9 line items twice and booking **£1,230.58 against a £732.35 order**.

There was no dedup because, before paging, an order could only be seen once.
`_collect_order_links` now keys on the href (or the reference in click-mode —
`row_index` repeats on every page and cannot stand in for it) and reports what
it ignored. Locked by `test_an_order_on_two_pages_is_collected_once`.

**Every &Dine order in the backfill reconciles to its portal order total**
within the mixed-VAT drift documented in `partners/anddine.yaml` (0–3% high,
because a flat ×1.2 over-taxes zero-rated cold food), and the four Set orders
land exactly on their totals.

### HomeCook — implemented 2026-09-02

`orders_url` pointed at `/dashboard/orders`, which has **no order table at all**;
the producer list is at `/dashboard/my-orders`. With that corrected and the six
selectors derived from the live DOM, the pipeline runs end to end:

```
[Stage 1] ✓ Authenticated
[Stage 2] ✓ Scraped 1 line items
  → {"order_id": "PO-20260819-84B", "order_date": "2026-09-01",
     "item_name": "Chicken Satay", "qty": 100}
```

It then stops at Stage 3, correctly: **no price**. HomeCooks exposes none, so
prices come from `mappings/menu_prices.csv`, which currently holds only
`Chicken Satay Skewers`, `Pad Thai` and `Thai Green Curry` — none of which match
the four products actually ordered:

- `Chicken Satay`
- `Tofu Satay (VG)`
- `Thai Green Chicken Curry`
- `Tofu Thai Green Curry with Jasmine Rice (VG)`

Note these are **wholesale production POs** (quantities of 100–200 units), so the
retail menu price is probably the wrong number — confirm the per-unit wholesale
price with the client rather than assuming the existing rows are typos. Once the
sheet has them, re-run without re-scraping:

```bash
python partner_scraper.py --partner homecook --date 2026-09-01 --from-scraped
```

### Ordit — authentication fixed, item detail not reachable

Login now works (it was pointed at the buyer-side door; see `partners/ordit.yaml`).
The order **list** reads fine — 6 orders for August, with reference, customer,
timestamp, total and item count. Item-level detail is the problem:

- Clicking an order changes the route to `/orders/history/{period}/OC-{ref}-{restaurantId}`
  but **renders no detail** in a headless context — after `networkidle` plus 10s
  the page body is 742 characters and contains no product names at all.
- Navigating straight to that URL just re-renders the list.
- `PRINT SUMMARY` / `PRINT ITEM LABELS` trigger print flows, not DOM dialogs.
- The per-order **EXPORT** button downloads `orders.csv` — but it is
  **order-level**, covering every order in the filtered view:

  ```
  id,restaurantProfile,status,deliveryDate,priceSumItems,deliveryFeeGross,total
  OC-MTYZ-1291,"Satay Street - Aldgate",Confirmed,"2026-08-06 12:30:00",44.4,0,44.4
  ```

So Ordit can deliver **order totals reliably today** (one CSV per date range, no
DOM selectors, immune to selector rot) but **not an item breakdown**. Choosing
between order-level ingestion via `orders.fallback_single_line` and chasing
item-level detail is a data-quality decision for the team, not a code one.

Two modal notes for whoever picks this up: the notification prompt needs **two**
clicks ("I'm not managing orders", then "Ok" on the confirm), and it blocks
clicks but **not** reads — so a read-only scrape can ignore it entirely.

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
