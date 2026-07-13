"""
talabat_automation.py
---------------------
Talabat UAE Settlement Report → Supy POS Format Pipeline

Stage 0: Email Fetch       (optional — pull .xlsx from Gmail inbox)
Stage 1: File Ingestion    (read + validate the Talabat .xlsx)
Stage 2: Transformation    (reshape detail section → Supy format)
Stage 3: Email             (attach .xlsx and send via Gmail SMTP)

The Talabat settlement report has a side-by-side layout:
  - Cols  1-11: Summary / compensation / cancellation tables  (ignored)
  - Cols 12-64: Order detail data                             (processed)
  - Row 3 (index 2): Column headers for the detail section
  - Rows 4+  (index 3+): One row per order

Usage:
    python talabat_automation.py --file <path/to/report.xlsx>
    python talabat_automation.py --from-email
    python talabat_automation.py --from-email --no-email
    python talabat_automation.py --file <path> --debug

Exit codes:
    0  success (including "no new email found" — not an error)
    1  IngestError   (file not found, wrong sheet, missing columns)
    2  TransformError
    3  EmailError
    4  FetchError    (IMAP connection or credential failure)
"""

import argparse
import email as email_lib
import email.utils
import imaplib
import json
import os
import pathlib
import smtplib
import sys
import time
import traceback
import uuid
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Optional

import pandas as pd
import yaml
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ──────────────────────────────────────────────────────────────────────────────

load_dotenv()

BASE_DIR      = pathlib.Path(__file__).parent
OUTPUT_DIR    = BASE_DIR / "output"
LOGS_DIR      = BASE_DIR / "logs"
DOWNLOADS_DIR = BASE_DIR / "downloads"

for d in (OUTPUT_DIR, LOGS_DIR, DOWNLOADS_DIR):
    d.mkdir(exist_ok=True)

with open(BASE_DIR / "talabat_config.yaml") as _f:
    CONFIG = yaml.safe_load(_f)

RUN_ID = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
_log_path: Optional[pathlib.Path] = None
_verbose = False


# ──────────────────────────────────────────────────────────────────────────────
# Custom Exceptions
# ──────────────────────────────────────────────────────────────────────────────

class FetchError(Exception):
    """IMAP connection or credential failure."""


class IngestError(Exception):
    """File missing, wrong format, or required columns absent."""


class TransformError(Exception):
    """Data transformation failure."""


class EmailError(Exception):
    """Email delivery failure — report was generated but not sent."""


# ──────────────────────────────────────────────────────────────────────────────
# Structured Logger
# ──────────────────────────────────────────────────────────────────────────────

def _init_logger(verbose: bool) -> None:
    global _log_path, _verbose
    _verbose = verbose
    _log_path = LOGS_DIR / f"talabat_{RUN_ID}.jsonl"


def log(stage: str, step: str, outcome: str, duration_ms: int = 0, extra: dict = None) -> None:
    entry = {
        "ts": datetime.utcnow().isoformat(),
        "run_id": RUN_ID,
        "pipeline": "talabat",
        "stage": stage,
        "step": step,
        "outcome": outcome,
        "duration_ms": duration_ms,
        **(extra or {}),
    }
    if _log_path:
        with open(_log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
    if _verbose:
        icon = "✓" if outcome == "ok" else "✗" if outcome == "error" else "→"
        print(f"  [{icon}] [{stage}] {step}  ({duration_ms}ms)")
    elif outcome == "error":
        print(
            f"  [✗] [{stage}] {step}: {extra.get('error', '') if extra else ''}",
            file=sys.stderr,
        )


# ──────────────────────────────────────────────────────────────────────────────
# Stage 0 — Email Fetch (optional)
# ──────────────────────────────────────────────────────────────────────────────

def stage_fetch_from_email() -> Optional[pathlib.Path]:
    """
    Check each configured Gmail inbox for an unread FLARE Talabat report.
    Returns the local path of the downloaded .xlsx, or None if nothing found.
    Raises FetchError on IMAP credential or connection failure.
    """
    t0 = time.monotonic()

    fetch_cfg        = CONFIG.get("email_fetch", {})
    inboxes          = fetch_cfg.get("inboxes", [])
    search_cfg       = fetch_cfg.get("search", {})
    sender_pattern   = search_cfg.get("sender_pattern", "flare").lower()
    subject_pattern  = search_cfg.get("subject_pattern", "talabat").lower()
    attachment_ext   = search_cfg.get("attachment_ext", ".xlsx").lower()
    trusted_senders  = {s.lower() for s in search_cfg.get("trusted_senders", [])}
    max_attach_bytes = int(search_cfg.get("max_attachment_bytes", 25 * 1024 * 1024))

    if not trusted_senders:
        raise FetchError(
            "email_fetch.search.trusted_senders must list at least one exact "
            "sender address in talabat_config.yaml — refusing to auto-ingest "
            "from an unauthenticated sender."
        )

    for inbox_cfg in inboxes:
        user_env     = inbox_cfg["user_env"]
        password_env = inbox_cfg["password_env"]
        inbox_user   = os.environ.get(user_env, "")
        inbox_pass   = os.environ.get(password_env, "")

        if not inbox_user or not inbox_pass:
            log("fetch", "connect", "skip",
                extra={"reason": f"{user_env} or {password_env} not set in env"})
            continue

        try:
            mail = imaplib.IMAP4_SSL("imap.gmail.com", 993, timeout=30)
            mail.login(inbox_user, inbox_pass)
            mail.select("INBOX")

            # Use server-side search to narrow results before fetching full messages.
            # Try FROM match first, then fall back to SUBJECT match; combine with OR.
            search_criteria = (
                f'(UNSEEN OR FROM "{sender_pattern}" SUBJECT "{subject_pattern}")'
            )
            _, msg_ids_data = mail.search(None, search_criteria)
            msg_ids = msg_ids_data[0].split() if msg_ids_data[0] else []

            if _verbose:
                print(f"  [→] {inbox_user}: {len(msg_ids)} candidate(s) after server filter")

            for msg_id in msg_ids:
                # Fetch headers only first — avoid downloading large attachments for non-matches
                _, hdr_data = mail.fetch(msg_id, "(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT)])")
                hdr_bytes   = hdr_data[0][1]
                hdr         = email_lib.message_from_bytes(hdr_bytes)

                # Authenticate the sender against an exact allow-list — the
                # From header is attacker-controlled and trivially spoofable,
                # so a loose substring match (e.g. "flare" anywhere in the
                # display name) is not sufficient to trust the attachment.
                _, from_addr = email_lib.utils.parseaddr(hdr.get("From", ""))
                subject_header = hdr.get("Subject", "").lower()

                if from_addr.lower() not in trusted_senders:
                    if _verbose:
                        print(f"  [→] Rejected untrusted sender: {from_addr!r}")
                    continue
                if subject_pattern not in subject_header:
                    continue

                # Full fetch only for confirmed matches
                _, msg_data = mail.fetch(msg_id, "(RFC822)")
                raw_bytes   = msg_data[0][1]
                msg         = email_lib.message_from_bytes(raw_bytes)

                # Find the first .xlsx attachment
                for part in msg.walk():
                    filename = part.get_filename()
                    if not filename or not filename.lower().endswith(attachment_ext):
                        continue

                    payload = part.get_payload(decode=True) or b""
                    if len(payload) > max_attach_bytes:
                        log("fetch", "download", "error", extra={
                            "reason": "attachment exceeds max_attachment_bytes",
                            "size": len(payload),
                            "limit": max_attach_bytes,
                        })
                        continue

                    out_path = DOWNLOADS_DIR / f"{RUN_ID}_flare_raw.xlsx"
                    out_path.write_bytes(payload)

                    # Mark email as read so it won't be re-processed tomorrow
                    mail.store(msg_id, "+FLAGS", "\\Seen")
                    mail.logout()

                    duration = int((time.monotonic() - t0) * 1000)
                    log("fetch", "download", "ok", duration_ms=duration, extra={
                        "inbox":    inbox_user,
                        "from":     msg.get("From", ""),
                        "subject":  msg.get("Subject", ""),
                        "filename": filename,
                        "saved_as": out_path.name,
                    })

                    if _verbose:
                        print(f"  [→] Downloaded '{filename}' → {out_path}")

                    return out_path

            mail.logout()

        except imaplib.IMAP4.error as exc:
            raise FetchError(f"IMAP error for {inbox_user}: {exc}") from exc

    duration = int((time.monotonic() - t0) * 1000)
    log("fetch", "search", "no_match", duration_ms=duration,
        extra={"inboxes_checked": len(inboxes),
               "sender_pattern": sender_pattern,
               "subject_pattern": subject_pattern})
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Stage 1 — File Ingestion
# ──────────────────────────────────────────────────────────────────────────────

def stage_ingest(file_path: pathlib.Path) -> pd.DataFrame:
    """
    Read the Talabat .xlsx, extract the order detail section, and return
    a raw DataFrame with original column names intact.
    """
    t0 = time.monotonic()

    if not file_path.exists():
        raise IngestError(f"File not found: {file_path}")
    if file_path.suffix.lower() not in (".xlsx", ".xls"):
        raise IngestError(f"Expected .xlsx or .xls, got: {file_path.suffix}")

    file_cfg    = CONFIG["file"]
    sheet_name  = file_cfg["sheet_name"]
    header_row  = file_cfg["detail_header_row"]   # 0-indexed (row 3 in Excel → index 2)
    anchor_col  = file_cfg["detail_anchor_column"]  # header string that marks the detail section's first column

    try:
        # Some Talabat exports prefix the detail section with a summary/
        # compensation table (side-by-side layout); others contain only the
        # detail section. The two layouts put the detail columns at different
        # offsets, so locate the start column by anchor header name instead of
        # a fixed index — this works for either layout.
        probe_df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            header=None,
            skiprows=header_row,
            nrows=1,
        )
        header_vals = [str(c).strip() for c in probe_df.iloc[0].tolist()]
        if anchor_col not in header_vals:
            raise IngestError(
                f"Could not locate detail section — anchor column "
                f"'{anchor_col}' not found in {file_path.name}"
            )
        start_col = header_vals.index(anchor_col)
        end_col   = len(header_vals)

        # Read header row separately to avoid pandas deduplicating column names.
        # A side-by-side layout's summary section shares some header names
        # (e.g. "Status", "Payment Method") with the detail section. Reading
        # both together causes pandas to append ".1" suffixes on the detail
        # columns.
        header_df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            header=None,
            skiprows=header_row,
            nrows=1,
            usecols=range(start_col, end_col),
        )
        col_names = [str(c).strip() for c in header_df.iloc[0].tolist()]

        # Read data rows (skip header + all rows before it)
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            header=None,
            skiprows=header_row + 1,        # skip up to and including the header row
            usecols=range(start_col, end_col),
        )
        df.columns = col_names
    except IngestError:
        raise
    except Exception as exc:
        raise IngestError(f"Could not read {file_path.name}: {exc}") from exc

    # Verify the expected sentinel column is present
    if "Order Id" not in df.columns:
        raise IngestError(
            f"Expected column 'Order Id' not found in {file_path.name}. "
            f"Columns found: {list(df.columns[:8])}"
        )

    duration = int((time.monotonic() - t0) * 1000)
    log("ingest", "read_excel", "ok", duration_ms=duration,
        extra={"file": file_path.name, "raw_rows": len(df), "raw_cols": len(df.columns)})

    if _verbose:
        print(f"  [→] Loaded {len(df)} rows × {len(df.columns)} cols from '{sheet_name}'")

    return df


# ──────────────────────────────────────────────────────────────────────────────
# Stage 2 — Transformation
# ──────────────────────────────────────────────────────────────────────────────

def stage_transform(df: pd.DataFrame, source_filename: str) -> tuple:
    """
    Apply the column mapping from talabat_config.yaml and produce the
    Supy-format .xlsx.  Returns (out_path, row_count, date_range_str).
    """
    t0 = time.monotonic()

    try:
        # ── 1. Filter rows ────────────────────────────────────────
        filter_cfg   = CONFIG["filter"]
        status_col   = filter_cfg["status_column"]
        keep_status  = set(filter_cfg["keep_status"])

        before = len(df)
        df = df[df[status_col].isin(keep_status)].copy()
        df.reset_index(drop=True, inplace=True)
        dropped = before - len(df)

        if _verbose:
            print(f"  [→] Status filter: kept {len(df)}, dropped {dropped} (not in {sorted(keep_status)})")

        # Drop rows with no Order Id (blank rows from the side-by-side layout)
        df = df[df["Order Id"].notna()].copy()
        df = df[df["Order Id"].astype(str).str.strip() != ""].copy()
        df.reset_index(drop=True, inplace=True)

        if _verbose:
            print(f"  [→] After blank-row cleanup: {len(df)} rows")

        # ── 2. Column mapping from config ─────────────────────────
        col_cfgs   = CONFIG["columns"]
        rename_map = {}
        drop_cols  = []

        for cfg in col_cfgs:
            if cfg.get("drop"):
                if cfg["raw"] in df.columns:
                    drop_cols.append(cfg["raw"])
            elif not cfg.get("inject") and cfg.get("raw") and cfg.get("target"):
                rename_map[cfg["raw"]] = cfg["target"]

        df.drop(columns=drop_cols, errors="ignore", inplace=True)
        df.rename(columns=rename_map, inplace=True)

        # Drop Status column after filtering — it must not appear in the output
        status_col = CONFIG["filter"]["status_column"]
        df.drop(columns=[status_col], errors="ignore", inplace=True)

        # ── 3. Inject columns ─────────────────────────────────────
        for cfg in col_cfgs:
            inject = cfg.get("inject")
            if not inject:
                continue
            target = cfg["target"]
            if inject == "empty":
                df[target] = ""
            elif inject == "constant_1":
                df[target] = 1
            elif inject == "vat_excl_from_incl":
                # UAE: prices are VAT-inclusive at 5%. excl. = incl. / 1.05
                incl_col = "Total sales incl. tax *"
                if incl_col in df.columns:
                    df[target] = (
                        pd.to_numeric(df[incl_col], errors="coerce").fillna(0.0) / 1.05
                    ).round(2)

        # ── 4. Type casting ───────────────────────────────────────
        output_date_format = CONFIG.get("output_date_format", "%d-%b-%Y")

        for cfg in col_cfgs:
            if cfg.get("drop") or cfg.get("inject"):
                continue
            target = cfg.get("target")
            dtype  = cfg.get("dtype")
            if not target or target not in df.columns:
                continue

            if dtype == "date":
                # Date / Time column arrives as a Python datetime object from openpyxl
                df[target] = pd.to_datetime(df[target], errors="coerce").dt.strftime(output_date_format)
                df[target] = df[target].fillna("")

            elif dtype == "int":
                df[target] = pd.to_numeric(df[target], errors="coerce").fillna(0).astype(int)

            elif dtype == "float":
                df[target] = (
                    df[target]
                    .astype(str)
                    .str.replace(r"[^\d.\-]", "", regex=True)
                    .replace("", "0")
                )
                df[target] = pd.to_numeric(df[target], errors="coerce").fillna(0.0).round(2)

            elif dtype == "str":
                # Large integer IDs (e.g. Order Id) arrive as float64 from Excel.
                # Convert float → int str to avoid "3599323355.0" formatting.
                if pd.api.types.is_float_dtype(df[target]):
                    df[target] = df[target].apply(
                        lambda x: str(int(x)) if pd.notna(x) else ""
                    )
                else:
                    df[target] = df[target].astype(str).str.strip()

        # ── 5. Force integer for Sold QTY * ──────────────────────
        if "Sold QTY *" in df.columns:
            df["Sold QTY *"] = df["Sold QTY *"].astype(int)

        # ── 6. Reorder to final column order ──────────────────────
        final_order = CONFIG.get("output_column_order", [])
        ordered     = [c for c in final_order if c in df.columns]
        extras      = [c for c in df.columns if c not in ordered]
        df = df[ordered + extras]

        # ── 6b. Neutralize spreadsheet formula injection ──────────
        # Values originate from an untrusted Talabat export (or an emailed
        # attachment). A string starting with =, +, -, @, or tab/CR is
        # interpreted as a formula by Excel/Sheets when the output file is
        # opened downstream — prefix a single quote to force text interpretation.
        _FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].map(
                    lambda v: "'" + v
                    if isinstance(v, str) and v.startswith(_FORMULA_PREFIXES)
                    else v
                )

        # ── 7. Build a date-range string for the filename/email ───
        date_col = "Sales Date *"
        if date_col in df.columns and len(df) > 0:
            dates = df[date_col].dropna()
            if len(dates) > 0:
                first_date = dates.iloc[0]
                last_date  = dates.iloc[-1]
                date_range = first_date if first_date == last_date else f"{first_date}_to_{last_date}"
            else:
                date_range = datetime.now().strftime(output_date_format)
        else:
            date_range = datetime.now().strftime(output_date_format)

        # ── 8. Export ─────────────────────────────────────────────
        stem     = pathlib.Path(source_filename).stem
        out_path = OUTPUT_DIR / f"talabat_{stem}_{RUN_ID[:8]}.xlsx"
        df.to_excel(str(out_path), index=False, engine="openpyxl")

    except (KeyError, ValueError, TypeError) as exc:
        raise TransformError(f"Transform failed: {exc}\n{traceback.format_exc()}") from exc
    except Exception as exc:
        raise TransformError(f"Unexpected transform error: {exc}\n{traceback.format_exc()}") from exc

    duration = int((time.monotonic() - t0) * 1000)
    log("transform", "export", "ok", duration_ms=duration,
        extra={"output": str(out_path), "rows": len(df)})

    if _verbose:
        print(f"  [→] {len(df)} rows written → {out_path}")

    return out_path, len(df), date_range


# ──────────────────────────────────────────────────────────────────────────────
# Stage 3 — Email
# ──────────────────────────────────────────────────────────────────────────────

def stage_email(out_path: pathlib.Path, row_count: int, date_range: str) -> None:
    t0 = time.monotonic()

    gmail_user     = os.environ.get("GMAIL_USER", "")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD", "")
    recipient      = os.environ.get("REPORT_RECIPIENT", gmail_user)

    if not gmail_user or not gmail_password:
        raise EmailError(
            "GMAIL_USER and GMAIL_APP_PASSWORD must be set in your .env file."
        )

    subject = f"Talabat POS Sales Report — {date_range}"
    body = (
        f"Hi,\n\n"
        f"Please find attached the Talabat POS Sales Report for {date_range}.\n\n"
        f"  • Orders: {row_count:,}\n"
        f"  • File: {out_path.name}\n"
        f"  • Run ID: {RUN_ID}\n\n"
        f"This report was generated automatically by the Talabat pipeline.\n\n"
        f"Regards,\nTalabat Automation"
    )

    msg            = MIMEMultipart()
    msg["From"]    = gmail_user
    msg["To"]      = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with open(out_path, "rb") as f:
        attachment = MIMEApplication(
            f.read(),
            _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        attachment.add_header("Content-Disposition", "attachment", filename=out_path.name)
        msg.attach(attachment)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, recipient, msg.as_string())
    except Exception as exc:
        raise EmailError(f"Failed to send email: {exc}") from exc

    log("email", "send", "ok",
        duration_ms=int((time.monotonic() - t0) * 1000),
        extra={"to": recipient, "subject": subject, "attachment": out_path.name})

    print(f"[Stage 3] ✓ Email sent → {recipient}\n")


# ──────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ──────────────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Talabat UAE Settlement Report → Supy POS Format Pipeline"
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--file", metavar="PATH",
        help="Path to the Talabat .xlsx settlement report",
    )
    source.add_argument(
        "--from-email", action="store_true",
        help="Fetch the report from configured Gmail inboxes (Stage 0)",
    )
    parser.add_argument(
        "--no-email", action="store_true",
        help="Skip email — save output file locally only",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Verbose output",
    )
    args = parser.parse_args()

    _init_logger(verbose=args.debug)

    print(f"\n[Talabat Pipeline] run_id={RUN_ID}\n")

    # ── Stage 0: Email Fetch (only when --from-email) ─────────────
    if args.from_email:
        print("[Stage 0] Checking inboxes for FLARE Talabat report...")
        try:
            file_path = stage_fetch_from_email()
        except FetchError as exc:
            print(f"[✗] Fetch failed: {exc}", file=sys.stderr)
            log("fetch", "connect", "error", extra={"error": str(exc)})
            return 4

        if file_path is None:
            print("[Stage 0] No new FLARE report found — nothing to process.\n")
            return 0

        print(f"[Stage 0] ✓ Downloaded → {file_path.name}\n")
    else:
        file_path = pathlib.Path(args.file).expanduser().resolve()

    print(f"  Source: {file_path}\n")

    # ── Stage 1: Ingest ───────────────────────────────────────────
    print("[Stage 1] Ingesting file...")
    try:
        raw_df = stage_ingest(file_path)
    except IngestError as exc:
        print(f"[✗] Ingest failed: {exc}", file=sys.stderr)
        log("ingest", "read_excel", "error", extra={"error": str(exc)})
        return 1

    print(f"[Stage 1] ✓ {len(raw_df)} rows × {len(raw_df.columns)} cols loaded\n")

    # ── Stage 2: Transform ────────────────────────────────────────
    print("[Stage 2] Transforming...")
    try:
        out_path, row_count, date_range = stage_transform(raw_df, file_path.name)
    except TransformError as exc:
        print(f"[✗] Transform failed: {exc}", file=sys.stderr)
        log("transform", "export", "error", extra={"error": str(exc)})
        return 2

    print(f"[Stage 2] ✓ {row_count} orders → {out_path.name}\n")

    # ── Stage 3: Email ────────────────────────────────────────────
    if args.no_email:
        print(f"[Stage 3] Skipped (--no-email)\n  Output: {out_path}\n")
        return 0

    print("[Stage 3] Sending email...")
    try:
        stage_email(out_path, row_count, date_range)
    except EmailError as exc:
        print(f"[✗] Email failed: {exc}", file=sys.stderr)
        log("email", "send", "error", extra={"error": str(exc)})
        print(f"  Output file preserved: {out_path}")
        return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
