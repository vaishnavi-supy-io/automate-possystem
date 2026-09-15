"""
sftp_ingest.py
--------------
Pick up POS sales files a client drops on SFTP and turn them into Supy upload
files. Driven entirely by a YAML config, so a new client is a config, not code.

Why this exists: the delivery-partner pipelines fight bot detection, expiring
sessions and portal restyles. A file drop has none of that — it runs headless,
needs no display, and cannot break because someone changed a CSS class. Where a
client can be moved onto SFTP, it should be.

    python sftp_ingest.py --config smashtag_sftp.yaml            # anything new
    python sftp_ingest.py --config smashtag_sftp.yaml --date 2026-09-07
    python sftp_ingest.py --config smashtag_sftp.yaml --all      # ignore history
    python sftp_ingest.py --config smashtag_sftp.yaml --list     # look, do nothing
    python sftp_ingest.py --config smashtag_sftp.yaml --no-email

Exit codes:  0 ok · 1 config/auth · 2 transfer · 3 transform · 4 email
"""

import argparse
import json
import os
import pathlib
import re
import smtplib
import sys
import time
from datetime import datetime, timedelta
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import paramiko
import yaml
from dotenv import load_dotenv
from openpyxl import Workbook

BASE_DIR = pathlib.Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

SUPY_HEADERS = ["Sales Date *", "POS Item ID *", "POS Item Name", "Sold QTY *",
                "Total Discount Value", "Total sales excl. tax *",
                "Total sales incl. tax *", "Order ID", "Sales Type Code"]


class ConfigError(Exception):
    pass


class TransferError(Exception):
    pass


class TransformError(Exception):
    pass


def load_config(path: str) -> dict:
    p = BASE_DIR / path if not os.path.isabs(path) else pathlib.Path(path)
    if not p.exists():
        raise ConfigError(f"No such config: {p}")
    cfg = yaml.safe_load(p.read_text())
    for section in ("client", "connection", "columns"):
        if section not in cfg:
            raise ConfigError(f"{p.name} is missing the '{section}' section")
    return cfg


def _slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_")


def _state_path(cfg: dict) -> pathlib.Path:
    d = BASE_DIR / "state" / f"sftp_{_slug(cfg['client']['name']).lower()}"
    d.mkdir(parents=True, exist_ok=True)
    return d / "processed.json"


def load_processed(cfg: dict) -> dict:
    p = _state_path(cfg)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def save_processed(cfg: dict, seen: dict) -> None:
    try:
        _state_path(cfg).write_text(json.dumps(seen, indent=2, sort_keys=True))
    except OSError:
        pass


def connect(cfg: dict):
    c = cfg["connection"]
    missing = [k for k in ("host_env", "username_env", "password_env") if not c.get(k)]
    if missing:
        raise ConfigError(f"connection is missing {', '.join(missing)}")
    creds = {}
    for field, key in (("host", "host_env"), ("username", "username_env"),
                       ("password", "password_env")):
        val = os.environ.get(c[key], "")
        if not val:
            raise ConfigError(
                f"{c[key]} is not set — add it to .env (see .env.example). "
                f"Never paste credentials into chat or a ticket.")
        creds[field] = val
    try:
        transport = paramiko.Transport((creds["host"], int(c.get("port", 22))))
        transport.banner_timeout = 30
        transport.connect(username=creds["username"], password=creds["password"])
        return transport, paramiko.SFTPClient.from_transport(transport)
    except paramiko.AuthenticationException:
        raise ConfigError(
            f"{cfg['client']['name']}: SFTP authentication failed for "
            f"{creds['username']}@{creds['host']}. The password in "
            f"{c['password_env']} is wrong, or the account is disabled.")
    except Exception as exc:
        raise TransferError(f"could not reach {creds['host']}: {exc}")


def list_candidates(sftp, cfg: dict) -> list:
    """Files matching the pattern, newest first, excluding any still uploading."""
    c = cfg["connection"]
    pattern = re.compile(c["filename_pattern"])
    min_age = int(c.get("min_age_seconds", 60))
    now = time.time()
    out = []
    for entry in sftp.listdir_attr(c.get("remote_dir", ".")):
        m = pattern.match(entry.filename)
        if not m:
            continue
        age = now - (entry.st_mtime or 0)
        if age < min_age:
            print(f"  [skip] {entry.filename} — modified {age:.0f}s ago, "
                  f"may still be uploading")
            continue
        out.append({
            "name": entry.filename,
            "size": entry.st_size,
            "mtime": entry.st_mtime,
            "date": m.groupdict().get("date"),
            "location": m.groupdict().get("location", ""),
        })
    return sorted(out, key=lambda f: f["mtime"], reverse=True)


def _num(value, cfg: dict) -> float:
    s = str(value or "").strip()
    if cfg.get("parsing", {}).get("strip_thousands", True):
        s = s.replace(",", "")
    if not s:
        if cfg.get("parsing", {}).get("blank_numeric_is_zero", True):
            return 0.0
        raise TransformError("blank numeric value with blank_numeric_is_zero off")
    return float(s)


def convert(local: pathlib.Path, cfg: dict, file_date: str) -> tuple:
    """CSV -> Supy rows. Returns (rows, totals)."""
    import csv
    cols = cfg["columns"]
    fmt = cfg.get("output", {}).get("date_format", "%d-%b-%Y")
    with local.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = [v for v in cols.values() if v not in (reader.fieldnames or [])]
        if missing:
            raise TransformError(
                f"{local.name} is missing expected column(s): {missing}\n"
                f"  found: {reader.fieldnames}")
        rows, qty, net, gross, disc = [], 0.0, 0.0, 0.0, 0.0
        for raw in reader:
            raw_date = (raw.get(cols["sales_date"]) or file_date or "").strip()
            try:
                when = datetime.fromisoformat(raw_date[:10])
            except ValueError:
                raise TransformError(f"unparseable date {raw_date!r} in {local.name}")
            q = _num(raw.get(cols["qty"]), cfg)
            n = _num(raw.get(cols["net"]), cfg)
            g = _num(raw.get(cols["gross"]), cfg)
            d = _num(raw.get(cols.get("discount", "")), cfg) if cols.get("discount") else 0.0
            qty += q; net += n; gross += g; disc += d
            rows.append([
                when.strftime(fmt),
                str(raw.get(cols["item_id"]) or "").strip(),
                str(raw.get(cols["item_name"]) or "").strip(),
                q, d, round(n, 2), round(g, 2), "", "",
            ])
    return rows, {"qty": qty, "net": net, "gross": gross, "discount": disc}


def convert_aggregate(local: pathlib.Path, cfg: dict, file_date: str,
                      location: str) -> tuple:
    """
    Transaction-level CSV -> daily totals per item.

    One row per item per till transaction becomes one row per item per day,
    which is what a sales upload expects. Row counts collapse hard: ~1,000
    transaction lines per store per day become a few hundred item lines.
    """
    import csv
    cols = cfg["columns"]
    agg_cfg = cfg.get("aggregate", {}) or {}
    include = {t.lower() for t in (agg_cfg.get("include_types") or [])}
    fmt = cfg.get("output", {}).get("date_format", "%d-%b-%Y")
    when = datetime.strptime(
        file_date, cfg["connection"].get("date_in_name_format", "%Y%m%d"))

    buckets, skipped = {}, {}
    with local.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        # This export pads its header with spaces (" Revenue Category"), which
        # silently breaks a plain lookup, so every key is stripped.
        fields = [(f or "").strip() for f in (reader.fieldnames or [])]
        missing = [v for k, v in cols.items()
                   if k in ("item_id", "item_name", "qty", "net", "gross")
                   and v not in fields]
        if missing:
            raise TransformError(
                f"{local.name} is missing expected column(s): {missing}\n"
                f"  found: {fields}")
        for raw in reader:
            r = {(k or "").strip(): (v or "").strip() for k, v in raw.items()}
            ttype = r.get(cols.get("txn_type", ""), "")
            if include and ttype.lower() not in include:
                skipped[ttype or "(blank)"] = skipped.get(ttype or "(blank)", 0) + 1
                continue
            key = (r.get(cols["item_id"], ""), r.get(cols["item_name"], ""))
            b = buckets.setdefault(key, {"qty": 0.0, "net": 0.0,
                                         "vat": 0.0, "gross": 0.0})
            b["qty"] += _num(r.get(cols["qty"]), cfg)
            b["net"] += _num(r.get(cols["net"]), cfg)
            b["gross"] += _num(r.get(cols["gross"]), cfg)
            if cols.get("vat"):
                b["vat"] += _num(r.get(cols["vat"]), cfg)

    rows, qty, net, gross = [], 0.0, 0.0, 0.0
    for (item_id, item_name), b in sorted(buckets.items(),
                                          key=lambda kv: -kv[1]["gross"]):
        qty += b["qty"]; net += b["net"]; gross += b["gross"]
        rows.append([when.strftime(fmt), item_id, item_name, b["qty"], 0,
                     round(b["net"], 2), round(b["gross"], 2), "", location])
    return rows, {"qty": qty, "net": net, "gross": gross, "discount": 0.0,
                  "skipped": skipped, "items": len(buckets)}


def write_xlsx(rows: list, cfg: dict, file_date: str,
               suffix: str = "") -> pathlib.Path:
    out_dir = BASE_DIR / "output"
    out_dir.mkdir(exist_ok=True)
    dest = _slug(cfg["client"].get("destination") or cfg["client"]["name"])
    if suffix:
        dest = f"{dest}_{_slug(suffix)}"
    path = out_dir / f"{dest}_{file_date}_{datetime.now():%Y%m%d}.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.append(SUPY_HEADERS)
    for r in rows:
        ws.append(r)
    wb.save(path)
    return path


def send_email(paths: list, cfg: dict, summary: str, recipients: list,
               group: str = "") -> None:
    user = os.environ.get("GMAIL_USER", "")
    pw = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not (user and pw and recipients):
        print("[!] Email skipped — GMAIL_USER / GMAIL_APP_PASSWORD / recipients "
              "not all set.", file=sys.stderr)
        return
    name = cfg["client"]["name"]
    if group:
        name = f"{name} — branch {group}"
    notes = (cfg.get("email", {}) or {}).get("notes", "")
    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = f"{name} — POS sales — {len(paths)} file(s)"
    body = (f"Hi,\n\n{name} sales, formatted for Supy upload. "
            f"One file per day.\n\n{summary}\n")
    if len(paths) > 7:
        body += (f"\n!! This is a BACKFILL of {len(paths)} days. Nothing from "
                 f"this client has been uploaded automatically before, so "
                 f"please check none of these days is already in Supy by hand "
                 f"— otherwise the sales will be counted twice.\n")
    if notes:
        body += f"\n{notes.strip()}\n"
    body += "\nRegards,\nSupy POS Integration\n"
    msg.attach(MIMEText(body, "plain"))
    for p in paths:
        with open(p, "rb") as fh:
            part = MIMEApplication(
                fh.read(),
                _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        part.add_header("Content-Disposition", "attachment", filename=p.name)
        msg.attach(part)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=90) as s:
        s.login(user, pw)
        s.sendmail(user, recipients, msg.as_string())
    print(f"  Email sent -> {', '.join(recipients)}")


def resolve_recipients(args, cfg: dict) -> list:
    if args.email_to:
        return args.email_to
    raw = (cfg.get("email", {}) or {}).get("recipients") or \
        os.environ.get("REPORT_RECIPIENT", "")
    if isinstance(raw, list):
        return raw
    return [a.strip() for a in re.split(r"[,;]", raw) if a.strip()]


def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest POS sales files from SFTP")
    ap.add_argument("--config", required=True)
    ap.add_argument("--date", help="Only this file date (YYYY-MM-DD)")
    ap.add_argument("--all", action="store_true",
                    help="Reprocess everything, ignoring what has been done")
    ap.add_argument("--list", action="store_true", help="List files and exit")
    ap.add_argument("--no-email", action="store_true")
    ap.add_argument("--email-to", action="append", metavar="ADDR")
    ap.add_argument("--limit", type=int, help="Process at most N files")
    ap.add_argument("--email-per", choices=["location"],
                    help="One email per branch instead of one for the whole run")
    args = ap.parse_args()

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"[x] {exc}", file=sys.stderr)
        return 1

    name = cfg["client"]["name"]
    print(f"\n{'=' * 62}\n {name} — SFTP ingest\n{'=' * 62}")

    try:
        transport, sftp = connect(cfg)
    except ConfigError as exc:
        print(f"[x] {exc}", file=sys.stderr)
        return 1
    except TransferError as exc:
        print(f"[x] {exc}", file=sys.stderr)
        return 2

    try:
        files = list_candidates(sftp, cfg)
        print(f"  {len(files)} matching file(s) on the server")
        if not files:
            print("[!] Nothing matches the filename pattern — has the export "
                  "changed?", file=sys.stderr)
            return 2

        newest_age = (time.time() - files[0]["mtime"]) / 3600
        warn_after = (cfg.get("alerting", {}) or {}).get(
            "warn_if_newest_older_than_hours")
        if warn_after and newest_age > float(warn_after):
            print(f"[!] Newest file is {newest_age:.0f}h old "
                  f"({files[0]['name']}). The client may have stopped sending.",
                  file=sys.stderr)

        if args.list:
            for f in files[:25]:
                ts = datetime.fromtimestamp(f["mtime"]).strftime("%Y-%m-%d %H:%M")
                print(f"    {f['name']:<52} {f['size']:>9,}  {ts}")
            return 0

        seen = {} if args.all else load_processed(cfg)
        todo = [f for f in files if args.all or f["name"] not in seen]
        if args.date:
            todo = [f for f in todo if f["date"] == args.date]
        if args.limit:
            todo = todo[:args.limit]

        if not todo:
            print("  Nothing new to process.")
            return 0
        print(f"  {len(todo)} file(s) to process\n")

        dl_dir = BASE_DIR / "downloads"
        dl_dir.mkdir(exist_ok=True)
        written, lines = [], []
        by_group, group_lines = {}, {}
        remote_dir = cfg["connection"].get("remote_dir", ".").rstrip("/")

        for f in todo:
            local = dl_dir / f["name"]
            sftp.get(f"{remote_dir}/{f['name']}" if remote_dir else f["name"],
                     str(local))
            if (cfg.get("source", {}) or {}).get("mode") == "aggregate_transactions":
                rows, tot = convert_aggregate(local, cfg, f["date"],
                                              f.get("location", ""))
            else:
                rows, tot = convert(local, cfg, f["date"])
            if not rows:
                print(f"  [!] {f['name']} produced 0 rows — skipped, not marked "
                      f"done.", file=sys.stderr)
                continue
            path = write_xlsx(rows, cfg, f["date"], f.get("location", ""))
            written.append(path)
            by_group.setdefault(f.get("location", ""), []).append((path, None))
            ratio = tot["gross"] / tot["net"] if tot["net"] else 0
            label = f"{f['date']}" + (f" · {f['location']}" if f.get("location") else "")
            line = (f"  {label:<22} {len(rows):>4} rows  "
                    f"qty {tot['qty']:>7,.0f}  net {tot['net']:>10,.2f}  "
                    f"gross {tot['gross']:>10,.2f}  (x{ratio:.4f})")
            print(line)
            lines.append(line)
            group_lines.setdefault(f.get("location", ""), []).append(line)
            seen[f["name"]] = {"processed_at": datetime.now().isoformat(
                timespec="seconds"), "size": f["size"], "rows": len(rows),
                "output": path.name}

        if not written:
            print("[x] No files produced.", file=sys.stderr)
            return 3

        # Only recorded once the files exist, so a crash mid-run reprocesses
        # rather than silently skipping a day.
        save_processed(cfg, seen)
        print(f"\n  {len(written)} file(s) written to {BASE_DIR / 'output'}")

        if not args.no_email:
            recipients = resolve_recipients(args, cfg)
            if not recipients:
                print("[!] No recipient configured — not emailed.",
                      file=sys.stderr)
                return 0
            group_by = args.email_per or (cfg.get("email", {}) or {}).get("group_by")
            try:
                if group_by == "location" and any(by_group):
                    # One email per branch: each site is its own Supy entity,
                    # so whoever uploads handles them one at a time.
                    print()
                    for loc in sorted(by_group):
                        paths = [p for p, _ in by_group[loc]]
                        send_email(paths, cfg, "\n".join(group_lines.get(loc, [])),
                                   recipients, group=loc)
                else:
                    send_email(written, cfg, "\n".join(lines), recipients)
            except Exception as exc:
                print(f"[x] Email failed: {exc}", file=sys.stderr)
                return 4
        return 0
    except TransformError as exc:
        print(f"[x] Transform error: {exc}", file=sys.stderr)
        return 3
    except Exception as exc:
        print(f"[x] Unexpected: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        try:
            sftp.close(); transport.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
