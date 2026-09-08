"""
run_all_partners.py
-------------------
Run every Street Food Ltd. delivery partner in one command and print a single
summary table.

Nine partners, two engines:

    Deliveroo                 → deliveroo_automation.py  (real report export)
    Just Eat                  → partner_scraper.py       (2 businesses)
    JustEat Business          → partner_scraper.py       (menu-price lookup)
    Ordit, Feedr, &Dine       → partner_scraper.py
    HomeCook                  → partner_scraper.py       (menu-price lookup)
    Uber Eats                 → partner_scraper.py       (PIN auth; interim)

One partner failing never stops the others — each runs in its own subprocess
and its exit code is collected. A partial day's data is more useful than none,
and the summary makes it obvious what still needs doing by hand.

Usage:
    python run_all_partners.py                        # yesterday, all partners
    python run_all_partners.py --date 2026-08-05
    python run_all_partners.py --from 2026-08-01 --to 2026-08-05
    python run_all_partners.py --only feedr,ordit     # subset
    python run_all_partners.py --skip uber_eats
    python run_all_partners.py --dry-run              # no files, no email
    python run_all_partners.py --no-email

Exit codes:
    0  every partner succeeded
    1  at least one partner failed (see the summary table)
"""

import argparse
import os
import pathlib
import re
import smtplib
import subprocess
import sys
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = pathlib.Path(__file__).parent
PARTNERS_DIR = BASE_DIR / "partners"
PYTHON = sys.executable

# Exit-code meanings, shared by both engines.
EXIT_LABELS = {
    0: "ok",
    1: "auth/config",
    2: "scrape/nav",
    3: "transform",
    4: "email",
}


def discover_targets() -> list:
    """Return [(name, engine)] — Deliveroo first, then every partners/*.yaml."""
    targets = []
    if (BASE_DIR / "deliveroo_automation.py").exists():
        targets.append(("deliveroo", "deliveroo_automation.py"))
    for path in sorted(PARTNERS_DIR.glob("*.yaml")):
        targets.append((path.stem, "partner_scraper.py"))
    return targets


def build_command(name: str, engine: str, args) -> list:
    cmd = [PYTHON, str(BASE_DIR / engine)]
    if engine == "partner_scraper.py":
        cmd += ["--partner", name]
    if args.date:
        cmd += ["--date", args.date]
    if args.date_from:
        cmd += ["--from", args.date_from]
    if args.date_to:
        cmd += ["--to", args.date_to]
    # Forward the recipient list so the PER-PARTNER reports go to the same
    # addresses as the summary. Without this the flag only ever reached the
    # roll-up, which is how customer.care@ ended up on the summary alone.
    for addr in (args.email_to or []):
        cmd += ["--email-to", addr]
    if args.no_email:
        cmd.append("--no-email")
    if args.force_login:
        cmd.append("--force-login")
    if args.debug:
        cmd.append("--debug")
    # deliveroo_automation.py has no --dry-run; --no-email is the closest
    # equivalent, so translate rather than passing an unknown flag.
    if args.dry_run:
        cmd.append("--dry-run" if engine == "partner_scraper.py" else "--no-email")
    return cmd


def resolve_recipients() -> list:
    """REPORT_RECIPIENT may hold a comma/semicolon-separated list of addresses."""
    raw = os.environ.get("REPORT_RECIPIENT", "") or os.environ.get("GMAIL_USER", "")
    seen, out = set(), []
    for addr in re.split(r"[,;]", raw):
        addr = addr.strip()
        if addr and addr.casefold() not in seen:
            seen.add(addr.casefold())
            out.append(addr)
    return out


def collect_outputs(since: datetime) -> list:
    """Every .xlsx written to output/ during this run, newest last."""
    out_dir = BASE_DIR / "output"
    if not out_dir.exists():
        return []
    cutoff = since.timestamp()
    files = [p for p in out_dir.glob("*.xlsx")
             if p.stat().st_mtime >= cutoff and not p.name.startswith("~$")]
    return sorted(files, key=lambda p: p.stat().st_mtime)


def send_summary(results: list, attachments: list, date_range: str,
                 elapsed: float, recipients: list) -> None:
    """One roll-up email for the whole run: table in the body, files attached."""
    gmail_user = os.environ.get("GMAIL_USER", "")
    gmail_password = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not gmail_user or not gmail_password:
        print("[!] Summary email skipped — GMAIL_USER / GMAIL_APP_PASSWORD not set.",
              file=sys.stderr)
        return

    ok = [r for r in results if r[1] == 0]
    failed = [r for r in results if r[1] != 0]

    subject = f"Street Food Ltd. — POS sales run {date_range} — {len(ok)}/{len(results)} OK"
    if failed:
        subject += f" — {len(failed)} NEED ATTENTION"

    table = [f"  {'Partner':<20} {'Result':<14} {'Time':>8}",
             f"  {'-'*20} {'-'*14} {'-'*8}"]
    for name, code, secs in results:
        mark = "OK " if code == 0 else "!! "
        table.append(f"  {name:<20} {mark}{EXIT_LABELS.get(code, f'exit {code}'):<11} {secs:>7.0f}s")

    body = (f"Hi,\n\nPOS sales run for {date_range}, formatted for Supy upload.\n\n"
            + "\n".join(table)
            + f"\n\n  * Completed in {elapsed:.0f}s\n"
              f"  * Files attached: {len(attachments)}\n")
    if failed:
        body += ("\n!! These partners did NOT produce a file and must be "
                 "uploaded by hand:\n"
                 + "".join(f"     - {n} ({EXIT_LABELS.get(c, f'exit {c}')})\n"
                           for n, c, _ in failed))
    else:
        body += "\n  All partners complete. Nothing to do by hand.\n"
    body += "\nRegards,\nSupy POS Integration"

    msg = MIMEMultipart()
    msg["From"] = gmail_user
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    for path in attachments:
        with open(path, "rb") as fh:
            part = MIMEApplication(
                fh.read(),
                _subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        part.add_header("Content-Disposition", "attachment", filename=path.name)
        msg.attach(part)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=60) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, recipients, msg.as_string())
    except Exception as exc:
        print(f"[!] Summary email failed: {exc}", file=sys.stderr)
        return
    print(f"\n  Summary email sent -> {', '.join(recipients)}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run every delivery partner pipeline")
    parser.add_argument("--date", metavar="YYYY-MM-DD", help="Single day")
    parser.add_argument("--from", dest="date_from", metavar="YYYY-MM-DD")
    parser.add_argument("--to", dest="date_to", metavar="YYYY-MM-DD")
    parser.add_argument("--only", help="Comma-separated subset to run")
    parser.add_argument("--skip", help="Comma-separated partners to skip")
    parser.add_argument("--no-email", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-login", action="store_true")
    parser.add_argument("--debug", action="store_true",
                        help="Headed browser — runs sequentially, needs a display")
    parser.add_argument("--email-to", metavar="ADDR", action="append",
                        help="Recipient for the run-summary email (repeatable). "
                             "Defaults to REPORT_RECIPIENT in .env")
    parser.add_argument("--no-summary-email", action="store_true",
                        help="Run normally but do not send the roll-up summary email")
    parser.add_argument("--list", action="store_true",
                        help="List discovered targets and exit")
    args = parser.parse_args()

    targets = discover_targets()

    if args.list:
        print("\n  Discovered pipelines:\n")
        for name, engine in targets:
            print(f"    {name:<20} {engine}")
        print()
        return 0

    if args.only:
        wanted = {s.strip() for s in args.only.split(",") if s.strip()}
        unknown = wanted - {n for n, _ in targets}
        if unknown:
            print(f"[✗] Unknown partner(s): {', '.join(sorted(unknown))}\n"
                  f"    Available: {', '.join(n for n, _ in targets)}",
                  file=sys.stderr)
            return 1
        targets = [(n, e) for n, e in targets if n in wanted]

    if args.skip:
        skip = {s.strip() for s in args.skip.split(",") if s.strip()}
        targets = [(n, e) for n, e in targets if n not in skip]

    if not targets:
        print("[✗] Nothing to run.", file=sys.stderr)
        return 1

    started = datetime.now()
    print(f"\n{'='*68}")
    print(f" Street Food Ltd. — running {len(targets)} partner pipeline(s)")
    print(f" Started: {started:%Y-%m-%d %H:%M:%S}")
    print(f"{'='*68}")

    results = []
    for name, engine in targets:
        print(f"\n{'─'*68}\n▶ {name}\n{'─'*68}")
        cmd = build_command(name, engine, args)
        t0 = datetime.now()
        try:
            proc = subprocess.run(cmd, cwd=str(BASE_DIR))
            code = proc.returncode
        except KeyboardInterrupt:
            print(f"\n[!] Interrupted during {name}.", file=sys.stderr)
            results.append((name, 130, (datetime.now() - t0).total_seconds()))
            break
        except Exception as exc:
            print(f"[✗] Could not launch {name}: {exc}", file=sys.stderr)
            code = 1
        results.append((name, code, (datetime.now() - t0).total_seconds()))

    # ── Summary ───────────────────────────────────────────────────────
    elapsed = (datetime.now() - started).total_seconds()
    ok = [r for r in results if r[1] == 0]
    failed = [r for r in results if r[1] != 0]

    print(f"\n{'='*68}")
    print(f" SUMMARY — {len(ok)}/{len(results)} succeeded in {elapsed:.0f}s")
    print(f"{'='*68}\n")
    print(f"  {'Partner':<20} {'Result':<14} {'Time':>8}")
    print(f"  {'-'*20} {'-'*14} {'-'*8}")
    for name, code, secs in results:
        mark = "✓" if code == 0 else "✗"
        label = EXIT_LABELS.get(code, f"exit {code}")
        print(f"  {name:<20} {mark} {label:<12} {secs:>7.0f}s")

    if failed:
        print(f"\n  {len(failed)} partner(s) need attention:\n")
        for name, code, _ in failed:
            print(f"    • {name} — {EXIT_LABELS.get(code, f'exit {code}')}"
                  f"  (see logs/{name}_*.jsonl and screenshots/{name}_*)")
        print("\n  Those partners' sales must be uploaded by hand today.")
    else:
        print("\n  All partners complete. Nothing to do by hand.")

    print(f"\n  Output files: {BASE_DIR / 'output'}\n")

    if not (args.no_summary_email or args.no_email or args.dry_run):
        recipients = args.email_to or resolve_recipients()
        if recipients:
            date_range = (args.date or
                          (f"{args.date_from} to {args.date_to}"
                           if args.date_from and args.date_to else "yesterday"))
            send_summary(results, collect_outputs(started), date_range,
                         elapsed, recipients)
        else:
            print("[!] Summary email skipped — no recipient configured.",
                  file=sys.stderr)

    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
