"""
monitor.py
----------
Detect feeds that have gone SILENT.

Every pipeline in this repo reports its own failures loudly. None of them can
detect the more dangerous case: a feed that simply stops arriving. No error is
raised, because nothing ran. Two real incidents inside two days:

  * BMD — GitHub disabled the cron workflows for repository inactivity.
    Reports stopped on 18 Aug and nobody noticed for three weeks.
  * BrewDog 28d2dae9 — a filename rule expecting digits silently skipped a
    hex-named branch. 77 days of a ~£1,700/day site went missing while every
    run printed success.

Both are indistinguishable from "a quiet period" unless something explicitly
checks that data ARRIVED. Four checks, each earned from a real failure:

  1. STALENESS       — is the newest sales date older than expected?
  2. BRANCH ABSENCE  — did a branch that was reporting stop reporting?
  3. UNMATCHED FILES — is anything on the server our pattern does not pick up?
  4. VOLUME DROP     — is today a fraction of the trailing median?

Check 3 is the one that would have caught 28d2dae9 in June.

    python monitor.py                 # check, print, alert on problems
    python monitor.py --no-email
    python monitor.py --date 2026-09-10
    python monitor.py --quiet-ok      # always exit 0 (for cron)

Exit codes: 0 all healthy · 1 warnings · 2 failures
"""

import argparse
import glob
import os
import pathlib
import re
import smtplib
import statistics
import sys
from datetime import datetime, timedelta
from email.mime.text import MIMEText

import yaml
from dotenv import load_dotenv

BASE_DIR = pathlib.Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

OK, WARN, FAIL = "OK", "WARN", "FAIL"
RANK = {OK: 0, WARN: 1, FAIL: 2}


class Finding:
    def __init__(self, feed, check, level, message):
        self.feed, self.check, self.level, self.message = feed, check, level, message

    def __str__(self):
        return f"[{self.level:<4}] {self.feed} · {self.check}: {self.message}"


def _sales_date(name: str):
    """
    Pull the sales date out of an output filename.

    Deliberately strict. A bare \\d{8} also matches BrewDog's location IDs —
    10000007 happily parses as a date under %Y%m%d — so candidates must start
    with 20 AND be a real calendar date. Filenames carry two dates (sales date
    then run date), and the sales date comes first.
    """
    m = re.search(r"(\d{4}-\d{2}-\d{2})", name)
    if m:
        try:
            return datetime.fromisoformat(m.group(1)).date()
        except ValueError:
            return None
    for cand in re.findall(r"(?<!\d)(20\d{6})(?!\d)", name):
        try:
            return datetime.strptime(cand, "%Y%m%d").date()
        except ValueError:
            continue
    return None


def _feed_globs(feed: dict) -> list:
    """A feed may declare one `pattern` or several `patterns`."""
    pats = feed.get("patterns") or ([feed["pattern"]] if feed.get("pattern") else [])
    return [p.replace("{date}", "*").replace("{partner}", "*") for p in pats]


def _dates_from_outputs(pattern_globs) -> dict:
    """Map sales-date -> list of files, read from output filenames."""
    if isinstance(pattern_globs, str):
        pattern_globs = [pattern_globs]
    out = {}
    for pg in pattern_globs:
        for f in glob.glob(str(BASE_DIR / pg)):
            d = _sales_date(pathlib.Path(f).name)
            if d:
                out.setdefault(d, []).append(f)
    return out


def check_staleness(feed: dict, defaults: dict, today) -> list:
    warn_d = feed.get("stale_warn_days", defaults["stale_warn_days"])
    fail_d = feed.get("stale_fail_days", defaults["stale_fail_days"])
    by_date = _dates_from_outputs(_feed_globs(feed))
    if not by_date:
        return [Finding(feed["name"], "staleness", FAIL,
                        "no output files found at all — has this feed ever run?")]
    newest = max(by_date)
    age = (today - newest).days
    if age >= fail_d:
        return [Finding(feed["name"], "staleness", FAIL,
                        f"newest sales date is {newest} ({age} days old). "
                        f"The feed has stopped.")]
    if age >= warn_d:
        return [Finding(feed["name"], "staleness", WARN,
                        f"newest sales date is {newest} ({age} days old).")]
    return [Finding(feed["name"], "staleness", OK,
                    f"newest sales date {newest} ({age}d old)")]


def check_branch_absence(feed: dict, defaults: dict, today) -> list:
    """
    A branch that was reporting and then stops is the 28d2dae9 shape of bug.

    Branch identity is taken from the filenames themselves rather than a fixed
    list, because a hard-coded roster goes stale when sites open or close — and
    a stale roster is how the absence gets excused.
    """
    window = feed.get("branch_absence_days", defaults["branch_absence_days"])
    seen = {}
    for f in [x for pg in _feed_globs(feed) for x in glob.glob(str(BASE_DIR / pg))]:
        name = pathlib.Path(f).name
        d = _sales_date(name)
        if not d:
            continue
        # Branch is everything before the sales date. Anchored on the date the
        # parser actually found, so an 8-digit location id cannot be mistaken
        # for it — which is exactly the bug this check exists to catch.
        token = d.isoformat() if d.isoformat() in name else d.strftime("%Y%m%d")
        b = name.split(f"_{token}")[0]
        seen.setdefault(b, set()).add(d)
    if not seen:
        return []
    recent_cut = today - timedelta(days=window)
    prior_cut = today - timedelta(days=window * 4)
    established = {b for b, ds in seen.items()
                   if any(prior_cut <= d < recent_cut for d in ds)}
    reporting = {b for b, ds in seen.items() if any(d >= recent_cut for d in ds)}
    missing = sorted(established - reporting)
    if missing:
        return [Finding(feed["name"], "branch absence", FAIL,
                        f"{len(missing)} branch(es) reported before but not in "
                        f"the last {window} days: {', '.join(missing[:6])}"
                        + (" …" if len(missing) > 6 else ""))]
    return [Finding(feed["name"], "branch absence", OK,
                    f"{len(reporting)} branch(es) reporting")]


def check_unmatched_files(feed: dict) -> list:
    """
    Files sitting on the server that our filename rule does not pick up.

    THIS IS THE CHECK THAT WOULD HAVE CAUGHT 28d2dae9 IN JUNE. A file that
    fails to match is not an error — it is simply never seen, so the run
    reports success while a site's sales go nowhere.
    """
    cfg_name = feed.get("config")
    if not cfg_name:
        return []
    try:
        import paramiko
        cfg = yaml.safe_load((BASE_DIR / cfg_name).read_text())
        c = cfg["connection"]
        host = os.environ.get(c["host_env"], "")
        user = os.environ.get(c["username_env"], "")
        pw = os.environ.get(c["password_env"], "")
        if not all((host, user, pw)):
            return [Finding(feed["name"], "unmatched files", WARN,
                            "credentials not set — cannot inspect the server")]
        t = paramiko.Transport((host, int(c.get("port", 22))))
        t.banner_timeout = 30
        t.connect(username=user, password=pw)
        s = paramiko.SFTPClient.from_transport(t)
        names = s.listdir(c.get("remote_dir", "."))
        s.close(); t.close()
    except Exception as exc:
        return [Finding(feed["name"], "unmatched files", WARN,
                        f"could not list the server: {type(exc).__name__}")]
    rx = re.compile(c["filename_pattern"])
    unmatched = [n for n in names
                 if not rx.match(n) and not n.startswith(".")
                 and not n.endswith("/")]
    if unmatched:
        return [Finding(feed["name"], "unmatched files", FAIL,
                        f"{len(unmatched)} file(s) on the server do NOT match "
                        f"filename_pattern and are being IGNORED: "
                        f"{', '.join(sorted(unmatched)[:5])}"
                        + (" …" if len(unmatched) > 5 else ""))]
    return [Finding(feed["name"], "unmatched files", OK,
                    f"all {len(names)} server file(s) matched")]


def check_volume(feed: dict, defaults: dict, today) -> list:
    """Today far below the trailing median usually means a partial file."""
    try:
        from openpyxl import load_workbook
    except ImportError:
        return []
    window = feed.get("volume_window_days", defaults["volume_window_days"])
    drop = feed.get("volume_drop_warn_pct", defaults["volume_drop_warn_pct"])
    by_date = _dates_from_outputs(_feed_globs(feed))
    totals = {}
    for d, files in by_date.items():
        if d < today - timedelta(days=window + 1):
            continue
        tot = 0.0
        for f in files:
            try:
                ws = load_workbook(f, data_only=True).active
                for r in ws.iter_rows(min_row=2, values_only=True):
                    if r and r[0] not in (None, ""):
                        try:
                            tot += float(r[6] or 0)
                        except (TypeError, ValueError):
                            pass
            except Exception:
                pass
        totals[d] = tot
    if len(totals) < 4:
        return []
    latest = max(totals)
    prior = [v for d, v in totals.items() if d != latest and v > 0]
    if not prior:
        return []
    med = statistics.median(prior)
    if med <= 0:
        return []
    pct = (med - totals[latest]) / med * 100
    if pct >= drop:
        return [Finding(feed["name"], "volume", WARN,
                        f"{latest} total {totals[latest]:,.0f} is {pct:.0f}% "
                        f"below the {len(prior)}-day median {med:,.0f}")]
    return [Finding(feed["name"], "volume", OK,
                    f"{latest} total {totals[latest]:,.0f} vs median {med:,.0f}")]


def send_alert(findings: list, cfg: dict, worst: str) -> None:
    em = cfg.get("email", {}) or {}
    to = em.get("recipients") or []
    user = os.environ.get("GMAIL_USER", "")
    pw = os.environ.get("GMAIL_APP_PASSWORD", "")
    if not (to and user and pw):
        print("[!] Alert not sent — recipients or Gmail credentials missing.",
              file=sys.stderr)
        return
    bad = [f for f in findings if f.level != OK]
    lines = [str(f) for f in bad] or ["(no problems)"]
    body = (
        f"POS feed monitor — {datetime.now():%Y-%m-%d %H:%M}\n\n"
        f"{len(bad)} problem(s) found.\n\n" + "\n".join(lines) +
        "\n\n---\nThis checks that data ARRIVED, which no pipeline can report "
        "on itself: when a feed stops, nothing runs, so nothing raises an "
        "error. Silence looks exactly like a quiet trading period.\n\n"
        "Two incidents this system exists because of:\n"
        "  * BMD — scheduled workflows disabled for inactivity; reports "
        "stopped 18 Aug, noticed three weeks later.\n"
        "  * BrewDog 28d2dae9 — branch excluded by a filename rule; 77 days "
        "of a ~1,700/day site missing while every run reported success.\n")
    msg = MIMEText(body, "plain")
    msg["From"] = user
    msg["To"] = ", ".join(to)
    msg["Subject"] = (f"{em.get('subject_prefix','[POS MONITOR]')} "
                      f"{worst} — {len(bad)} problem(s)")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=60) as s:
        s.login(user, pw)
        s.sendmail(user, to, msg.as_string())
    print(f"  alert sent -> {', '.join(to)}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Detect feeds that have gone silent")
    ap.add_argument("--config", default="monitor_config.yaml")
    ap.add_argument("--date", help="Treat this as today (YYYY-MM-DD)")
    ap.add_argument("--no-email", action="store_true")
    ap.add_argument("--quiet-ok", action="store_true",
                    help="Always exit 0 — for cron that should not report failure")
    ap.add_argument("--skip-server", action="store_true",
                    help="Skip SFTP listing checks (offline use)")
    args = ap.parse_args()

    cfg = yaml.safe_load((BASE_DIR / args.config).read_text())
    defaults = cfg.get("defaults", {})
    today = (datetime.fromisoformat(args.date).date() if args.date
             else datetime.now().date())

    print(f"\n{'=' * 68}\n POS feed monitor — {today}\n{'=' * 68}\n")
    findings = []
    for feed in cfg.get("feeds", []):
        findings += check_staleness(feed, defaults, today)
        findings += check_branch_absence(feed, defaults, today)
        if not args.skip_server:
            findings += check_unmatched_files(feed)
        findings += check_volume(feed, defaults, today)

    for f in findings:
        print(f"  {f}")

    bad = [f for f in findings if f.level != OK]
    worst = OK
    for f in findings:
        if RANK[f.level] > RANK[worst]:
            worst = f.level
    print(f"\n  {len(bad)} problem(s); worst = {worst}\n")

    if bad and not args.no_email:
        send_alert(findings, cfg, worst)
    elif not bad:
        print("  All feeds healthy — no alert sent "
              "(a daily 'all fine' email trains people to ignore it).")

    if args.quiet_ok:
        return 0
    return {OK: 0, WARN: 1, FAIL: 2}[worst]


if __name__ == "__main__":
    sys.exit(main())
