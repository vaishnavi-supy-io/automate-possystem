"""
set_credential.py
-----------------
Safely write a credential into .env without it ever being echoed.

The value is read with getpass (no terminal echo), so it does not appear on
screen, in shell history, in this repo's logs, or in any AI transcript. Only
the key name is ever printed.

Usage:
    python set_credential.py ANDDINE_PASSWORD
    python set_credential.py --partner anddine        # prompts for user + password
    python set_credential.py --list                   # show which keys are set
    python set_credential.py --check anddine          # is this partner ready?

Existing lines are replaced in place; new keys are appended. Nothing else in
.env is touched.
"""

import argparse
import getpass
import os
import pathlib
import re
import sys

BASE_DIR = pathlib.Path(__file__).parent
ENV_PATH = BASE_DIR / ".env"

# partner → [(env key, prompt label, is_secret), ...]
# Most partners are a username plus one secret. Dines branches also carry a
# manager PIN, so this is a list of fields rather than a fixed pair.
def _login(prefix: str, secret_label: str = "password") -> list:
    return [(f"{prefix}_USERNAME", "username / email", False),
            (f"{prefix}_PASSWORD", secret_label, True)]


# Dines — one login PER BRANCH, each with its own manager PIN. All NINE Supy
# branches are listed so `--partner dines_<x>` works the moment a credential
# sheet arrives. Only canary_wharf / victoria / oxford / paddington are set as
# of 2026-09-01; the other five were added on the instruction to cover all
# nine branches and are still to be supplied.
DINES_PARTNERS = {
    "dines_cw":  "DINES_CW",     # Black Bear Burger Canary Wharf
    "dines_vic": "DINES_VIC",    # Black Bear Burger Victoria
    "dines_os":  "DINES_OS",     # 20ft Chicken Oxford St
    "dines_pd":  "DINES_PD",     # Black Bear Burger Paddington
    "dines_sh":  "DINES_SH",     # Black Bear Burger Shoreditch
    "dines_bx":  "DINES_BX",     # Black Bear Burger Brixton
    "dines_cm":  "DINES_CM",     # Black Bear Burger Camden
    "dines_em":  "DINES_EM",     # Black Bear Burger Exmouth Market
    "dines_wf":  "DINES_WF",     # Black Bear Burger Westfield
}


def _dines(prefix: str) -> list:
    return _login(prefix) + [(f"{prefix}_PIN", "manager PIN", True)]


PARTNER_KEYS = {
    "deliveroo":        _login("DELIVEROO"),
    "just_eat":         _login("JUST_EAT"),
    "justeat_business": _login("JUSTEAT_BUSINESS"),
    "ordit":            _login("ORDIT"),
    "feedr":            _login("FEEDR"),
    "anddine":          _login("ANDDINE"),
    "homecook":         _login("HOMECOOK"),
    "uber_eats":        [("UBER_EATS_USERNAME", "username / email", False),
                         ("UBER_EATS_PIN", "PIN", True)],
    # Micros Symphony (Oracle R&A). COMPANY mirrors the Independent tenant's
    # PORTAL_COMPANY field; leave it blank at the prompt if this deployment's
    # sign-in has no tenant step.
    "symphony":         [("SYMPHONY_USERNAME", "username / email", False),
                         ("SYMPHONY_COMPANY", "company / tenant (blank if none)", False),
                         ("SYMPHONY_PASSWORD", "password", True)],
    **{key: _dines(prefix) for key, prefix in DINES_PARTNERS.items()},
}


def read_env_lines() -> list:
    if not ENV_PATH.exists():
        return []
    return ENV_PATH.read_text().splitlines()


def set_key(key: str, value: str) -> str:
    """Replace or append KEY=value. Returns 'updated' or 'added'."""
    lines = read_env_lines()
    prefix = f"{key}="
    found = False
    out = []
    for line in lines:
        if line.startswith(prefix):
            out.append(f"{key}={value}")
            found = True
        else:
            out.append(line)
    if not found:
        out.append(f"{key}={value}")

    ENV_PATH.write_text("\n".join(out) + "\n")
    # Owner-only: this file holds the customer's portal credentials.
    ENV_PATH.chmod(0o600)
    return "updated" if found else "added"


KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


def load_from_file(path: pathlib.Path, delete_source: bool) -> int:
    """Copy KEY=VALUE lines from a plaintext file into .env.

    Values are never printed, never logged and never returned — only key names
    and character counts. Blank lines, comments and anything that is not a
    valid KEY=VALUE pair are skipped and reported by line number only, so a
    malformed line never leaks its contents.
    """
    if not path.exists():
        print(f"  [!] Not found: {path}", file=sys.stderr)
        return 1

    written, skipped = [], []
    for lineno, raw in enumerate(path.read_text().splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            skipped.append(lineno)
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip().strip('"').strip("'")
        if not KEY_RE.match(key) or not value:
            skipped.append(lineno)
            continue
        action = set_key(key, value)
        written.append((key, action, len(value)))
        del value          # keep it out of the frame for any traceback

    print(f"\n  .env → {ENV_PATH}  (mode 600)\n")
    for key, action, size in written:
        print(f"  ✓ {key:26s} {action:8s} ({size} chars, value not displayed)")
    if skipped:
        print(f"\n  [!] skipped {len(skipped)} line(s) that were not KEY=VALUE: "
              f"lines {', '.join(map(str, skipped))}")
    if not written:
        print("  [!] nothing written — expected lines like DINES_CW_PIN=...")
        return 1

    if delete_source:
        # Overwrite before unlinking so the plaintext is not left in free space.
        try:
            size = path.stat().st_size
            with open(path, "r+b") as fh:
                fh.write(b"\0" * size)
                fh.flush()
                os.fsync(fh.fileno())
            path.unlink()
            print(f"\n  ✓ source file overwritten and deleted: {path}")
        except Exception as exc:
            print(f"\n  [!] could not delete {path}: {exc}. Delete it yourself.",
                  file=sys.stderr)
    else:
        print(f"\n  [!] {path} still holds these credentials in plaintext. "
              f"Delete it, or re-run with --delete-source.")
    return 0


# Dines credential sheets arrive as a labelled table, often two branches
# side by side in tab-separated columns:
#
#   CW - Canary Wharf              VIC - victoria
#   Login URL: dashboard...        Login URL: dashboard...
#   Username: ed+mhcw@...          Username: ed+MHVC@...
#   Password: ...                  Password: ...
#   Manager PIN: 5009              Manager PIN: 3192
#
# The branch is identified from the username's mh** token, which is
# unambiguous, so no password is ever inspected to decide where it belongs.
# Tokens are only known for the four Market Hall branches whose sheets have
# been seen. The five branches added 2026-09-01 have NO known token, so a
# table containing them parses as "no recognised mh** token" and the write is
# refused — which is the intended outcome. Guessing a token here would post
# one branch's password against another. Add a token only after seeing that
# branch's real username, or set those keys one at a time:
#     python set_credential.py --partner dines_sh
DINES_USER_TOKENS = {"mhcw": "DINES_CW", "mhvc": "DINES_VIC",
                     "mhos": "DINES_OS", "mhpd": "DINES_PD"}
# Prefixes the block parser can recognise — NOT every configured branch.
DINES_BLOCK_PREFIXES = tuple(DINES_USER_TOKENS.values())
LABEL_RE = re.compile(r"^(username|password|manager\s*pin|pin)\s*[:=]\s*(.+)$", re.I)
CELL_SPLIT_RE = re.compile(r"\t+| {2,}")
FIELD_FOR_LABEL = {"username": "USERNAME", "password": "PASSWORD",
                   "manager pin": "PIN", "pin": "PIN"}


def _shred(path: pathlib.Path) -> bool:
    """Overwrite a plaintext credential file before unlinking it.

    Mirrors the inline logic in load_from_file(): zero the bytes first so the
    secret is not left recoverable in free space, then remove the file.
    """
    try:
        size = path.stat().st_size
        with open(path, "r+b") as fh:
            fh.write(b"\0" * size)
            fh.flush()
            os.fsync(fh.fileno())
        path.unlink()
        return True
    except Exception as exc:
        print(f"\n  [!] could not delete {path}: {exc}. Delete it yourself.",
              file=sys.stderr)
        return False


# Labels a human actually writes on a credential note, mapped to the field
# suffix this tool stores. Anything not listed (Link:, Portal:, Note:) is
# ignored rather than guessed — a URL is not a credential.
LABEL_TO_FIELD = {
    "username": "USERNAME", "user name": "USERNAME", "user": "USERNAME",
    "email": "USERNAME", "email or user name": "USERNAME", "login": "USERNAME",
    "password": "PASSWORD", "pass": "PASSWORD", "pwd": "PASSWORD",
    "enterprise name": "COMPANY", "enterprise": "COMPANY",
    "company": "COMPANY", "company name": "COMPANY", "tenant": "COMPANY",
    "organisation": "COMPANY", "organization": "COMPANY", "org": "COMPANY",
}
LABEL_IGNORED = {"link", "url", "portal", "site", "address", "note", "notes",
                 "comment", "environment", "env"}


def load_labelled_file(path: pathlib.Path, prefix: str, delete_source: bool,
                       dry_run: bool = False) -> int:
    """Load a hand-written 'Label : value' credential note into .env.

    load_from_file() only understands KEY=VALUE, which a note typed by a person
    almost never is. This maps human labels onto {PREFIX}_USERNAME /
    _PASSWORD / _COMPANY instead.

    Values are never printed, never logged and never returned — only key names
    and character counts. Unrecognised labels are reported by name only (a
    label is not a secret); their values are never shown. Nothing is written
    when dry_run is set.
    """
    if not path.exists():
        print(f"  [!] Not found: {path}", file=sys.stderr)
        return 1

    prefix = prefix.upper().rstrip("_")
    resolved, ignored, unknown = [], [], []

    for lineno, raw in enumerate(path.read_text().splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # Split on the FIRST ':' or '=' only, so a value containing either
        # (a URL, a password with '=') survives intact.
        m = re.match(r"^([^:=]{1,60})[:=](.*)$", line)
        if not m:
            unknown.append((lineno, "(no ':' or '=' separator)"))
            continue
        label = re.sub(r"\s+", " ", m.group(1).strip().lower())
        value = m.group(2).strip().strip('"').strip("'")
        if not value:
            continue
        if label in LABEL_IGNORED:
            ignored.append(label)
            del value
            continue
        field = LABEL_TO_FIELD.get(label)
        if not field:
            unknown.append((lineno, label))
            del value
            continue
        key = f"{prefix}_{field}"
        if not KEY_RE.match(key):
            unknown.append((lineno, label))
            del value
            continue
        action = "would set" if dry_run else set_key(key, value)
        resolved.append((key, action, len(value)))
        del value

    print(f"\n  .env → {ENV_PATH}" + ("  (DRY RUN — nothing written)" if dry_run
                                       else "  (mode 600)") + "\n")
    for key, action, size in resolved:
        print(f"  {key:26s} {action:10s} {size} chars")
    if ignored:
        print(f"\n  ignored (not a credential): {', '.join(sorted(set(ignored)))}")
    if unknown:
        print("\n  [!] unrecognised — value NOT read:")
        for lineno, label in unknown:
            print(f"      line {lineno}: {label!r}")
        print("      Add the label to LABEL_TO_FIELD, or set it with:"
              f"\n        set_credential.py {prefix}_<FIELD>")
    if not resolved:
        print("\n  [!] nothing loadable found.")
        return 1
    if delete_source and not dry_run:
        if _shred(path):
            print(f"\n  source file overwritten and deleted: {path}")
    elif not dry_run:
        print(f"\n  [!] source file still holds these secrets in plaintext:"
              f"\n      {path}\n      Re-run with --delete-source to shred it.")
    print()
    return 0


def load_dines_block_file(path: pathlib.Path, delete_source: bool,
                          dry_run: bool = False) -> int:
    """Parse a labelled Dines credential table into .env.

    Columns are matched by CHARACTER OFFSET, not by split index: a two-column
    sheet rarely uses identical separators on every line, and an index-based
    split silently pairs a password with the wrong branch. Each username
    anchors a column at its offset; every later labelled value joins the
    nearest anchor.

    Values are never printed — only key names and lengths. Nothing is written
    when dry_run is set, so the pairing can be checked before committing.
    """
    if not path.exists():
        print(f"  [!] Not found: {path}", file=sys.stderr)
        return 1

    # A real TSV (e.g. a Google Sheets export) has reliable tab-delimited
    # columns, so the cell INDEX is the spreadsheet column. Only fall back to
    # character offsets for hand-typed files with ragged spacing, where index
    # would drift on any line that starts with an extra separator.
    text = path.read_text()
    use_tabs = "\t" in text

    anchors: dict[int, str] = {}            # column key -> DINES_XX
    found: dict[str, dict[str, int]] = {}   # prefix -> {FIELD: length}
    ambiguous: list[str] = []
    pending: list[tuple[str, str]] = []

    LABEL_AT_START = re.compile(
        r"^(username|password|manager\s*pin|pin)\s*[:=]\s*(.+)$", re.I)

    def cells_of(line: str):
        """Yield (column_key, label, value) for each labelled cell in a line."""
        if use_tabs:
            for idx, cell in enumerate(line.split("\t")):
                m = LABEL_AT_START.match(cell.strip())
                if m:
                    yield idx, m.group(1), m.group(2)
        else:
            for m in re.finditer(
                    r"(username|password|manager\s*pin|pin)\s*[:=]\s*"
                    r"([^\t]{1,200}?)(?=\t|\s{2,}|$)", line, re.I):
                yield m.start(), m.group(1), m.group(2)

    for raw in text.splitlines():
        if not raw.strip():
            continue
        if re.search(r"username\s*[:=]", raw, re.I):
            anchors = {}                     # a username line starts a new block
        for col, label_raw, value_raw in cells_of(raw):
            label = re.sub(r"\s+", " ", label_raw.strip().lower())
            value = value_raw.strip().strip('"').strip("'")
            if not value:
                continue

            if label == "username":
                token = next((t for t in DINES_USER_TOKENS if t in value.lower()),
                             None)
                if token is None:
                    ambiguous.append("a username had no recognised mh** token")
                    continue
                anchors[col] = DINES_USER_TOKENS[token]
                prefix = anchors[col]
            elif use_tabs:
                prefix = anchors.get(col)
                if prefix is None:
                    ambiguous.append(f"{label} in column {col} has no username "
                                     f"above it in the same column")
                    continue
            else:
                if not anchors:
                    ambiguous.append(f"{label} appeared before any username")
                    continue
                prefix = min(anchors.items(), key=lambda a: abs(a[0] - col))[1]

            field = FIELD_FOR_LABEL[label]
            slot = found.setdefault(prefix, {})
            if field in slot:
                ambiguous.append(f"{prefix}_{field} appeared twice — "
                                 f"columns are being mis-paired")
                continue
            slot[field] = len(value)
            pending.append((f"{prefix}_{field}", value))

    complete = [pfx for pfx in DINES_BLOCK_PREFIXES
                if len(found.get(pfx, {})) == 3]
    incomplete = [pfx for pfx in DINES_BLOCK_PREFIXES
                  if len(found.get(pfx, {})) not in (0, 3)]

    print(f"\n  parsed from {path.name}:\n")
    for pfx in DINES_BLOCK_PREFIXES:
        slot = found.get(pfx, {})
        marks = "  ".join(f"{f}={slot[f]}ch" if f in slot else f"{f}=MISSING"
                          for f in ("USERNAME", "PASSWORD", "PIN"))
        print(f"  {'✓' if len(slot) == 3 else '✗'} {pfx:10s} {marks}")
    if ambiguous:
        print("\n  [!] problems:")
        for a in dict.fromkeys(ambiguous):
            print(f"      - {a}")

    if incomplete or ambiguous:
        print("\n  [!] REFUSING TO WRITE — the table did not parse cleanly. "
              "Nothing has been changed and the source file is untouched.")
        return 1

    if dry_run:
        print(f"\n  dry run — nothing written. Re-run without --dry-run to "
              f"commit {len(pending)} key(s).")
        return 0

    for key, value in pending:
        action = set_key(key, value)
        print(f"  ✓ {key:22s} {action} ({len(value)} chars, value not displayed)")
        del value

    print(f"\n  .env → {ENV_PATH}  (mode 600)")

    if delete_source:
        if len(complete) != len(DINES_BLOCK_PREFIXES):
            print(f"\n  [!] keeping {path}: only {len(complete)}/"
                  f"{len(DINES_BLOCK_PREFIXES)} branches complete. "
                  f"Delete it yourself once verified.")
            return 0
        try:
            size = path.stat().st_size
            with open(path, "r+b") as fh:
                fh.write(b"\0" * size)
                fh.flush()
                os.fsync(fh.fileno())
            path.unlink()
            print(f"  ✓ source file overwritten and deleted: {path}")
        except Exception as exc:
            print(f"  [!] could not delete {path}: {exc}", file=sys.stderr)
    else:
        print(f"\n  [!] {path} still holds plaintext credentials — delete it.")
    return 0


def key_is_set(key: str) -> bool:
    for line in read_env_lines():
        if line.startswith(f"{key}="):
            return bool(line.split("=", 1)[1].strip())
    return False


def prompt_and_set(key: str, label: str, secret: bool = True) -> None:
    if key_is_set(key):
        answer = input(f"  {key} is already set. Overwrite? [y/N] ").strip().lower()
        if answer != "y":
            print(f"  {key} left unchanged.")
            return
    value = (getpass.getpass(f"  {key} ({label}, not echoed): ") if secret
             else input(f"  {key} ({label}): ")).strip()
    if not value:
        print(f"  [!] Empty value — {key} left unchanged.")
        return
    action = set_key(key, value)
    print(f"  ✓ {key} {action} ({len(value)} chars written, value not displayed)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Write a credential into .env without echoing it")
    parser.add_argument("key", nargs="?", help="Env var name, e.g. DINES_CW_PIN")
    parser.add_argument("--partner", choices=sorted(PARTNER_KEYS),
                        help="Prompt for this partner's username and secret")
    parser.add_argument("--list", action="store_true",
                        help="Show which partner keys are set (never values)")
    parser.add_argument("--check", choices=sorted(PARTNER_KEYS),
                        help="Report whether a partner is ready to run")
    parser.add_argument("--from-file", type=pathlib.Path, metavar="PATH",
                        help="Bulk-load KEY=VALUE lines from a plaintext file "
                             "into .env. Values are never printed. Pair with "
                             "--delete-source to shred the file afterwards")
    parser.add_argument("--labels", metavar="PREFIX",
                        help="With --from-file: parse a hand-written "
                             "'Label : value' note (Username/Password/"
                             "Enterprise name) into PREFIX_USERNAME, "
                             "PREFIX_PASSWORD, PREFIX_COMPANY. Values are "
                             "never printed.")
    parser.add_argument("--dines-table", action="store_true",
                        help="With --from-file: parse a labelled Dines "
                             "credential table (Username:/Password:/Manager PIN:) "
                             "instead of KEY=VALUE lines")
    parser.add_argument("--dry-run", action="store_true",
                        help="With --from-file: show what would be written "
                             "(key names and lengths only) and change nothing")
    parser.add_argument("--delete-source", action="store_true",
                        help="With --from-file: overwrite and delete the source "
                             "file once its keys are in .env")
    args = parser.parse_args()

    if args.from_file:
        if args.labels:
            return load_labelled_file(args.from_file, args.labels,
                                      args.delete_source, args.dry_run)
        if args.dines_table:
            return load_dines_block_file(args.from_file, args.delete_source,
                                         args.dry_run)
        return load_from_file(args.from_file, args.delete_source)

    if args.list:
        print(f"\n  .env → {ENV_PATH}\n")
        print(f"  {'partner':<18} {'status':<12} keys")
        print(f"  {'-'*18} {'-'*12} {'-'*44}")
        for partner, fields in sorted(PARTNER_KEYS.items()):
            marks = " ".join(f"{'✓' if key_is_set(k) else '·'}{k}" for k, _, _ in fields)
            ready = all(key_is_set(k) for k, _, _ in fields)
            print(f"  {partner:<18} {'✓ ready' if ready else '· incomplete':<12} {marks}")
        print("\n  (values are never displayed)\n")
        return 0

    if args.check:
        fields = PARTNER_KEYS[args.check]
        missing = [k for k, _, _ in fields if not key_is_set(k)]
        if missing:
            print(f"  {args.check}: ✗ missing {', '.join(missing)}")
            return 1
        print(f"  {args.check}: ✓ ready to run")
        return 0

    if args.partner:
        print(f"\n  Credentials for {args.partner}")
        print("  (secrets are read without echo and never printed)\n")
        for key, label, is_secret in PARTNER_KEYS[args.partner]:
            prompt_and_set(key, label, secret=is_secret)
        print()
        return 0

    if not args.key:
        parser.error("give a KEY, or --partner NAME, or --list")

    prompt_and_set(args.key, "value", secret=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
