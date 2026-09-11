"""
Render the feed-health board as a standalone HTML file.

    .venv/bin/python export_dashboard.py            -> site/index.html

Why this exists: Cloudflare Pages (and GitHub Pages) serve static files. They
cannot run Streamlit, which is a live Python server holding a websocket per
viewer. A static snapshot is the part that CAN be hosted — it loses the date
slider and refreshes only when regenerated, but it carries the thing that
matters, which is the grid.

⚠️  THE OUTPUT CONTAINS CLIENT REVENUE. BrewDog, Bake My Day, Street Food and
Smash Tag, by branch and by day. Cloudflare Pages is PUBLIC by default. Put
Cloudflare Access in front of the project before pointing anyone at the URL,
or this is every client's takings on an open link.
"""

from __future__ import annotations

import collections
import glob
import html
import pathlib
import re
from datetime import date, datetime, timedelta

BASE = pathlib.Path(__file__).parent
OUT = BASE / "site"

FEEDS = {
    "BrewDog UK & Ireland": ["output/BrewDog_*.xlsx"],
    "Smash Tag": ["output/Smash_Tag_*.xlsx"],
    "Bake My Day": ["output/sapapad_BMD_*.xlsx", "output/sapapad_Bmd_*.xlsx",
                    "output/sapapad_Abu_Dhabi*.xlsx"],
    "Street Food Ltd": ["output/deliveroo_sales_*.xlsx", "output/anddine_*.xlsx",
                        "output/feedr_*.xlsx", "output/ordit_*.xlsx",
                        "output/homecook_*.xlsx", "output/justeat_business_*.xlsx"],
    "Pinza · Falafel · Heal": ["output/pinza_*.xlsx", "output/falafel_*.xlsx",
                               "output/heal_*.xlsx"],
}

WINDOW = 21


def sales_date(name: str):
    """Strict: a bare \\d{8} also matches BrewDog location ids like 10000007."""
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


def collect():
    out = {}
    for label, pats in FEEDS.items():
        branches = collections.defaultdict(set)
        for pat in pats:
            for f in glob.glob(str(BASE / pat)):
                n = pathlib.Path(f).name
                d = sales_date(n)
                if not d:
                    continue
                tok = d.isoformat() if d.isoformat() in n else d.strftime("%Y%m%d")
                branches[n.split(f"_{tok}")[0]].add(d)
        if branches:
            out[label] = dict(branches)
    return out


def render() -> str:
    today = date.today()
    days = [today - timedelta(days=i) for i in range(WINDOW - 1, -1, -1)]
    data = collect()

    rows_html, complete, gaps_total = [], 0, 0
    for label, branches in data.items():
        items = []
        for branch, dates in sorted(branches.items()):
            present = {d for d in dates if d in days}
            if not present:
                # Never reported in this window — a closed or new site, not a
                # failure. Marking it red would drown the board in noise.
                continue
            gaps = [d for d in days if d not in present and d >= min(present)]
            complete += 0 if gaps else 1
            gaps_total += 1 if gaps else 0
            cells = "".join(
                f'<i class="{"d" if d in present else "d miss"}" '
                f'title="{d:%d %b %Y}"></i>' for d in days)
            note = ("" if not gaps else
                    f'<div class="gap">No file for '
                    f'{", ".join(f"{d:%d %b}" for d in gaps[:6])}'
                    f'{" …" if len(gaps) > 6 else ""}</div>')
            items.append(
                f'<div class="row"><div class="who">{html.escape(branch)}'
                f'<small>{len(present)}/{len(days)} days</small></div>'
                f'<div><div class="days">{cells}</div>{note}</div></div>')
        if items:
            rows_html.append(
                f'<section><h2>{html.escape(label)}</h2>{"".join(items)}</section>')

    return f"""<title>POS Feed Health</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Archivo:wght@600;700&family=IBM+Plex+Mono:wght@400&family=IBM+Plex+Sans:wght@400;500;600&display=swap">
<style>
 :root{{--paper:#FFFFFF;--card:#FFFFFF;--sunk:#F7F4FC;--ink:#1B1235;
  --ink2:#4A3D66;--muted:#7A6E93;--rule:#E8E2F3;--rule2:#F1ECF8;
  --purple:#5E2CA5;--purple-2:#7C4DC4;--purple-bg:#F3EEFA;
  --ok:#2F7D57;--miss:#C0392B}}
 @media(prefers-color-scheme:dark){{:root:not([data-theme=light]){{--paper:#130D22;
  --card:#1C1433;--sunk:#241A40;--ink:#F2EEFA;--ink2:#CFC5E4;--muted:#9A8FB5;
  --rule:#312348;--rule2:#261B3A;--purple:#B38BEB;--purple-2:#9B6FE0;
  --purple-bg:#241A40;--ok:#6DC894;--miss:#E8897C}}}}
 :root[data-theme=dark]{{--paper:#130D22;--card:#1C1433;--sunk:#241A40;
  --ink:#F2EEFA;--ink2:#CFC5E4;--muted:#9A8FB5;--rule:#312348;--rule2:#261B3A;
  --purple:#B38BEB;--purple-2:#9B6FE0;--purple-bg:#241A40;--ok:#6DC894;--miss:#E8897C}}
 *{{box-sizing:border-box}}
 body{{background:var(--paper);color:var(--ink);font-family:"IBM Plex Sans",
  -apple-system,sans-serif;font-size:16px;line-height:1.55;margin:0;
  padding:0 0 4rem;-webkit-font-smoothing:antialiased}}
 .banner{{background:var(--purple);color:#fff;padding:clamp(1.6rem,4vw,2.6rem)
  clamp(1rem,4vw,2rem) clamp(1.4rem,3vw,2rem)}}
 .banner .inner{{max-width:62rem;margin:0 auto}}
 .mark{{font-family:Archivo,sans-serif;font-weight:700;font-size:.82rem;
  letter-spacing:.22em;text-transform:uppercase;opacity:.75;margin:0 0 .5rem}}
 h1{{font-family:Archivo,sans-serif;font-weight:700;
  font-size:clamp(1.8rem,4.4vw,2.5rem);letter-spacing:-.025em;margin:0 0 .35rem;color:#fff}}
 .sub{{color:#fff;opacity:.82;margin:0;max-width:48ch}}
 .wrap{{max-width:62rem;margin:0 auto;padding:0 clamp(1rem,4vw,2rem)}}
 .stamp{{font-family:"IBM Plex Mono",monospace;font-size:.75rem;color:var(--muted);
  margin:1.4rem 0 1rem}}
 .tally{{display:flex;gap:2rem;flex-wrap:wrap;background:var(--purple-bg);
  border:1px solid var(--rule);border-radius:10px;padding:1.1rem 1.3rem;margin-bottom:1.2rem}}
 .tally b{{font-family:Archivo,sans-serif;font-size:1.9rem;display:block;
  letter-spacing:-.02em;color:var(--purple)}}
 .tally span{{font-family:"IBM Plex Mono",monospace;font-size:.72rem;
  letter-spacing:.09em;color:var(--muted)}}
 .legend{{display:flex;gap:1.4rem;flex-wrap:wrap;font-size:.85rem;color:var(--muted);
  margin-bottom:1.6rem}}
 .legend i{{width:12px;height:12px;border-radius:3px;display:inline-block;
  margin-right:.35rem;vertical-align:-1px}}
 section{{background:var(--card);border:1px solid var(--rule);border-radius:10px;
  padding:1.15rem 1.3rem;margin-bottom:1.1rem}}
 h2{{font-family:Archivo,sans-serif;font-weight:600;font-size:1.15rem;
  letter-spacing:-.01em;margin:0 0 .7rem;color:var(--purple)}}
 .row{{display:grid;grid-template-columns:13rem 1fr;gap:.9rem;align-items:start;
  padding:.5rem 0;border-top:1px solid var(--rule2)}}
 .row:first-of-type{{border-top:none}}
 .who{{font-size:.88rem;font-weight:500;word-break:break-word}}
 .who small{{display:block;color:var(--muted);font-weight:400;
  font-family:"IBM Plex Mono",monospace;font-size:.72rem}}
 .days{{display:flex;gap:3px;flex-wrap:wrap}}
 .d{{width:14px;height:14px;border-radius:3px;background:var(--ok);display:inline-block}}
 .d.miss{{background:var(--miss)}}
 .gap{{font-size:.78rem;color:var(--muted);margin-top:.35rem}}
 .warn{{background:var(--sunk);border-left:3px solid var(--purple-2);
  border-radius:8px;padding:1rem 1.15rem;color:var(--ink2);font-size:.92rem;
  margin-bottom:1.1rem}}
 .warn b{{color:var(--ink)}}
 footer{{border-top:1px solid var(--rule);padding-top:1.1rem;color:var(--muted);
  font-size:.84rem;margin-top:1.5rem}}
 @media(max-width:40rem){{.row{{grid-template-columns:1fr;gap:.35rem}}}}
</style>
<div class="banner"><div class="inner">
 <p class="mark">Supy</p>
 <h1>POS feed health</h1>
 <p class="sub">What arrived, per branch, per day — built from the files actually produced and emailed.</p>
</div></div>
<div class="wrap">
 <p class="stamp">Snapshot generated {datetime.now():%d %b %Y, %H:%M} · last {WINDOW} days</p>

 <div class="tally">
  <div><b>{complete + gaps_total}</b><span>BRANCHES REPORTING</span></div>
  <div><b style="color:var(--ok)">{complete}</b><span>COMPLETE</span></div>
  <div><b style="color:var(--miss)">{gaps_total}</b><span>WITH GAPS</span></div>
 </div>

 <div class="legend">
  <span><i style="background:var(--ok)"></i>Sales arrived</span>
  <span><i style="background:var(--miss)"></i>Nothing arrived</span>
 </div>

 <div class="warn">
  <b>This shows that a file was produced — not that Supy ingested it.</b>
  Detecting the case where the POS has sales and Supy received nothing needs
  Supy-side access, which is not yet connected. No green here should be read
  as “Supy has this data”.
 </div>

 {"".join(rows_html)}

 <footer>A gap is not automatically a fault — some channels trade weekly and
 closures are normal. It is worth investigating when a branch that reported
 every day suddenly stops; that pattern went unnoticed for 77 days on BrewDog
 branch 28d2dae9.</footer>
</div>"""


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    page = render()
    (OUT / "index.html").write_text(page)
    print(f"wrote {OUT / 'index.html'}  ({len(page):,} bytes)")
    print("\n⚠️  Contains client revenue by branch and day.")
    print("   Cloudflare Pages is PUBLIC by default — put Cloudflare Access")
    print("   in front of the project before sharing the URL.")
