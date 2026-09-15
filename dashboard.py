"""
POS feed dashboard.

    .venv/bin/streamlit run dashboard.py

Shows what ARRIVED, per client, per branch, per day. The grid is the point:
green means the day's sales reached us, red means they did not, grey means
the site was not expected to trade. A manager reads the pattern in a second,
which no table of numbers achieves.

It reads the real output/ files rather than a separate database, so it can
never disagree with what was actually produced and emailed. There is no
ingestion step to go stale.

Honesty rules, both learned the hard way this week:

  * A day with no file is NOT automatically a failure. BrewDog 28d2dae9 was
    invisible for 77 days because a filename rule skipped it, and every run
    reported success — so "no file" and "no sales" must look different here.
  * The POS -> Supy comparison is NOT wired. The dashboard says so plainly
    rather than showing green for something it cannot see.
"""

from __future__ import annotations

import collections
import glob
import os
import pathlib
import re
from datetime import date, datetime, timedelta

import streamlit as st

BASE = pathlib.Path(__file__).parent

# label -> (glob patterns, how a branch is named inside the filename)
FEEDS = {
    "BrewDog UK & Ireland": ["output/BrewDog_*.xlsx"],
    "Smash Tag":            ["output/Smash_Tag_*.xlsx"],
    "Bake My Day":          ["output/sapapad_*.xlsx"],
    "Street Food Ltd":      ["output/deliveroo_sales_*.xlsx", "output/anddine_*.xlsx",
                             "output/feedr_*.xlsx", "output/ordit_*.xlsx",
                             "output/homecook_*.xlsx", "output/justeat_business_*.xlsx"],
    "Pinza / Falafel / Heal": ["output/pinza_*.xlsx", "output/falafel_*.xlsx",
                               "output/heal_*.xlsx"],
}

OK, MISS, NA = "#2C7A52", "#A32F28", "#D5DAE0"


def sales_date(name: str):
    """
    Read the sales date from a filename.

    Strict on purpose: a bare \\d{8} also matches BrewDog's location ids —
    10000007 parses happily as a date — so candidates must start with 20 and
    be a real calendar date.
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


@st.cache_data(ttl=120)
def load_feed(patterns: tuple):
    """branch -> {date: [files]} built from the filenames on disk."""
    out = collections.defaultdict(lambda: collections.defaultdict(list))
    for pat in patterns:
        for f in glob.glob(str(BASE / pat)):
            name = pathlib.Path(f).name
            d = sales_date(name)
            if not d:
                continue
            token = d.isoformat() if d.isoformat() in name else d.strftime("%Y%m%d")
            branch = name.split(f"_{token}")[0]
            out[branch][d].append(f)
    return {b: dict(v) for b, v in out.items()}


def grid_html(days: list, present: set) -> str:
    cells = []
    for d in days:
        colour = OK if d in present else MISS
        cells.append(
            f'<span title="{d:%d %b %Y}" style="display:inline-block;width:14px;'
            f'height:14px;border-radius:3px;background:{colour};margin-right:3px;'
            f'margin-bottom:3px"></span>')
    return f'<div style="line-height:1">{"".join(cells)}</div>'


st.set_page_config(page_title="POS Feed Health", page_icon="📡", layout="wide")

st.markdown("""
<style>
  .block-container { padding-top: 2.2rem; max-width: 78rem; }
  h1 { letter-spacing: -.02em; }
  .legend span { margin-right: 1.3rem; font-size: .85rem; color: #6B7280; }
  .legend i { width:12px;height:12px;border-radius:3px;display:inline-block;
              margin-right:.35rem;vertical-align:-1px; }
</style>""", unsafe_allow_html=True)

st.title("POS feed health")
st.caption("What actually arrived, per client, per day. Built from the files "
           "that were produced and emailed — not a separate record that could "
           "drift from them.")

window = st.slider("Days shown", 7, 60, 21)
today = date.today()
days = [today - timedelta(days=i) for i in range(window - 1, -1, -1)]

st.markdown(
    f'<div class="legend"><span><i style="background:{OK}"></i>Sales arrived</span>'
    f'<span><i style="background:{MISS}"></i>Nothing arrived</span>'
    f'<span><i style="background:{NA}"></i>Not expected</span></div>',
    unsafe_allow_html=True)

healthy = missing = 0
sections = []
for label, patterns in FEEDS.items():
    data = load_feed(tuple(patterns))
    if not data:
        continue
    rows = []
    for branch, by_date in sorted(data.items()):
        present = {d for d in by_date if d in days}
        # A branch that has NEVER reported in the window is not a failure —
        # it may be a site that closed, or one we have simply never had data
        # for. Only count gaps for branches that reported at least once.
        if not present:
            continue
        gaps = [d for d in days if d not in present and d >= min(present)]
        rows.append((branch, present, gaps))
        if gaps:
            missing += 1
        else:
            healthy += 1
    if rows:
        sections.append((label, rows))

c1, c2, c3 = st.columns(3)
c1.metric("Branches reporting", healthy + missing)
c2.metric("Complete", healthy)
c3.metric("With gaps", missing, delta=None if not missing else f"{missing} to check",
          delta_color="inverse")

st.divider()

for label, rows in sections:
    st.subheader(label)
    for branch, present, gaps in rows:
        a, b = st.columns([1, 3])
        a.markdown(f"**{branch}**  \n<span style='color:#6B7280;font-size:.8rem'>"
                   f"{len(present)}/{len(days)} days</span>", unsafe_allow_html=True)
        b.markdown(grid_html(days, present), unsafe_allow_html=True)
        if gaps:
            b.caption("No file for: " +
                      ", ".join(f"{d:%d %b}" for d in gaps[:8]) +
                      (" …" if len(gaps) > 8 else ""))
    st.divider()

st.subheader("POS → Supy comparison")
st.warning(
    "**Not connected.** This dashboard can show that a file was produced, but "
    "not whether Supy ingested it. Detecting the TREAT'S case — POS has sales, "
    "Supy received nothing — needs a Supy read API or database access "
    "(`sync_sentinel/supy_client.py`). Until then no green here would be "
    "truthful, so none is shown.")

st.caption(
    "A day with no file is not automatically a fault: some channels trade "
    "weekly, and closures are normal. It is worth checking when a branch that "
    "reported every day suddenly stops — that pattern is what went unnoticed "
    "for 77 days on BrewDog 28d2dae9.")
