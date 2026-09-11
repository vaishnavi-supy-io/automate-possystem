"""
Slack notification.

Written to the rule that decides whether a monitor gets used or muted: lead
with the business consequence, not the mechanism. "SYNC_FAILURE cursor=..."
is accurate and useless; "TREAT'S hasn't sent sales for 3 days" is what makes
someone act.

Four things every alert does:
  * names the impact in the first line
  * quantifies the money sitting unsynced
  * says who should act AND who shouldn't
  * separates what is known from what is suspected

And one rule about frequency: post on CHANGE, not on state. Once when it
breaks, once if it worsens, once when it recovers. A message every cycle for
the same incident is how a channel gets muted — and a muted channel is
exactly how a three-day outage becomes a client email.

The webhook URL is read from the environment. Never hard-code it and never
paste it into a ticket or chat: anyone holding it can post to the channel.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Optional

from .diagnose import Diagnosis
from .engine import Assessment, Verdict

WEBHOOK_ENV = "SLACK_WEBHOOK_URL"

# Plain words for people who do not work on the integration.
PLAIN = {
    Verdict.BROKEN: "hasn't sent sales",
    Verdict.PARTIAL: "only sent part of its sales",
    Verdict.RECONCILE_FAIL: "sales totals don't match",
    Verdict.POS_UNAVAILABLE: "till system couldn't be reached",
    Verdict.UNKNOWN: "can't be checked yet",
    Verdict.HEALTHY: "is fine",
    Verdict.NO_ACTIVITY: "was closed",
}

COLOUR = {
    Verdict.BROKEN: "#A32F28",
    Verdict.PARTIAL: "#9A6B14",
    Verdict.RECONCILE_FAIL: "#9A6B14",
    Verdict.POS_UNAVAILABLE: "#6B7280",
    Verdict.UNKNOWN: "#6B7280",
    Verdict.HEALTHY: "#2C7A52",
    Verdict.NO_ACTIVITY: "#2C7A52",
}

# Who owns each verdict. Saying who should NOT act matters as much: the
# instinct is to ring the restaurant, and for a sync fault that wastes
# everyone's morning.
OWNER = {
    Verdict.BROKEN: ("Integrations",
                     "This is not a restaurant problem — no need to contact the site."),
    Verdict.PARTIAL: ("Integrations",
                      "The site is trading normally; only some of it arrived."),
    Verdict.RECONCILE_FAIL: ("Integrations",
                             "Both sides have the sales; the totals disagree."),
    Verdict.POS_UNAVAILABLE: ("Integrations",
                              "We could not reach the till system, so we cannot "
                              "yet say whether anything is wrong."),
    Verdict.UNKNOWN: ("Integrations", "Monitoring is not fully connected yet."),
}


def _money(v: float, cur: str = "") -> str:
    return f"{cur}{v:,.0f}".strip()


def build_blocks(a: Assessment, dx: Optional[Diagnosis] = None,
                 currency: str = "", missing_dates: Optional[list] = None,
                 unsynced_total: Optional[float] = None) -> dict:
    """Slack Block Kit payload. Returns the full message body."""
    ev = a.evidence
    loc = ev.location
    headline = f"{loc} {PLAIN.get(a.verdict, 'needs checking')}"
    if a.verdict is Verdict.BROKEN and missing_dates:
        headline = (f"{loc} hasn't sent sales for "
                    f"{len(missing_dates)} day{'s' if len(missing_dates) > 1 else ''}")

    blocks = [{
        "type": "header",
        "text": {"type": "plain_text", "text": headline[:150], "emoji": True},
    }]

    if a.verdict is Verdict.BROKEN:
        lead = ("The till has been recording sales normally. They just aren't "
                "reaching Supy.")
    elif a.verdict is Verdict.PARTIAL:
        lead = "Some of the day's sales arrived, but not all of them."
    elif a.verdict is Verdict.RECONCILE_FAIL:
        lead = ("The same number of sales arrived on both sides, but the money "
                "doesn't add up to the same figure.")
    elif a.verdict is Verdict.POS_UNAVAILABLE:
        lead = ("We couldn't get an answer from the till system, so we can't "
                "tell whether sales are missing or the site simply didn't trade.")
    else:
        lead = ""
    if lead:
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": lead}})

    facts = []
    if missing_dates:
        facts.append(f"*Missing:* {', '.join(missing_dates)}")
    if unsynced_total:
        facts.append(f"*Sales sitting in the POS:* {_money(unsynced_total, currency)}")
    if ev.sync and ev.sync.last_success:
        facts.append(f"*Last day that arrived:* {ev.sync.last_success[:10]}")
    if ev.pos_sales and ev.supy_sales and a.verdict in (
            Verdict.PARTIAL, Verdict.RECONCILE_FAIL):
        facts.append(f"*In the POS:* {_money(ev.pos_sales.gross, currency)}   "
                     f"*Reached Supy:* {_money(ev.supy_sales.gross, currency)}")
    if facts:
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": "\n".join(facts)}})

    if dx:
        # Hedged on purpose — the agent is usually right and occasionally not,
        # and an alert that overstates its certainty gets distrusted after the
        # first wrong call.
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                       f"*What we think is wrong:* {dx.likely_cause}"}})
        if dx.next_checks:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                           "*Worth checking first:* " + dx.next_checks[0]}})

    owner, caveat = OWNER.get(a.verdict, ("", ""))
    if owner:
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text":
                       f"*Who needs to act:* {owner}. {caveat}"}})

    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text":
                   f"Business date {ev.business_date} · {ev.pos or 'POS'} · "
                   f"checked {a.checked_at}"}]})

    return {"attachments": [{"color": COLOUR.get(a.verdict, "#6B7280"),
                             "blocks": blocks}]}


def build_recovery_blocks(location: str, business_dates: list) -> dict:
    """Closing an incident is worth a message — silence reads as still-broken."""
    days = ", ".join(business_dates)
    return {"attachments": [{"color": COLOUR[Verdict.HEALTHY], "blocks": [
        {"type": "section", "text": {"type": "mrkdwn", "text":
         f":white_check_mark: *{location}* is sending sales again.\n"
         f"The days that were missing have now arrived: {days}."}}]}]}


def send(payload: dict, webhook: Optional[str] = None) -> bool:
    """
    POST to Slack. Returns False rather than raising — a failed notification
    must never take down the monitoring run that produced it.
    """
    url = webhook or os.environ.get(WEBHOOK_ENV, "")
    if not url:
        print(f"[!] {WEBHOOK_ENV} not set — alert not sent.")
        return False
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return 200 <= r.status < 300
    except urllib.error.HTTPError as e:
        # Slack returns the reason in the body; the status alone is unhelpful.
        print(f"[!] Slack rejected the message: {e.code} "
              f"{e.read().decode('utf-8', 'replace')[:120]}")
        return False
    except Exception as exc:
        print(f"[!] Slack notification failed: {type(exc).__name__}")
        return False
