"""
tests/test_vat_direction.py
---------------------------
Pins the VAT direction of every pipeline.

Charlotte confirmed the rule on 2026-09-03:

    "That's the standard calculation for UK accounts if you only have tax
     inclusive. So yes please use that to attain tax exclusive sales."

Note the CONDITION — "if you only have tax inclusive". The rule is not
"always divide by 1.2"; it is:

    source is GROSS only        -> excl = incl / 1.2
    source is NET               -> incl = excl * 1.2
    source gives BOTH figures   -> derive nothing, copy both

This repo runs sources in all three shapes, including two Deliveroo reports
that disagree with each other. A flipped direction changes every money column
while quantities stay correct, so it cannot be caught by eye — hence these
tests.

Offline: config assertions plus arithmetic. No browser, no network.
"""

import pathlib
import sys

import pytest
import yaml

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

BASE = pathlib.Path(__file__).parent.parent
UK_DIVISOR = 1.2
UAE_DIVISOR = 1.05


def load(rel):
    return yaml.safe_load((BASE / rel).read_text())


# ── UK: sources that give GROSS only → divide ───────────────────────────


def test_dines_divides_because_the_source_is_gross_only():
    cfg = load("dines_config.yaml")
    assert cfg["vat_direction"] == "inclusive"
    assert cfg["vat_rate"] == 0.20
    injects = [c.get("inject") for c in cfg["columns"]]
    assert "derive_excl_from_incl" in injects, (
        "Dines must derive excl FROM incl; the dashboard gives gross only")


def test_deliveroo_partner_hub_divides():
    """Partner Hub Items Sold shows customer-facing (gross) prices."""
    cfg = load("deliveroo_config.yaml")
    assert cfg["tax"]["divisor"] == UK_DIVISOR
    injects = [str(c.get("inject") or "") for c in cfg["columns"]]
    assert any(i.startswith("derive_excl_tax:") for i in injects), (
        "Deliveroo Partner Hub must derive excl by DIVIDING the gross figure")


@pytest.mark.parametrize("partner", ["feedr", "homecook", "justeat_business",
                                     "just_eat", "ordit"])
def test_gross_partners_divide(partner):
    pricing = load(f"partners/{partner}.yaml")["pricing"]
    assert pricing["tax_divisor"] == UK_DIVISOR
    # Absent means True in partner_scraper; both are "divide".
    assert pricing.get("price_includes_tax", True) is True, (
        f"{partner} is configured as NET — check this is deliberate")


# ── UK: a source that gives NET → multiply ──────────────────────────────


def test_anddine_multiplies_because_its_prices_exclude_vat():
    """&Dine shows "£13.29 / unit Excl. VAT" — dividing would be wrong.

    Verified against order SAT-IVB2M: 13.29 x 1.2 = 15.95, matching the
    list's order total.
    """
    pricing = load("partners/anddine.yaml")["pricing"]
    assert pricing["price_includes_tax"] is False
    assert pricing["tax_divisor"] == UK_DIVISOR
    assert round(13.29 * UK_DIVISOR, 2) == 15.95


def test_the_two_deliveroo_pipelines_run_opposite_directions():
    """Guard against 'fixing' one to match the other.

    blackbear_convert.py   Looker email export, NET   -> multiply
    deliveroo_automation.py  Partner Hub, gross        -> divide
    """
    src = (BASE / "blackbear_convert.py").read_text()
    assert "1 + vat_rate" in src or "* (1 + " in src or "vat_rate)" in src, (
        "the Black Bear email converter should ADD VAT to a net figure")
    cfg = load("deliveroo_config.yaml")
    assert cfg["tax"]["divisor"] == UK_DIVISOR


# ── Non-UK rates: Charlotte's 1.2 is UK-specific ────────────────────────


def test_talabat_uses_the_uae_rate_not_the_uk_one():
    """UAE VAT is 5%. Applying the UK 1.2 here would be a 15% error."""
    raw = (BASE / "talabat_config.yaml").read_text()
    assert "1.05" in raw
    assert round(105.0 / UAE_DIVISOR, 2) == 100.0


def test_sapapad_derives_nothing_because_the_source_gives_both():
    """BMD's CSV carries excl AND incl, so no divisor should exist at all.

    This is the third branch of the rule: when both figures are provided,
    deriving one is a chance to introduce an error for no benefit.
    """
    cfg = load("sapapad_config.yaml")
    targets = {c.get("raw"): c.get("target") for c in cfg["columns"]}
    assert targets.get("Total Amount Excluding Tax") == "Total sales excl. tax *"
    assert targets.get("Total Amount") == "Total sales incl. tax *"
    raw = (BASE / "sapapad_config.yaml").read_text()
    assert "tax_divisor" not in raw and "vat_rate" not in raw, (
        "Sapapad must not derive tax — the source already provides both")


# ── The arithmetic itself ───────────────────────────────────────────────


@pytest.mark.parametrize("incl,excl", [(15.50, 12.92), (13.95, 11.62),
                                       (2.50, 2.08), (1.00, 0.83),
                                       (460.00, 383.33)])
def test_divide_matches_the_figures_we_shipped(incl, excl):
    """Rows from files actually emailed on 2026-09-02 (Dines + Deliveroo)."""
    assert round(incl / UK_DIVISOR, 2) == excl


def test_dividing_a_net_figure_silently_understates_sales():
    """Why direction matters: quantities look fine, money is 30% out."""
    net = 100.00
    correct_incl = round(net * UK_DIVISOR, 2)
    wrong_excl = round(net / UK_DIVISOR, 2)
    assert correct_incl == 120.00
    assert wrong_excl == 83.33
    assert abs(correct_incl - wrong_excl) / correct_incl > 0.30
