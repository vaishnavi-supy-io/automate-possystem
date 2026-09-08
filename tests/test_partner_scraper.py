"""
tests/test_partner_scraper.py
-----------------------------
Offline unit tests for the generic per-order scraping engine.

The browser half cannot be tested without live portals, so these tests cover
everything downstream of scraping — parsing, filtering, price resolution, tax
derivation, and multi-destination splitting. That is where a silent bug would
corrupt a customer's food-cost numbers.

Run:
    /path/to/.venv/bin/python -m pytest tests/ -v
"""

import pathlib
import sys
from datetime import date, datetime

import pandas as pd
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import partner_scraper as ps  # noqa: E402

D5 = datetime(2026, 8, 5)
D1 = datetime(2026, 8, 1)


# ── Parsing ───────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("£12.50", 12.50),
    ("12.50", 12.50),
    ("  £7.00  ", 7.00),
    ("9.99 GBP", 9.99),
    ("£1,250.00", 1250.00),
    (7.5, 7.50),
    (12, 12.00),
    ("", 0.0),
    (None, 0.0),
    ("free", 0.0),
])
def test_parse_money(raw, expected):
    assert ps.parse_money(raw) == expected


@pytest.mark.parametrize("raw,expected", [
    ("3", 3), ("x3", 3), ("3 pcs", 3), ("3.0", 3),
    (2, 2), (2.0, 2), ("", 0), (None, 0), ("many", 0),
])
def test_parse_qty(raw, expected):
    assert ps.parse_qty(raw) == expected


def test_parse_date_prefers_configured_format():
    # 05/08/2026 is 5 August in the UK, not 8 May.
    assert ps.parse_date("05/08/2026", "%d/%m/%Y") == datetime(2026, 8, 5)


def test_parse_date_falls_back_to_day_first():
    """Without a configured format, UK day-first ordering must still hold."""
    assert ps.parse_date("05/08/2026").date() == datetime(2026, 8, 5).date()


def test_yearless_date_infers_the_current_year():
    """
    Feedr's day headers read "Friday 31 Jul" with no year. pandas resolves that
    to year 0001, which silently pushed every order out of range and made the
    run report "no sales" instead of failing.
    """
    ref = datetime(2026, 8, 6)
    assert ps.parse_date("Friday 31 Jul", reference=ref) == datetime(2026, 7, 31)
    assert ps.parse_date("Wednesday 5 Aug", reference=ref) == datetime(2026, 8, 5)
    assert ps.parse_date("31 Jul", reference=ref) == datetime(2026, 7, 31)


def test_yearless_date_rolls_back_across_new_year():
    """Reading a '31 Dec' header on 2 Jan must mean LAST year, not this one."""
    assert (ps.parse_date("Thursday 31 Dec", reference=datetime(2027, 1, 2))
            == datetime(2026, 12, 31))


def test_explicit_year_is_never_overridden():
    ref = datetime(2026, 8, 6)
    assert ps.parse_date("Friday 31 Jul 2024", reference=ref).year == 2024


def test_feedr_line_item_selector_excludes_the_totals_table():
    """
    The Feedr detail page has two tables sharing a row class; the second is a
    totals footer. Unscoped it becomes a bogus item (qty 246, name '£4.50') and
    inflates the upload by the delivery charge.
    """
    sel = ps.load_partner_config("feedr")["selectors"]["line_item_rows"]
    assert "OrderTotals" in sel and ":not(" in sel


def test_feedr_price_cell_is_a_line_total_not_a_unit_price():
    """A qty-8 row shows £59.76 (8 × £7.47); per-unit would multiply by 8."""
    pricing = ps.load_partner_config("feedr")["pricing"]
    assert pricing["unit_price_is_per_item"] is False
    assert pricing["price_includes_tax"] is True


def test_feedr_opens_orders_by_click_not_href():
    """Feedr rows are SPA click targets with no href."""
    cfg = ps.load_partner_config("feedr")
    assert cfg["orders"]["open_mode"] == "click"
    assert not cfg["selectors"]["order_link"]


def test_parse_date_returns_none_on_junk():
    assert ps.parse_date("not a date") is None
    assert ps.parse_date("") is None
    assert ps.parse_date(None) is None


# ── Filtering ─────────────────────────────────────────────────────────────────

def test_status_matches_is_case_insensitive_substring():
    assert ps.status_matches("Completed", ["completed"])
    assert ps.status_matches("Order Delivered ✓", ["Delivered"])
    assert not ps.status_matches("Cancelled", ["Completed", "Delivered"])


def test_empty_keep_list_accepts_everything():
    assert ps.status_matches("Anything", [])
    assert ps.status_matches("", [])


def test_homecook_three_statuses():
    keep = ["Completed", "Dispatched", "Delivered"]
    for good in ("Completed", "Dispatched", "Delivered"):
        assert ps.status_matches(good, keep)
    for bad in ("Pending", "Cancelled", "Refunded"):
        assert not ps.status_matches(bad, keep)


def test_in_date_range_is_inclusive_both_ends():
    assert ps.in_date_range(D1, D1, D5)
    assert ps.in_date_range(D5, D1, D5)
    assert ps.in_date_range(datetime(2026, 8, 3), D1, D5)
    assert not ps.in_date_range(datetime(2026, 7, 31), D1, D5)
    assert not ps.in_date_range(datetime(2026, 8, 6), D1, D5)


def test_in_date_range_ignores_time_of_day():
    assert ps.in_date_range(datetime(2026, 8, 5, 23, 59), D5, D5)
    assert ps.in_date_range(datetime(2026, 8, 5, 0, 1), D5, D5)


def test_none_date_is_never_in_range():
    assert not ps.in_date_range(None, D1, D5)


# ── Supy frame construction + tax ─────────────────────────────────────────────

SCRAPED_CFG = {"pricing": {"source": "scraped",
                           "unit_price_is_per_item": True,
                           "tax_divisor": 1.2}}


def test_line_total_is_unit_price_times_qty():
    rows = [{"order_id": "A1", "order_date": D5,
             "item_name": "Pad Thai", "qty": 3, "unit_price_inc_tax": 9.00}]
    df = ps.build_supy_frame(rows, SCRAPED_CFG)

    assert df.loc[0, "Total sales incl. tax *"] == 27.00
    assert df.loc[0, "Total sales excl. tax *"] == 22.50   # 27 / 1.2
    assert df.loc[0, "Sold QTY *"] == 3


def test_exclusive_prices_derive_inclusive_upward():
    """
    &Dine shows prices EXCLUDING VAT, the opposite of Feedr / Just Eat.
    Verified against real order SAT-IVB2M: £13.29 excl → £15.95 incl, which
    matches the portal's own Order Total. Dividing here would understate sales.
    """
    cfg = {"pricing": {"price_includes_tax": False,
                       "unit_price_is_per_item": True, "tax_divisor": 1.2}}
    rows = [{"order_id": "15511", "order_date": D5,
             "item_name": "Bento Box - Chicken Satay Peanut Sauce",
             "qty": 1, "unit_price_inc_tax": 13.29}]
    df = ps.build_supy_frame(rows, cfg)

    assert df.loc[0, "Total sales excl. tax *"] == 13.29
    assert df.loc[0, "Total sales incl. tax *"] == 15.95


def test_inclusive_and_exclusive_modes_are_not_the_same():
    """Guard against the two tax directions being silently conflated."""
    rows = [{"order_id": "A", "order_date": D5, "item_name": "X",
             "qty": 1, "unit_price_inc_tax": 10.00}]
    incl = ps.build_supy_frame(rows, {"pricing": {"price_includes_tax": True,
                                                  "tax_divisor": 1.2}})
    excl = ps.build_supy_frame(rows, {"pricing": {"price_includes_tax": False,
                                                  "tax_divisor": 1.2}})
    assert incl.loc[0, "Total sales incl. tax *"] == 10.00
    assert excl.loc[0, "Total sales incl. tax *"] == 12.00
    assert incl.loc[0, "Total sales excl. tax *"] == 8.33
    assert excl.loc[0, "Total sales excl. tax *"] == 10.00


def test_row_level_vat_override_beats_the_partner_default():
    """
    &Dine item prices are VAT-exclusive, but the order-level fallback line for a
    "Set" order carries the list's Order Total, which is VAT-INCLUSIVE. Without
    a per-row override the VAT would be applied twice (£390 → £468).
    """
    cfg = {"pricing": {"price_includes_tax": False, "tax_divisor": 1.2,
                       "unit_price_is_per_item": True}}
    rows = [{"order_id": "15489", "order_date": D5,
             "item_name": "&Dine Set Order SAT-5IVOM", "qty": 1,
             "unit_price_inc_tax": 390.00, "price_includes_tax": True}]
    df = ps.build_supy_frame(rows, cfg)

    assert df.loc[0, "Total sales incl. tax *"] == 390.00
    assert df.loc[0, "Total sales excl. tax *"] == 325.00


def test_anddine_books_priceless_set_orders_as_one_line():
    """Set orders expose no item prices, so they are booked at the order total."""
    orders = ps.load_partner_config("anddine")["orders"]
    assert orders["fallback_single_line"] is True
    assert "{reference}" in orders["fallback_name_template"]


def test_anddine_captures_the_order_total_needed_for_the_fallback():
    sel = ps.load_partner_config("anddine")["selectors"]
    assert sel["order_total"] and sel["order_reference"]


def test_anddine_config_declares_exclusive_pricing():
    """Regression guard: flipping this silently misstates &Dine sales by 20%."""
    pricing = ps.load_partner_config("anddine")["pricing"]
    assert pricing["price_includes_tax"] is False


def test_parse_money_ignores_trailing_prose():
    """
    &Dine renders '£13.29 / unit Excl. VAT'. The old full-strip approach also
    swallowed the dot from 'Excl.', which only worked by luck.
    """
    assert ps.parse_money("£13.29 / unit Excl. VAT") == 13.29
    assert ps.parse_money("£3.50 / unit Excl. VAT") == 3.50
    assert ps.parse_money("Total: £1,250.00 incl. VAT") == 1250.00


def test_parse_qty_handles_anddine_multiplication_sign():
    """&Dine renders quantity as '1 ×'."""
    assert ps.parse_qty("1 ×") == 1
    assert ps.parse_qty("12 ×") == 12


def test_unit_price_is_line_total_when_configured():
    cfg = {"pricing": {"unit_price_is_per_item": False, "tax_divisor": 1.2}}
    rows = [{"order_id": "A1", "order_date": D5,
             "item_name": "Pad Thai", "qty": 3, "unit_price_inc_tax": 27.00}]
    df = ps.build_supy_frame(rows, cfg)

    assert df.loc[0, "Total sales incl. tax *"] == 27.00


def test_item_name_doubles_as_pos_item_id():
    rows = [{"order_id": "A1", "order_date": D5,
             "item_name": "Chicken Satay Skewers", "qty": 1,
             "unit_price_inc_tax": 7.50}]
    df = ps.build_supy_frame(rows, SCRAPED_CFG)

    assert df.loc[0, "POS Item ID *"] == "Chicken Satay Skewers"
    assert df.loc[0, "POS Item Name"] == "Chicken Satay Skewers"


def test_order_id_is_preserved_not_aggregated():
    """One output row per order line — Order ID must survive."""
    rows = [
        {"order_id": "A1", "order_date": D5, "item_name": "Pad Thai",
         "qty": 1, "unit_price_inc_tax": 9.00},
        {"order_id": "A2", "order_date": D5, "item_name": "Pad Thai",
         "qty": 1, "unit_price_inc_tax": 9.00},
    ]
    df = ps.build_supy_frame(rows, SCRAPED_CFG)

    assert len(df) == 2
    assert set(df["Order ID"]) == {"A1", "A2"}


def test_output_columns_match_supy_template_exactly():
    rows = [{"order_id": "A1", "order_date": D5, "item_name": "X",
             "qty": 1, "unit_price_inc_tax": 1.20}]
    df = ps.build_supy_frame(rows, SCRAPED_CFG)
    assert list(df.columns) == ps.SUPY_COLUMNS


def test_zero_tax_divisor_is_rejected():
    cfg = {"pricing": {"tax_divisor": 0}}
    rows = [{"order_id": "A1", "order_date": D5, "item_name": "X",
             "qty": 1, "unit_price_inc_tax": 1.00}]
    with pytest.raises(ps.TransformError, match="must not be zero"):
        ps.build_supy_frame(rows, cfg)


def test_rounding_is_two_decimal_places():
    # 10.00 / 1.2 == 8.333... → 8.33
    rows = [{"order_id": "A1", "order_date": D5, "item_name": "X",
             "qty": 1, "unit_price_inc_tax": 10.00}]
    df = ps.build_supy_frame(rows, SCRAPED_CFG)
    assert df.loc[0, "Total sales excl. tax *"] == 8.33


# ── Menu-price lookup ─────────────────────────────────────────────────────────

def _lookup_cfg(csv_rel: str) -> dict:
    return {"pricing": {"source": "lookup", "lookup_csv": csv_rel,
                        "unit_price_is_per_item": True, "tax_divisor": 1.2}}


def test_lookup_resolves_prices_by_name(tmp_path, monkeypatch):
    csv = tmp_path / "prices.csv"
    csv.write_text("item_name,price_inc_tax\nPad Thai,9.00\n")
    monkeypatch.setattr(ps, "BASE_DIR", tmp_path)

    rows = [{"order_id": "A1", "order_date": D5, "item_name": "Pad Thai",
             "qty": 2, "unit_price_inc_tax": 0.0}]
    resolved = ps.resolve_prices(rows, _lookup_cfg("prices.csv"), "homecook")

    assert resolved[0]["unit_price_inc_tax"] == 9.00


def test_lookup_matching_is_case_and_whitespace_tolerant(tmp_path, monkeypatch):
    csv = tmp_path / "prices.csv"
    csv.write_text("item_name,price_inc_tax\nPad Thai,9.00\n")
    monkeypatch.setattr(ps, "BASE_DIR", tmp_path)

    rows = [{"order_id": "A1", "order_date": D5,
             "item_name": "  pad   THAI ", "qty": 1, "unit_price_inc_tax": 0.0}]
    resolved = ps.resolve_prices(rows, _lookup_cfg("prices.csv"), "homecook")

    assert resolved[0]["unit_price_inc_tax"] == 9.00


def test_unmatched_item_fails_the_run(tmp_path, monkeypatch):
    """
    An item with no price must NEVER be uploaded at zero — that silently
    corrupts the customer's food-cost numbers.
    """
    csv = tmp_path / "prices.csv"
    csv.write_text("item_name,price_inc_tax\nPad Thai,9.00\n")
    monkeypatch.setattr(ps, "BASE_DIR", tmp_path)

    rows = [{"order_id": "A1", "order_date": D5, "item_name": "Mystery Special",
             "qty": 1, "unit_price_inc_tax": 0.0}]

    with pytest.raises(ps.TransformError) as exc:
        ps.resolve_prices(rows, _lookup_cfg("prices.csv"), "homecook")

    msg = str(exc.value)
    assert "Mystery Special" in msg
    assert "zero" in msg.lower()


def test_comment_lines_in_price_csv_are_ignored(tmp_path, monkeypatch):
    """The shipped menu_prices.csv template documents itself with # comments."""
    csv = tmp_path / "prices.csv"
    csv.write_text(
        "item_name,price_inc_tax\n"
        "# this is documentation, not an item\n"
        "# another comment\n"
        "Pad Thai,9.00\n"
    )
    monkeypatch.setattr(ps, "BASE_DIR", tmp_path)

    prices = ps.load_menu_prices(_lookup_cfg("prices.csv"), "homecook")

    assert prices == {"pad thai": 9.00}


def test_missing_price_csv_names_the_path(tmp_path, monkeypatch):
    monkeypatch.setattr(ps, "BASE_DIR", tmp_path)
    with pytest.raises(ps.ConfigError, match="Menu-price sheet not found"):
        ps.load_menu_prices(_lookup_cfg("nope.csv"), "homecook")


def test_price_csv_missing_column_is_rejected(tmp_path, monkeypatch):
    csv = tmp_path / "prices.csv"
    csv.write_text("item_name,cost\nPad Thai,9.00\n")
    monkeypatch.setattr(ps, "BASE_DIR", tmp_path)
    with pytest.raises(ps.ConfigError, match="price_inc_tax"):
        ps.load_menu_prices(_lookup_cfg("prices.csv"), "homecook")


def test_scraped_source_leaves_prices_untouched():
    rows = [{"item_name": "X", "unit_price_inc_tax": 4.20}]
    out = ps.resolve_prices(rows, SCRAPED_CFG, "feedr")
    assert out[0]["unit_price_inc_tax"] == 4.20


# ── Multi-destination splitting (Just Eat) ────────────────────────────────────

def test_just_eat_splits_by_destination():
    """153849 → Street Food Ltd., 282355 → Thai Street Boxpark."""
    rows = [
        {"item_name": "Pad Thai", "destination": "Street Food Ltd."},
        {"item_name": "Pad Thai", "destination": "Thai Street Boxpark"},
        {"item_name": "Satay", "destination": "Street Food Ltd."},
    ]
    grouped = ps.group_by_destination(rows, {})

    assert set(grouped) == {"Street Food Ltd.", "Thai Street Boxpark"}
    assert len(grouped["Street Food Ltd."]) == 2
    assert len(grouped["Thai Street Boxpark"]) == 1


def test_single_tenant_falls_back_to_config_destination():
    rows = [{"item_name": "Pad Thai"}]
    grouped = ps.group_by_destination(rows, {"destination": "Street Food Ltd."})
    assert set(grouped) == {"Street Food Ltd."}


def test_missing_destination_is_an_error_not_a_silent_blank():
    rows = [{"item_name": "Pad Thai"}]
    with pytest.raises(ps.TransformError, match="no destination"):
        ps.group_by_destination(rows, {})


# ── Shipped partner configs ───────────────────────────────────────────────────

ALL_PARTNERS = ps.available_partners()


def test_all_nine_partners_are_accounted_for():
    """Eight scraped partners + Deliveroo's own pipeline = nine."""
    assert set(ALL_PARTNERS) == {
        "anddine", "feedr", "homecook", "just_eat",
        "justeat_business", "ordit", "uber_eats",
    }
    assert (ps.BASE_DIR / "deliveroo_config.yaml").exists()


@pytest.mark.parametrize("partner", ALL_PARTNERS)
def test_partner_config_is_structurally_valid(partner):
    cfg = ps.load_partner_config(partner)

    assert cfg["portal"]["login_url"].startswith("https://")
    assert cfg["portal"]["orders_url"].startswith("https://")

    auth = cfg["auth"]
    assert auth["type"] in ("password", "pin")
    assert auth["username_env"] and auth["password_env"]

    pricing = cfg["pricing"]
    assert pricing["source"] in ("scraped", "lookup")
    assert float(pricing["tax_divisor"]) == 1.2

    # Every partner must route somewhere: either a top-level destination or
    # a destination on each business.
    if cfg.get("businesses"):
        assert all(b.get("destination") for b in cfg["businesses"])
    else:
        assert cfg.get("destination")


@pytest.mark.parametrize("partner", ALL_PARTNERS)
def test_lookup_partners_point_at_an_existing_sheet(partner):
    cfg = ps.load_partner_config(partner)
    if cfg["pricing"]["source"] != "lookup":
        pytest.skip(f"{partner} scrapes its own prices")
    path = ps.BASE_DIR / cfg["pricing"]["lookup_csv"]
    assert path.exists(), f"{partner} needs {path} to exist"


def test_just_eat_has_both_businesses_and_two_destinations():
    cfg = ps.load_partner_config("just_eat")
    ids = {b["id"] for b in cfg["businesses"]}
    dests = {b["destination"] for b in cfg["businesses"]}

    assert ids == {"153849", "282355"}
    assert dests == {"Street Food Ltd.", "Thai Street Boxpark"}


def test_uber_eats_uses_pin_auth():
    cfg = ps.load_partner_config("uber_eats")
    assert cfg["auth"]["type"] == "pin"
    assert cfg["auth"]["password_env"] == "UBER_EATS_PIN"


def test_price_lookup_partners_are_exactly_the_two_without_portal_prices():
    lookup = {p for p in ALL_PARTNERS
              if ps.load_partner_config(p)["pricing"]["source"] == "lookup"}
    assert lookup == {"justeat_business", "homecook"}


# ── Credential handling ───────────────────────────────────────────────────────

def test_missing_credential_error_warns_against_pasting(monkeypatch):
    monkeypatch.delenv("FEEDR_PASSWORD", raising=False)
    with pytest.raises(ps.AuthError) as exc:
        ps._get_credential("FEEDR_PASSWORD", "feedr", "password")
    assert "Never paste credentials" in str(exc.value)


def test_unconfigured_selectors_are_reported_with_the_fix():
    """Built from an inline config so it does not depend on which real
    partner happens to still be unresolved."""
    cfg = {"selectors": {"username_field": "", "password_field": "",
                         "login_button": ""}}
    with pytest.raises(ps.ConfigError) as exc:
        ps.require_selectors(cfg, ps.REQUIRED_AUTH_SELECTORS, "somepartner")
    msg = str(exc.value)
    assert "selectors.username_field" in msg
    assert "debug_selectors.py --partner somepartner" in msg


# Login selectors confirmed against the live pages on 2026-08-06.
# Uber Eats is OTP-gated and JustEat Business is behind a Cloudflare challenge,
# so both are legitimately still blank — see their config headers.
LOGIN_RESOLVED = ["anddine", "feedr", "homecook", "just_eat", "justeat_business",
                  "ordit"]
LOGIN_BLOCKED = ["uber_eats"]


@pytest.mark.parametrize("partner", LOGIN_RESOLVED)
def test_resolved_partners_keep_their_login_selectors(partner):
    """Regression guard: these were discovered from the live pages; a blank
    here means someone reverted real, verified values."""
    cfg = ps.load_partner_config(partner)
    ps.require_selectors(cfg, ps.REQUIRED_AUTH_SELECTORS, partner)


@pytest.mark.parametrize("partner", LOGIN_BLOCKED)
def test_blocked_partners_fail_loudly_rather_than_guess(partner):
    """These must stay blank until captured headed — a guessed selector that
    silently matches the wrong element is worse than a clear failure."""
    cfg = ps.load_partner_config(partner)
    with pytest.raises(ps.ConfigError):
        ps.require_selectors(cfg, ps.REQUIRED_AUTH_SELECTORS, partner)


def test_resolved_and_blocked_together_cover_every_partner():
    assert set(LOGIN_RESOLVED) | set(LOGIN_BLOCKED) == set(ALL_PARTNERS)


def test_justeat_business_requires_a_persistent_profile():
    """
    This portal is behind a Cloudflare challenge. A fresh context is challenged
    every run; only a persistent profile keeps the clearance cookie.
    """
    cfg = ps.load_partner_config("justeat_business")
    assert cfg["browser"]["persistent_profile"] is True
    assert cfg["browser"]["challenge_wait_seconds"] >= 10


def test_profile_path_is_per_partner_and_under_state():
    """
    debug_selectors.py --profile seeds state/<partner>/browser_profile. If this
    convention changes on one side only, seeding silently stops helping the
    real pipeline — which is exactly the bug this guards.
    """
    path = ps._profile_path("justeat_business")
    assert path.parent.name == "justeat_business"
    assert path.name == "browser_profile"
    assert path.parent.parent == ps.STATE_ROOT


def test_justeat_business_two_step_login_is_fully_mapped():
    """
    Confirmed by walking the real flow: email screen → Keycloak realm that
    re-asks username + password. Not a magic-link flow.
    """
    sel = ps.load_partner_config("justeat_business")["selectors"]
    assert sel["username_field"] == "#email_rs"
    assert sel["username_submit_button"]
    assert sel["username_field_step2"] == "#username"
    assert sel["password_field"] == "#password"
    assert sel["login_button"] == "#kc-login"


def test_ordit_and_just_eat_avoid_unstable_generated_ids():
    """
    Ordit's Vuetify ids (#input-33) and Deliveroo's hashed CSS classes shift on
    every deploy. Selectors must not depend on them.
    """
    for partner in ("ordit", "just_eat"):
        sel = ps.load_partner_config(partner)["selectors"]
        for key in ("username_field", "password_field"):
            assert "input-3" not in sel[key], (
                f"{partner}.{key} relies on a generated Vuetify id")


# ── Recipients: --email-to must reach the per-partner reports ───────────
#
# Added 2026-09-02. resolve_recipients() previously read REPORT_RECIPIENT only,
# so --email-to on run_all_partners.py reached the roll-up summary but never
# the per-partner report emails. customer.care@ was on the summary and nothing
# else, while the README claimed the flag covered both.


def test_email_to_override_wins_over_report_recipient(monkeypatch):
    monkeypatch.setenv("REPORT_RECIPIENT", "vaishnavi@supy.io,charlotte@supy.io")
    assert ps.resolve_recipients(["customer.care@supy.io"]) == \
        ["customer.care@supy.io"]


def test_no_override_falls_back_to_report_recipient(monkeypatch):
    monkeypatch.setenv("REPORT_RECIPIENT", "vaishnavi@supy.io; charlotte@supy.io")
    assert ps.resolve_recipients() == ["vaishnavi@supy.io", "charlotte@supy.io"]
    assert ps.resolve_recipients([]) == ["vaishnavi@supy.io", "charlotte@supy.io"]
    assert ps.resolve_recipients(None) == ["vaishnavi@supy.io", "charlotte@supy.io"]


def test_override_accepts_several_flags_and_embedded_lists(monkeypatch):
    """--email-to may be repeated, and each value may itself be a list."""
    monkeypatch.delenv("REPORT_RECIPIENT", raising=False)
    got = ps.resolve_recipients(["a@supy.io", "b@supy.io,c@supy.io"])
    assert got == ["a@supy.io", "b@supy.io", "c@supy.io"]


def test_override_dedupes_case_insensitively(monkeypatch):
    monkeypatch.delenv("REPORT_RECIPIENT", raising=False)
    assert ps.resolve_recipients(["A@supy.io", "a@supy.io", " a@SUPY.io "]) == \
        ["A@supy.io"]


def test_runner_forwards_email_to_each_subprocess():
    """The runner must pass every --email-to through to the engine, or the
    per-partner reports silently go to REPORT_RECIPIENT instead."""
    import argparse
    import run_all_partners as rap

    args = argparse.Namespace(
        date="2026-09-01", date_from=None, date_to=None, no_email=False,
        force_login=False, debug=False, dry_run=False,
        email_to=["vaishnavi@supy.io", "customer.care@supy.io"])
    cmd = rap.build_command("anddine", "partner_scraper.py", args)
    assert cmd.count("--email-to") == 2
    assert "vaishnavi@supy.io" in cmd and "customer.care@supy.io" in cmd
    # and nothing is forwarded when the flag is absent
    args.email_to = None
    assert "--email-to" not in rap.build_command("anddine", "partner_scraper.py", args)


# ── Pagination cap: warn only when pages were actually missed ───────────
#
# Added 2026-09-02. pagination_cap_hit used to fire whenever
# seen_pages >= max_pages, which is true on EVERY run of a portal with no
# pager (Feedr: one list bounded by a 60-day date filter, max_pages: 1).
# That false alarm led to a real report being called incomplete when it was
# correct — verified against the live list: 11 rows after widening, scrolling
# loads no more, and only one day group fell in the requested range.


class _FakeRow:
    def query_selector(self, selector):
        return None                      # no status / date / total / reference

    def text_content(self):
        return "row"


class _FakeNext:
    def is_enabled(self):
        return True


class _FakePage:
    """Minimal page: two rows, and a next-control only if one is configured."""

    def __init__(self, next_selector=""):
        self._next_selector = next_selector

    def wait_for_selector(self, selector, timeout=None):
        return None

    def query_selector_all(self, selector):
        return [_FakeRow(), _FakeRow()]

    def query_selector(self, selector):
        if self._next_selector and selector == self._next_selector:
            return _FakeNext()
        return None

    def wait_for_load_state(self, state, timeout=None):
        return None


def _cfg(next_page="", max_pages=1):
    return {
        "selectors": {"order_rows": ".row", "next_page": next_page},
        "filters": {"keep_status": []},
        "dates": {},
        "pagination": {"max_pages": max_pages},
        "orders": {"open_mode": "click"},
        "browser": {},
    }


def _capture_log(monkeypatch):
    calls = []
    monkeypatch.setattr(ps, "log",
                        lambda *a, **k: calls.append((a, k)))
    return calls


def test_no_cap_warning_when_the_portal_has_no_pager(monkeypatch):
    """Feedr's shape: one list, no pager, max_pages 1 — complete, not truncated."""
    calls = _capture_log(monkeypatch)
    got = ps._collect_order_links(_FakePage(), _cfg(next_page=""),
                                  datetime(2026, 8, 27), datetime(2026, 9, 1))
    assert len(got) == 2
    assert not any("pagination_cap_hit" in str(c) for c in calls), \
        "warned about truncation on a list that has no pager at all"


def test_cap_warning_when_a_further_page_really_exists(monkeypatch):
    """A pager with the cap set to 1 genuinely loses data — this must warn."""
    calls = _capture_log(monkeypatch)
    ps._collect_order_links(_FakePage(next_selector=".next"),
                            _cfg(next_page=".next", max_pages=1),
                            datetime(2026, 8, 27), datetime(2026, 9, 1))
    assert any("pagination_cap_hit" in str(c) for c in calls), \
        "silently truncated a paginated list without warning"


def test_feedr_is_configured_as_a_pagerless_list():
    """If Feedr ever grows a pager, next_page must be set or data goes missing
    while the run still reports success."""
    cfg = ps.load_partner_config("feedr")
    assert not cfg["selectors"]["next_page"], \
        "feedr now has a pager — raise pagination.max_pages too"
    assert cfg["selectors"].get("list_pre_click"), \
        "feedr relies on widening the list to 60 days before collecting"


# ── CDP attach: drive a browser the user already cleared ────────────────
#
# Added 2026-09-02. Cloudflare blocks every Playwright-LAUNCHED browser on
# partner-hub.just-eat.co.uk, headless and headed alike, and never offers the
# checkbox — so the only browser route is attaching to a real Chrome a person
# logged into. These tests cover endpoint resolution; the attach itself needs
# a live Chrome and is smoke-tested separately.


def test_cdp_endpoint_comes_from_partner_config(monkeypatch):
    monkeypatch.delenv("PARTNER_CDP_ENDPOINT", raising=False)
    cfg = {"browser": {"cdp_endpoint": "http://localhost:9222"}}
    assert ps._cdp_endpoint(cfg) == "http://localhost:9222"


def test_env_overrides_the_configured_endpoint(monkeypatch):
    monkeypatch.setenv("PARTNER_CDP_ENDPOINT", "http://localhost:9333")
    cfg = {"browser": {"cdp_endpoint": "http://localhost:9222"}}
    assert ps._cdp_endpoint(cfg) == "http://localhost:9333"


def test_no_endpoint_means_launch_our_own_browser(monkeypatch):
    monkeypatch.delenv("PARTNER_CDP_ENDPOINT", raising=False)
    for cfg in ({}, {"browser": {}}, {"browser": {"cdp_endpoint": ""}},
                {"browser": {"cdp_endpoint": None}}):
        assert ps._cdp_endpoint(cfg) == "", cfg


def test_endpoint_whitespace_is_ignored(monkeypatch):
    """A trailing newline from an .env edit must not become part of the URL."""
    monkeypatch.setenv("PARTNER_CDP_ENDPOINT", "  http://localhost:9222\n")
    assert ps._cdp_endpoint({}) == "http://localhost:9222"


def test_just_eat_is_configured_to_attach():
    """just_eat must not launch its own browser — it will be blocked."""
    cfg = ps.load_partner_config("just_eat")
    assert (cfg.get("browser") or {}).get("cdp_endpoint"), (
        "just_eat needs browser.cdp_endpoint; a Playwright-launched browser "
        "cannot clear this portal's bot challenge")


def test_other_partners_still_launch_their_own_browser():
    """Attaching is only for the blocked portal — everything else is unaffected."""
    for partner in ("anddine", "feedr", "homecook", "ordit"):
        cfg = ps.load_partner_config(partner)
        assert not (cfg.get("browser") or {}).get("cdp_endpoint"), partner


# ── Relative day headers (found 2026-09-03, after a wrong report) ───────
#
# Feedr labels its most recent day groups "Today (Wed 02 Sep)" / "Yesterday
# (Wed 02 Sep)" instead of "Wednesday 02 Sep". Neither the configured
# "%A %d %b" nor pandas could read that, so every order under such a header
# was dropped as out-of-range and the run reported — and EMAILED — "no sales
# for 02-Sep-2026" while exiting 0. There was an order that day.

REF = datetime(2026, 9, 3)


@pytest.mark.parametrize("fmt", ["%A %d %b", "", None])
def test_relative_headers_resolve_from_their_bracketed_date(fmt):
    """The bracketed date is absolute, so it stays right whenever the run
    happens — preferred over arithmetic on 'Today'/'Yesterday'."""
    assert ps.parse_date("Yesterday (Wed 02 Sep)", fmt, reference=REF).date() \
        == date(2026, 9, 2)
    assert ps.parse_date("Today (Thu 03 Sep)", fmt, reference=REF).date() \
        == date(2026, 9, 3)


def test_bare_relative_headers_fall_back_to_arithmetic():
    assert ps.parse_date("Today", "", reference=REF).date() == date(2026, 9, 3)
    assert ps.parse_date("Yesterday", "", reference=REF).date() == date(2026, 9, 2)


def test_bare_relative_header_is_not_mangled_by_dayfirst():
    """The fallback must not hand pandas an ISO string: with dayfirst=True,
    "2026-09-03" is read as 3 March."""
    assert ps.parse_date("Today", "", reference=REF).month == 9


@pytest.mark.parametrize("fmt", ["%A %d %b", ""])
def test_yearless_headers_get_the_right_year_on_both_parse_paths(fmt):
    """strptime yields exactly 1900 and pandas yields year 1. The inference
    used to run only on the pandas path AND only for year < 1900, so a
    configured yearless format dated everything to 1900 — outside any range,
    another silent "no sales"."""
    assert ps.parse_date("Thursday 27 Aug", fmt, reference=REF).date() \
        == date(2026, 8, 27)
    assert ps.parse_date("Friday 31 Jul", fmt, reference=REF).date() \
        == date(2026, 7, 31)


def test_absolute_dates_are_untouched_by_the_relative_handling():
    assert ps.parse_date("2026-08-27", "", reference=REF).date() == date(2026, 8, 27)
    assert ps.parse_date("05/08/26", "%d/%m/%y", reference=REF).date() == date(2026, 8, 5)


def test_non_relative_text_still_returns_none():
    for junk in ("", "   ", "not a date", "Tomorrowland"):
        assert ps.parse_date(junk, "", reference=REF) is None
