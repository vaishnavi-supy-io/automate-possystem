"""
tests/test_sapaad_modifiers.py
------------------------------
Offline unit tests for the Sapaad paid-modifiers half of the pipeline
(Marketing -> Top Paid Modifiers), covering:

  * the modifier name doubling as its POS code,
  * excl. tax derived as incl / 1.05 only when the export is gross-only,
  * modifier rows appended beneath the grossing rows in one sheet,
  * verification counting both raw files rather than the grossing one alone.

No browser, no credentials, no network required.

Run:
    /path/to/.venv/bin/python -m pytest tests/test_sapaad_modifiers.py -v
"""

import pathlib
import sys

import pandas as pd
import pytest
import yaml

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import sapapad_automation as sp  # noqa: E402

# sapapad_automation resolves --config from sys.argv at import time and lands
# on BMD, which has no modifiers block. Rather than rewrite sys.argv — that is
# process-global and would hand every other test module the wrong tenant — swap
# the already-parsed CONFIG for the duration of each test here.
TENANT_CONFIG = yaml.safe_load((ROOT / "sapaad_pinza_config.yaml").read_text())


@pytest.fixture(autouse=True)
def pinza_config(monkeypatch):
    monkeypatch.setattr(sp, "CONFIG", TENANT_CONFIG)


GROSSING_CSV = (
    "Item Name,Category,Item Code,Total Sold,"
    "Total Amount Excluding Tax,Total Amount\n"
    "Margherita,Pizza,,10,100.00,105.00\n"
    "Diavola,Pizza,,5,50.00,52.50\n"
)

MODIFIERS_CSV = (
    "Modifier Name,Total Sold,Total Amount\n"
    "Extra Cheese,8,42.00\n"
    "Truffle Oil,3,21.00\n"
)


@pytest.fixture
def raws(tmp_path, monkeypatch):
    """Write both raw CSVs and point the pipeline's output at tmp_path."""
    gross = tmp_path / "pinza_Branch_RUN_raw.csv"
    mods = tmp_path / "pinza_Branch_RUN_modifiers_raw.csv"
    gross.write_text(GROSSING_CSV)
    mods.write_text(MODIFIERS_CSV)
    monkeypatch.setattr(sp, "OUTPUT_DIR", tmp_path)
    return gross, mods


def read_out(path):
    return pd.read_excel(path, engine="openpyxl")


# ── The modifier rows themselves ─────────────────────────────────────────────

def test_modifier_name_is_used_as_both_pos_code_and_item_name(raws):
    _gross, mods = raws
    df = sp._build_modifier_rows(mods, "10-Sep-2026")

    assert list(df["POS Item Name"]) == ["Extra Cheese", "Truffle Oil"]
    # Modifiers are absent from the item master, so the SOP reuses the name.
    assert list(df["POS Item ID *"]) == ["Extra Cheese", "Truffle Oil"]


def test_excl_tax_is_derived_from_gross_at_five_percent(raws):
    _gross, mods = raws
    df = sp._build_modifier_rows(mods, "10-Sep-2026")

    assert list(df["Total sales incl. tax *"]) == [42.00, 21.00]
    assert list(df["Total sales excl. tax *"]) == [40.00, 20.00]


def test_stated_excl_tax_is_used_verbatim_not_re_derived(tmp_path, monkeypatch):
    """A column the export states must never be recomputed from its own gross."""
    mods = tmp_path / "mods.csv"
    mods.write_text(
        "Modifier Name,Total Sold,Total Amount,Total Amount Excluding Tax\n"
        "Extra Cheese,8,42.00,39.11\n"
    )
    df = sp._build_modifier_rows(mods, "10-Sep-2026")

    assert df["Total sales excl. tax *"].iloc[0] == 39.11  # not 42/1.05 = 40.00


def test_unrecognised_headers_fail_with_the_real_headers_named(tmp_path):
    mods = tmp_path / "mods.csv"
    mods.write_text("Something,Else\nfoo,1\n")

    with pytest.raises(sp.TransformError) as exc:
        sp._build_modifier_rows(mods, "10-Sep-2026")

    assert "Something" in str(exc.value)  # tells you what the file really has


def test_blank_modifier_names_are_dropped(tmp_path):
    mods = tmp_path / "mods.csv"
    mods.write_text(
        "Modifier Name,Total Sold,Total Amount\n"
        "Extra Cheese,8,42.00\n"
        " ,0,0.00\n"
    )
    assert len(sp._build_modifier_rows(mods, "10-Sep-2026")) == 1


# ── Appending into the grossing sheet ────────────────────────────────────────

def test_modifiers_are_appended_below_the_grossing_rows(raws):
    gross, mods = raws
    out_path, rows, _date = sp.stage_transform(
        gross, branch_name="Branch", modifiers_raw=mods
    )
    df = read_out(out_path)

    assert rows == 4
    assert list(df["POS Item Name"]) == [
        "Margherita", "Diavola",        # grossing items first...
        "Extra Cheese", "Truffle Oil",  # ...modifiers continue underneath
    ]
    # One sheet, one column set — the modifier half must not widen it.
    assert list(df.columns) == sp.CONFIG["output_column_order"]


def test_modifier_rows_carry_the_same_sales_date(raws):
    gross, mods = raws
    out_path, _rows, report_date = sp.stage_transform(
        gross, branch_name="Branch", modifiers_raw=mods
    )
    df = read_out(out_path)

    assert df["Sales Date *"].nunique() == 1
    assert df["Sales Date *"].iloc[0] == report_date


def test_without_modifiers_the_output_is_grossing_only(raws):
    gross, _mods = raws
    out_path, rows, _date = sp.stage_transform(gross, branch_name="Branch")

    assert rows == 2
    assert list(read_out(out_path)["POS Item Name"]) == ["Margherita", "Diavola"]


# ── Verification ─────────────────────────────────────────────────────────────

def test_verification_counts_both_raw_files(raws):
    """Without this the appended rows would trip the 5% row-count gate."""
    gross, mods = raws
    out_path, _rows, _date = sp.stage_transform(
        gross, branch_name="Branch", modifiers_raw=mods
    )

    result = sp.stage_verify(gross, out_path, modifiers_raw_path=mods)

    # Scoped to the Layer 1 integrity checks: the item-ID match rate also fails
    # here, but only because this tenant's mapping file is still empty, which
    # test_modifier_rows_do_not_inflate_the_item_id_match_rate asserts on
    # purpose.
    integrity = [e for e in result.errors
                 if "Row count mismatch" in e or "Revenue mismatch" in e]
    assert integrity == [], integrity


def test_verification_fails_when_modifiers_are_ignored(raws):
    """Guards the fix: the old comparison saw 4 output rows against 2 raw."""
    gross, mods = raws
    out_path, _rows, _date = sp.stage_transform(
        gross, branch_name="Branch", modifiers_raw=mods
    )

    result = sp.stage_verify(gross, out_path, modifiers_raw_path=None)
    assert any("Row count mismatch" in e for e in result.errors)


def test_modifier_rows_do_not_inflate_the_item_id_match_rate(raws):
    """
    Modifier rows always carry an ID. If they counted toward the match rate, a
    completely unmapped grossing half (the state of all three tenants today)
    would score 50% instead of 0% and could slip past the 80% gate.
    """
    gross, mods = raws
    out_path, _rows, _date = sp.stage_transform(
        gross, branch_name="Branch", modifiers_raw=mods
    )

    result = sp.stage_verify(gross, out_path, modifiers_raw_path=mods)
    rate_msgs = [m for m in result.errors + result.warnings if "match rate" in m]
    assert rate_msgs, "expected the unmapped grossing rows to be reported"
    assert "0/2" in rate_msgs[0] or "0%" in rate_msgs[0], rate_msgs
