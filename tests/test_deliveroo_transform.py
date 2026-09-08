"""
tests/test_deliveroo_transform.py
---------------------------------
Offline unit tests for the Deliveroo pipeline's Stage 3 (transform).

No browser, no credentials, no network required.

Run:
    /path/to/.venv/bin/python -m pytest tests/ -v
    # or from project root after activating venv:
    python -m pytest tests/ -v
"""

import pathlib
import sys
from datetime import datetime

import pandas as pd
import pytest

# Add project root to sys.path so we can import deliveroo_automation.py
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import deliveroo_automation as da  # noqa: E402

DATE_FROM = datetime(2026, 8, 5)
DATE_TO = datetime(2026, 8, 5)


# ── Helpers ───────────────────────────────────────────────────────────────────

def raw_headers() -> list:
    """The raw column names the config currently expects."""
    return [c["raw"] for c in da.CONFIG["columns"] if c.get("raw")]


def make_raw_csv(path: pathlib.Path, rows: list, headers: list = None) -> None:
    """Write a synthetic Deliveroo Items Sold export."""
    headers = headers if headers is not None else raw_headers()
    pd.DataFrame(rows, columns=headers).to_csv(path, index=False)


# ── Happy path ────────────────────────────────────────────────────────────────

def test_transform_maps_and_derives_correctly(tmp_path):
    raw = tmp_path / "deliveroo_raw.csv"
    make_raw_csv(raw, [
        ["Chicken Satay Skewers", 12, 60.00],
        ["Pad Thai",               5, 45.00],
    ])

    out_path, row_count, date_range = da.stage_transform(raw, DATE_FROM, DATE_TO)

    assert row_count == 2
    assert out_path is not None and out_path.exists()

    df = pd.read_excel(out_path)

    # Column order matches the Supy upload template exactly
    assert list(df.columns) == da.CONFIG["output_column_order"]

    # Mapped values
    assert df.loc[0, "POS Item Name"] == "Chicken Satay Skewers"
    assert df.loc[0, "Sold QTY *"] == 12
    assert df.loc[0, "Total sales incl. tax *"] == 60.00

    # Item name doubles as the POS Item ID
    assert df.loc[0, "POS Item ID *"] == "Chicken Satay Skewers"

    # excl. tax == incl. tax / 1.2, rounded to 2dp
    assert df.loc[0, "Total sales excl. tax *"] == 50.00
    assert df.loc[1, "Total sales excl. tax *"] == 37.50

    # Sales date comes from the requested range
    assert df.loc[0, "Sales Date *"] == "05-Aug-2026"

    # Aggregate-only report → no per-order granularity
    assert pd.isna(df.loc[0, "Order ID"]) or df.loc[0, "Order ID"] == ""

    out_path.unlink()


def test_currency_symbols_are_stripped(tmp_path):
    raw = tmp_path / "deliveroo_raw.csv"
    make_raw_csv(raw, [["Thai Green Curry", "3", "£24.00"]])

    out_path, row_count, _ = da.stage_transform(raw, DATE_FROM, DATE_TO)

    df = pd.read_excel(out_path)
    assert row_count == 1
    assert df.loc[0, "Total sales incl. tax *"] == 24.00
    assert df.loc[0, "Total sales excl. tax *"] == 20.00

    out_path.unlink()


# ── "No sales" is a valid outcome, not a failure ──────────────────────────────

def test_zero_byte_file_reports_no_sales(tmp_path):
    raw = tmp_path / "empty.csv"
    raw.touch()

    out_path, row_count, date_range = da.stage_transform(raw, DATE_FROM, DATE_TO)

    assert row_count == 0
    assert out_path is None
    assert date_range == "05-Aug-2026"


def test_header_only_file_reports_no_sales(tmp_path):
    raw = tmp_path / "headers_only.csv"
    make_raw_csv(raw, [])

    out_path, row_count, _ = da.stage_transform(raw, DATE_FROM, DATE_TO)

    assert row_count == 0
    assert out_path is None


def test_totals_only_file_reports_no_sales(tmp_path):
    """A report containing just an aggregate row has no items to upload."""
    raw = tmp_path / "totals_only.csv"
    make_raw_csv(raw, [["", 17, 105.00]])

    out_path, row_count, _ = da.stage_transform(raw, DATE_FROM, DATE_TO)

    assert row_count == 0
    assert out_path is None


def test_aggregate_row_is_dropped_but_items_kept(tmp_path):
    raw = tmp_path / "with_totals.csv"
    make_raw_csv(raw, [
        ["Chicken Satay Skewers", 12, 60.00],
        ["",                      12, 60.00],   # "Total" row — no item name
    ])

    out_path, row_count, _ = da.stage_transform(raw, DATE_FROM, DATE_TO)

    assert row_count == 1
    df = pd.read_excel(out_path)
    assert df.loc[0, "POS Item Name"] == "Chicken Satay Skewers"

    out_path.unlink()


# ── Fail loudly rather than emit wrong numbers ────────────────────────────────

def test_missing_configured_column_raises_and_lists_actuals(tmp_path):
    """
    If Deliveroo renames a column, the run must fail — silently shipping a
    report with missing sales figures is worse than shipping nothing.
    """
    raw = tmp_path / "renamed.csv"
    make_raw_csv(raw,
                 [["Pad Thai", 5, 45.00]],
                 headers=["Item name", "Quantity sold", "Total gross sales"])

    with pytest.raises(da.TransformError) as exc:
        da.stage_transform(raw, DATE_FROM, DATE_TO)

    msg = str(exc.value)
    assert "missing configured columns" in msg
    # The message must name both what was expected and what was actually found,
    # so the fix is a one-line config edit.
    assert "Gross sales" in msg
    assert "Total gross sales" in msg


# ── Date range handling ───────────────────────────────────────────────────────

def test_multi_day_range_display(tmp_path):
    raw = tmp_path / "range.csv"
    make_raw_csv(raw, [["Pad Thai", 5, 45.00]])

    out_path, _, date_range = da.stage_transform(
        raw, datetime(2026, 8, 1), datetime(2026, 8, 5))

    assert date_range == "01-Aug-2026 → 05-Aug-2026"
    out_path.unlink()


# ── Config sanity ─────────────────────────────────────────────────────────────

def test_every_output_column_is_produced_by_config():
    """Each column in output_column_order must have a mapping or an inject."""
    produced = {c["target"] for c in da.CONFIG["columns"]
                if c.get("target") and not c.get("drop")}
    missing = [c for c in da.CONFIG["output_column_order"] if c not in produced]
    assert not missing, f"output_column_order lists unproduced columns: {missing}"
