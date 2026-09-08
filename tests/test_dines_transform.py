"""
tests/test_dines_transform.py
-----------------------------
Offline unit tests for the Dines transform (Stage 3).

No browser, no credentials, no network. The VAT direction is the thing most
worth pinning down: Dines reports VAT-INCLUSIVE values and we divide by 1.2,
which is the opposite of the Deliveroo pipeline.

Run:
    /path/to/.venv/bin/python -m pytest tests/ -v
"""

import pathlib
import sys
from datetime import datetime

import pandas as pd
import pytest

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import dines_automation as da  # noqa: E402

BRANCH = {"key": "victoria", "supy_branch": "Black Bear Burger Victoria",
          "env_prefix": "DINES_VIC"}
WHEN = datetime(2026, 8, 30)


@pytest.fixture(autouse=True)
def isolate_output(tmp_path, monkeypatch):
    """Keep tests out of the real output tree.

    OUTPUT_DIR is anchored to the repo, not the working directory, so
    monkeypatch.chdir alone does NOT redirect writes — a test once overwrote a
    real branch file with fixture data.

    LOGS_DIR goes the same way: a test that calls main() would otherwise drop
    a run log into the repo's logs/ tree alongside the real runs.
    """
    monkeypatch.setattr(da, "OUTPUT_DIR", tmp_path / "output")
    monkeypatch.setattr(da, "LOGS_DIR", tmp_path / "logs")
    (tmp_path / "logs").mkdir(exist_ok=True)


def write_export(path: pathlib.Path, rows, cols=("Product", "Qty", "Gross Product Sales")):
    pd.DataFrame(rows, columns=list(cols)).to_csv(path, index=False)
    return path


# ── The documented field mapping ────────────────────────────────────────


def test_columns_and_vat_follow_the_spec(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = write_export(tmp_path / "e.csv", [["Black Bear Burger", 10, 120.00]])
    out_path, rows, _ = da.stage_transform(src, BRANCH, WHEN)
    df = pd.read_excel(out_path)
    assert rows == 1
    r = df.iloc[0]
    assert r["POS Item Name"] == "Black Bear Burger"
    assert r["Sold QTY *"] == 10                     # Qty -> Sold QTY
    assert r["Total sales incl. tax *"] == 120.00     # Gross Product Sales -> incl
    assert r["Total sales excl. tax *"] == 100.00     # incl / 1.2, NOT incl * 1.2
    assert r["Total Discount Value"] == 0             # spec says 0, not blank
    assert r["Sales Date *"] == "30-Aug-2026"


def test_output_column_order_matches_supy(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = write_export(tmp_path / "e.csv", [["Fries", 3, 12.00]])
    out_path, _, _ = da.stage_transform(src, BRANCH, WHEN)
    assert list(pd.read_excel(out_path).columns) == da.SUPY_COLUMNS


def test_vat_is_divided_not_multiplied(tmp_path, monkeypatch):
    """Guards the one mistake that silently corrupts every money column."""
    monkeypatch.chdir(tmp_path)
    src = write_export(tmp_path / "e.csv", [["Item", 1, 60.00]])
    out_path, _, _ = da.stage_transform(src, BRANCH, WHEN)
    excl = pd.read_excel(out_path).iloc[0]["Total sales excl. tax *"]
    assert excl == 50.00
    assert excl < 60.00, "excl. tax must be LOWER than incl. tax"


# ── Robustness ──────────────────────────────────────────────────────────


def test_currency_symbols_and_commas_are_stripped(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = write_export(tmp_path / "e.csv", [["Big Order", "12", "£1,234.56"]])
    out_path, _, _ = da.stage_transform(src, BRANCH, WHEN)
    r = pd.read_excel(out_path).iloc[0]
    assert r["Total sales incl. tax *"] == 1234.56
    assert r["Total sales excl. tax *"] == 1028.80


def test_total_footer_row_is_dropped(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = write_export(tmp_path / "e.csv",
                       [["Fries", 2, 8.00], ["Total", 2, 8.00]])
    out_path, rows, _ = da.stage_transform(src, BRANCH, WHEN)
    assert rows == 1
    assert "Total" not in pd.read_excel(out_path)["POS Item Name"].tolist()


def test_blank_product_rows_are_dropped(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = write_export(tmp_path / "e.csv",
                       [["Fries", 2, 8.00], [None, 5, 20.00]])
    _, rows, _ = da.stage_transform(src, BRANCH, WHEN)
    assert rows == 1


def test_empty_export_is_no_sales_not_an_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = tmp_path / "empty.csv"
    src.write_bytes(b"")
    path, rows, _ = da.stage_transform(src, BRANCH, WHEN)
    assert path is None and rows == 0


def test_missing_expected_column_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = write_export(tmp_path / "e.csv", [["Fries", 2]], cols=("Product", "Qty"))
    with pytest.raises(da.TransformError, match="missing expected column"):
        da.stage_transform(src, BRANCH, WHEN)


# ── PLU behaviour (codes still unconfirmed for Dines) ───────────────────


def test_pos_item_id_falls_back_to_name_without_a_plu_sheet(tmp_path, monkeypatch):
    """With no PLU source at all, the ID must be the visible item name —
    never a silently wrong code."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setitem(da.CONFIG["plu"], "file", None)
    src = write_export(tmp_path / "e.csv", [["Cajun Fries", 3, 18.00]])
    out_path, _, notes = da.stage_transform(src, BRANCH, WHEN)
    r = pd.read_excel(out_path).iloc[0]
    assert r["POS Item ID *"] == "Cajun Fries"
    assert any("PLU source: NONE" in n for n in notes)


def test_plu_sheet_supplies_codes_when_given(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    import openpyxl
    wb = openpyxl.Workbook()
    wb.active.title = "PLU CODE"
    wb.active.append(["PoS Item Name", "PoS Item ID"])
    wb.active.append(["Cajun Fries", "sku-136"])
    plu = tmp_path / "plu.xlsx"
    wb.save(plu)

    src = write_export(tmp_path / "e.csv", [["Cajun Fries", 3, 18.00]])
    out_path, _, _ = da.stage_transform(src, BRANCH, WHEN, [plu])
    assert pd.read_excel(out_path).iloc[0]["POS Item ID *"] == "sku-136"


# ── Credentials never leak ──────────────────────────────────────────────


def test_missing_credentials_name_the_key_not_the_value(monkeypatch):
    for suffix in ("USERNAME", "PASSWORD", "PIN"):
        monkeypatch.delenv(f"DINES_VIC_{suffix}", raising=False)
    with pytest.raises(da.ConfigError) as exc:
        da.credentials(BRANCH)
    msg = str(exc.value)
    assert "DINES_VIC_USERNAME" in msg and "set_credential.py" in msg


def test_recipients_dedupe_and_split(monkeypatch):
    monkeypatch.setenv("DINES_REPORT_RECIPIENT",
                       "a@supy.io, b@supy.io; A@supy.io")
    assert da.recipients(None) == ["a@supy.io", "b@supy.io"]
    assert da.recipients(["z@supy.io"]) == ["z@supy.io"]


# ── Thousands separators (found 2026-09-01) ─────────────────────────────


def test_thousands_separator_in_qty_is_read_not_zeroed(tmp_path, monkeypatch):
    """Dines writes '1,234' past 999. Coercing that to 0 would report a
    best-seller as zero sold while its revenue stayed correct."""
    monkeypatch.chdir(tmp_path)
    src = write_export(tmp_path / "e.csv", [["Fries", "1,234", "5,678.90"]])
    out_path, _, _ = da.stage_transform(src, BRANCH, WHEN)
    r = pd.read_excel(out_path).iloc[0]
    assert r["Sold QTY *"] == 1234
    assert r["Total sales incl. tax *"] == 5678.90
    assert r["Total sales excl. tax *"] == 4732.42


def test_unreadable_qty_raises_rather_than_defaulting_to_zero(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = write_export(tmp_path / "e.csv", [["Fries", "twelve", "10.00"]])
    with pytest.raises(da.TransformError, match="whole number"):
        da.stage_transform(src, BRANCH, WHEN)


# ── Nine-branch coverage (added 2026-09-01) ─────────────────────────────
#
# All nine Supy branches are configured; only the ones with .env credentials
# actually run. These tests pin the config shape and the skip behaviour, so a
# branch can never be half-added (a key typo would otherwise show up as a
# silent skip that looks exactly like "credentials not supplied yet").


def test_all_nine_supy_branches_are_configured():
    branches = da.branches()
    assert len(branches) == 9
    assert len({b["key"] for b in branches}) == 9
    assert len({b["env_prefix"] for b in branches}) == 9
    assert len({b["supy_branch"] for b in branches}) == 9


def test_dines_branch_names_match_the_deliveroo_branch_mapping():
    """The same nine Supy branches the Deliveroo converter writes to.

    A name that differs by even a word lands sales in a branch Supy does not
    recognise, so the two pipelines are held to one spelling.
    """
    import csv
    mapping = pathlib.Path(__file__).parent.parent / "mappings" \
        / "blackbear_branches.csv"
    with mapping.open() as fh:
        supy = {row["supy_branch"] for row in csv.DictReader(fh)}
    assert {b["supy_branch"] for b in da.branches()} == supy


def test_missing_credentials_reports_key_names_only(monkeypatch):
    monkeypatch.setenv("DINES_SH_USERNAME", "secret-user@example.com")
    monkeypatch.setenv("DINES_SH_PASSWORD", "s3cr3t-value")
    monkeypatch.delenv("DINES_SH_PIN", raising=False)
    branch = da.find_branch("shoreditch")
    missing = da.missing_credentials(branch)
    assert missing == ["DINES_SH_PIN"]
    blob = " ".join(missing) + da.credential_error(branch, missing)
    assert "s3cr3t-value" not in blob and "secret-user@example.com" not in blob


def test_partial_credentials_are_not_treated_as_ready(monkeypatch):
    """Two of three keys is not runnable — a PIN-less login stalls at the pad."""
    monkeypatch.setenv("DINES_BX_USERNAME", "u")
    monkeypatch.setenv("DINES_BX_PASSWORD", "p")
    monkeypatch.delenv("DINES_BX_PIN", raising=False)
    assert da.missing_credentials(da.find_branch("brixton")) == ["DINES_BX_PIN"]


def test_blank_credential_counts_as_missing(monkeypatch):
    monkeypatch.setenv("DINES_WF_USERNAME", "   ")
    assert "DINES_WF_USERNAME" in da.missing_credentials(
        da.find_branch("westfield"))


def test_all_branches_run_skips_branches_without_credentials(monkeypatch,
                                                             capsys):
    """--all-branches must exit 0 and launch no browser when a branch is
    awaiting credentials, or the daily job alarms every morning until all
    nine are set up."""
    monkeypatch.setattr(da, "CONFIG", {**da.CONFIG, "branches": [
        {"key": "westfield", "supy_branch": "Black Bear Burger Westfield",
         "env_prefix": "DINES_WF"}]})
    for suffix in ("USERNAME", "PASSWORD", "PIN"):
        monkeypatch.delenv(f"DINES_WF_{suffix}", raising=False)

    def no_browser():
        raise AssertionError("a browser was launched for an unconfigured branch")

    monkeypatch.setattr(da, "sync_playwright", no_browser)
    monkeypatch.setattr(sys, "argv", ["dines_automation.py", "--all-branches",
                                      "--no-email"])
    assert da.main() == 0
    out = capsys.readouterr().out
    assert "no_creds" in out and "westfield" in out


def test_named_branch_without_credentials_fails_loudly(monkeypatch, capsys):
    """Asking for one branch by name is not a request to skip it."""
    for suffix in ("USERNAME", "PASSWORD", "PIN"):
        monkeypatch.delenv(f"DINES_CM_{suffix}", raising=False)
    monkeypatch.setattr(da, "sync_playwright",
                        lambda: (_ for _ in ()).throw(AssertionError("launched")))
    monkeypatch.setattr(sys, "argv", ["dines_automation.py", "--branch",
                                      "camden", "--no-email"])
    assert da.main() == 1
    assert "DINES_CM_USERNAME" in capsys.readouterr().err
