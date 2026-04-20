"""
tests/test_transform.py
-----------------------
Offline unit tests for Stage 3 (transform).

No browser, no credentials, no network required.

Run:
    /path/to/.venv/bin/python -m pytest tests/ -v
    # or from project root after activating venv:
    python -m pytest tests/ -v
"""

import pathlib
import sys
import tempfile

import openpyxl
import pandas as pd
import pytest

# Add project root to sys.path so we can import automation.py
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

# ── Helpers ───────────────────────────────────────────────────────────────────

def make_raw_excel(path: pathlib.Path, business_date: str = "15/04/2026") -> None:
    """
    Build a synthetic raw file that matches the Oracle BI export format:
      Row 0: metadata  — Business Dates  | <date>
      Row 1: metadata  — Locations       | Find Salt-Abu Dhabi
      Row 2: metadata  — Revenue Centers | Find Salt-Abu Dhabi
      Row 3: metadata  — Menu Items      | All
      Row 4: blank row
      Row 5: data headers
      Row 6: Totals aggregate row  (Menu Item # = NaN / blank)
      Row 7+: actual data rows
    """
    wb = openpyxl.Workbook()
    ws = wb.active

    # Metadata rows
    ws.append(["Business Dates",  business_date,     "", "", "", "", "", "", "", "", "", ""])
    ws.append(["Locations",       "Find Salt-Abu Dhabi", "", "", "", "", "", "", "", "", "", ""])
    ws.append(["Revenue Centers", "Find Salt-Abu Dhabi", "", "", "", "", "", "", "", "", "", ""])
    ws.append(["Menu Items",      "All",             "", "", "", "", "", "", "", "", "", ""])
    ws.append(["", "", "", "", "", "", "", "", "", "", "", ""])  # blank

    # Data header row
    ws.append([
        "Menu Item Name", "Menu Item #", "Menu Item Def", "Price Level",
        "Qty Sold", "Returns",
        "Net VAT before Disc.", "Discounts VAT", "Net VAT after Disc.",
        "Gross before Disc.", "Discounts", "Gross after Disc.",
    ])

    # Totals aggregate row (Menu Item # is blank → should be stripped)
    ws.append(["Totals:", "", 1, 1, 2317, 0, 50600.95, -575.81, 50025.14, 53131.00, -604.60, 52526.40])

    # Data rows
    ws.append(["Original Beef Slider",   10000003, 1, 1, 215, 0, 7809.52, -182.86, 7626.67, 8200.00, -192.00, 8008.00])
    ws.append(["Chicken Cheetos Slider", 10000011, 1, 1, 105, 0, 4623.81,   -8.95, 4614.86, 4855.00,   -9.40, 4845.60])
    ws.append(["Original Fries",         10000016, 1, 1, 181, 0, 2768.57,  -35.62, 2732.95, 2907.00,  -37.40, 2869.60])

    wb.save(str(path))


def make_raw_csv(path: pathlib.Path, business_date: str = "15/04/2026") -> None:
    """Same layout but as CSV for CSV-path testing."""
    rows = [
        f"Business Dates,{business_date},,,,,,,,,,",
        "Locations,Find Salt-Abu Dhabi,,,,,,,,,,",
        "Revenue Centers,Find Salt-Abu Dhabi,,,,,,,,,,",
        "Menu Items,All,,,,,,,,,,",
        ",,,,,,,,,,,",
        "Menu Item Name,Menu Item #,Menu Item Def,Price Level,Qty Sold,Returns,Net VAT before Disc.,Discounts VAT,Net VAT after Disc.,Gross before Disc.,Discounts,Gross after Disc.",
        "Totals:,,1,1,2317,0,50600.95,-575.81,50025.14,53131.00,-604.60,52526.40",
        "Original Beef Slider,10000003,1,1,215,0,7809.52,-182.86,7626.67,8200.00,-192.00,8008.00",
        "Chicken Cheetos Slider,10000011,1,1,105,0,4623.81,-8.95,4614.86,4855.00,-9.40,4845.60",
        "Original Fries,10000016,1,1,181,0,2768.57,-35.62,2732.95,2907.00,-37.40,2869.60",
    ]
    path.write_text("\n".join(rows))


# ── Import the function under test ────────────────────────────────────────────
from automation import stage_transform, _detect_header_row


# ── Tests: _detect_header_row ─────────────────────────────────────────────────

class TestDetectHeaderRow:
    def test_xlsx_detects_correct_row_index(self, tmp_path):
        raw = tmp_path / "raw.xlsx"
        make_raw_excel(raw)
        header_row, business_date = _detect_header_row(raw)
        # Row 5 (0-indexed) is the header row
        assert header_row == 5

    def test_xlsx_extracts_business_date(self, tmp_path):
        raw = tmp_path / "raw.xlsx"
        make_raw_excel(raw, business_date="20/04/2026")
        _, business_date = _detect_header_row(raw)
        assert business_date == "20/04/2026"

    def test_csv_detects_correct_row_index(self, tmp_path):
        raw = tmp_path / "raw.csv"
        make_raw_csv(raw)
        header_row, business_date = _detect_header_row(raw)
        assert header_row == 5

    def test_csv_extracts_business_date(self, tmp_path):
        raw = tmp_path / "raw.csv"
        make_raw_csv(raw, business_date="01/04/2026")
        _, business_date = _detect_header_row(raw)
        assert business_date == "01/04/2026"


# ── Tests: stage_transform ────────────────────────────────────────────────────

class TestStageTransform:
    @pytest.fixture
    def output_xlsx(self, tmp_path):
        """Run transform on a synthetic raw Excel and return the output DataFrame."""
        raw = tmp_path / "20260415T120000_abc12345_raw.xlsx"
        make_raw_excel(raw)
        out_path = stage_transform(raw)
        assert out_path.exists(), "Output file was not created"
        return pd.read_excel(str(out_path))

    # ── Schema ────────────────────────────────────────────────────────────────

    def test_output_has_correct_columns(self, output_xlsx):
        expected = [
            "Sales Date *", "POS Item ID *", "POS Item Name", "Sold QTY *",
            "Total Discount Value", "Total sales excl. tax *", "Total sales incl. tax *",
            "Order ID", "Sales Type Code",
        ]
        assert list(output_xlsx.columns) == expected

    def test_no_dropped_columns_in_output(self, output_xlsx):
        dropped = [
            "Menu Item Def", "Price Level", "Returns",
            "Net VAT before Disc.", "Discounts VAT", "Gross before Disc.",
        ]
        for col in dropped:
            assert col not in output_xlsx.columns, f"Dropped column '{col}' found in output"

    # ── Row filtering ─────────────────────────────────────────────────────────

    def test_totals_row_stripped(self, output_xlsx):
        # "Totals:" row has blank Menu Item # → must not appear in output
        first_col_values = output_xlsx["POS Item Name"].astype(str).tolist()
        assert "Totals:" not in first_col_values

    def test_correct_row_count(self, output_xlsx):
        # 3 data rows, Totals stripped
        assert len(output_xlsx) == 3

    # ── Data values ───────────────────────────────────────────────────────────

    def test_pos_item_id_values(self, output_xlsx):
        ids = output_xlsx["POS Item ID *"].tolist()
        assert ids == [10000003, 10000011, 10000016]

    def test_pos_item_name_values(self, output_xlsx):
        names = output_xlsx["POS Item Name"].tolist()
        assert names == ["Original Beef Slider", "Chicken Cheetos Slider", "Original Fries"]

    def test_sold_qty_values(self, output_xlsx):
        qtys = output_xlsx["Sold QTY *"].tolist()
        assert qtys == [215, 105, 181]

    # ── Date formatting ───────────────────────────────────────────────────────

    def test_sales_date_format(self, output_xlsx):
        # Input: "15/04/2026" → expected output: "15-Apr-2026"
        dates = output_xlsx["Sales Date *"].tolist()
        assert all(d == "15-Apr-2026" for d in dates), f"Unexpected dates: {dates}"

    def test_sales_date_different_month(self, tmp_path):
        raw = tmp_path / "raw.xlsx"
        make_raw_excel(raw, business_date="01/01/2026")
        out = stage_transform(raw)
        df = pd.read_excel(str(out))
        assert all(d == "01-Jan-2026" for d in df["Sales Date *"].tolist())

    # ── Currency / float ──────────────────────────────────────────────────────

    def test_total_discount_value_rounded(self, output_xlsx):
        discounts = output_xlsx["Total Discount Value"].tolist()
        # Raw: -192.00, -9.40, -37.40
        assert discounts == [-192.0, -9.4, -37.4]

    def test_total_sales_excl_tax_rounded(self, output_xlsx):
        excl = output_xlsx["Total sales excl. tax *"].tolist()
        # Maps from "Net VAT after Disc." column
        assert excl == [7626.67, 4614.86, 2732.95]

    def test_total_sales_incl_tax_rounded(self, output_xlsx):
        incl = output_xlsx["Total sales incl. tax *"].tolist()
        # Maps from "Gross after Disc." column
        assert incl == [8008.0, 4845.6, 2869.6]

    # ── Injected empty columns ────────────────────────────────────────────────

    def test_order_id_is_empty(self, output_xlsx):
        assert all(str(v) in ("", "nan") for v in output_xlsx["Order ID"].tolist())

    def test_sales_type_code_is_empty(self, output_xlsx):
        assert all(str(v) in ("", "nan") for v in output_xlsx["Sales Type Code"].tolist())

    # ── Column order ──────────────────────────────────────────────────────────

    def test_column_order_matches_spec(self, output_xlsx):
        cols = list(output_xlsx.columns)
        assert cols[0] == "Sales Date *"
        assert cols[1] == "POS Item ID *"
        assert cols[-1] == "Sales Type Code"

    # ── CSV input path ────────────────────────────────────────────────────────

    def test_csv_input_produces_same_schema(self, tmp_path):
        raw = tmp_path / "raw.csv"
        make_raw_csv(raw)
        out = stage_transform(raw)
        df = pd.read_excel(str(out))
        expected_cols = [
            "Sales Date *", "POS Item ID *", "POS Item Name", "Sold QTY *",
            "Total Discount Value", "Total sales excl. tax *", "Total sales incl. tax *",
            "Order ID", "Sales Type Code",
        ]
        assert list(df.columns) == expected_cols
        assert len(df) == 3


# ── Tests: config validation ──────────────────────────────────────────────────

class TestConfig:
    def test_config_yaml_loads(self):
        import yaml
        cfg_path = pathlib.Path(__file__).parent.parent / "config.yaml"
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        assert cfg is not None

    def test_required_top_level_keys(self):
        import yaml
        cfg_path = pathlib.Path(__file__).parent.parent / "config.yaml"
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        for key in ("portal", "selectors", "navigation", "columns", "output_column_order"):
            assert key in cfg, f"Missing required config key: '{key}'"

    def test_navigation_chain_has_three_steps(self):
        import yaml
        cfg_path = pathlib.Path(__file__).parent.parent / "config.yaml"
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        assert len(cfg["navigation"]) == 3

    def test_output_column_order_matches_non_dropped_targets(self):
        import yaml
        cfg_path = pathlib.Path(__file__).parent.parent / "config.yaml"
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        output_order = set(cfg["output_column_order"])
        mapped_targets = {
            c["target"] for c in cfg["columns"]
            if not c.get("drop") and c.get("target")
        }
        assert output_order == mapped_targets, (
            f"output_column_order and column targets are out of sync.\n"
            f"  In output_column_order but not in columns: {output_order - mapped_targets}\n"
            f"  In columns but not in output_column_order: {mapped_targets - output_order}"
        )
