"""Golden baseline regression tests.

Compares current ETL output CSVs against golden baselines.
Adapted from verify_migration.py comparison logic.

Run with: pytest -m slow
"""

import pytest
import polars as pl
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import LOCAL_STAGING_PATH

GOLDEN_DIR = LOCAL_STAGING_PATH / "golden_baselines"
FLOAT_TOL = 1e-6

# Columns that change based on when the script runs
SKIP_COLUMNS = {
    "IsCurrentMonth", "IsCurrentQuarter", "IsCurrentYear", "IsCurrentISOWeek",
}

CSV_FILES = [
    "All_Sales_Python.csv",
    "All_Customers_Python.csv",
    "All_Items_Python.csv",
    "Customer_Quality_Monthly_Python.csv",
    "DimPeriod_Python.csv",
]


@pytest.mark.slow
@pytest.mark.parametrize("csv_name", CSV_FILES)
def test_golden_baseline(csv_name, golden_baselines_path):
    """Compare a single CSV output against its golden baseline."""
    golden_path = golden_baselines_path / csv_name
    current_path = LOCAL_STAGING_PATH / csv_name

    if not golden_path.exists():
        pytest.skip(f"No golden baseline for {csv_name}")
    if not current_path.exists():
        pytest.skip(f"Output file missing: {csv_name}")

    golden = pl.read_csv(golden_path, infer_schema_length=10000, try_parse_dates=False)
    current = pl.read_csv(current_path, infer_schema_length=10000, try_parse_dates=False)

    # Row count
    assert golden.height == current.height, (
        f"{csv_name}: row count mismatch (golden={golden.height}, current={current.height})"
    )

    # Column names
    golden_cols = set(golden.columns)
    current_cols = set(current.columns)
    assert golden_cols == current_cols, (
        f"{csv_name}: column mismatch — missing={golden_cols - current_cols}, extra={current_cols - golden_cols}"
    )

    # Cell-level comparison on shared columns
    shared_cols = sorted((golden_cols & current_cols) - SKIP_COLUMNS)
    min_rows = min(golden.height, current.height)

    if min_rows > 0 and shared_cols:
        g = golden.select(shared_cols).sort(shared_cols).head(min_rows)
        c = current.select(shared_cols).sort(shared_cols).head(min_rows)

        mismatched_cols = []
        for col in shared_cols:
            g_str = g[col].cast(pl.Utf8).fill_null("")
            c_str = c[col].cast(pl.Utf8).fill_null("")
            mismatches = (g_str != c_str).sum()

            if mismatches > 0:
                # Try float comparison
                try:
                    g_f = g[col].cast(pl.Float64, strict=False)
                    c_f = c[col].cast(pl.Float64, strict=False)
                    both_valid = g_f.is_not_null() & c_f.is_not_null()
                    if both_valid.sum() > 0:
                        diff = (g_f - c_f).abs()
                        float_mismatches = diff.filter(both_valid).gt(FLOAT_TOL).sum()
                        null_diff = (g_f.is_null() != c_f.is_null()).sum()
                        if float_mismatches + null_diff > 0:
                            mismatched_cols.append(f"{col}: {float_mismatches + null_diff} mismatches")
                    else:
                        mismatched_cols.append(f"{col}: {mismatches} mismatches")
                except Exception:
                    mismatched_cols.append(f"{col}: {mismatches} mismatches")

        assert not mismatched_cols, (
            f"{csv_name}: cell-level mismatches:\n" + "\n".join(f"  - {m}" for m in mismatched_cols)
        )
