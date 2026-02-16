"""
Verify Polars migration output against golden baseline CSVs.

Compares row counts, column names, and cell-level values (with float tolerance).
Run after each migration phase to ensure no regressions.

Usage:
    python verify_migration.py                  # compare all 5 outputs
    python verify_migration.py All_Items        # compare a single output
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import LOCAL_STAGING_PATH

GOLDEN_DIR = LOCAL_STAGING_PATH / "golden_baselines"

FILES = [
    "All_Sales_Python.csv",
    "All_Customers_Python.csv",
    "All_Items_Python.csv",
    "Customer_Quality_Monthly_Python.csv",
    "DimPeriod_Python.csv",
]

FLOAT_TOL = 1e-6

# Columns that change based on when the script runs (relative to "today")
SKIP_COLUMNS = {
    "IsCurrentMonth", "IsCurrentQuarter", "IsCurrentYear", "IsCurrentISOWeek",
}


def compare_csv(name: str) -> bool:
    """Compare a single CSV against its golden baseline. Returns True if match."""
    import polars as pl

    golden_path = GOLDEN_DIR / name
    current_path = LOCAL_STAGING_PATH / name

    if not golden_path.exists():
        print(f"  [SKIP] {name}: no golden baseline")
        return True
    if not current_path.exists():
        print(f"  [FAIL] {name}: output file missing")
        return False

    golden = pl.read_csv(golden_path, infer_schema_length=10000, try_parse_dates=False)
    current = pl.read_csv(current_path, infer_schema_length=10000, try_parse_dates=False)

    issues = []

    # 1. Row count
    if golden.height != current.height:
        issues.append(f"Row count: golden={golden.height:,}, current={current.height:,}")

    # 2. Column names
    golden_cols = set(golden.columns)
    current_cols = set(current.columns)
    missing = golden_cols - current_cols
    extra = current_cols - golden_cols
    if missing:
        issues.append(f"Missing columns: {missing}")
    if extra:
        issues.append(f"Extra columns: {extra}")

    # 3. Column order
    if golden.columns != current.columns and not missing and not extra:
        issues.append("Column order differs")

    # 4. Cell-level comparison (only on shared columns and min rows)
    #    Sort both by all shared columns to eliminate tie-break differences
    shared_cols = sorted((golden_cols & current_cols) - SKIP_COLUMNS)
    min_rows = min(golden.height, current.height)

    if min_rows > 0 and shared_cols:
        g = golden.select(shared_cols).sort(shared_cols).head(min_rows)
        c = current.select(shared_cols).sort(shared_cols).head(min_rows)

        for col in shared_cols:
            g_col = g[col]
            c_col = c[col]

            # Cast both to string for comparison of mixed types
            g_str = g_col.cast(pl.Utf8).fill_null("")
            c_str = c_col.cast(pl.Utf8).fill_null("")

            mismatches = (g_str != c_str).sum()

            if mismatches > 0:
                # Try float comparison for numeric-looking columns
                try:
                    g_f = g_col.cast(pl.Float64, strict=False)
                    c_f = c_col.cast(pl.Float64, strict=False)
                    both_valid = g_f.is_not_null() & c_f.is_not_null()
                    if both_valid.sum() > 0:
                        diff = (g_f - c_f).abs()
                        float_mismatches = diff.filter(both_valid).gt(FLOAT_TOL).sum()
                        # Null pattern differences
                        null_diff = (g_f.is_null() != c_f.is_null()).sum()
                        total_real = float_mismatches + null_diff
                        if total_real > 0:
                            issues.append(
                                f"  {col}: {total_real:,} value mismatches "
                                f"(float tol={FLOAT_TOL})"
                            )
                    else:
                        issues.append(f"  {col}: {mismatches:,} value mismatches")
                except Exception:
                    issues.append(f"  {col}: {mismatches:,} value mismatches")

    if issues:
        print(f"  [FAIL] {name}:")
        for issue in issues:
            print(f"    - {issue}")
        return False
    else:
        print(f"  [OK]   {name} ({golden.height:,} rows, {len(golden.columns)} cols)")
        return True


def main():
    print("=" * 60)
    print("MIGRATION VERIFICATION")
    print("=" * 60)
    print(f"Golden baselines: {GOLDEN_DIR}")
    print(f"Current output:   {LOCAL_STAGING_PATH}")
    print()

    # Filter to specific file if argument given
    target = sys.argv[1].lower() if len(sys.argv) > 1 else None
    files = FILES
    if target:
        files = [f for f in FILES if target in f.lower()]
        if not files:
            print(f"No matching file for '{target}'")
            sys.exit(1)

    passed = 0
    failed = 0
    for name in files:
        if compare_csv(name):
            passed += 1
        else:
            failed += 1

    print()
    print(f"Results: {passed} passed, {failed} failed")

    if failed > 0:
        print("\n[FAIL] Migration verification FAILED")
        sys.exit(1)
    else:
        print("\n[OK] All outputs match golden baselines")


if __name__ == "__main__":
    main()
