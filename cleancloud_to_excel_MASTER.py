"""
MASTER WORKFLOW - CleanCloud to Excel (with Auto DimPeriod)
Author: Abdulla

OPTIMIZED WORKFLOW:
1. Check & update DimPeriod (auto 3-month lookahead)
2. Load CleanCloud CSVs once (shared across transforms)
3. Run all transforms in sequence
4. Fast refresh (~10-15 seconds)

ARCHITECTURE:
- helpers.py â†’ Pure utility functions (imported by all)
- generate_dimperiod.py â†’ DimPeriod module (imported here)
- transform_*.py â†’ Transforms (run in sequence)
"""

import subprocess
import sys
import time
from pathlib import Path
import os
from typing import Tuple, Dict
import pandas as pd


# =====================================================================
# CONFIGURATION (centralized in config.py)
# =====================================================================

PYTHON_SCRIPT_FOLDER = Path(__file__).resolve().parent
sys.path.insert(0, str(PYTHON_SCRIPT_FOLDER))

from config import LOCAL_STAGING_PATH, DOWNLOADS_PATH
from logger_config import setup_logger

# Setup logger
logger = setup_logger(__name__)

# Create local staging folder if it doesn't exist
if not LOCAL_STAGING_PATH.exists():
    LOCAL_STAGING_PATH.mkdir(parents=True, exist_ok=True)
    logger.info(f"[OK] Created local staging folder: {LOCAL_STAGING_PATH}\n")

# Python transforms to run (in order)
TRANSFORMS = [
    {
        'module_name': 'transform_all_customers',
        'transform_name': 'all_customers_df',
        'description': 'Customer master data'
    },
    {
        'module_name': 'transform_all_sales',
        'transform_name': 'all_sales_df',
        'description': 'Orders + Subscriptions'
    },
    {
        'module_name': 'transform_all_items',
        'transform_name': 'all_items_df',
        'description': 'Item-level data'
    },
    {
        'module_name': 'transform_customer_quality_monthly',
        'transform_name': 'customer_quality_df',
        'description': 'Monthly quality metrics'
    }
]


# =====================================================================
# LOAD SOURCE CSVs (SHARED ACROSS TRANSFORMS)
# =====================================================================

def load_source_csvs() -> Dict[str, pd.DataFrame]:
    """
    Load all source CSVs once (shared across transforms).

    Returns:
        Dict with DataFrames: customers_csv, orders_csv, invoices_csv, legacy_csv, items_csv
    """
    logger.info("\n" + "=" * 70)
    logger.info("LOADING SOURCE CSVs (SHARED)")
    logger.info("=" * 70)
    logger.info("")

    from helpers import find_cleancloud_file

    shared_data = {}

    try:
        # CC Customers
        logger.info("  Loading CC customers...")
        customers_path = find_cleancloud_file('customer')
        shared_data['customers_csv'] = pd.read_csv(customers_path, encoding='utf-8', low_memory=False)
        logger.info(f"    [OK] {len(shared_data['customers_csv']):,} rows")

        # CC Orders
        logger.info("  Loading CC orders...")
        orders_path = find_cleancloud_file('orders')
        shared_data['orders_csv'] = pd.read_csv(orders_path, encoding='utf-8', low_memory=False)
        logger.info(f"    [OK] {len(shared_data['orders_csv']):,} rows")

        # Invoices
        logger.info("  Loading invoices...")
        invoices_path = find_cleancloud_file('invoice')
        shared_data['invoices_csv'] = pd.read_csv(invoices_path, encoding='utf-8', low_memory=False)
        logger.info(f"    [OK] {len(shared_data['invoices_csv']):,} rows")

        # CC Items
        logger.info("  Loading CC items...")
        items_path = find_cleancloud_file('item')
        shared_data['items_csv'] = pd.read_csv(items_path, encoding='utf-8', low_memory=False)
        logger.info(f"    [OK] {len(shared_data['items_csv']):,} rows")

        # Legacy orders
        logger.info("  Loading legacy orders...")
        legacy_path = LOCAL_STAGING_PATH / "RePos_Archive.csv"
        if legacy_path.exists():
            shared_data['legacy_csv'] = pd.read_csv(legacy_path, encoding='utf-8', low_memory=False)
            logger.info(f"    [OK] {len(shared_data['legacy_csv']):,} rows")
        else:
            logger.warning(f"    [WARN] Legacy file not found: {legacy_path}")
            shared_data['legacy_csv'] = pd.DataFrame()

        logger.info("")
        logger.info("  [DONE] All source CSVs loaded")
        return shared_data

    except Exception as e:
        logger.error(f"\n[ERROR] Failed to load source CSVs: {str(e)}")
        raise


# =====================================================================
# STEP 0: CHECK & UPDATE DIMPERIOD (INTEGRATED!)
# =====================================================================

def check_and_update_dimperiod() -> bool:
    """
    Check if DimPeriod needs updating and regenerate if needed
    Imports generate_dimperiod module (keeps it separate!)
    """
    
    logger.info("\n" + "=" * 70)
    logger.info("STEP 0: DIMPERIOD AUTO-UPDATE CHECK")
    logger.info("=" * 70)
    logger.info("")

    dimperiod_path = LOCAL_STAGING_PATH / "DimPeriod_Python.csv"

    try:
        # Import generate_dimperiod module (separate file!)
        import generate_dimperiod as dimperiod

        # Check if update needed
        needs_update, reason, current_max = dimperiod.check_dimperiod_needs_update(
            str(dimperiod_path),
            months_forward=3
        )

        if not needs_update:
            logger.info(f"[OK] DimPeriod is current (covers to {current_max})")
            logger.info(f"  No regeneration needed")
            return True

        # Needs update - regenerate
        logger.warning(f"[WARN] DimPeriod needs update: {reason}")
        logger.info(f"  Regenerating with 3-month lookahead...")
        
        start_time = time.time()
        
        # Call generator function
        df = dimperiod.generate_dimperiod(
            output_path=str(dimperiod_path),
            months_forward=3,
            start_year=2023,  # Start from 2023 (covers all operational data)
            verbose=False  # We'll print our own messages
        )
        
        elapsed = time.time() - start_time

        # Get date range info
        first_date = df['Date'].iloc[0]
        last_date = df['Date'].iloc[-1]
        row_count = len(df)

        logger.info(f"\n[OK] DimPeriod regenerated successfully in {elapsed:.1f}s")
        logger.info(f"  Date range: {first_date} to {last_date}")
        logger.info(f"  Total rows: {row_count:,}")
        logger.info(f"  File: {dimperiod_path.name}")
        
        return True
        
    except ImportError as e:
        logger.error(f"[ERROR]œ— Cannot import generate_dimperiod module: {str(e)}")
        logger.info(f"  Make sure generate_dimperiod.py is in: {PYTHON_SCRIPT_FOLDER}")
        logger.info(f"  Continuing with existing DimPeriod (if available)")
        return False
        
    except Exception as e:
        logger.error(f"[ERROR]œ— DimPeriod check FAILED: {str(e)}")
        logger.info(f"  Continuing with existing DimPeriod (if available)")
        return False


# =====================================================================
# RUN TRANSFORMS IN-PROCESS (NO SUBPROCESS)
# =====================================================================

def run_transform_inprocess(module_name: str, transform_name: str, description: str, shared_data: Dict) -> Tuple[bool, float]:
    """
    Run a transform in-process (direct import, no subprocess).

    Args:
        module_name: Python module name (e.g. 'transform_all_customers')
        transform_name: Key for storing result in shared_data
        description: Human-readable name
        shared_data: Dict with pre-loaded DataFrames

    Returns:
        (success: bool, elapsed: float)
    """
    logger.info(f"\n{'-' * 70}")
    logger.info(f"Running: {description}")
    logger.info(f"Module: {module_name}")
    logger.info(f"{'-' * 70}\n")

    start_time = time.time()

    try:
        # Import module dynamically
        module = __import__(module_name)

        # Call run() function
        df_result, output_path = module.run(shared_data)

        # Store result in shared_data for downstream transforms
        shared_data[transform_name] = df_result

        elapsed = time.time() - start_time
        logger.info(f"\n[DONE] {description} completed in {elapsed:.1f} seconds")

        return True, elapsed

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"\n[ERROR] {description} FAILED after {elapsed:.1f} seconds")
        logger.error(f"Error: {str(e)}")

        # Log full traceback
        import traceback
        logger.debug(traceback.format_exc())

        return False, elapsed


# =====================================================================
# INTER-STAGE VALIDATION
# =====================================================================

# Expected key columns per transform output
_KEY_COLUMNS = {
    'all_customers_df':    ['CustomerID_Std'],
    'all_sales_df':        ['CustomerID_Std', 'OrderID_Std'],
    'all_items_df':        ['CustomerID_Std', 'OrderID_Std'],
    'customer_quality_df': ['CustomerID_Std'],
}


def _validate_transform_output(transform_name: str, df: pd.DataFrame) -> None:
    """Validate a single transform's output for row counts and null key columns."""
    issues = []

    # Row count sanity check
    if len(df) == 0:
        issues.append("EMPTY: 0 rows produced")

    # Null key columns check
    for key_col in _KEY_COLUMNS.get(transform_name, []):
        if key_col in df.columns:
            null_count = df[key_col].isna().sum()
            if null_count > 0:
                pct = null_count / len(df) * 100
                issues.append(f"NULL KEYS: {null_count:,} ({pct:.1f}%) null values in {key_col}")

    if issues:
        logger.warning(f"  [VALIDATION] {transform_name}:")
        for issue in issues:
            logger.warning(f"    - {issue}")
    else:
        logger.info(f"  [VALIDATION] {transform_name}: OK ({len(df):,} rows, keys clean)")


def _validate_cross_transform(shared_data: Dict) -> None:
    """Cross-transform validation: orphan orders, customer coverage."""
    logger.info("\n" + "-" * 70)
    logger.info("CROSS-TRANSFORM VALIDATION")
    logger.info("-" * 70)

    sales_df = shared_data.get('all_sales_df')
    items_df = shared_data.get('all_items_df')
    customers_df = shared_data.get('all_customers_df')

    if sales_df is None or items_df is None:
        logger.warning("  [SKIP] Sales or items data not available for cross-validation")
        return

    # 1. Orphan order check: items with no matching sales order
    sales_orders = set(sales_df['OrderID_Std'].dropna().unique())
    items_orders = set(items_df['OrderID_Std'].dropna().unique())
    orphan_orders = items_orders - sales_orders
    orphan_count = len(orphan_orders)
    orphan_pct = orphan_count / len(items_orders) * 100 if items_orders else 0

    if orphan_count > 0:
        # Count affected item rows
        orphan_item_rows = items_df[items_df['OrderID_Std'].isin(orphan_orders)]
        logger.warning(
            f"  [WARN] Orphan orders: {orphan_count:,} orders in items with no sales match "
            f"({orphan_pct:.1f}% of item orders, {len(orphan_item_rows):,} item rows)"
        )
        logger.info(f"    Known issue: CleanCloud CSV export mismatch (not ETL bug)")
    else:
        logger.info(f"  [OK] No orphan orders (all item orders found in sales)")

    # 2. Customer coverage: sales customers missing from customers table
    if customers_df is not None:
        sales_customers = set(sales_df['CustomerID_Std'].dropna().unique())
        known_customers = set(customers_df['CustomerID_Std'].dropna().unique())
        missing_customers = sales_customers - known_customers
        if missing_customers:
            logger.warning(
                f"  [WARN] {len(missing_customers)} customer(s) in sales but not in customers table"
            )
        else:
            logger.info(f"  [OK] All sales customers found in customers table")

    logger.info("")


def run_all_transforms() -> bool:
    """Run all Python transformation scripts"""
    
    logger.info("\n" + "=" * 70)
    logger.info("CLEANCLOUD TO EXCEL WORKFLOW (WITH AUTO DIMPERIOD)")
    logger.info("=" * 70)
    logger.info("")
    logger.info("This will:")
    logger.info("  1. Check & update DimPeriod (auto 3-month lookahead)")
    logger.info("  2. Process CleanCloud files from Downloads folder")
    logger.info("  3. No renaming needed!")
    logger.info("")
    
    # Check folders exist
    if not PYTHON_SCRIPT_FOLDER.exists():
        logger.info(f"\n[ERROR] Python script folder not found: {PYTHON_SCRIPT_FOLDER}")
        return False
    
    if not DOWNLOADS_PATH.exists():
        logger.info(f"\n[ERROR] Downloads folder not found: {DOWNLOADS_PATH}")
        return False
    
    # Track results
    results = []
    total_start = time.time()
    
    # STEP 0: Check & update DimPeriod
    dimperiod_success = check_and_update_dimperiod()

    # STEP 1: Load source CSVs once
    try:
        shared_data = load_source_csvs()
    except Exception as e:
        logger.error(f"\n[ERROR] Failed to load source CSVs")
        return False

    # STEP 2: Run each transform (in-process, sharing data)
    for transform in TRANSFORMS:
        success, elapsed = run_transform_inprocess(
            transform['module_name'],
            transform['transform_name'],
            transform['description'],
            shared_data
        )

        results.append({
            'name': transform['description'],
            'success': success,
            'elapsed': elapsed
        })

        # If a transform fails, stop
        if not success:
            logger.error(f"\n[ERROR] Stopping due to failure in {transform['description']}")
            return False

        # Inter-stage validation: check output integrity
        df_result = shared_data.get(transform['transform_name'])
        if df_result is not None:
            _validate_transform_output(transform['transform_name'], df_result)

    # STEP 3: Cross-transform validation (orphan orders, key integrity)
    _validate_cross_transform(shared_data)

    total_elapsed = time.time() - total_start
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("TRANSFORMATION SUMMARY")
    logger.info("=" * 70)
    logger.info("")
    
    # DimPeriod status
    if dimperiod_success:
        logger.info(f"{'DimPeriod Check':<30} [[OK]] (auto-updated)")
    else:
        logger.info(f"{'DimPeriod Check':<30} [[WARN]] (check manually)")
    
    # Transform results
    for result in results:
        status = "[DONE]" if result['success'] else "[ERROR]"
        logger.info(f"{result['name']:<30} {status} ({result['elapsed']:>5.1f}s)")
    
    logger.info(f"{'-' * 70}")
    logger.info(f"{'TOTAL TIME':<30} {total_elapsed:>10.1f}s")
    logger.info("")
    
    logger.info("[DONE] All transformations successful!")
    return True


# =====================================================================
# MAIN WORKFLOW
# =====================================================================

def main() -> None:
    """Run the complete workflow"""
    
    logger.info("\n" + "=" * 70)
    logger.info("CLEANCLOUD TO EXCEL - OPTIMIZED WORKFLOW")
    logger.info("=" * 70)
    logger.info("")
    logger.info("FEATURES:")
    logger.info("  [OK] Auto DimPeriod update (3-month lookahead)")
    logger.info("  [OK] Finds CleanCloud files in Downloads folder")
    logger.info("  [OK] Processes data directly (no renaming!)")
    logger.info("  [OK] Output ready for Excel refresh")
    logger.info("")
    logger.info("Expected time: ~10-20 seconds")
    logger.info("")
    
    workflow_start = time.time()
    
    # Run transformations
    success = run_all_transforms()
    
    if not success:
        logger.info("\n" + "=" * 70)
        logger.info("[ERROR] WORKFLOW STOPPED")
        logger.info("=" * 70)
        logger.info("\nTransformation failed.")
        logger.info("\nMost common issues:")
        logger.info("  - Missing CleanCloud files in Downloads folder")
        logger.info("  - Missing generate_dimperiod.py in PythonScript folder")
        logger.info("  - Download all 4 reports from CleanCloud:")
        logger.info("    1. Orders")
        logger.info("    2. Items")
        logger.info("    3. Invoices")
        logger.info("    4. Customers")
        input("\nPress Enter to exit...")
        return
    
    # Success!
    workflow_elapsed = time.time() - workflow_start

    logger.info("\n" + "=" * 70)
    logger.info("[OK] WORKFLOW COMPLETE!")
    logger.info("=" * 70)
    logger.info("")
    logger.info(f"Total time: {workflow_elapsed:.1f} seconds")
    logger.info("")
    logger.info("Files created:")
    logger.info("  [OK] DimPeriod_Python.csv (auto-updated)")
    logger.info("  [OK] All_Customers_Python.csv")
    logger.info("  [OK] All_Sales_Python.csv")
    logger.info("  [OK] All_Items_Python.csv")
    logger.info("  [OK] Customer_Quality_Monthly_Python.csv")
    logger.info("")
    logger.info("NEXT STEPS:")
    logger.info("  1. Open your Excel PowerPivot workbook")
    logger.info("  2. Click 'Refresh All'")
    logger.info("  3. Your data is updated!")
    logger.info("")
    logger.info("NOTE: DimPeriod automatically extends 3 months forward")
    logger.info("      No manual updates needed!")
    logger.info("")
    logger.info("=" * 70)
    
    input("\nPress Enter to exit...")


# =====================================================================
# RUN
# =====================================================================

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.info("\n" + "=" * 70)
        logger.info("[ERROR] UNEXPECTED ERROR OCCURRED")
        logger.info("=" * 70)
        logger.info(f"\n{str(e)}")
        logger.info("")
        logger.info("Please check:")
        logger.info("  1. All folder paths are correct")
        logger.info("  2. Python scripts are in PythonScript folder")
        logger.info("  3. CleanCloud files are in Downloads folder")
        logger.info("")
        import traceback
        traceback.print_exc()
        input("\nPress Enter to exit...")
