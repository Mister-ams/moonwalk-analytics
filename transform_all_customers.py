"""
All_Customers Transformation Script - OPTIMIZED VERSION
Combines CC customers and Legacy customers into master customer table

OPTIMIZATIONS:
- Uses shared helpers (no local duplicates of find_cleancloud_file, standardize_*)
- Vectorized date/store/ID operations
- Callable run() function for single-process master script
"""

import pandas as pd
import numpy as np
import warnings
import os
from typing import Optional, Dict, Tuple
warnings.filterwarnings('ignore')

from helpers import (
    find_cleancloud_file, vectorized_to_date, vectorized_store_std,
    vectorized_customer_id_std, fx_pad_digits, format_dates_for_csv,
    DOWNLOADS_PATH, SALES_DATA_PATH
)

from logger_config import setup_logger
logger = setup_logger(__name__)


# =====================================================================
# MAIN TRANSFORMATION
# =====================================================================

def run(shared_data: Optional[Dict[str, pd.DataFrame]] = None) -> Tuple[pd.DataFrame, str]:
    """
    Run All_Customers transformation.

    Args:
        shared_data: dict with pre-loaded DataFrames:
            - 'customers_csv': CC customers DataFrame
            If None, loads from disk.
    
    Returns:
        (df_final, output_path) tuple
    """
    logger.info("=" * 70)
    logger.info("ALL_CUSTOMERS TRANSFORMATION - OPTIMIZED")
    logger.info("=" * 70)
    logger.info("")
    output_path = os.path.join(SALES_DATA_PATH, "All_Customers_Python.csv")
    legacy_path = os.path.join(SALES_DATA_PATH, "RePos_Archive.csv")

    # =====================================================================
    # PHASE 1: LOAD CC CUSTOMERS
    # =====================================================================
    logger.info("Phase 1: Loading CC customers...")

    if shared_data and 'customers_csv' in shared_data:
        df_cc = shared_data['customers_csv'].copy()
        logger.info(f"  [OK] Using pre-loaded {len(df_cc):,} CC customer rows")
    else:
        cc_path = find_cleancloud_file('customer')
        df_cc = pd.read_csv(cc_path, encoding='utf-8', low_memory=False)
        logger.info(f"  [OK] Loaded {len(df_cc):,} CC customer rows")

    # Keep only needed columns
    cc_wanted = ['Customer ID', 'Name', 'Store ID', 'Signed Up Date', 'Route #', 'Business ID']
    existing_cc = [col for col in cc_wanted if col in df_cc.columns]
    df_cc = df_cc[existing_cc].copy()

    # Vectorized: Source, Store, CustomerID, Dates
    df_cc['Source_System'] = 'CC_2025'

    df_cc['CustomerID_Std'] = vectorized_customer_id_std(
        df_cc['Customer ID'],
        pd.Series('CC_2025', index=df_cc.index)
    )

    df_cc['CustomerName'] = df_cc['Name'].where(
        df_cc['Name'].notna() & (df_cc['Name'].astype(str).str.strip() != ''),
        other=None
    )
    # Clean up whitespace on valid names
    valid_names = df_cc['CustomerName'].notna()
    df_cc.loc[valid_names, 'CustomerName'] = df_cc.loc[valid_names, 'CustomerName'].astype(str).str.strip()

    df_cc['Store_Std'] = vectorized_store_std(df_cc['Store ID'])

    df_cc['SignedUp_Date'] = vectorized_to_date(df_cc.get('Signed Up Date', pd.Series(dtype='object')))

    df_cc['CohortMonth'] = df_cc['SignedUp_Date'].dt.to_period('M').dt.to_timestamp()
    df_cc.loc[df_cc['SignedUp_Date'].isna(), 'CohortMonth'] = pd.NaT

    # Route #
    if 'Route #' in df_cc.columns:
        df_cc['Route #'] = pd.to_numeric(df_cc['Route #'], errors='coerce').fillna(0).astype(int)
    else:
        df_cc['Route #'] = 0

    # IsBusinessAccount
    if 'Business ID' in df_cc.columns:
        df_cc['IsBusinessAccount'] = (pd.to_numeric(df_cc['Business ID'], errors='coerce').fillna(0) > 0).astype(int)
    else:
        df_cc['IsBusinessAccount'] = 0

    final_cols = [
        'CustomerID_Std', 'CustomerName', 'Store_Std', 'SignedUp_Date',
        'CohortMonth', 'Route #', 'IsBusinessAccount', 'Source_System'
    ]
    df_cc_clean = df_cc[final_cols].copy()

    logger.info(f"  [OK] Processed {len(df_cc_clean):,} CC customers")
    logger.info(f"  [OK] Business accounts: {df_cc_clean['IsBusinessAccount'].sum():,}")

    # =====================================================================
    # PHASE 2: LOAD LEGACY CUSTOMERS
    # =====================================================================
    logger.info("\nPhase 2: Loading Legacy customers...")

    if shared_data and 'legacy_csv' in shared_data:
        df_legacy = shared_data['legacy_csv'].copy()
    else:
        df_legacy = pd.read_csv(legacy_path, encoding='utf-8', low_memory=False)

    initial_legacy_count = len(df_legacy)
    logger.info(f"  [OK] Loaded {initial_legacy_count:,} legacy order rows")

    legacy_wanted = ['Customer ID', 'Customer', 'Placed']
    existing_legacy = [col for col in legacy_wanted if col in df_legacy.columns]
    df_legacy = df_legacy[existing_legacy].copy()

    # Vectorized date parsing
    if 'Placed' in df_legacy.columns:
        df_legacy['Placed'] = vectorized_to_date(df_legacy['Placed'])

    # Vectorized CustomerID_Std
    df_legacy['CustomerID_Std'] = vectorized_customer_id_std(
        df_legacy['Customer ID'],
        pd.Series('Legacy', index=df_legacy.index)
    )

    # Group by CustomerID_Std
    legacy_grouped = df_legacy.groupby('CustomerID_Std').agg(
        CustomerName=('Customer', lambda x: x.dropna().iloc[0] if len(x.dropna()) > 0 else None),
        SignedUp_Date=('Placed', 'min') if 'Placed' in df_legacy.columns else ('Customer ID', lambda x: None)
    ).reset_index()

    # CohortMonth
    legacy_grouped['CohortMonth'] = legacy_grouped['SignedUp_Date'].dt.to_period('M').dt.to_timestamp()
    legacy_grouped.loc[legacy_grouped['SignedUp_Date'].isna(), 'CohortMonth'] = pd.NaT

    legacy_grouped['Store_Std'] = 'Moon Walk'
    legacy_grouped['Route #'] = 0
    legacy_grouped['IsBusinessAccount'] = 0
    legacy_grouped['Source_System'] = 'Legacy'

    df_legacy_clean = legacy_grouped[final_cols].copy()
    logger.info(f"  [OK] Processed {len(df_legacy_clean):,} unique Legacy customers")

    # =====================================================================
    # PHASE 3: COMBINE & OUTPUT
    # =====================================================================
    logger.info("\nPhase 3: Combining CC and Legacy customers...")

    df_all = pd.concat([df_cc_clean, df_legacy_clean], ignore_index=True)
    logger.info(f"  [OK] Combined: {len(df_all):,} total customers")

    # Format dates as DD-Mon-YYYY for PowerQuery (unambiguous, no locale issues)
    df_all = format_dates_for_csv(df_all, ['SignedUp_Date', 'CohortMonth'])

    # Sort
    df_all = df_all.sort_values('CustomerID_Std').reset_index(drop=True)

    # Save
    df_all.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"  [OK] Saved to: {output_path}")

    # =====================================================================
    # VALIDATION SUMMARY
    # =====================================================================
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)

    logger.info(f"\nCustomer Counts:")
    logger.info(f"  CC Customers:                {(df_all['Source_System'] == 'CC_2025').sum():>8,}")
    logger.info(f"  Legacy Customers:            {(df_all['Source_System'] == 'Legacy').sum():>8,}")
    logger.info(f"  {'-' * 40}")
    logger.info(f"  TOTAL:                       {len(df_all):>8,}")

    logger.info(f"\nStore Distribution:")
    for store in df_all['Store_Std'].unique():
        count = (df_all['Store_Std'] == store).sum()
        pct = count / len(df_all) * 100
        logger.info(f"  {store:<20}: {count:>6,} ({pct:>5.1f}%)")

    business_count = df_all['IsBusinessAccount'].sum()
    logger.info(f"\nBusiness Accounts:           {business_count:>8,} ({business_count/len(df_all)*100:>5.1f}%)")

    null_names = df_all['CustomerName'].isna().sum()
    null_cohort = (df_all['CohortMonth'] == '').sum()
    logger.info(f"  Null names:                  {null_names:>8,}")
    logger.info(f"  Null cohorts:                {null_cohort:>8,}")

    logger.info("\n" + "=" * 70)
    logger.info("[DONE] ALL_CUSTOMERS TRANSFORMATION COMPLETE!")
    logger.info("=" * 70)
    logger.info("")
    return df_all, output_path


# =====================================================================
# STANDALONE EXECUTION
# =====================================================================

if __name__ == "__main__":
    run()
