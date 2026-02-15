"""
All_Items Transformation Script - OPTIMIZED VERSION
Converts CC Items CSV with categorization and B2B filtering

OPTIMIZATIONS:
- Uses shared helpers (no local find_cleancloud_file)
- Vectorized operations
- Callable run() function for single-process master script
- Uses Placed date only (no Orders CSV merge needed)
"""

import pandas as pd
import numpy as np
import warnings
import os
from typing import Optional, Dict, Tuple
warnings.filterwarnings('ignore')

from helpers import (
    find_cleancloud_file, vectorized_to_date, vectorized_store_std,
    vectorized_customer_id_std, vectorized_order_id_std,
    vectorized_item_category, vectorized_service_type,
    format_dates_for_csv,
    DOWNLOADS_PATH,
)
from config import LOCAL_STAGING_PATH

from logger_config import setup_logger
logger = setup_logger(__name__)


# =====================================================================
# MAIN TRANSFORMATION
# =====================================================================

def run(shared_data: Optional[Dict[str, pd.DataFrame]] = None) -> Tuple[pd.DataFrame, str]:
    """
    Run All_Items transformation.

    Args:
        shared_data: dict with pre-loaded DataFrames:
            - 'items_csv': CC Items DataFrame
            - 'customers_csv': CC Customers DataFrame (for business account list)
            If None, loads from disk.
    
    Returns:
        (df_final, output_path) tuple
    """
    logger.info("=" * 70)
    logger.info("ALL_ITEMS TRANSFORMATION - OPTIMIZED")
    logger.info("=" * 70)
    logger.info("")
    output_path = os.path.join(LOCAL_STAGING_PATH, "All_Items_Python.csv")

    # =====================================================================
    # PHASE 1: LOAD BUSINESS ACCOUNTS
    # =====================================================================
    logger.info("Phase 1: Loading business accounts...")

    if shared_data and 'customers_csv' in shared_data:
        df_customers = shared_data['customers_csv'].copy()
        logger.info(f"  [OK] Using pre-loaded {len(df_customers):,} customers")
    else:
        customers_path = find_cleancloud_file('customer')
        df_customers = pd.read_csv(customers_path, encoding='utf-8')
        logger.info(f"  [OK] Loaded {len(df_customers):,} customers")

    # Get business account list
    business_accounts = df_customers[df_customers['Business ID'].notna()]['Customer ID'].tolist()
    business_account_list = []

    for cust_id in business_accounts:
        std_id_series = vectorized_customer_id_std(
            pd.Series([cust_id]),
            pd.Series(['CC_2025'])
        )
        std_id = std_id_series.iloc[0] if len(std_id_series) > 0 and pd.notna(std_id_series.iloc[0]) else None
        if std_id:
            business_account_list.append(std_id)

    logger.info(f"  [OK] Identified {len(business_account_list)} business accounts")

    # =====================================================================
    # PHASE 2: LOAD ITEMS CSV
    # =====================================================================
    logger.info("\nPhase 2: Loading items...")

    if shared_data and 'items_csv' in shared_data:
        df = shared_data['items_csv'].copy()
        logger.info(f"  [OK] Using pre-loaded {len(df):,} item rows")
    else:
        items_path = find_cleancloud_file('item')
        df = pd.read_csv(items_path, encoding='utf-8', low_memory=False)
        logger.info(f"  [OK] Loaded {len(df):,} item rows")

    initial_count = len(df)

    # =====================================================================
    # PHASE 2C: CREATE ITEMDATE AND ITEMCOHORTMONTH (PLACED DATE ONLY)
    # =====================================================================
    logger.info("\nPhase 2c: Creating ItemDate and ItemCohortMonth (using Placed date only)...")

    # Parse Placed date from Items CSV
    df['ItemDate'] = vectorized_to_date(df['Placed'])

    # Create ItemCohortMonth (MONTHLY granularity) from ItemDate
    df['ItemCohortMonth'] = df['ItemDate'].dt.to_period('M').dt.to_timestamp()
    df.loc[df['ItemDate'].isna(), 'ItemCohortMonth'] = pd.NaT

    # Count valid dates
    valid_dates = df['ItemDate'].notna().sum()

    logger.info(f"  [OK] ItemDate and ItemCohortMonth created:")
    logger.info(f"    - Valid dates:  {valid_dates:,} items ({valid_dates/len(df)*100:.1f}%)")
    logger.info(f"  [INFO] ItemDate = Placed date (when order was placed)")
    logger.info(f"  [INFO] ItemCohortMonth = month start (for monthly analysis)")
    logger.info(f"  [INFO] This matches CleanCloud source monthly distribution")

    # Drop intermediate Placed column
    df = df.drop(columns=['Placed'], errors='ignore')

    # =====================================================================
    # PHASE 3: REMOVE UNNECESSARY COLUMNS EARLY
    # =====================================================================
    logger.info("\nPhase 3: Removing unnecessary columns...")

    columns_to_remove = [
        'Pieces per Product', 'Total Pcs', 'Item Notes',
        'Email', 'Phone', 'Address',
        'Paid', 'Payment Type', 'Order Status',
        'Retail', 'Price Mod', 'Cost Price', 'Price per Item'
    ]

    existing_to_remove = [col for col in columns_to_remove if col in df.columns]
    df = df.drop(columns=existing_to_remove)

    logger.info(f"  [OK] Removed {len(existing_to_remove)} unnecessary columns")

    # =====================================================================
    # PHASE 4: ADD SOURCE
    # =====================================================================
    df['Source'] = 'CC_2025'

    # =====================================================================
    # PHASE 5: VECTORIZED STANDARDIZATION
    # =====================================================================
    logger.info("\nPhase 5: Standardizing stores and IDs...")

    df['Store_Std'] = vectorized_store_std(df['Store ID'])

    # Filter for known stores only
    df = df[df['Store_Std'].notna()].copy()
    logger.info(f"  [OK] After store filter: {len(df):,} rows ({initial_count - len(df):,} removed)")

    df['CustomerID_Std'] = vectorized_customer_id_std(
        df['Customer ID'],
        pd.Series('CC_2025', index=df.index)
    )

    df['OrderID_Std'] = vectorized_order_id_std(
        df['Order ID'],
        df['Store_Std'],
        pd.Series('CC_2025', index=df.index),
        pd.Series('Order', index=df.index),  # All items are from regular orders
        pd.Series(range(1, len(df) + 1), index=df.index)  # Sequential index (not used for orders)
    )

    logger.info(f"  [OK] Standardized Customer and Order IDs")

    # =====================================================================
    # PHASE 6: TEXT STANDARDIZATION FOR CATEGORIZATION
    # =====================================================================
    logger.info("\nPhase 6: Standardizing text for categorization...")

    df['Item_Std'] = df['Item'].fillna('').astype(str).str.lower().str.replace(r"[\s\-&']", '', regex=True)
    df['Section_Std'] = df['Section'].fillna('').astype(str).str.lower().str.replace(r"[\s\-&']", '', regex=True)

    logger.info(f"  [OK] Standardized item and section text")

    # =====================================================================
    # PHASE 7: ITEM CATEGORIZATION
    # =====================================================================
    logger.info("\nPhase 7: Categorizing items...")

    df['Item_Category'] = vectorized_item_category(df['Item'], df['Section'])

    # Show category distribution
    category_counts = df['Item_Category'].value_counts()
    logger.info(f"  [OK] Item categories:")
    for category, count in category_counts.items():
        logger.info(f"    - {category}: {count:,}")

    # =====================================================================
    # PHASE 8: SERVICE TYPE CATEGORIZATION
    # =====================================================================
    logger.info("\nPhase 8: Categorizing service types...")

    df['Service_Type'] = vectorized_service_type(df['Section'])

    # Show service type distribution
    service_counts = df['Service_Type'].value_counts()
    logger.info(f"  [OK] Service types:")
    for service, count in service_counts.items():
        logger.info(f"    - {service}: {count:,}")

    # =====================================================================
    # PHASE 9: ADD BUSINESS ACCOUNT FLAG (DO NOT EXCLUDE!)
    # =====================================================================
    logger.info("\nPhase 9: Flagging business accounts...")

    df['IsBusinessAccount'] = df['CustomerID_Std'].isin(business_account_list).astype(int)

    b2b_count = (df['IsBusinessAccount'] == 1).sum()
    b2c_count = (df['IsBusinessAccount'] == 0).sum()

    logger.info(f"  [OK] Business account items: {b2b_count:,} ({b2b_count/len(df)*100:.1f}%)")
    logger.info(f"  [OK] Consumer items: {b2c_count:,} ({b2c_count/len(df)*100:.1f}%)")
    logger.info(f"  [INFO] All items included for operational load tracking")

    # =====================================================================
    # PHASE 10: FINAL CLEANUP
    # =====================================================================
    logger.info("\nPhase 10: Final cleanup...")

    columns_to_remove_final = [
        'Store ID', 'Item ID', 'Section ID', 'Order ID',
        'Product ID', 'Custom Product ID', 'Customer ID', 'Custom ID',
        'Customer', 'Item_Std', 'Section_Std'
    ]

    existing_to_remove_final = [col for col in columns_to_remove_final if col in df.columns]
    df = df.drop(columns=existing_to_remove_final)

    logger.info(f"  [OK] Removed {len(existing_to_remove_final)} intermediate columns")

    # =====================================================================
    # PHASE 11: SET DATA TYPES
    # =====================================================================
    logger.info("\nPhase 11: Setting data types...")

    dtype_map = {
        'Total': 'float64',
        'Quantity': 'int64',
        'Express': 'int64',
        'IsBusinessAccount': 'int64',
        'Item_Category': 'str',
        'Service_Type': 'str',
        'Store_Std': 'str',
        'CustomerID_Std': 'str',
        'OrderID_Std': 'str',
        'Item': 'str',
        'Section': 'str',
        'Source': 'str'
    }

    for col, dtype in dtype_map.items():
        if col in df.columns:
            if dtype == 'str':
                df[col] = df[col].astype(str)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if dtype == 'int64':
                    df[col] = df[col].fillna(0).astype('int64')
                else:
                    df[col] = df[col].astype(dtype)

    logger.info(f"  [OK] Set data types")

    # =====================================================================
    # PHASE 12: FINAL OUTPUT
    # =====================================================================
    logger.info("\nPhase 12: Preparing final output...")

    final_columns = [
        'Source',
        'Store_Std',
        'CustomerID_Std',
        'OrderID_Std',
        'ItemDate',
        'ItemCohortMonth',
        'Item',
        'Section',
        'Quantity',
        'Total',
        'Express',
        'Item_Category',
        'Service_Type',
        'IsBusinessAccount'
    ]

    available_columns = [col for col in final_columns if col in df.columns]
    df_final = df[available_columns].copy()

    # Format dates as DD-Mon-YYYY for PowerQuery (unambiguous, no locale issues)
    df_final = format_dates_for_csv(df_final, ['ItemDate', 'ItemCohortMonth'])

    # Sort for consistency
    df_final = df_final.sort_values(['Store_Std', 'OrderID_Std', 'Item'])
    df_final = df_final.reset_index(drop=True)

    logger.info(f"  [OK] Final output: {len(df_final):,} rows × {len(df_final.columns)} columns")

    # =====================================================================
    # PHASE 13: SAVE OUTPUT
    # =====================================================================
    logger.info("\nPhase 13: Saving output...")

    df_final.to_csv(output_path, index=False, encoding='utf-8')

    logger.info(f"  [OK] Saved to: {output_path}")

    # =====================================================================
    # VALIDATION SUMMARY
    # =====================================================================
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)

    logger.info(f"\nRow Count:")
    logger.info(f"  Initial:              {initial_count:>8,}")
    logger.info(f"  Final (ALL items):    {len(df_final):>8,}")

    logger.info(f"\nCustomer Types:")
    b2b_items = (df_final['IsBusinessAccount'] == 1).sum()
    b2c_items = (df_final['IsBusinessAccount'] == 0).sum()
    logger.info(f"  B2B Items:            {b2b_items:>8,} ({b2b_items/len(df_final)*100:>5.1f}%)")
    logger.info(f"  B2C Items:            {b2c_items:>8,} ({b2c_items/len(df_final)*100:>5.1f}%)")

    logger.info(f"\nKey Metrics:")
    logger.info(f"  Unique Orders:        {df_final['OrderID_Std'].nunique():>8,}")
    logger.info(f"  Unique Customers:     {df_final['CustomerID_Std'].nunique():>8,}")
    logger.info(f"  Total Items:          {df_final['Quantity'].sum():>8,}")
    logger.info(f"  Total Revenue:        ${df_final['Total'].sum():>11,.2f}")

    logger.info(f"\nItem Categories:")
    for category, count in df_final['Item_Category'].value_counts().items():
        pct = count / len(df_final) * 100
        logger.info(f"  {category:<20}: {count:>6,} ({pct:>5.1f}%)")

    logger.info(f"\nService Types:")
    for service, count in df_final['Service_Type'].value_counts().items():
        pct = count / len(df_final) * 100
        logger.info(f"  {service:<20}: {count:>6,} ({pct:>5.1f}%)")

    logger.info("\n" + "=" * 70)
    logger.info("[DONE] ALL_ITEMS TRANSFORMATION COMPLETE!")
    logger.info("=" * 70)
    logger.info("")
    return df_final, output_path


# =====================================================================
# STANDALONE EXECUTION
# =====================================================================

if __name__ == "__main__":
    run()
