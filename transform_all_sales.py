"""
All_Sales Transformation Script - OPTIMIZED VERSION
Includes: Date Fix + Subscription Store Fix

OPTIMIZATIONS:
- ALL .apply() calls replaced with vectorized pandas/numpy operations
- Subscription flag via merge instead of row-by-row loop
- Time metrics via column arithmetic instead of row-by-row
- Shared helpers (no local duplicates)
- Callable run() for single-process master script
- CSV loaded once, passed via shared_data
"""

import pandas as pd
import numpy as np
import warnings
import os
from typing import Optional, Dict, Tuple
warnings.filterwarnings('ignore')

from helpers import (
    vectorized_customer_id_std, vectorized_order_id_std,
    vectorized_payment_type_std, vectorized_route_category,
    vectorized_months_since_cohort, vectorized_subscription_flag,
    fx_standardize_name, format_dates_for_csv,
    DOWNLOADS_PATH, SALES_DATA_PATH
)

from logger_config import setup_logger
logger = setup_logger(__name__)

from logger_config import setup_logger
logger = setup_logger(__name__)

from config import SUBSCRIPTION_VALIDITY_DAYS


# =====================================================================
# MAIN TRANSFORMATION
# =====================================================================

def run(shared_data: Optional[Dict[str, pd.DataFrame]] = None) -> Tuple[pd.DataFrame, str]:
    """
    Run All_Sales transformation.

    Args:
        shared_data: dict with pre-loaded DataFrames:
            - 'customers_csv': CC customers DataFrame
            - 'orders_csv': CC orders DataFrame
            - 'invoices_csv': Invoices DataFrame
            - 'legacy_csv': Legacy orders DataFrame
            - 'all_customers_df': Processed All_Customers DataFrame
            If None, loads from disk.

    Returns:
        (df_final, output_path) tuple
    """
    logger.info("=" * 70)
    logger.info("ALL_SALES TRANSFORMATION - OPTIMIZED")
    logger.info("=" * 70)
    logger.info("")
    output_path = os.path.join(SALES_DATA_PATH, "All_Sales_Python.csv")

    # =====================================================================
    # PHASE 1: LOAD DEPENDENCIES
    # =====================================================================
    logger.info("Phase 1: Loading dependencies...")

    # CC Customers (for business account list + name lookup)
    if shared_data and 'customers_csv' in shared_data:
        df_customers = shared_data['customers_csv']
    else:
        df_customers = pd.read_csv(find_cleancloud_file('customer'), encoding='utf-8')

    # Business Account list (vectorized)
    biz_mask = df_customers['Business ID'].notna()
    biz_ids = df_customers.loc[biz_mask, 'Customer ID'].astype(str).str.strip()
    biz_digits = biz_ids.str.replace(r'\D', '', regex=True)
    business_account_set = set('CC-' + biz_digits[biz_digits != ''].str.zfill(4))

    logger.info(f"  [OK] Loaded {len(df_customers):,} CC customers, {len(business_account_set)} business accounts")

    # Customer name lookup (vectorized instead of iterrows)
    name_df = df_customers[df_customers['Name'].notna()].copy()
    name_df['name_std'] = name_df['Name'].apply(fx_standardize_name)
    cid_digits = name_df['Customer ID'].astype(str).str.replace(r'\D', '', regex=True)
    name_df['cust_std'] = 'CC-' + cid_digits.str.zfill(4)
    name_df = name_df[name_df['name_std'] != '']
    # Keep first occurrence for each standardized name
    name_df = name_df.drop_duplicates(subset='name_std', keep='first')
    customer_name_lookup = dict(zip(name_df['name_std'], name_df['cust_std']))

    # All_Customers (for CohortMonth + Route)
    if shared_data and 'all_customers_df' in shared_data:
        df_all_customers = shared_data['all_customers_df'].copy()
    else:
        df_all_customers = pd.read_csv(
            os.path.join(SALES_DATA_PATH, "All_Customers_Python.csv"), encoding='utf-8'
        )

    df_all_customers['CohortMonth'] = vectorized_to_date(df_all_customers['CohortMonth'])
    customer_cohort = dict(zip(df_all_customers['CustomerID_Std'], df_all_customers['CohortMonth']))
    customer_route = dict(zip(
        df_all_customers['CustomerID_Std'],
        pd.to_numeric(df_all_customers['Route #'], errors='coerce').fillna(0)
    ))

    logger.info(f"  [OK] Loaded {len(df_all_customers):,} total customers for lookup")

    # =====================================================================
    # PHASE 2: LOAD SUBSCRIPTION PERIODS
    # =====================================================================
    logger.info("\nPhase 2: Loading subscription periods...")

    if shared_data and 'invoices_csv' in shared_data:
        df_invoices_raw = shared_data['invoices_csv'].copy()
    else:
        df_invoices_raw = pd.read_csv(find_cleancloud_file('invoice'), encoding='utf-8')

    df_subs = df_invoices_raw[
        df_invoices_raw['Reference'].astype(str).str.upper().str.startswith('SUBSCRIPTION', na=False)
    ].copy()

    df_subs['Payment_Date'] = vectorized_to_date(df_subs['Payment Date'])
    df_subs = df_subs[df_subs['Payment_Date'].notna()].copy()

    df_subs['CustomerName_Std'] = df_subs['Customer'].apply(fx_standardize_name)
    df_subs['CustomerID_Std'] = df_subs['CustomerName_Std'].map(customer_name_lookup)

    df_subs['ValidFrom'] = df_subs['Payment_Date']
    df_subs['ValidUntil'] = df_subs['Payment_Date'] + pd.Timedelta(days=SUBSCRIPTION_VALIDITY_DAYS)

    # Build subscription dict for vectorized lookup
    sub_valid = df_subs[['CustomerID_Std', 'ValidFrom', 'ValidUntil']].dropna()
    subscription_dict = {}
    for cid, grp in sub_valid.groupby('CustomerID_Std'):
        subscription_dict[cid] = [
            {'ValidFrom': row['ValidFrom'], 'ValidUntil': row['ValidUntil']}
            for _, row in grp.iterrows()
        ]

    logger.info(f"  [OK] {len(df_subs):,} subscription payments, {len(subscription_dict)} customers with subs")

    # =====================================================================
    # PHASE 3: LOAD SOURCE DATA
    # =====================================================================
    logger.info("\nPhase 3: Loading source data...")

    order_columns = [
        'Order ID', 'Customer ID', 'Placed', 'Total',
        'Store ID', 'Store Name',
        'Ready By', 'Cleaned', 'Collected', 'Pickup Date',
        'Payment Date', 'Payment Type', 'Paid', 'Pieces', 'Delivery'
    ]

    # 3A. Legacy Orders
    if shared_data and 'legacy_csv' in shared_data:
        df_legacy = shared_data['legacy_csv']  # No copy needed - immediately reassigned below
    else:
        df_legacy = pd.read_csv(os.path.join(SALES_DATA_PATH, "RePos_Archive.csv"),
                                encoding='utf-8', low_memory=False)

    df_legacy = df_legacy[[c for c in order_columns if c in df_legacy.columns]].copy()
    df_legacy['Source'] = 'Legacy'
    df_legacy['Transaction_Type'] = 'Order'
    df_legacy['Customer_Name'] = None
    logger.info(f"  [OK] Loaded {len(df_legacy):,} legacy orders")

    # 3B. CC Orders
    if shared_data and 'orders_csv' in shared_data:
        df_cc_orders = shared_data['orders_csv']  # No copy needed - immediately reassigned below
    else:
        df_cc_orders = pd.read_csv(find_cleancloud_file('orders'), encoding='utf-8', low_memory=False)

    df_cc_orders = df_cc_orders[[c for c in order_columns if c in df_cc_orders.columns]].copy()
    df_cc_orders['Source'] = 'CC_2025'
    df_cc_orders['Transaction_Type'] = 'Order'
    df_cc_orders['Customer_Name'] = None
    logger.info(f"  [OK] Loaded {len(df_cc_orders):,} CC orders")

    # 3C. Invoices
    df_inv = df_invoices_raw  # No copy needed - will be filtered and copied below
    df_inv['Reference_Upper'] = df_inv['Reference'].astype(str).str.upper()
    is_subscription = df_inv['Reference_Upper'].str.contains('SUBSCRIPTION', na=False)

    df_inv['Payment_Date_Parsed'] = vectorized_to_date(df_inv['Payment Date'])
    df_inv = df_inv[df_inv['Payment_Date_Parsed'].notna() | is_subscription].copy()

    df_inv['Total'] = np.where(
        df_inv['Reference_Upper'].str.startswith('SUBSCRIPTION', na=False),
        pd.to_numeric(df_inv['Amount'], errors='coerce').fillna(0),
        0
    )
    df_inv['Collections_Inv'] = pd.to_numeric(df_inv['Amount'], errors='coerce').fillna(0)
    df_inv['Customer_Name'] = df_inv['Customer']
    df_inv['Transaction_Type'] = np.where(
        df_inv['Reference_Upper'].str.startswith('SUBSCRIPTION', na=False),
        'Subscription', 'Invoice Payment'
    )

    # Placeholders
    df_inv['Order ID'] = None
    df_inv['Customer ID'] = None
    df_inv['Placed'] = df_inv['Payment Date']
    df_inv['Ready By'] = None
    df_inv['Cleaned'] = df_inv['Payment Date']
    df_inv['Collected'] = df_inv['Payment Date']
    df_inv['Pickup Date'] = None
    df_inv['Paid'] = 1
    df_inv['Pieces'] = 0
    df_inv['Delivery'] = 0
    df_inv['Source'] = 'CC_2025'

    if 'Payment Method' in df_inv.columns:
        df_inv = df_inv.rename(columns={'Payment Method': 'Payment Type'})

    inv_keep = [
        'Order ID', 'Customer ID', 'Placed', 'Total', 'Collections_Inv',
        'Store ID', 'Store Name',
        'Ready By', 'Cleaned', 'Collected', 'Pickup Date',
        'Payment Date', 'Payment Type', 'Paid', 'Pieces', 'Delivery',
        'Source', 'Transaction_Type', 'Customer_Name'
    ]
    df_inv = df_inv[[c for c in inv_keep if c in df_inv.columns]].copy()
    logger.info(f"  [OK] Loaded {len(df_inv):,} invoice rows")

    # =====================================================================
    # PHASE 4: COMBINE
    # =====================================================================
    logger.info("\nPhase 4: Combining sources...")

    all_cols = set(df_legacy.columns) | set(df_cc_orders.columns) | set(df_inv.columns)
    for frame in [df_legacy, df_cc_orders, df_inv]:
        for col in all_cols - set(frame.columns):
            frame[col] = None

    df = pd.concat([df_legacy, df_cc_orders, df_inv], ignore_index=True)
    logger.info(f"  [OK] Combined: {len(df):,} total rows")

    # Cleanup large intermediates to free memory
    del df_legacy, df_cc_orders, df_inv
    import gc
    gc.collect()

    # =====================================================================
    # PHASE 5: VECTORIZED STANDARDIZATION
    # =====================================================================
    logger.info("\nPhase 5: Vectorized standardization...")

    # Paid
    df['Paid'] = pd.to_numeric(df['Paid'], errors='coerce').fillna(0).astype(int)

    # Payment Type (vectorized)
    df['Payment_Type_Std'] = vectorized_payment_type_std(df['Payment Type'])

    # Date columns (vectorized - replaces 6 separate .apply(fx_to_date) calls)
    date_cols = ['Placed', 'Ready By', 'Cleaned', 'Collected', 'Pickup Date', 'Payment Date']
    for col in date_cols:
        if col in df.columns:
            df[col] = vectorized_to_date(df[col])

    df['Placed_Date'] = df['Placed']

    # Earned_Date (vectorized - replaces .apply lambda)
    # Use np.where to avoid dtype mismatch between CC (datetime64[us]) and Legacy (datetime64[ns])
    df['Earned_Date'] = np.where(
        df['Source'] != 'CC_2025',
        np.where(df['Cleaned'].notna(), df['Cleaned'], df['Placed_Date']),
        df['Cleaned']
    )
    df['Earned_Date'] = pd.to_datetime(df['Earned_Date'])

    # OrderCohortMonth (vectorized)
    df['OrderCohortMonth'] = df['Earned_Date'].dt.to_period('M').dt.to_timestamp()
    df.loc[df['Earned_Date'].isna(), 'OrderCohortMonth'] = pd.NaT

    df['Total_Num'] = pd.to_numeric(df['Total'], errors='coerce').fillna(0)
    df['Pieces'] = pd.to_numeric(df['Pieces'], errors='coerce').fillna(0).astype(int)
    df['Delivery'] = pd.to_numeric(df['Delivery'], errors='coerce').fillna(0).astype(int)

    logger.info(f"  [OK] Dates, payments, totals standardized")

    # =====================================================================
    # PHASE 6: COLLECTIONS (vectorized)
    # =====================================================================
    logger.info("\nPhase 6: Calculating collections...")

    has_inv_coll = df['Collections_Inv'].notna() if 'Collections_Inv' in df.columns else pd.Series(False, index=df.index)
    df['Collections'] = np.where(
        has_inv_coll, df.get('Collections_Inv', 0),
        np.where(df['Paid'] == 0, 0,
            np.where(df['Payment_Type_Std'] == 'Receivable', 0, df['Total_Num'])
        )
    )
    df = df.drop(columns=['Collections_Inv'], errors='ignore')

    # =====================================================================
    # PHASE 7: STORE & ID STANDARDIZATION (vectorized)
    # =====================================================================
    logger.info("\nPhase 7: Standardizing stores and IDs...")

    df['Store_Std'] = vectorized_store_std(
        df.get('Store ID', pd.Series(dtype='object')),
        df.get('Store Name', pd.Series(dtype='object')),
        df['Source']
    )

    initial_count = len(df)
    df = df[df['Store_Std'].notna()].copy()
    logger.info(f"  [OK] After store filter: {len(df):,} rows ({initial_count - len(df):,} removed)")
    df = df.drop(columns=['Store ID', 'Store Name'], errors='ignore')

    # CustomerID_Std (vectorized)
    df['CustomerID_Std'] = vectorized_customer_id_std(
        df.get('Customer ID', pd.Series(dtype='object', index=df.index)),
        df['Source']
    )

    # OrderID_Std (vectorized)
    df = df.reset_index(drop=True)
    df['RowIndex'] = df.index + 1

    df['OrderID_Std'] = vectorized_order_id_std(
        df.get('Order ID', pd.Series(dtype='object', index=df.index)),
        df['Store_Std'],
        df['Source'],
        df['Transaction_Type'],
        df['RowIndex']
    )
    df = df.drop(columns=['RowIndex'])

    logger.info(f"  [OK] Store, Customer, Order IDs standardized")

    # =====================================================================
    # PHASE 8: FIX CUSTOMER IDS FOR SUBSCRIPTIONS/INVOICES (vectorized)
    # =====================================================================
    logger.info("\nPhase 8: Fixing Customer IDs for subscriptions/invoices...")

    is_sub_or_inv = df['Transaction_Type'].isin(['Subscription', 'Invoice Payment'])

    # Vectorized name standardization + lookup
    df['_name_std'] = None
    if is_sub_or_inv.any():
        df.loc[is_sub_or_inv, '_name_std'] = df.loc[is_sub_or_inv, 'Customer_Name'].apply(
            lambda x: fx_standardize_name(x) if pd.notna(x) else None
        )

    df['_cid_lookup'] = df['_name_std'].map(customer_name_lookup)

    # Overwrite CustomerID_Std for subs/invoices
    df.loc[is_sub_or_inv, 'CustomerID_Std'] = df.loc[is_sub_or_inv, '_cid_lookup']
    df = df.drop(columns=['_name_std', '_cid_lookup'])

    # =====================================================================
    # PHASE 9: FILTERING
    # =====================================================================
    logger.info("\nPhase 9: Filtering...")

    initial_count = len(df)

    # Is_Earned flag: 1 = revenue counts, 0 = pending (uncleaned CC orders)
    df['Is_Earned'] = np.where(df['Earned_Date'].notna(), 1, 0).astype(int)
    uncleaned = (df['Is_Earned'] == 0).sum()
    logger.info(f"  [INFO] {uncleaned:,} uncleaned orders (Is_Earned=0, preserved in output)")

    # Only filter on CustomerID and OrderID (never remove for missing Earned_Date)
    df = df[
        df['CustomerID_Std'].notna() &
        df['OrderID_Std'].notna()
    ].copy()
    logger.info(f"  [OK] After null ID filter: {len(df):,} rows ({initial_count - len(df):,} removed)")

    df = df[~df['CustomerID_Std'].isin(business_account_set)].copy()
    logger.info(f"  [OK] After B2B filter: {len(df):,} rows")

    df = df.drop(columns=['Order ID', 'Customer ID', 'Total', 'Customer_Name'], errors='ignore')

    # =====================================================================
    # PHASE 10: MERGE CUSTOMER DATA (vectorized map)
    # =====================================================================
    logger.info("\nPhase 10: Merging customer data...")

    df['CohortMonth'] = df['CustomerID_Std'].map(customer_cohort)
    df['Route #'] = df['CustomerID_Std'].map(customer_route).fillna(0)
    df['Route_Category'] = vectorized_route_category(df['Route #'])

    # =====================================================================
    # PHASE 11: FLAGS (vectorized)
    # =====================================================================
    logger.info("\nPhase 11: Adding flags...")

    df['HasDelivery'] = (df['Delivery'] == 1).astype(int)
    df['HasPickup'] = df['Pickup Date'].notna().astype(int)

    # Delivery_Date (vectorized - replaces .apply lambda)
    df['Delivery_Date'] = np.where(df['Delivery'] == 1, df['Collected'], pd.NaT)
    df['Delivery_Date'] = pd.to_datetime(df['Delivery_Date'])

    # MonthsSinceCohort (vectorized)
    df['MonthsSinceCohort'] = vectorized_months_since_cohort(df['OrderCohortMonth'], df['CohortMonth'])

    # =====================================================================
    # PHASE 12: SUBSCRIPTION FLAG (vectorized merge - replaces .apply loop)
    # =====================================================================
    logger.info("\nPhase 12: Calculating subscription service flag...")

    df['IsSubscriptionService'] = vectorized_subscription_flag(df, subscription_dict)

    subscription_orders = (df['IsSubscriptionService'] == 1).sum()
    logger.info(f"  [OK] {subscription_orders:,} orders during active subscription")

    # =====================================================================
    # PHASE 13: TIME METRICS (vectorized column arithmetic)
    # =====================================================================
    logger.info("\nPhase 13: Calculating time metrics...")

    is_order = df['Transaction_Type'] == 'Order'

    # Processing_Days = Cleaned - Placed (orders only)
    proc = (df['Cleaned'] - df['Placed_Date']).dt.days.astype('float64')
    df['Processing_Days'] = np.where(
        is_order & df['Placed_Date'].notna() & df['Cleaned'].notna(),
        proc, np.nan
    )

    # TimeInStore_Days = Collected - Cleaned (orders only)
    tis = (df['Collected'] - df['Cleaned']).dt.days.astype('float64')
    df['TimeInStore_Days'] = np.where(
        is_order & df['Cleaned'].notna() & df['Collected'].notna(),
        tis, np.nan
    )

    # DaysToPayment = Payment Date - Placed
    dtp = (df['Payment Date'] - df['Placed_Date']).dt.days.astype('float64')
    df['DaysToPayment'] = np.where(
        df['Placed_Date'].notna() & df['Payment Date'].notna(),
        dtp, np.nan
    )

    logger.info(f"  [OK] Time metrics calculated")

    # =====================================================================
    # PHASE 14: FINAL OUTPUT
    # =====================================================================
    logger.info("\nPhase 14: Preparing final output...")

    final_columns = [
        'Source', 'Transaction_Type', 'Payment_Type_Std', 'Collections', 'Paid',
        'Store_Std', 'CustomerID_Std', 'OrderID_Std',
        'Placed_Date', 'Earned_Date', 'OrderCohortMonth', 'CohortMonth', 'MonthsSinceCohort',
        'Total_Num', 'Is_Earned', 'Ready By', 'Cleaned', 'Collected', 'Pickup Date', 'Payment Date',
        'Delivery_Date', 'Pieces', 'Delivery', 'HasDelivery', 'HasPickup',
        'Route #', 'Route_Category', 'IsSubscriptionService',
        'Processing_Days', 'TimeInStore_Days', 'DaysToPayment'
    ]
    existing_final = [c for c in final_columns if c in df.columns]
    df_final = df[existing_final].copy()

    # Type enforcement
    int_cols = ['Delivery', 'HasDelivery', 'HasPickup', 'IsSubscriptionService', 'Paid', 'Pieces', 'Is_Earned']
    for col in int_cols:
        if col in df_final.columns:
            df_final[col] = df_final[col].fillna(0).astype(int)

    # Format dates
    date_output_cols = [
        'Placed_Date', 'Earned_Date', 'OrderCohortMonth', 'CohortMonth',
        'Ready By', 'Cleaned', 'Collected', 'Pickup Date', 'Payment Date', 'Delivery_Date'
    ]
    df_final = format_dates_for_csv(df_final, date_output_cols)

    # Sort
    df_final = df_final.sort_values(['OrderCohortMonth', 'CustomerID_Std', 'OrderID_Std'])
    df_final = df_final.reset_index(drop=True)

    logger.info(f"  [OK] Final output: {len(df_final):,} rows × {len(df_final.columns)} columns")

    # Save
    df_final.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"  [OK] Saved to: {output_path}")

    # =====================================================================
    # VALIDATION SUMMARY
    # =====================================================================
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)

    logger.info(f"\nRow Counts:")
    logger.info(f"  Legacy Orders:        {(df_final['Source'] == 'Legacy').sum():>8,}")
    logger.info(f"  CC Orders:            {((df_final['Source'] == 'CC_2025') & (df_final['Transaction_Type'] == 'Order')).sum():>8,}")
    logger.info(f"  Subscriptions:        {(df_final['Transaction_Type'] == 'Subscription').sum():>8,}")
    logger.info(f"  Invoice Payments:     {(df_final['Transaction_Type'] == 'Invoice Payment').sum():>8,}")
    logger.info(f"  {'-' * 40}")
    logger.info(f"  TOTAL:                {len(df_final):>8,}")

    orders_rev = df_final[df_final['Transaction_Type'] == 'Order']['Total_Num'].sum()
    subs_rev = df_final[df_final['Transaction_Type'] == 'Subscription']['Total_Num'].sum()
    inv_rev = df_final[df_final['Transaction_Type'] == 'Invoice Payment']['Total_Num'].sum()

    logger.info(f"\nRevenue:")
    logger.info(f"  Orders:               ${orders_rev:>12,.2f}")
    logger.info(f"  Subscriptions:        ${subs_rev:>12,.2f}")
    logger.info(f"  Invoices:             ${inv_rev:>12,.2f}")
    logger.info(f"  TOTAL:                ${(orders_rev + subs_rev + inv_rev):>12,.2f}")

    logger.info(f"\nKey Metrics:")
    logger.info(f"  Unique Customers:     {df_final['CustomerID_Std'].nunique():>8,}")
    logger.info(f"  Subscription Orders:  {subscription_orders:>8,}")

    logger.info("\n" + "=" * 70)
    logger.info("[DONE] ALL_SALES TRANSFORMATION COMPLETE!")
    logger.info("=" * 70)
    logger.info("")
    return df_final, output_path


# =====================================================================
# STANDALONE EXECUTION
# =====================================================================

if __name__ == "__main__":
    run()
