"""
Customer_Quality_Monthly Transformation Script - OPTIMIZED VERSION
Only outputs 8 columns

OPTIMIZATIONS:
- Replaced 2x groupby().apply(lambda: pd.Series) with fast .agg() + merges
- Separate subscriber flag via boolean groupby (not per-group lambda)
- Callable run() for single-process master script
- Accepts pre-loaded DataFrames
"""

import pandas as pd
import numpy as np
import warnings
import os
from typing import Optional, Dict, Tuple
warnings.filterwarnings('ignore')

from helpers import SALES_DATA_PATH, vectorized_to_date, format_dates_for_csv

from logger_config import setup_logger
logger = setup_logger(__name__)


# =====================================================================
# MAIN TRANSFORMATION
# =====================================================================

def run(shared_data: Optional[Dict[str, pd.DataFrame]] = None) -> Tuple[pd.DataFrame, str]:
    """
    Run Customer_Quality_Monthly transformation.

    Args:
        shared_data: dict with pre-loaded DataFrames:
            - 'all_sales_df': Processed All_Sales DataFrame
            - 'all_items_df': Processed All_Items DataFrame
            If None, loads from disk.

    Returns:
        (df_final, output_path) tuple
    """
    logger.info("=" * 70)
    logger.info("CUSTOMER_QUALITY_MONTHLY TRANSFORMATION - OPTIMIZED")
    logger.info("=" * 70)
    logger.info("")
    output_path = os.path.join(SALES_DATA_PATH, "Customer_Quality_Monthly_Python.csv")

    # =====================================================================
    # PHASE 1: LOAD DATA
    # =====================================================================
    logger.info("Phase 1: Loading data...")

    if shared_data and 'all_sales_df' in shared_data:
        df_sales = shared_data['all_sales_df'].copy()
    else:
        df_sales = pd.read_csv(
            os.path.join(SALES_DATA_PATH, "All_Sales_Python.csv"), encoding='utf-8'
        )

    df_sales['OrderCohortMonth'] = vectorized_to_date(df_sales['OrderCohortMonth'])
    
    # Only use earned rows for quality metrics (uncleaned orders have no revenue yet)
    if 'Is_Earned' in df_sales.columns:
        unearned = (df_sales['Is_Earned'] == 0).sum()
        df_sales = df_sales[df_sales['Is_Earned'] == 1].copy()
        if unearned > 0:
            logger.info(f"  [INFO] Excluded {unearned:,} unearned rows (Is_Earned=0)")
    
    logger.info(f"  [OK] Loaded {len(df_sales):,} sales rows")

    if shared_data and 'all_items_df' in shared_data:
        df_items_all = shared_data['all_items_df'].copy()
    else:
        df_items_all = pd.read_csv(
            os.path.join(SALES_DATA_PATH, "All_Items_Python.csv"), encoding='utf-8'
        )

    # Filter to B2C only
    if 'IsBusinessAccount' in df_items_all.columns:
        df_items = df_items_all[df_items_all['IsBusinessAccount'] == 0].copy()
    else:
        df_items = df_items_all.copy()

    b2b_excluded = len(df_items_all) - len(df_items)
    logger.info(f"  [OK] Loaded {len(df_items):,} B2C item rows (excluded {b2b_excluded:,} B2B)")

    # =====================================================================
    # PHASE 2: MONTHLY REVENUE + SUBSCRIBER STATUS
    # (replaces groupby().apply(pd.Series) with fast .agg() + merges)
    # =====================================================================
    logger.info("\nPhase 2: Calculating monthly revenue and subscriber status...")

    group_keys = ['CustomerID_Std', 'OrderCohortMonth']

    # Total revenue per customer-month
    monthly_rev = df_sales.groupby(group_keys)['Total_Num'].sum().reset_index(name='Monthly_Revenue')

    # Order revenue
    order_mask = df_sales['Transaction_Type'] == 'Order'
    order_rev = df_sales[order_mask].groupby(group_keys)['Total_Num'].sum().reset_index(name='Order_Revenue')

    # Subscription revenue
    sub_mask = df_sales['Transaction_Type'] == 'Subscription'
    sub_rev = df_sales[sub_mask].groupby(group_keys)['Total_Num'].sum().reset_index(name='Subscription_Revenue')

    # Subscriber flag: has subscription payment with amount > 0
    has_sub_payment = df_sales[sub_mask & (df_sales['Total_Num'] > 0)].groupby(
        group_keys
    ).size().reset_index(name='_sub_pay')

    # Or has orders during subscription coverage
    if 'IsSubscriptionService' in df_sales.columns:
        has_sub_svc = df_sales[df_sales['IsSubscriptionService'].fillna(0) > 0].groupby(
            group_keys
        ).size().reset_index(name='_sub_svc')
    else:
        has_sub_svc = pd.DataFrame(columns=group_keys + ['_sub_svc'])

    # Merge all together
    sales_grouped = monthly_rev.merge(order_rev, on=group_keys, how='left')
    sales_grouped = sales_grouped.merge(sub_rev, on=group_keys, how='left')
    sales_grouped = sales_grouped.merge(has_sub_payment[group_keys + ['_sub_pay']], on=group_keys, how='left')
    sales_grouped = sales_grouped.merge(has_sub_svc[group_keys + ['_sub_svc']], on=group_keys, how='left')

    sales_grouped['Order_Revenue'] = sales_grouped['Order_Revenue'].fillna(0)
    sales_grouped['Subscription_Revenue'] = sales_grouped['Subscription_Revenue'].fillna(0)
    sales_grouped['Is_Subscriber'] = (
        (sales_grouped['_sub_pay'].fillna(0) > 0) | (sales_grouped['_sub_svc'].fillna(0) > 0)
    ).astype(int)
    sales_grouped = sales_grouped.drop(columns=['_sub_pay', '_sub_svc'])

    logger.info(f"  [OK] {len(sales_grouped):,} customer-month combinations")
    logger.info(f"  [OK] Subscribers: {sales_grouped['Is_Subscriber'].sum():,} customer-months")

    # =====================================================================
    # PHASE 3: MONTHLY ITEMS (OPERATIONAL DATES)
    # =====================================================================
    logger.info("\nPhase 3: Calculating monthly items...")

    df_items['ItemCohortMonth'] = vectorized_to_date(df_items['ItemCohortMonth'])

    items_grouped = df_items.groupby(['CustomerID_Std', 'ItemCohortMonth']).agg(
        Monthly_Items=('Quantity', 'sum')
    ).reset_index()

    logger.info(f"  [OK] {len(items_grouped):,} customer-months with items")

    # =====================================================================
    # PHASE 4: SERVICE DIVERSITY
    # (replaces 2nd groupby().apply(pd.Series) with fast .agg() + merges)
    # =====================================================================
    logger.info("\nPhase 4: Calculating service diversity...")

    service_breakdown = df_items.groupby(['CustomerID_Std', 'ItemCohortMonth', 'Service_Type']).agg(
        Service_Revenue=('Total', 'sum')
    ).reset_index()

    # Get ItemCohortMonth for each order
    df_items_order_map = df_items[['OrderID_Std', 'CustomerID_Std', 'ItemCohortMonth']].drop_duplicates()
    # OrderCohortMonth already parsed in Phase 2; ensure datetime dtype persists
    if not pd.api.types.is_datetime64_any_dtype(df_sales['OrderCohortMonth']):
        df_sales['OrderCohortMonth'] = vectorized_to_date(df_sales['OrderCohortMonth'])

    sales_with_item_month = df_sales.merge(
        df_items_order_map[['OrderID_Std', 'ItemCohortMonth']].drop_duplicates(),
        on='OrderID_Std', how='left'
    )
    sales_with_item_month['ItemCohortMonth'] = sales_with_item_month['ItemCohortMonth'].fillna(
        sales_with_item_month['OrderCohortMonth']
    )

    fallback_count = (~sales_with_item_month['OrderID_Std'].isin(df_items['OrderID_Std'])).sum()
    logger.info(f"  [OK] Using OrderCohortMonth fallback for {fallback_count:,} orders (Legacy)")

    # Revenue by Customer + ItemCohortMonth (FAST version)
    item_group_keys = ['CustomerID_Std', 'ItemCohortMonth']

    rev_by_item_month = sales_with_item_month.groupby(item_group_keys)['Total_Num'].sum().reset_index(
        name='Monthly_Revenue')

    order_rev_im = sales_with_item_month[
        sales_with_item_month['Transaction_Type'] == 'Order'
    ].groupby(item_group_keys)['Total_Num'].sum().reset_index(name='Order_Revenue')

    sub_rev_im = sales_with_item_month[
        sales_with_item_month['Transaction_Type'] == 'Subscription'
    ].groupby(item_group_keys)['Total_Num'].sum().reset_index(name='Subscription_Revenue')

    # Subscriber flag by item month
    sub_pay_im = sales_with_item_month[
        (sales_with_item_month['Transaction_Type'] == 'Subscription') &
        (sales_with_item_month['Total_Num'] > 0)
    ].groupby(item_group_keys).size().reset_index(name='_sub_pay')

    if 'IsSubscriptionService' in sales_with_item_month.columns:
        sub_svc_im = sales_with_item_month[
            sales_with_item_month['IsSubscriptionService'].fillna(0) > 0
        ].groupby(item_group_keys).size().reset_index(name='_sub_svc')
    else:
        sub_svc_im = pd.DataFrame(columns=item_group_keys + ['_sub_svc'])

    sales_by_item_month = rev_by_item_month.merge(order_rev_im, on=item_group_keys, how='left')
    sales_by_item_month = sales_by_item_month.merge(sub_rev_im, on=item_group_keys, how='left')
    sales_by_item_month = sales_by_item_month.merge(sub_pay_im, on=item_group_keys, how='left')
    sales_by_item_month = sales_by_item_month.merge(sub_svc_im, on=item_group_keys, how='left')

    sales_by_item_month['Order_Revenue'] = sales_by_item_month['Order_Revenue'].fillna(0)
    sales_by_item_month['Subscription_Revenue'] = sales_by_item_month['Subscription_Revenue'].fillna(0)
    sales_by_item_month['Is_Subscriber'] = (
        (sales_by_item_month['_sub_pay'].fillna(0) > 0) |
        (sales_by_item_month['_sub_svc'].fillna(0) > 0)
    ).astype(int)
    sales_by_item_month = sales_by_item_month.drop(columns=['_sub_pay', '_sub_svc'])

    # Service percentage and 10% threshold
    service_with_totals = service_breakdown.merge(
        sales_by_item_month[['CustomerID_Std', 'ItemCohortMonth', 'Monthly_Revenue']],
        on=item_group_keys, how='inner'
    )
    service_with_totals['Service_Pct'] = (
        service_with_totals['Service_Revenue'] / service_with_totals['Monthly_Revenue']
    ).fillna(0)

    services_above_10pct = service_with_totals[service_with_totals['Service_Pct'] >= 0.10]
    service_counts = services_above_10pct.groupby(item_group_keys).size().reset_index(
        name='Services_Used_10pct')

    logger.info(f"  [OK] {len(services_above_10pct):,} service entries above 10% threshold")

    # =====================================================================
    # PHASE 5: MERGE ALL DATA
    # =====================================================================
    logger.info("\nPhase 5: Merging all metrics...")

    df_combined = sales_by_item_month.merge(items_grouped, on=item_group_keys, how='left')
    df_combined = df_combined.merge(service_counts, on=item_group_keys, how='left')

    df_combined['Monthly_Items'] = df_combined['Monthly_Items'].fillna(0).astype(int)
    df_combined['Services_Used_10pct'] = df_combined['Services_Used_10pct'].fillna(0).astype(int)
    df_combined['Order_Revenue'] = df_combined['Order_Revenue'].fillna(0)
    df_combined['Subscription_Revenue'] = df_combined['Subscription_Revenue'].fillna(0)
    df_combined['Is_Subscriber'] = df_combined['Is_Subscriber'].fillna(0).astype(int)

    # Min 1 service if customer has items
    df_combined.loc[
        (df_combined['Services_Used_10pct'] == 0) & (df_combined['Monthly_Items'] > 0),
        'Services_Used_10pct'
    ] = 1

    legacy_count = ((df_combined['Monthly_Items'] == 0) & (df_combined['Monthly_Revenue'] > 0)).sum()
    logger.info(f"  [OK] {len(df_combined):,} customer-month rows")
    logger.info(f"  [INFO] Legacy/no-items: {legacy_count:,}")

    # =====================================================================
    # PHASE 6: IS_MULTI_SERVICE
    # =====================================================================
    logger.info("\nPhase 6: Calculating Is_Multi_Service...")

    df_combined['Is_Multi_Service'] = (
        (df_combined['Services_Used_10pct'] >= 2) | (df_combined['Is_Subscriber'] == 1)
    ).astype(int)

    multi = df_combined['Is_Multi_Service'].sum()
    logger.info(f"  [OK] Multi-Service: {multi:,} ({multi/len(df_combined)*100:.1f}%)")

    # =====================================================================
    # PHASE 7: FINAL OUTPUT
    # =====================================================================
    logger.info("\nPhase 7: Preparing final output...")

    df_combined = df_combined.rename(columns={'ItemCohortMonth': 'OrderCohortMonth'})

    final_columns = [
        'CustomerID_Std', 'OrderCohortMonth',
        'Order_Revenue', 'Subscription_Revenue', 'Monthly_Revenue',
        'Monthly_Items', 'Services_Used_10pct', 'Is_Multi_Service'
    ]
    df_final = df_combined[final_columns].copy()

    if not pd.api.types.is_datetime64_any_dtype(df_final['OrderCohortMonth']):
        df_final['OrderCohortMonth'] = vectorized_to_date(df_final['OrderCohortMonth'])
    # Output dates as DD-Mon-YYYY for PowerQuery (unambiguous, no locale issues)
    df_final = format_dates_for_csv(df_final, ['OrderCohortMonth'])

    df_final = df_final.sort_values(['OrderCohortMonth', 'CustomerID_Std']).reset_index(drop=True)

    logger.info(f"  [OK] Final: {len(df_final):,} rows × {len(df_final.columns)} columns")

    # Save
    df_final.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"  [OK] Saved to: {output_path}")

    # =====================================================================
    # VALIDATION SUMMARY
    # =====================================================================
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)

    logger.info(f"\nOverall:")
    logger.info(f"  Customer-Months: {len(df_final):>8,}")
    logger.info(f"  Unique Customers: {df_final['CustomerID_Std'].nunique():>8,}")
    logger.info(f"  Date Range: {df_final['OrderCohortMonth'].min()} to {df_final['OrderCohortMonth'].max()}")

    total_order = df_final['Order_Revenue'].sum()
    total_sub = df_final['Subscription_Revenue'].sum()
    total_rev = df_final['Monthly_Revenue'].sum()
    logger.info(f"\nRevenue:")
    logger.info(f"  Orders:       ${total_order:>12,.2f}")
    logger.info(f"  Subscriptions: ${total_sub:>12,.2f}")
    logger.info(f"  Total:        ${total_rev:>12,.2f}")

    logger.info(f"\nActivity:")
    logger.info(f"  Total Items:   {df_final['Monthly_Items'].sum():>8,}")
    logger.info(f"  Avg Items/Mo:  {df_final['Monthly_Items'].mean():>8,.1f}")

    legacy_m = ((df_final['Monthly_Items'] == 0) & (df_final['Monthly_Revenue'] > 0)).sum()
    cc_m = (df_final['Monthly_Items'] > 0).sum()
    logger.info(f"\nLegacy vs CC:")
    logger.info(f"  Legacy: {legacy_m:>8,} ({legacy_m/len(df_final)*100:.1f}%)")
    logger.info(f"  CC:     {cc_m:>8,} ({cc_m/len(df_final)*100:.1f}%)")

    logger.info(f"\nService Diversity:")
    logger.info(f"  Multi-Service: {df_final['Is_Multi_Service'].sum():>8,}")
    for sc in sorted(df_final['Services_Used_10pct'].unique()):
        cnt = (df_final['Services_Used_10pct'] == sc).sum()
        logger.info(f"  {sc} service(s): {cnt:>8,} ({cnt/len(df_final)*100:.1f}%)")

    logger.info("\n" + "=" * 70)
    logger.info("[DONE] CUSTOMER_QUALITY_MONTHLY COMPLETE!")
    logger.info("=" * 70)
    logger.info("")
    return df_final, output_path


# =====================================================================
# STANDALONE EXECUTION
# =====================================================================

if __name__ == "__main__":
    run()
