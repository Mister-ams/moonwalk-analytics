"""
Customer_Quality_Monthly Transformation Script (Polars)
Only outputs 8 columns
"""

import polars as pl
import warnings
import os
from typing import Optional, Dict, Tuple, Union
warnings.filterwarnings('ignore')

from helpers import polars_to_date, polars_format_dates_for_csv
from config import LOCAL_STAGING_PATH

from logger_config import setup_logger
logger = setup_logger(__name__)


# =====================================================================
# MAIN TRANSFORMATION
# =====================================================================

def run(shared_data: Optional[Dict[str, Union[pl.DataFrame]]] = None) -> Tuple[pl.DataFrame, str]:
    """
    Run Customer_Quality_Monthly transformation.

    Args:
        shared_data: dict with pre-loaded DataFrames (Polars):
            - 'all_sales_df': Processed All_Sales DataFrame
            - 'all_items_df': Processed All_Items DataFrame
            If None, loads from disk.

    Returns:
        (df_final, output_path) tuple
    """
    logger.info("=" * 70)
    logger.info("CUSTOMER_QUALITY_MONTHLY TRANSFORMATION - POLARS")
    logger.info("=" * 70)
    logger.info("")
    output_path = os.path.join(LOCAL_STAGING_PATH, "Customer_Quality_Monthly_Python.csv")

    # =====================================================================
    # PHASE 1: LOAD DATA
    # =====================================================================
    logger.info("Phase 1: Loading data...")

    if shared_data and 'all_sales_df' in shared_data:
        df_sales = shared_data['all_sales_df'].clone()
    else:
        df_sales = pl.read_csv(
            os.path.join(LOCAL_STAGING_PATH, "All_Sales_Python.csv"),
            infer_schema_length=10000
        )

    # Parse OrderCohortMonth if string
    if df_sales["OrderCohortMonth"].dtype == pl.Utf8:
        df_sales = polars_to_date(df_sales, "OrderCohortMonth")

    # Only use earned rows
    if "Is_Earned" in df_sales.columns:
        unearned = df_sales.filter(pl.col("Is_Earned") == 0).height
        df_sales = df_sales.filter(pl.col("Is_Earned") == 1)
        if unearned > 0:
            logger.info(f"  [INFO] Excluded {unearned:,} unearned rows (Is_Earned=0)")

    logger.info(f"  [OK] Loaded {df_sales.height:,} sales rows")

    if shared_data and 'all_items_df' in shared_data:
        df_items_all = shared_data['all_items_df'].clone()
    else:
        df_items_all = pl.read_csv(
            os.path.join(LOCAL_STAGING_PATH, "All_Items_Python.csv"),
            infer_schema_length=10000
        )

    # Filter to B2C only
    if "IsBusinessAccount" in df_items_all.columns:
        df_items = df_items_all.filter(pl.col("IsBusinessAccount") == 0)
    else:
        df_items = df_items_all

    b2b_excluded = df_items_all.height - df_items.height
    logger.info(f"  [OK] Loaded {df_items.height:,} B2C item rows (excluded {b2b_excluded:,} B2B)")

    # =====================================================================
    # PHASE 2: MONTHLY REVENUE + SUBSCRIBER STATUS
    # =====================================================================
    logger.info("\nPhase 2: Calculating monthly revenue and subscriber status...")

    group_keys = ["CustomerID_Std", "OrderCohortMonth"]

    # Ensure numeric types
    df_sales = df_sales.with_columns([
        pl.col("Total_Num").cast(pl.Float64, strict=False).fill_null(0),
    ])

    is_order = pl.col("Transaction_Type") == "Order"
    is_sub = pl.col("Transaction_Type") == "Subscription"
    has_sub_svc = (
        pl.col("IsSubscriptionService").cast(pl.Float64, strict=False).fill_null(0) > 0
    ) if "IsSubscriptionService" in df_sales.columns else pl.lit(False)

    sales_grouped = df_sales.group_by(group_keys).agg([
        pl.col("Total_Num").sum().alias("Monthly_Revenue"),
        pl.col("Total_Num").filter(is_order).sum().alias("Order_Revenue"),
        pl.col("Total_Num").filter(is_sub).sum().alias("Subscription_Revenue"),
        ((is_sub & (pl.col("Total_Num") > 0)).sum() > 0).alias("_has_sub_pay"),
        (has_sub_svc.sum() > 0).alias("_has_sub_svc"),
    ])

    sales_grouped = sales_grouped.with_columns(
        (pl.col("_has_sub_pay") | pl.col("_has_sub_svc")).cast(pl.Int32).alias("Is_Subscriber")
    ).drop(["_has_sub_pay", "_has_sub_svc"])

    logger.info(f"  [OK] {sales_grouped.height:,} customer-month combinations")
    logger.info(f"  [OK] Subscribers: {sales_grouped.filter(pl.col('Is_Subscriber') == 1).height:,} customer-months")

    # =====================================================================
    # PHASE 3: MONTHLY ITEMS
    # =====================================================================
    logger.info("\nPhase 3: Calculating monthly items...")

    # Parse ItemCohortMonth if string
    if df_items["ItemCohortMonth"].dtype == pl.Utf8:
        df_items = polars_to_date(df_items, "ItemCohortMonth")

    items_grouped = df_items.group_by(["CustomerID_Std", "ItemCohortMonth"]).agg(
        pl.col("Quantity").cast(pl.Int64, strict=False).fill_null(0).sum().alias("Monthly_Items")
    )

    logger.info(f"  [OK] {items_grouped.height:,} customer-months with items")

    # =====================================================================
    # PHASE 4: SERVICE DIVERSITY
    # =====================================================================
    logger.info("\nPhase 4: Calculating service diversity...")

    service_breakdown = df_items.group_by(["CustomerID_Std", "ItemCohortMonth", "Service_Type"]).agg(
        pl.col("Total").cast(pl.Float64, strict=False).fill_null(0).sum().alias("Service_Revenue")
    )

    # Get ItemCohortMonth for each order
    df_items_order_map = df_items.select(["OrderID_Std", "CustomerID_Std", "ItemCohortMonth"]).unique()

    # Ensure OrderCohortMonth is datetime
    if df_sales["OrderCohortMonth"].dtype == pl.Utf8:
        df_sales = polars_to_date(df_sales, "OrderCohortMonth")

    sales_with_item_month = df_sales.join(
        df_items_order_map.select(["OrderID_Std", "ItemCohortMonth"]).unique(),
        on="OrderID_Std", how="left"
    )

    # Fallback ItemCohortMonth to OrderCohortMonth where missing
    sales_with_item_month = sales_with_item_month.with_columns(
        pl.col("ItemCohortMonth").fill_null(pl.col("OrderCohortMonth")).alias("ItemCohortMonth")
    )

    fallback_count = sales_with_item_month.filter(
        ~pl.col("OrderID_Std").is_in(df_items["OrderID_Std"])
    ).height
    logger.info(f"  [OK] Using OrderCohortMonth fallback for {fallback_count:,} orders (Legacy)")

    # Sales aggregated by item month
    item_group_keys = ["CustomerID_Std", "ItemCohortMonth"]

    is_order_im = pl.col("Transaction_Type") == "Order"
    is_sub_im = pl.col("Transaction_Type") == "Subscription"
    has_sub_svc_im = (
        pl.col("IsSubscriptionService").cast(pl.Float64, strict=False).fill_null(0) > 0
    ) if "IsSubscriptionService" in sales_with_item_month.columns else pl.lit(False)

    sales_by_item_month = sales_with_item_month.group_by(item_group_keys).agg([
        pl.col("Total_Num").sum().alias("Monthly_Revenue"),
        pl.col("Total_Num").filter(is_order_im).sum().alias("Order_Revenue"),
        pl.col("Total_Num").filter(is_sub_im).sum().alias("Subscription_Revenue"),
        ((is_sub_im & (pl.col("Total_Num") > 0)).sum() > 0).alias("_has_sub_pay"),
        (has_sub_svc_im.sum() > 0).alias("_has_sub_svc"),
    ])

    sales_by_item_month = sales_by_item_month.with_columns(
        (pl.col("_has_sub_pay") | pl.col("_has_sub_svc")).cast(pl.Int32).alias("Is_Subscriber")
    ).drop(["_has_sub_pay", "_has_sub_svc"])

    # Service percentage and 10% threshold
    service_with_totals = service_breakdown.join(
        sales_by_item_month.select(["CustomerID_Std", "ItemCohortMonth", "Monthly_Revenue"]),
        on=item_group_keys, how="inner"
    )
    service_with_totals = service_with_totals.with_columns(
        pl.when(pl.col("Monthly_Revenue") > 0)
        .then(pl.col("Service_Revenue") / pl.col("Monthly_Revenue"))
        .otherwise(0.0)
        .fill_null(0)
        .alias("Service_Pct")
    )

    services_above_10pct = service_with_totals.filter(pl.col("Service_Pct") >= 0.10)
    service_counts = services_above_10pct.group_by(item_group_keys).len().rename({"len": "Services_Used_10pct"})

    logger.info(f"  [OK] {services_above_10pct.height:,} service entries above 10% threshold")

    # =====================================================================
    # PHASE 5: MERGE ALL DATA
    # =====================================================================
    logger.info("\nPhase 5: Merging all metrics...")

    df_combined = sales_by_item_month.join(items_grouped, on=item_group_keys, how="left")
    df_combined = df_combined.join(service_counts, on=item_group_keys, how="left")

    df_combined = df_combined.with_columns([
        pl.col("Monthly_Items").fill_null(0).cast(pl.Int32),
        pl.col("Services_Used_10pct").fill_null(0).cast(pl.Int32),
        pl.col("Order_Revenue").fill_null(0),
        pl.col("Subscription_Revenue").fill_null(0),
        pl.col("Is_Subscriber").fill_null(0).cast(pl.Int32),
    ])

    # Min 1 service if customer has items
    df_combined = df_combined.with_columns(
        pl.when((pl.col("Services_Used_10pct") == 0) & (pl.col("Monthly_Items") > 0))
        .then(pl.lit(1))
        .otherwise(pl.col("Services_Used_10pct"))
        .cast(pl.Int32)
        .alias("Services_Used_10pct")
    )

    legacy_count = df_combined.filter(
        (pl.col("Monthly_Items") == 0) & (pl.col("Monthly_Revenue") > 0)
    ).height
    logger.info(f"  [OK] {df_combined.height:,} customer-month rows")
    logger.info(f"  [INFO] Legacy/no-items: {legacy_count:,}")

    # =====================================================================
    # PHASE 6: IS_MULTI_SERVICE
    # =====================================================================
    logger.info("\nPhase 6: Calculating Is_Multi_Service...")

    df_combined = df_combined.with_columns(
        ((pl.col("Services_Used_10pct") >= 2) | (pl.col("Is_Subscriber") == 1))
        .cast(pl.Int32).alias("Is_Multi_Service")
    )

    multi = df_combined.filter(pl.col("Is_Multi_Service") == 1).height
    logger.info(f"  [OK] Multi-Service: {multi:,} ({multi/df_combined.height*100:.1f}%)")

    # =====================================================================
    # PHASE 7: FINAL OUTPUT
    # =====================================================================
    logger.info("\nPhase 7: Preparing final output...")

    df_combined = df_combined.rename({"ItemCohortMonth": "OrderCohortMonth"})

    final_columns = [
        'CustomerID_Std', 'OrderCohortMonth',
        'Order_Revenue', 'Subscription_Revenue', 'Monthly_Revenue',
        'Monthly_Items', 'Services_Used_10pct', 'Is_Multi_Service'
    ]
    df_final = df_combined.select(final_columns)

    # Format dates
    df_final = polars_format_dates_for_csv(df_final, ['OrderCohortMonth'])

    # Sort
    df_final = df_final.sort(['OrderCohortMonth', 'CustomerID_Std'])

    logger.info(f"  [OK] Final: {df_final.height:,} rows x {len(df_final.columns)} columns")

    # Save
    df_final.write_csv(output_path)
    logger.info(f"  [OK] Saved to: {output_path}")

    # =====================================================================
    # VALIDATION SUMMARY
    # =====================================================================
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)

    logger.info(f"\nOverall:")
    logger.info(f"  Customer-Months: {df_final.height:>8,}")
    logger.info(f"  Unique Customers: {df_final['CustomerID_Std'].n_unique():>8,}")
    ocm_col = df_final["OrderCohortMonth"]
    logger.info(f"  Date Range: {ocm_col.min()} to {ocm_col.max()}")

    total_order = df_final["Order_Revenue"].cast(pl.Float64).sum()
    total_sub = df_final["Subscription_Revenue"].cast(pl.Float64).sum()
    total_rev = df_final["Monthly_Revenue"].cast(pl.Float64).sum()
    logger.info(f"\nRevenue:")
    logger.info(f"  Orders:       ${total_order:>12,.2f}")
    logger.info(f"  Subscriptions: ${total_sub:>12,.2f}")
    logger.info(f"  Total:        ${total_rev:>12,.2f}")

    logger.info(f"\nActivity:")
    logger.info(f"  Total Items:   {df_final['Monthly_Items'].sum():>8,}")
    monthly_items_mean = df_final["Monthly_Items"].mean()
    logger.info(f"  Avg Items/Mo:  {monthly_items_mean:>8,.1f}")

    legacy_m = df_final.filter(
        (pl.col("Monthly_Items") == 0) & (pl.col("Monthly_Revenue").cast(pl.Float64) > 0)
    ).height
    cc_m = df_final.filter(pl.col("Monthly_Items") > 0).height
    logger.info(f"\nLegacy vs CC:")
    logger.info(f"  Legacy: {legacy_m:>8,} ({legacy_m/df_final.height*100:.1f}%)")
    logger.info(f"  CC:     {cc_m:>8,} ({cc_m/df_final.height*100:.1f}%)")

    multi_svc = df_final.filter(pl.col("Is_Multi_Service") == 1).height
    logger.info(f"\nService Diversity:")
    logger.info(f"  Multi-Service: {multi_svc:>8,}")
    for row in df_final.group_by("Services_Used_10pct").len().sort("Services_Used_10pct").iter_rows():
        cnt = row[1]
        logger.info(f"  {row[0]} service(s): {cnt:>8,} ({cnt/df_final.height*100:.1f}%)")

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
