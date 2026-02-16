"""
All_Sales Transformation Script (Polars)
Includes: Date Fix + Subscription Store Fix
"""

import polars as pl
import warnings
import os
from typing import Optional, Dict, Tuple, Union
warnings.filterwarnings('ignore')

from helpers import (
    find_cleancloud_file, polars_to_date, polars_store_std,
    polars_customer_id_std, polars_order_id_std,
    polars_payment_type_std, polars_route_category,
    polars_months_since_cohort, polars_subscription_flag,
    polars_name_standardize, polars_format_dates_for_csv,
)
from config import LOCAL_STAGING_PATH, SUBSCRIPTION_VALIDITY_DAYS

from logger_config import setup_logger
logger = setup_logger(__name__)


# =====================================================================
# MAIN TRANSFORMATION
# =====================================================================

def run(shared_data: Optional[Dict[str, Union[pl.DataFrame]]] = None) -> Tuple[pl.DataFrame, str]:
    """
    Run All_Sales transformation.

    Args:
        shared_data: dict with pre-loaded DataFrames (Polars):
            - 'customers_csv', 'orders_csv', 'invoices_csv', 'legacy_csv'
            - 'all_customers_df': Processed All_Customers DataFrame
            If None, loads from disk.

    Returns:
        (df_final, output_path) tuple
    """
    logger.info("=" * 70)
    logger.info("ALL_SALES TRANSFORMATION - POLARS")
    logger.info("=" * 70)
    logger.info("")
    output_path = os.path.join(LOCAL_STAGING_PATH, "All_Sales_Python.csv")

    # =====================================================================
    # PHASE 1: LOAD DEPENDENCIES
    # =====================================================================
    logger.info("Phase 1: Loading dependencies...")

    # CC Customers
    if shared_data and 'customers_csv' in shared_data:
        df_customers = shared_data['customers_csv']
    else:
        df_customers = pl.read_csv(find_cleancloud_file('customer'), infer_schema_length=10000)

    # Business Account set
    biz_mask = (pl.col("Business ID").cast(pl.Utf8).fill_null("") != "")
    biz_ids = (
        df_customers.filter(biz_mask)
        .select(pl.col("Customer ID").cast(pl.Utf8).str.replace_all(r"\D", ""))
        .to_series()
    )
    business_account_set = set(f"CC-{d.zfill(4)}" for d in biz_ids.to_list() if d)

    logger.info(f"  [OK] Loaded {df_customers.height:,} CC customers, {len(business_account_set)} business accounts")

    # Customer name lookup
    name_df = df_customers.filter(
        pl.col("Name").is_not_null() & (pl.col("Name").cast(pl.Utf8) != "")
    ).with_columns([
        polars_name_standardize(pl.col("Name")).alias("name_std"),
        (pl.lit("CC-") + pl.col("Customer ID").cast(pl.Utf8).str.replace_all(r"\D", "").str.zfill(4))
            .alias("cust_std"),
    ]).filter(pl.col("name_std") != "")
    name_df = name_df.unique(subset="name_std", keep="first")
    customer_name_lookup = dict(zip(
        name_df["name_std"].to_list(),
        name_df["cust_std"].to_list()
    ))

    # All_Customers (for CohortMonth + Route)
    if shared_data and 'all_customers_df' in shared_data:
        df_all_customers = shared_data['all_customers_df'].clone()
    else:
        df_all_customers = pl.read_csv(
            os.path.join(LOCAL_STAGING_PATH, "All_Customers_Python.csv"),
            infer_schema_length=10000
        )

    # Parse CohortMonth if string
    if df_all_customers["CohortMonth"].dtype == pl.Utf8:
        df_all_customers = polars_to_date(df_all_customers, "CohortMonth")

    customer_cohort = dict(zip(
        df_all_customers["CustomerID_Std"].to_list(),
        df_all_customers["CohortMonth"].to_list()
    ))
    customer_route = dict(zip(
        df_all_customers["CustomerID_Std"].to_list(),
        df_all_customers["Route #"].cast(pl.Float64, strict=False).fill_null(0).to_list()
    ))

    logger.info(f"  [OK] Loaded {df_all_customers.height:,} total customers for lookup")

    # =====================================================================
    # PHASE 2: LOAD SUBSCRIPTION PERIODS
    # =====================================================================
    logger.info("\nPhase 2: Loading subscription periods...")

    if shared_data and 'invoices_csv' in shared_data:
        df_invoices_raw = shared_data['invoices_csv'].clone()
    else:
        df_invoices_raw = pl.read_csv(find_cleancloud_file('invoice'), infer_schema_length=10000)

    df_subs = df_invoices_raw.filter(
        pl.col("Reference").cast(pl.Utf8).str.to_uppercase().str.starts_with("SUBSCRIPTION")
    )

    df_subs = polars_to_date(df_subs, "Payment Date", alias="Payment_Date")
    df_subs = df_subs.filter(pl.col("Payment_Date").is_not_null())

    df_subs = df_subs.with_columns(
        polars_name_standardize(pl.col("Customer")).alias("CustomerName_Std")
    )
    # Map name -> CustomerID_Std
    name_series = df_subs["CustomerName_Std"].to_list()
    cid_mapped = [customer_name_lookup.get(n) for n in name_series]
    df_subs = df_subs.with_columns(pl.Series("CustomerID_Std", cid_mapped))

    df_subs = df_subs.with_columns([
        pl.col("Payment_Date").alias("ValidFrom"),
        (pl.col("Payment_Date") + pl.duration(days=SUBSCRIPTION_VALIDITY_DAYS)).alias("ValidUntil"),
    ])

    # Build subscription dict
    sub_valid = df_subs.select(["CustomerID_Std", "ValidFrom", "ValidUntil"]).drop_nulls()
    subscription_dict = {}
    for row in sub_valid.iter_rows(named=True):
        cid = row["CustomerID_Std"]
        if cid not in subscription_dict:
            subscription_dict[cid] = []
        subscription_dict[cid].append({"ValidFrom": row["ValidFrom"], "ValidUntil": row["ValidUntil"]})

    logger.info(f"  [OK] {df_subs.height:,} subscription payments, {len(subscription_dict)} customers with subs")

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
        df_legacy = shared_data['legacy_csv'].clone()
    else:
        df_legacy = pl.read_csv(os.path.join(LOCAL_STAGING_PATH, "RePos_Archive.csv"), infer_schema_length=10000)

    existing_legacy_cols = [c for c in order_columns if c in df_legacy.columns]
    df_legacy = df_legacy.select(existing_legacy_cols)
    df_legacy = df_legacy.with_columns([
        pl.lit("Legacy").alias("Source"),
        pl.lit("Order").alias("Transaction_Type"),
        pl.lit(None, dtype=pl.Utf8).alias("Customer_Name"),
    ])
    logger.info(f"  [OK] Loaded {df_legacy.height:,} legacy orders")

    # 3B. CC Orders
    if shared_data and 'orders_csv' in shared_data:
        df_cc_orders = shared_data['orders_csv'].clone()
    else:
        df_cc_orders = pl.read_csv(find_cleancloud_file('orders'), infer_schema_length=10000)

    existing_cc_cols = [c for c in order_columns if c in df_cc_orders.columns]
    df_cc_orders = df_cc_orders.select(existing_cc_cols)
    df_cc_orders = df_cc_orders.with_columns([
        pl.lit("CC_2025").alias("Source"),
        pl.lit("Order").alias("Transaction_Type"),
        pl.lit(None, dtype=pl.Utf8).alias("Customer_Name"),
    ])
    logger.info(f"  [OK] Loaded {df_cc_orders.height:,} CC orders")

    # 3C. Invoices
    df_inv = df_invoices_raw.clone()
    df_inv = df_inv.with_columns(
        pl.col("Reference").cast(pl.Utf8).str.to_uppercase().alias("Reference_Upper")
    )
    is_subscription = pl.col("Reference_Upper").str.starts_with("SUBSCRIPTION")

    df_inv = polars_to_date(df_inv, "Payment Date", alias="Payment_Date_Parsed")
    df_inv = df_inv.filter(pl.col("Payment_Date_Parsed").is_not_null() | is_subscription)

    df_inv = df_inv.with_columns([
        pl.when(is_subscription)
        .then(pl.col("Amount").cast(pl.Float64, strict=False).fill_null(0))
        .otherwise(pl.lit(0.0))
        .alias("Total"),

        pl.col("Amount").cast(pl.Float64, strict=False).fill_null(0).alias("Collections_Inv"),
        pl.col("Customer").alias("Customer_Name"),

        pl.when(is_subscription)
        .then(pl.lit("Subscription"))
        .otherwise(pl.lit("Invoice Payment"))
        .alias("Transaction_Type"),

        pl.lit(None, dtype=pl.Utf8).alias("Order ID"),
        pl.lit(None, dtype=pl.Utf8).alias("Customer ID"),
        pl.col("Payment Date").alias("Placed"),
        pl.lit(None, dtype=pl.Utf8).alias("Ready By"),
        pl.col("Payment Date").alias("Cleaned"),
        pl.col("Payment Date").alias("Collected"),
        pl.lit(None, dtype=pl.Utf8).alias("Pickup Date"),
        pl.lit(1).alias("Paid"),
        pl.lit(0).alias("Pieces"),
        pl.lit(0).alias("Delivery"),
        pl.lit("CC_2025").alias("Source"),
    ])

    # Rename Payment Method -> Payment Type if exists
    if "Payment Method" in df_inv.columns:
        df_inv = df_inv.rename({"Payment Method": "Payment Type"})

    inv_keep = [
        'Order ID', 'Customer ID', 'Placed', 'Total', 'Collections_Inv',
        'Store ID', 'Store Name',
        'Ready By', 'Cleaned', 'Collected', 'Pickup Date',
        'Payment Date', 'Payment Type', 'Paid', 'Pieces', 'Delivery',
        'Source', 'Transaction_Type', 'Customer_Name'
    ]
    existing_inv_cols = [c for c in inv_keep if c in df_inv.columns]
    df_inv = df_inv.select(existing_inv_cols)
    logger.info(f"  [OK] Loaded {df_inv.height:,} invoice rows")

    # =====================================================================
    # PHASE 4: COMBINE
    # =====================================================================
    logger.info("\nPhase 4: Combining sources...")

    df = pl.concat([df_legacy, df_cc_orders, df_inv], how="diagonal_relaxed")
    logger.info(f"  [OK] Combined: {df.height:,} total rows")

    del df_legacy, df_cc_orders, df_inv

    # =====================================================================
    # PHASE 5: VECTORIZED STANDARDIZATION
    # =====================================================================
    logger.info("\nPhase 5: Vectorized standardization...")

    # Paid
    df = df.with_columns(
        pl.col("Paid").cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int32).alias("Paid")
    )

    # Payment Type
    df = df.with_columns(polars_payment_type_std("Payment Type").alias("Payment_Type_Std"))

    # Date columns
    date_cols = ['Placed', 'Ready By', 'Cleaned', 'Collected', 'Pickup Date', 'Payment Date']
    for col in date_cols:
        if col in df.columns:
            df = polars_to_date(df, col)

    df = df.with_columns(pl.col("Placed").alias("Placed_Date"))

    # Earned_Date
    df = df.with_columns(
        pl.when(pl.col("Source") != "CC_2025")
        .then(
            pl.when(pl.col("Cleaned").is_not_null())
            .then(pl.col("Cleaned"))
            .otherwise(pl.col("Placed_Date"))
        )
        .otherwise(pl.col("Cleaned"))
        .alias("Earned_Date")
    )

    # OrderCohortMonth
    df = df.with_columns(
        pl.col("Earned_Date").dt.truncate("1mo").alias("OrderCohortMonth")
    )

    df = df.with_columns([
        pl.col("Total").cast(pl.Float64, strict=False).fill_null(0).alias("Total_Num"),
        pl.col("Pieces").cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int32).alias("Pieces"),
        pl.col("Delivery").cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int32).alias("Delivery"),
    ])

    logger.info(f"  [OK] Dates, payments, totals standardized")

    # =====================================================================
    # PHASE 6: COLLECTIONS
    # =====================================================================
    logger.info("\nPhase 6: Calculating collections...")

    has_inv_coll = pl.col("Collections_Inv").is_not_null() if "Collections_Inv" in df.columns else pl.lit(False)
    df = df.with_columns(
        pl.when(has_inv_coll)
        .then(pl.col("Collections_Inv").cast(pl.Float64, strict=False).fill_null(0))
        .when(pl.col("Paid") == 0)
        .then(pl.lit(0.0))
        .when(pl.col("Payment_Type_Std") == "Receivable")
        .then(pl.lit(0.0))
        .otherwise(pl.col("Total_Num"))
        .alias("Collections")
    )
    if "Collections_Inv" in df.columns:
        df = df.drop("Collections_Inv")

    # =====================================================================
    # PHASE 7: STORE & ID STANDARDIZATION
    # =====================================================================
    logger.info("\nPhase 7: Standardizing stores and IDs...")

    store_id_col = "Store ID" if "Store ID" in df.columns else None
    store_name_col = "Store Name" if "Store Name" in df.columns else None

    if store_id_col:
        df = df.with_columns(
            polars_store_std(store_id_col, store_name_col, "Source").alias("Store_Std")
        )
    else:
        # No store ID column — fallback for legacy
        df = df.with_columns(
            pl.when(pl.col("Source") == "Legacy").then(pl.lit("Moon Walk"))
            .otherwise(pl.lit(None, dtype=pl.Utf8))
            .alias("Store_Std")
        )

    initial_count = df.height
    df = df.filter(pl.col("Store_Std").is_not_null())
    logger.info(f"  [OK] After store filter: {df.height:,} rows ({initial_count - df.height:,} removed)")

    drop_cols = [c for c in ["Store ID", "Store Name"] if c in df.columns]
    if drop_cols:
        df = df.drop(drop_cols)

    # CustomerID_Std
    cid_col = "Customer ID" if "Customer ID" in df.columns else None
    if cid_col:
        df = df.with_columns(polars_customer_id_std(cid_col, "Source").alias("CustomerID_Std"))
    else:
        df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("CustomerID_Std"))

    # OrderID_Std
    df = df.with_columns(pl.lit("Order").alias("_tt_fallback"))
    if "Transaction_Type" not in df.columns:
        df = df.with_columns(pl.col("_tt_fallback").alias("Transaction_Type"))

    df = polars_order_id_std(df, "Order ID", "Store_Std", "Source", "Transaction_Type")
    df = df.drop("_tt_fallback")

    logger.info(f"  [OK] Store, Customer, Order IDs standardized")

    # =====================================================================
    # PHASE 8: FIX CUSTOMER IDS FOR SUBSCRIPTIONS/INVOICES
    # =====================================================================
    logger.info("\nPhase 8: Fixing Customer IDs for subscriptions/invoices...")

    is_sub_or_inv = pl.col("Transaction_Type").is_in(["Subscription", "Invoice Payment"])

    # Build name_std column for subs/invoices
    df = df.with_columns(
        pl.when(is_sub_or_inv & pl.col("Customer_Name").is_not_null())
        .then(polars_name_standardize(pl.col("Customer_Name")))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("_name_std")
    )

    # Map name -> CustomerID_Std
    name_std_list = df["_name_std"].to_list()
    cid_lookup = [customer_name_lookup.get(n) if n else None for n in name_std_list]
    df = df.with_columns(pl.Series("_cid_lookup", cid_lookup))

    # Overwrite CustomerID_Std for subs/invoices
    df = df.with_columns(
        pl.when(is_sub_or_inv & pl.col("_cid_lookup").is_not_null())
        .then(pl.col("_cid_lookup"))
        .otherwise(pl.col("CustomerID_Std"))
        .alias("CustomerID_Std")
    )
    df = df.drop(["_name_std", "_cid_lookup"])

    # =====================================================================
    # PHASE 9: FILTERING
    # =====================================================================
    logger.info("\nPhase 9: Filtering...")

    initial_count = df.height

    # Is_Earned flag
    df = df.with_columns(
        pl.when(pl.col("Earned_Date").is_not_null()).then(pl.lit(1)).otherwise(pl.lit(0))
        .cast(pl.Int32).alias("Is_Earned")
    )
    uncleaned = df.filter(pl.col("Is_Earned") == 0).height
    logger.info(f"  [INFO] {uncleaned:,} uncleaned orders (Is_Earned=0, preserved in output)")

    # Filter null IDs
    df = df.filter(pl.col("CustomerID_Std").is_not_null() & pl.col("OrderID_Std").is_not_null())
    logger.info(f"  [OK] After null ID filter: {df.height:,} rows ({initial_count - df.height:,} removed)")

    df = df.filter(~pl.col("CustomerID_Std").is_in(list(business_account_set)))
    logger.info(f"  [OK] After B2B filter: {df.height:,} rows")

    drop_raw = [c for c in ["Order ID", "Customer ID", "Total", "Customer_Name"] if c in df.columns]
    if drop_raw:
        df = df.drop(drop_raw)

    # =====================================================================
    # PHASE 10: MERGE CUSTOMER DATA
    # =====================================================================
    logger.info("\nPhase 10: Merging customer data...")

    cid_list = df["CustomerID_Std"].to_list()
    cohort_vals = [customer_cohort.get(c) for c in cid_list]
    route_vals = [customer_route.get(c, 0.0) for c in cid_list]

    df = df.with_columns([
        pl.Series("CohortMonth", cohort_vals).cast(pl.Datetime("us"), strict=False),
        pl.Series("Route #", route_vals).cast(pl.Float64),
    ])

    df = df.with_columns(polars_route_category("Route #").alias("Route_Category"))

    # =====================================================================
    # PHASE 11: FLAGS
    # =====================================================================
    logger.info("\nPhase 11: Adding flags...")

    df = df.with_columns([
        (pl.col("Delivery") == 1).cast(pl.Int32).alias("HasDelivery"),
        pl.col("Pickup Date").is_not_null().cast(pl.Int32).alias("HasPickup"),
        pl.when(pl.col("Delivery") == 1)
        .then(pl.col("Collected"))
        .otherwise(pl.lit(None, dtype=pl.Datetime("us")))
        .alias("Delivery_Date"),
    ])

    # MonthsSinceCohort
    df = df.with_columns(
        polars_months_since_cohort("OrderCohortMonth", "CohortMonth").alias("MonthsSinceCohort")
    )

    # =====================================================================
    # PHASE 12: SUBSCRIPTION FLAG
    # =====================================================================
    logger.info("\nPhase 12: Calculating subscription service flag...")

    df = polars_subscription_flag(df, subscription_dict)

    subscription_orders = df.filter(pl.col("IsSubscriptionService") == 1).height
    logger.info(f"  [OK] {subscription_orders:,} orders during active subscription")

    # =====================================================================
    # PHASE 13: TIME METRICS
    # =====================================================================
    logger.info("\nPhase 13: Calculating time metrics...")

    is_order = pl.col("Transaction_Type") == "Order"

    df = df.with_columns([
        pl.when(
            is_order & pl.col("Placed_Date").is_not_null() & pl.col("Cleaned").is_not_null()
        )
        .then((pl.col("Cleaned") - pl.col("Placed_Date")).dt.total_days().cast(pl.Float64))
        .otherwise(pl.lit(None, dtype=pl.Float64))
        .alias("Processing_Days"),

        pl.when(
            is_order & pl.col("Cleaned").is_not_null() & pl.col("Collected").is_not_null()
        )
        .then((pl.col("Collected") - pl.col("Cleaned")).dt.total_days().cast(pl.Float64))
        .otherwise(pl.lit(None, dtype=pl.Float64))
        .alias("TimeInStore_Days"),

        pl.when(
            pl.col("Placed_Date").is_not_null() & pl.col("Payment Date").is_not_null()
        )
        .then((pl.col("Payment Date") - pl.col("Placed_Date")).dt.total_days().cast(pl.Float64))
        .otherwise(pl.lit(None, dtype=pl.Float64))
        .alias("DaysToPayment"),
    ])

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
    df_final = df.select(existing_final)

    # Type enforcement
    int_cols = ['Delivery', 'HasDelivery', 'HasPickup', 'IsSubscriptionService', 'Paid', 'Pieces', 'Is_Earned']
    cast_exprs = []
    for col in int_cols:
        if col in df_final.columns:
            cast_exprs.append(pl.col(col).cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int32).alias(col))
    if cast_exprs:
        df_final = df_final.with_columns(cast_exprs)

    # Format dates
    date_output_cols = [
        'Placed_Date', 'Earned_Date', 'OrderCohortMonth', 'CohortMonth',
        'Ready By', 'Cleaned', 'Collected', 'Pickup Date', 'Payment Date', 'Delivery_Date'
    ]
    df_final = polars_format_dates_for_csv(df_final, date_output_cols)

    # Sort
    df_final = df_final.sort(['OrderCohortMonth', 'CustomerID_Std', 'OrderID_Std'])

    logger.info(f"  [OK] Final output: {df_final.height:,} rows x {len(df_final.columns)} columns")

    # Save
    df_final.write_csv(output_path)
    logger.info(f"  [OK] Saved to: {output_path}")

    # =====================================================================
    # VALIDATION SUMMARY
    # =====================================================================
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)

    logger.info(f"\nRow Counts:")
    logger.info(f"  Legacy Orders:        {df_final.filter(pl.col('Source') == 'Legacy').height:>8,}")
    logger.info(f"  CC Orders:            {df_final.filter((pl.col('Source') == 'CC_2025') & (pl.col('Transaction_Type') == 'Order')).height:>8,}")
    logger.info(f"  Subscriptions:        {df_final.filter(pl.col('Transaction_Type') == 'Subscription').height:>8,}")
    logger.info(f"  Invoice Payments:     {df_final.filter(pl.col('Transaction_Type') == 'Invoice Payment').height:>8,}")
    logger.info(f"  {'-' * 40}")
    logger.info(f"  TOTAL:                {df_final.height:>8,}")

    total_num = df_final["Total_Num"].cast(pl.Float64, strict=False).fill_null(0)
    orders_rev = df_final.filter(pl.col("Transaction_Type") == "Order").select(pl.col("Total_Num").sum()).item()
    subs_rev = df_final.filter(pl.col("Transaction_Type") == "Subscription").select(pl.col("Total_Num").sum()).item()
    inv_rev = df_final.filter(pl.col("Transaction_Type") == "Invoice Payment").select(pl.col("Total_Num").sum()).item()

    logger.info(f"\nRevenue:")
    logger.info(f"  Orders:               ${orders_rev:>12,.2f}")
    logger.info(f"  Subscriptions:        ${subs_rev:>12,.2f}")
    logger.info(f"  Invoices:             ${inv_rev:>12,.2f}")
    logger.info(f"  TOTAL:                ${(orders_rev + subs_rev + inv_rev):>12,.2f}")

    logger.info(f"\nKey Metrics:")
    logger.info(f"  Unique Customers:     {df_final['CustomerID_Std'].n_unique():>8,}")
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
