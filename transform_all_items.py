"""
All_Items Transformation Script (Polars)
Converts CC Items CSV with categorization and B2B filtering
"""

import polars as pl
import warnings
import os
from typing import Optional, Dict, Tuple, Union

from helpers import (
    find_cleancloud_file, polars_to_date, polars_store_std,
    polars_customer_id_std, polars_item_category, polars_service_type,
    polars_format_dates_for_csv,
)
from config import LOCAL_STAGING_PATH, MOONWALK_STORE_ID, HIELO_STORE_ID

from logger_config import setup_logger
logger = setup_logger(__name__)


# =====================================================================
# MAIN TRANSFORMATION
# =====================================================================

def run(shared_data: Optional[Dict[str, Union[pl.DataFrame]]] = None) -> Tuple[pl.DataFrame, str]:
    """
    Run All_Items transformation.

    Args:
        shared_data: dict with pre-loaded DataFrames:
            - 'items_csv': CC Items DataFrame (Polars)
            - 'customers_csv': CC Customers DataFrame (Polars)
            If None, loads from disk.

    Returns:
        (df_final, output_path) tuple
    """
    logger.info("=" * 70)
    logger.info("ALL_ITEMS TRANSFORMATION - POLARS")
    logger.info("=" * 70)
    logger.info("")
    output_path = os.path.join(LOCAL_STAGING_PATH, "All_Items_Python.csv")

    # =====================================================================
    # PHASE 1: LOAD BUSINESS ACCOUNTS
    # =====================================================================
    logger.info("Phase 1: Loading business accounts...")

    if shared_data and 'customers_csv' in shared_data:
        df_customers = shared_data['customers_csv']
        logger.info(f"  [OK] Using pre-loaded {df_customers.height:,} customers")
    else:
        customers_path = find_cleancloud_file('customer')
        df_customers = pl.read_csv(customers_path, infer_schema_length=10000)
        logger.info(f"  [OK] Loaded {df_customers.height:,} customers")

    # Get business account list (vectorized — no per-row loop)
    biz_ids = (
        df_customers
        .filter(
            pl.col("Business ID").is_not_null()
            & (pl.col("Business ID").cast(pl.Utf8) != "")
        )
        .select(pl.col("Customer ID").cast(pl.Utf8).str.replace_all(r"\D", ""))
        .to_series()
    )
    business_account_list = [f"CC-{d.zfill(4)}" for d in biz_ids.to_list() if d]

    logger.info(f"  [OK] Identified {len(business_account_list)} business accounts")

    # =====================================================================
    # PHASE 2: LOAD ITEMS CSV
    # =====================================================================
    logger.info("\nPhase 2: Loading items...")

    if shared_data and 'items_csv' in shared_data:
        df = shared_data['items_csv'].clone()
        logger.info(f"  [OK] Using pre-loaded {df.height:,} item rows")
    else:
        items_path = find_cleancloud_file('item')
        df = pl.read_csv(items_path, infer_schema_length=10000)
        logger.info(f"  [OK] Loaded {df.height:,} item rows")

    initial_count = df.height

    # =====================================================================
    # PHASE 2C: CREATE ITEMDATE AND ITEMCOHORTMONTH
    # =====================================================================
    logger.info("\nPhase 2c: Creating ItemDate and ItemCohortMonth...")

    df = polars_to_date(df, "Placed", alias="ItemDate")
    df = df.with_columns(
        pl.col("ItemDate").dt.truncate("1mo").alias("ItemCohortMonth")
    )

    valid_dates = df.filter(pl.col("ItemDate").is_not_null()).height
    logger.info(f"  [OK] ItemDate and ItemCohortMonth created:")
    logger.info(f"    - Valid dates:  {valid_dates:,} items ({valid_dates/df.height*100:.1f}%)")

    df = df.drop("Placed")

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
    df = df.drop(existing_to_remove)
    logger.info(f"  [OK] Removed {len(existing_to_remove)} unnecessary columns")

    # =====================================================================
    # PHASE 4: ADD SOURCE
    # =====================================================================
    df = df.with_columns(pl.lit("CC_2025").alias("Source"))

    # =====================================================================
    # PHASE 5: VECTORIZED STANDARDIZATION
    # =====================================================================
    logger.info("\nPhase 5: Standardizing stores and IDs...")

    df = df.with_columns(
        polars_store_std("Store ID").alias("Store_Std")
    )

    # Filter for known stores only
    before = df.height
    df = df.filter(pl.col("Store_Std").is_not_null())
    logger.info(f"  [OK] After store filter: {df.height:,} rows ({before - df.height:,} removed)")

    df = df.with_columns(
        polars_customer_id_std("Customer ID", "Source").alias("CustomerID_Std")
    )

    # OrderID_Std for items: all are regular orders, use store prefix + padded order ID
    raw_oid = pl.col("Order ID").cast(pl.Utf8).str.replace_all(r"\D", "")
    padded_oid = raw_oid.str.zfill(5)
    is_hielo = pl.col("Store_Std") == "Hielo"
    df = df.with_columns(
        pl.when(is_hielo)
        .then(pl.lit("H-") + padded_oid)
        .otherwise(pl.lit("M-") + padded_oid)
        .alias("OrderID_Std")
    )

    logger.info(f"  [OK] Standardized Customer and Order IDs")

    # =====================================================================
    # PHASE 7: ITEM CATEGORIZATION
    # =====================================================================
    logger.info("\nPhase 7: Categorizing items...")

    df = df.with_columns(
        polars_item_category("Item", "Section").alias("Item_Category")
    )

    category_counts = df.group_by("Item_Category").len().sort("len", descending=True)
    logger.info(f"  [OK] Item categories:")
    for row in category_counts.iter_rows():
        logger.info(f"    - {row[0]}: {row[1]:,}")

    # =====================================================================
    # PHASE 8: SERVICE TYPE CATEGORIZATION
    # =====================================================================
    logger.info("\nPhase 8: Categorizing service types...")

    df = df.with_columns(
        polars_service_type("Section").alias("Service_Type")
    )

    service_counts = df.group_by("Service_Type").len().sort("len", descending=True)
    logger.info(f"  [OK] Service types:")
    for row in service_counts.iter_rows():
        logger.info(f"    - {row[0]}: {row[1]:,}")

    # =====================================================================
    # PHASE 9: ADD BUSINESS ACCOUNT FLAG
    # =====================================================================
    logger.info("\nPhase 9: Flagging business accounts...")

    df = df.with_columns(
        pl.col("CustomerID_Std").is_in(business_account_list).cast(pl.Int32).alias("IsBusinessAccount")
    )

    b2b_count = df.filter(pl.col("IsBusinessAccount") == 1).height
    b2c_count = df.filter(pl.col("IsBusinessAccount") == 0).height
    logger.info(f"  [OK] Business account items: {b2b_count:,} ({b2b_count/df.height*100:.1f}%)")
    logger.info(f"  [OK] Consumer items: {b2c_count:,} ({b2c_count/df.height*100:.1f}%)")

    # =====================================================================
    # PHASE 10: FINAL CLEANUP
    # =====================================================================
    logger.info("\nPhase 10: Final cleanup...")

    columns_to_remove_final = [
        'Store ID', 'Item ID', 'Section ID', 'Order ID',
        'Product ID', 'Custom Product ID', 'Customer ID', 'Custom ID',
        'Customer'
    ]
    existing_to_remove_final = [col for col in columns_to_remove_final if col in df.columns]
    df = df.drop(existing_to_remove_final)
    logger.info(f"  [OK] Removed {len(existing_to_remove_final)} intermediate columns")

    # =====================================================================
    # PHASE 11: SET DATA TYPES
    # =====================================================================
    logger.info("\nPhase 11: Setting data types...")

    df = df.with_columns([
        pl.col("Total").cast(pl.Float64, strict=False).fill_null(0.0),
        pl.col("Quantity").cast(pl.Int64, strict=False).fill_null(0),
        pl.col("Express").cast(pl.Int64, strict=False).fill_null(0),
    ])
    logger.info(f"  [OK] Set data types")

    # =====================================================================
    # PHASE 12: FINAL OUTPUT
    # =====================================================================
    logger.info("\nPhase 12: Preparing final output...")

    final_columns = [
        'Source', 'Store_Std', 'CustomerID_Std', 'OrderID_Std',
        'ItemDate', 'ItemCohortMonth', 'Item', 'Section',
        'Quantity', 'Total', 'Express',
        'Item_Category', 'Service_Type', 'IsBusinessAccount'
    ]
    available_columns = [col for col in final_columns if col in df.columns]
    df_final = df.select(available_columns)

    # Format dates
    df_final = polars_format_dates_for_csv(df_final, ['ItemDate', 'ItemCohortMonth'])

    # Sort for consistency
    df_final = df_final.sort(['Store_Std', 'OrderID_Std', 'Item'])

    logger.info(f"  [OK] Final output: {df_final.height:,} rows x {len(df_final.columns)} columns")

    # =====================================================================
    # PHASE 13: SAVE OUTPUT
    # =====================================================================
    logger.info("\nPhase 13: Saving output...")

    df_final.write_csv(output_path)
    logger.info(f"  [OK] Saved to: {output_path}")

    # =====================================================================
    # VALIDATION SUMMARY
    # =====================================================================
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)

    logger.info(f"\nRow Count:")
    logger.info(f"  Initial:              {initial_count:>8,}")
    logger.info(f"  Final (ALL items):    {df_final.height:>8,}")

    logger.info(f"\nCustomer Types:")
    b2b_items = df_final.filter(pl.col("IsBusinessAccount") == 1).height
    b2c_items = df_final.filter(pl.col("IsBusinessAccount") == 0).height
    logger.info(f"  B2B Items:            {b2b_items:>8,} ({b2b_items/df_final.height*100:>5.1f}%)")
    logger.info(f"  B2C Items:            {b2c_items:>8,} ({b2c_items/df_final.height*100:>5.1f}%)")

    logger.info(f"\nKey Metrics:")
    logger.info(f"  Unique Orders:        {df_final['OrderID_Std'].n_unique():>8,}")
    logger.info(f"  Unique Customers:     {df_final['CustomerID_Std'].n_unique():>8,}")

    qty_col = df_final['Quantity'].cast(pl.Int64, strict=False).fill_null(0)
    total_col = df_final['Total'].cast(pl.Float64, strict=False).fill_null(0.0)
    logger.info(f"  Total Items:          {qty_col.sum():>8,}")
    logger.info(f"  Total Revenue:        ${total_col.sum():>11,.2f}")

    logger.info(f"\nItem Categories:")
    for row in df_final.group_by("Item_Category").len().sort("len", descending=True).iter_rows():
        pct = row[1] / df_final.height * 100
        logger.info(f"  {row[0]:<20}: {row[1]:>6,} ({pct:>5.1f}%)")

    logger.info(f"\nService Types:")
    for row in df_final.group_by("Service_Type").len().sort("len", descending=True).iter_rows():
        pct = row[1] / df_final.height * 100
        logger.info(f"  {row[0]:<20}: {row[1]:>6,} ({pct:>5.1f}%)")

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
