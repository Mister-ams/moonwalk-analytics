"""
All_Customers Transformation Script (Polars)
Combines CC customers and Legacy customers into master customer table
"""

import polars as pl
import warnings
import os
from typing import Optional, Dict, Tuple, Union

from helpers import (
    find_cleancloud_file,
    polars_to_date,
    polars_store_std,
    polars_customer_id_std,
    polars_format_dates_for_csv,
)
from config import LOCAL_STAGING_PATH

from logger_config import setup_logger

logger = setup_logger(__name__)


# =====================================================================
# MAIN TRANSFORMATION
# =====================================================================


def run(shared_data: Optional[Dict[str, Union[pl.DataFrame]]] = None) -> Tuple[pl.DataFrame, str]:
    """
    Run All_Customers transformation.

    Args:
        shared_data: dict with pre-loaded DataFrames:
            - 'customers_csv': CC customers DataFrame (Polars)
            If None, loads from disk.

    Returns:
        (df_final, output_path) tuple
    """
    logger.info("=" * 70)
    logger.info("ALL_CUSTOMERS TRANSFORMATION - POLARS")
    logger.info("=" * 70)
    logger.info("")
    output_path = os.path.join(LOCAL_STAGING_PATH, "All_Customers_Python.csv")
    legacy_path = os.path.join(LOCAL_STAGING_PATH, "RePos_Archive.csv")

    # =====================================================================
    # PHASE 1: LOAD CC CUSTOMERS
    # =====================================================================
    logger.info("Phase 1: Loading CC customers...")

    if shared_data and "customers_csv" in shared_data:
        df_cc = shared_data["customers_csv"].clone()
        logger.info(f"  [OK] Using pre-loaded {df_cc.height:,} CC customer rows")
    else:
        cc_path = find_cleancloud_file("customer")
        df_cc = pl.read_csv(cc_path, infer_schema_length=10000)
        logger.info(f"  [OK] Loaded {df_cc.height:,} CC customer rows")

    # Keep only needed columns (Phone + Email for Invoice Automation lookup)
    cc_wanted = [
        "Customer ID",
        "Name",
        "Store ID",
        "Signed Up Date",
        "Route #",
        "Business ID",
        "Phone",
        "Email",
    ]
    existing_cc = [col for col in cc_wanted if col in df_cc.columns]
    df_cc = df_cc.select(existing_cc)

    # Source
    df_cc = df_cc.with_columns(pl.lit("CC_2025").alias("Source_System"))

    # CustomerID_Raw — preserve original numeric ID for cross-system lookup
    df_cc = df_cc.with_columns(pl.col("Customer ID").cast(pl.Int32, strict=False).alias("CustomerID_Raw"))

    # CustomerID_Std
    df_cc = df_cc.with_columns(polars_customer_id_std("Customer ID", "Source_System").alias("CustomerID_Std"))

    # CustomerName
    df_cc = df_cc.with_columns(
        pl.when(pl.col("Name").is_not_null() & (pl.col("Name").cast(pl.Utf8).str.strip_chars() != ""))
        .then(pl.col("Name").cast(pl.Utf8).str.strip_chars())
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("CustomerName")
    )

    # Store_Std
    df_cc = df_cc.with_columns(polars_store_std("Store ID").alias("Store_Std"))

    # SignedUp_Date
    if "Signed Up Date" in df_cc.columns:
        df_cc = polars_to_date(df_cc, "Signed Up Date", alias="SignedUp_Date")
    else:
        df_cc = df_cc.with_columns(pl.lit(None, dtype=pl.Datetime("us")).alias("SignedUp_Date"))

    # CohortMonth
    df_cc = df_cc.with_columns(pl.col("SignedUp_Date").dt.truncate("1mo").alias("CohortMonth"))

    # Route #
    if "Route #" in df_cc.columns:
        df_cc = df_cc.with_columns(
            pl.col("Route #").cast(pl.Float64, strict=False).fill_null(0).cast(pl.Int32).alias("Route #")
        )
    else:
        df_cc = df_cc.with_columns(pl.lit(0).cast(pl.Int32).alias("Route #"))

    # IsBusinessAccount
    if "Business ID" in df_cc.columns:
        df_cc = df_cc.with_columns(
            (pl.col("Business ID").cast(pl.Utf8).fill_null("") != "").cast(pl.Int32).alias("IsBusinessAccount")
        )
    else:
        df_cc = df_cc.with_columns(pl.lit(0).cast(pl.Int32).alias("IsBusinessAccount"))

    # Phone — strip whitespace, null out junk values ("0", empty)
    if "Phone" in df_cc.columns:
        df_cc = df_cc.with_columns(
            pl.when(
                pl.col("Phone").cast(pl.Utf8).str.strip_chars().is_in(["", "0", "00", "000000", "00000000"])
                | pl.col("Phone").is_null()
            )
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(pl.col("Phone").cast(pl.Utf8).str.strip_chars())
            .alias("Phone")
        )
    else:
        df_cc = df_cc.with_columns(pl.lit(None, dtype=pl.Utf8).alias("Phone"))

    # Email — strip whitespace, null out empty
    if "Email" in df_cc.columns:
        df_cc = df_cc.with_columns(
            pl.when(pl.col("Email").is_null() | (pl.col("Email").cast(pl.Utf8).str.strip_chars() == ""))
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(pl.col("Email").cast(pl.Utf8).str.strip_chars().str.to_lowercase())
            .alias("Email")
        )
    else:
        df_cc = df_cc.with_columns(pl.lit(None, dtype=pl.Utf8).alias("Email"))

    final_cols = [
        "CustomerID_Std",
        "CustomerID_Raw",
        "CustomerName",
        "Store_Std",
        "SignedUp_Date",
        "CohortMonth",
        "Route #",
        "IsBusinessAccount",
        "Source_System",
        "Phone",
        "Email",
    ]
    df_cc_clean = df_cc.select(final_cols)

    logger.info(f"  [OK] Processed {df_cc_clean.height:,} CC customers")
    logger.info(f"  [OK] Business accounts: {df_cc_clean.filter(pl.col('IsBusinessAccount') == 1).height:,}")

    # =====================================================================
    # PHASE 2: LOAD LEGACY CUSTOMERS
    # =====================================================================
    logger.info("\nPhase 2: Loading Legacy customers...")

    if shared_data and "legacy_csv" in shared_data:
        df_legacy = shared_data["legacy_csv"].clone()
    else:
        df_legacy = pl.read_csv(legacy_path, infer_schema_length=10000)

    initial_legacy_count = df_legacy.height
    logger.info(f"  [OK] Loaded {initial_legacy_count:,} legacy order rows")

    legacy_wanted = ["Customer ID", "Customer", "Placed"]
    existing_legacy = [col for col in legacy_wanted if col in df_legacy.columns]
    df_legacy = df_legacy.select(existing_legacy)

    # Parse Placed date
    if "Placed" in df_legacy.columns:
        df_legacy = polars_to_date(df_legacy, "Placed")

    # CustomerID_Raw
    df_legacy = df_legacy.with_columns(pl.col("Customer ID").cast(pl.Int32, strict=False).alias("CustomerID_Raw"))

    # CustomerID_Std
    df_legacy = df_legacy.with_columns(pl.lit("Legacy").alias("_source"))
    df_legacy = df_legacy.with_columns(polars_customer_id_std("Customer ID", "_source").alias("CustomerID_Std"))

    # Group by CustomerID_Std: first non-null name, earliest date, first raw ID
    legacy_grouped = df_legacy.group_by("CustomerID_Std").agg(
        [
            pl.col("CustomerID_Raw").first(),
            pl.col("Customer").drop_nulls().first().alias("CustomerName"),
            pl.col("Placed").min().alias("SignedUp_Date"),
        ]
    )

    # CohortMonth
    legacy_grouped = legacy_grouped.with_columns(pl.col("SignedUp_Date").dt.truncate("1mo").alias("CohortMonth"))

    legacy_grouped = legacy_grouped.with_columns(
        [
            pl.lit("Moon Walk").alias("Store_Std"),
            pl.lit(0).cast(pl.Int32).alias("Route #"),
            pl.lit(0).cast(pl.Int32).alias("IsBusinessAccount"),
            pl.lit("Legacy").alias("Source_System"),
            pl.lit(None, dtype=pl.Utf8).alias("Phone"),
            pl.lit(None, dtype=pl.Utf8).alias("Email"),
        ]
    )

    df_legacy_clean = legacy_grouped.select(final_cols)
    logger.info(f"  [OK] Processed {df_legacy_clean.height:,} unique Legacy customers")

    # =====================================================================
    # PHASE 3: COMBINE & OUTPUT
    # =====================================================================
    logger.info("\nPhase 3: Combining CC and Legacy customers...")

    df_all = pl.concat([df_cc_clean, df_legacy_clean], how="diagonal_relaxed")
    logger.info(f"  [OK] Combined: {df_all.height:,} total customers")

    # Format dates
    df_all = polars_format_dates_for_csv(df_all, ["SignedUp_Date", "CohortMonth"])

    # Sort
    df_all = df_all.sort("CustomerID_Std")

    # Save
    df_all.write_csv(output_path)
    logger.info(f"  [OK] Saved to: {output_path}")

    # =====================================================================
    # VALIDATION SUMMARY
    # =====================================================================
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)

    logger.info(f"\nCustomer Counts:")
    logger.info(f"  CC Customers:                {df_all.filter(pl.col('Source_System') == 'CC_2025').height:>8,}")
    logger.info(f"  Legacy Customers:            {df_all.filter(pl.col('Source_System') == 'Legacy').height:>8,}")
    logger.info(f"  {'-' * 40}")
    logger.info(f"  TOTAL:                       {df_all.height:>8,}")

    logger.info(f"\nStore Distribution:")
    for row in df_all.group_by("Store_Std").len().sort("Store_Std").iter_rows():
        pct = row[1] / df_all.height * 100
        logger.info(f"  {row[0]:<20}: {row[1]:>6,} ({pct:>5.1f}%)")

    business_count = df_all.filter(pl.col("IsBusinessAccount") == 1).height
    logger.info(f"\nBusiness Accounts:           {business_count:>8,} ({business_count / df_all.height * 100:>5.1f}%)")

    null_names = df_all.filter(pl.col("CustomerName").is_null()).height
    null_cohort = df_all.filter(pl.col("CohortMonth").is_null()).height
    has_phone = df_all.filter(pl.col("Phone").is_not_null()).height
    has_email = df_all.filter(pl.col("Email").is_not_null()).height
    logger.info(f"  Null names:                  {null_names:>8,}")
    logger.info(f"  Null cohorts:                {null_cohort:>8,}")
    logger.info(f"  With phone:                  {has_phone:>8,} ({has_phone / df_all.height * 100:>5.1f}%)")
    logger.info(f"  With email:                  {has_email:>8,} ({has_email / df_all.height * 100:>5.1f}%)")

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
