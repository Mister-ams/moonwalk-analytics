"""
Helper Functions for Moonwalk ETL Pipeline (Polars)

Shared utility functions used across all transform scripts.
All vectorized helpers use Polars expressions for high-performance data processing.
"""

import polars as pl
from datetime import date
from pathlib import Path
from typing import Optional, Dict, List, Any

from config import (
    DOWNLOADS_PATH, LOCAL_STAGING_PATH,
    EXCEL_SERIAL_DATE_BASE, MOONWALK_STORE_ID, HIELO_STORE_ID,
    SUBSCRIPTION_VALIDITY_DAYS,
)

from logger_config import setup_logger
logger = setup_logger(__name__)


# =====================================================================
# FILE DISCOVERY (unchanged — pure Path logic)
# =====================================================================

def find_cleancloud_file(pattern: str, downloads_path: Optional[str] = None, required: bool = True) -> Optional[str]:
    """Find the most recent CleanCloud file matching pattern in Downloads."""
    downloads = Path(downloads_path or DOWNLOADS_PATH)
    pattern_lower = pattern.lower()

    matches = [
        f for f in downloads.glob("*.csv")
        if pattern_lower in f.name.lower()
        and not f.name.startswith('Excel_')
        and 'CC-' in f.name
    ]

    if not matches:
        if required:
            raise FileNotFoundError(
                f"\nERROR: No CleanCloud file found matching '{pattern}' in Downloads folder.\n"
                f"Please download the {pattern} report from CleanCloud first.\n"
                f"Expected filename format: CC-{pattern.title()}-*.csv\n"
                f"Looking in: {downloads}"
            )
        return None

    latest = max(matches, key=lambda p: p.stat().st_mtime)
    logger.info(f"  [OK] Found: {latest.name}")
    return str(latest)


# =====================================================================
# DATE CONVERSION
# =====================================================================

def polars_to_date(df: pl.DataFrame, col: str, alias: Optional[str] = None) -> pl.DataFrame:
    """
    Parse a column with mixed date formats into Date/Datetime.

    Handles:
      - ISO dates: '2025-01-15', '2025-03-21 01:29:10'
      - Human dates: '21 Mar 2025 00:39'
      - Excel serial numbers: 45292
      - Null/empty values -> null

    Returns DataFrame with the column replaced (or aliased).
    Warns when >5% of non-null values fail to parse.
    """
    out_name = alias or col

    # Build expression using column reference
    raw = pl.col(col).cast(pl.Utf8).str.strip_chars()

    is_blank = raw.is_null() | (raw == pl.lit("")) | (raw == pl.lit("nan")) | (raw == pl.lit("None"))
    is_numeric = raw.str.contains(r"^\d{1,5}(?:\.0*)?$") & ~is_blank
    is_text = ~is_numeric & ~is_blank

    # Excel serial numbers
    numeric_days = raw.str.replace(r"\.0*$", "").str.to_integer(strict=False)
    serial_date_expr = (
        pl.lit(date(1899, 12, 30)).cast(pl.Datetime("us"))
        + pl.duration(days=numeric_days)
    )

    # Text date formats
    parsed_iso = raw.str.to_datetime("%Y-%m-%d", strict=False)
    parsed_iso_full = raw.str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False)
    parsed_human = raw.str.to_datetime("%d %b %Y %H:%M", strict=False)
    parsed_human2 = raw.str.to_datetime("%d %b %Y", strict=False)

    combined_expr = (
        pl.when(is_numeric & numeric_days.is_not_null() & numeric_days.is_between(2, 99998))
        .then(serial_date_expr)
        .when(is_text & parsed_iso.is_not_null())
        .then(parsed_iso)
        .when(is_text & parsed_iso_full.is_not_null())
        .then(parsed_iso_full)
        .when(is_text & parsed_human.is_not_null())
        .then(parsed_human)
        .when(is_text & parsed_human2.is_not_null())
        .then(parsed_human2)
        .otherwise(pl.lit(None, dtype=pl.Datetime("us")))
        .alias(out_name)
    )

    # Materialize
    result = df.with_columns(combined_expr)

    # NaT rate warning (computed on materialized column)
    raw_series = df[col]
    raw_str = raw_series.cast(pl.Utf8, strict=False).fill_null("")
    non_null_mask = (raw_str != "") & (raw_str != "nan") & (raw_str != "None")
    non_null_count = non_null_mask.sum()

    if non_null_count > 0:
        result_col = result[out_name]
        nat_count = int(result_col.filter(non_null_mask).is_null().sum())
        nat_pct = nat_count / non_null_count * 100
        col_label = f" [{col}]"
        if nat_pct > 5:
            logger.warning(
                f"  [WARN] polars_to_date{col_label}: {nat_count:,}/{non_null_count:,} "
                f"({nat_pct:.1f}%) parsed to null -- possible format change in source CSV"
            )
        elif nat_count > 0:
            logger.debug(
                f"  polars_to_date{col_label}: {nat_count:,}/{non_null_count:,} "
                f"({nat_pct:.1f}%) parsed to null"
            )

    return result


# =====================================================================
# NAME STANDARDIZATION
# =====================================================================

def polars_name_standardize(expr: pl.Expr) -> pl.Expr:
    """Vectorized name standardization: uppercase, strip spaces/hyphens/dots/apostrophes/commas."""
    return (
        expr.fill_null("")
        .cast(pl.Utf8)
        .str.to_uppercase()
        .str.strip_chars()
        .str.replace_all(" ", "", literal=True)
        .str.replace_all("-", "", literal=True)
        .str.replace_all(".", "", literal=True)
        .str.replace_all("'", "", literal=True)
        .str.replace_all(",", "", literal=True)
    )


# =====================================================================
# STORE STANDARDIZATION
# =====================================================================

def polars_store_std(store_id_col: str,
                     store_name_col: Optional[str] = None,
                     source_col: Optional[str] = None) -> pl.Expr:
    """
    Vectorized store standardization.
    Returns 'Moon Walk', 'Hielo', or null.
    """
    digits = pl.col(store_id_col).cast(pl.Utf8).str.replace_all(r"\D", "")

    result = (
        pl.when(digits == MOONWALK_STORE_ID).then(pl.lit("Moon Walk"))
        .when(digits == HIELO_STORE_ID).then(pl.lit("Hielo"))
    )

    # Fallback to Store Name
    if store_name_col:
        upper_name = pl.col(store_name_col).cast(pl.Utf8).str.to_uppercase()
        result = (
            result
            .when(upper_name.str.contains("MOON")).then(pl.lit("Moon Walk"))
            .when(upper_name.str.contains("HIELO")).then(pl.lit("Hielo"))
        )

    # Fallback for Legacy
    if source_col:
        result = result.when(pl.col(source_col) == "Legacy").then(pl.lit("Moon Walk"))

    return result.otherwise(pl.lit(None, dtype=pl.Utf8))


# =====================================================================
# CUSTOMER ID STANDARDIZATION
# =====================================================================

def polars_customer_id_std(customer_id_col: str, source_col: str) -> pl.Expr:
    """
    Vectorized CustomerID_Std: returns 'MW-xxxx' or 'CC-xxxx'.
    """
    raw = pl.col(customer_id_col).cast(pl.Utf8).str.strip_chars()

    # Already formatted (MW- or CC- prefix)
    is_preformatted = raw.str.starts_with("MW-") | raw.str.starts_with("CC-")

    # Extract digits and pad
    digits = raw.str.replace_all(r"\D", "")
    padded = digits.str.zfill(4)

    is_legacy = pl.col(source_col) == "Legacy"

    return (
        pl.when(is_preformatted)
        .then(raw)
        .when(pl.col(customer_id_col).is_not_null() & (digits != ""))
        .then(
            pl.when(is_legacy)
            .then(pl.lit("MW-") + padded)
            .otherwise(pl.lit("CC-") + padded)
        )
        .otherwise(pl.lit(None, dtype=pl.Utf8))
    )


# =====================================================================
# ORDER ID STANDARDIZATION
# =====================================================================

def polars_order_id_std(df: pl.DataFrame,
                        order_id_col: str = "Order ID",
                        store_std_col: str = "Store_Std",
                        source_col: str = "Source",
                        transaction_type_col: str = "Transaction_Type",
                        ) -> pl.DataFrame:
    """
    Vectorized OrderID_Std.

    Multi-stage when/then chain:
    1. Subscription -> S-xxxxx (row index)
    2. Invoice Payment -> I-xxxxx (row index)
    3. Already R-xxxxx -> keep
    4. Legacy R format (no dash) -> R-rest
    5. Already H- or M- -> keep
    6. Remaining: pad digits, prefix by store
    """
    n = df.height
    idx_pad = pl.Series("_row_idx", range(1, n + 1)).cast(pl.Utf8).str.zfill(5)
    df = df.with_columns(idx_pad.alias("_row_idx"))

    raw = pl.col(order_id_col).cast(pl.Utf8).str.strip_chars()
    digits = raw.str.replace_all(r"\D", "")
    padded = digits.str.zfill(5)

    is_sub = pl.col(transaction_type_col) == "Subscription"
    is_inv = pl.col(transaction_type_col) == "Invoice Payment"
    starts_r_dash = raw.str.starts_with("R-")
    starts_r_no_dash = raw.str.starts_with("R") & ~raw.str.starts_with("R-")
    starts_h_or_m = raw.str.contains(r"^[HM]-")
    is_hielo = pl.col(store_std_col) == "Hielo"
    has_order_id = pl.col(order_id_col).is_not_null()

    order_id_std = (
        pl.when(is_sub)
        .then(pl.lit("S-") + pl.col("_row_idx"))
        .when(is_inv)
        .then(pl.lit("I-") + pl.col("_row_idx"))
        .when(has_order_id & starts_r_dash)
        .then(raw)
        .when(has_order_id & starts_r_no_dash)
        .then(pl.lit("R-") + raw.str.slice(1))
        .when(has_order_id & starts_h_or_m)
        .then(raw)
        .when(has_order_id & (digits != ""))
        .then(
            pl.when(is_hielo)
            .then(pl.lit("H-") + padded)
            .otherwise(pl.lit("M-") + padded)
        )
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("OrderID_Std")
    )

    return df.with_columns(order_id_std).drop("_row_idx")


# =====================================================================
# PAYMENT TYPE STANDARDIZATION
# =====================================================================

def polars_payment_type_std(col: str = "Payment Type") -> pl.Expr:
    """Vectorized payment type standardization."""
    pt = pl.col(col).cast(pl.Utf8).str.to_uppercase().str.strip_chars()
    return (
        pl.when(pt.str.contains("CASH")).then(pl.lit("Cash"))
        .when(pt.str.contains("CARD|TERMINAL")).then(pl.lit("Terminal"))
        .when(pt.str.contains("BANK|STRIPE")).then(pl.lit("Stripe"))
        .when(pt.str.contains("INVOICE")).then(pl.lit("Receivable"))
        .otherwise(pl.lit("Other"))
    )


# =====================================================================
# ITEM CATEGORIZATION
# =====================================================================

def polars_item_category(item_col: str = "Item", section_col: str = "Section") -> pl.Expr:
    """Vectorized item categorization using regex on combined text."""
    combined = (
        pl.col(item_col).fill_null("").cast(pl.Utf8).str.to_lowercase()
            .str.replace_all(r"[\s\-&']", "")
        + pl.col(section_col).fill_null("").cast(pl.Utf8).str.to_lowercase()
            .str.replace_all(r"[\s\-&']", "")
    )

    trad = combined.str.contains(
        "kandura|kandoora|thobe|abaya|sheyla|shayla|hijab|ghutra|jalabeya"
    )
    linen = combined.str.contains(
        "duvet|comforter|bedsheet|sheet|pillowcase|pillow|towel|curtain|tablecloth"
    )
    prof = combined.str.contains(
        "uniform|suit|blazer|jacket|shirt|blouse|top|polo|pant|trouser"
    )
    extras = combined.str.contains(
        "shoe|carpet|tailor|alteration"
    )

    return (
        pl.when(trad).then(pl.lit("Traditional Wear"))
        .when(~trad & linen).then(pl.lit("Home Linens"))
        .when(~trad & ~linen & prof).then(pl.lit("Professional Wear"))
        .when(~trad & ~linen & ~prof & extras).then(pl.lit("Extras"))
        .otherwise(pl.lit("Others"))
    )


# =====================================================================
# SERVICE TYPE CATEGORIZATION
# =====================================================================

def polars_service_type(section_col: str = "Section") -> pl.Expr:
    """Vectorized service type categorization."""
    s = pl.col(section_col).fill_null("").cast(pl.Utf8).str.to_lowercase().str.replace_all(r"[\s\-&']", "")

    is_dry = s.str.contains("drycle|dryclean")
    is_wash = s.str.contains("wash|laund")
    is_press = s.str.contains("press|iron")

    return (
        pl.when(is_dry).then(pl.lit("Dry Cleaning"))
        .when(~is_dry & is_wash).then(pl.lit("Wash & Press"))
        .when(~is_dry & ~is_wash & is_press).then(pl.lit("Press Only"))
        .otherwise(pl.lit("Other Service"))
    )


# =====================================================================
# ROUTE CATEGORY
# =====================================================================

def polars_route_category(route_col: str = "Route #") -> pl.Expr:
    """Vectorized route category."""
    route = pl.col(route_col).cast(pl.Float64, strict=False).fill_null(0)
    return (
        pl.when((route >= 1) & (route <= 3)).then(pl.lit("Inside Abu Dhabi"))
        .when(route > 3).then(pl.lit("Outer Abu Dhabi"))
        .otherwise(pl.lit("Other"))
    )


# =====================================================================
# MONTHS SINCE COHORT
# =====================================================================

def polars_months_since_cohort(order_month_col: str, cohort_month_col: str) -> pl.Expr:
    """Vectorized month difference calculation."""
    om = pl.col(order_month_col)
    cm = pl.col(cohort_month_col)
    return (
        (om.dt.year() - cm.dt.year()) * 12
        + (om.dt.month() - cm.dt.month())
    ).cast(pl.Int32, strict=False)


# =====================================================================
# SUBSCRIPTION FLAG
# =====================================================================

def _merge_overlapping_periods(periods: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge overlapping or adjacent subscription periods for a single customer."""
    if len(periods) <= 1:
        return periods
    sorted_periods = sorted(periods, key=lambda p: p["ValidFrom"])
    merged = [sorted_periods[0].copy()]
    for p in sorted_periods[1:]:
        last = merged[-1]
        if p["ValidFrom"] <= last["ValidUntil"]:
            last["ValidUntil"] = max(last["ValidUntil"], p["ValidUntil"])
        else:
            merged.append(p.copy())
    return merged


def polars_subscription_flag(
    df: pl.DataFrame,
    subscription_dict: Dict[str, List[Dict[str, Any]]],
) -> pl.DataFrame:
    """
    Vectorized subscription flag via join + date range filter.

    df must have: CustomerID_Std, Earned_Date, Transaction_Type, OrderID_Std
    Returns df with IsSubscriptionService column (0/1).
    """
    # Start with all zeros
    df = df.with_columns(pl.lit(0).alias("IsSubscriptionService"))

    # Only check orders
    orders_mask = (
        (pl.col("Transaction_Type") == "Order")
        & pl.col("Earned_Date").is_not_null()
        & pl.col("CustomerID_Std").is_not_null()
    )

    if not subscription_dict:
        return df

    # Merge overlapping periods per customer to prevent cartesian join explosion
    merged_dict = {}
    overlap_count = 0
    for cid, periods in subscription_dict.items():
        merged = _merge_overlapping_periods(periods)
        if len(merged) < len(periods):
            overlap_count += len(periods) - len(merged)
        merged_dict[cid] = merged

    if overlap_count > 0:
        logger.warning(
            f"  [WARN] Merged {overlap_count} overlapping subscription periods"
        )

    # Build subscription periods DataFrame from merged periods
    sub_records = []
    for cid, periods in merged_dict.items():
        for p in periods:
            sub_records.append({
                "CustomerID_Std": cid,
                "ValidFrom": p["ValidFrom"],
                "ValidUntil": p["ValidUntil"],
            })

    if not sub_records:
        return df

    sub_df = pl.DataFrame(sub_records)
    # Ensure datetime types match
    sub_df = sub_df.with_columns([
        pl.col("ValidFrom").cast(pl.Datetime("us"), strict=False),
        pl.col("ValidUntil").cast(pl.Datetime("us"), strict=False),
    ])

    # Get orders that could have subscriptions
    orders = df.filter(orders_mask).select(["CustomerID_Std", "Earned_Date", "OrderID_Std"])

    # Join on customer, then filter by date range
    merged = orders.join(sub_df, on="CustomerID_Std", how="inner")
    covered = merged.filter(
        (pl.col("Earned_Date") >= pl.col("ValidFrom"))
        & (pl.col("Earned_Date") <= pl.col("ValidUntil"))
    )
    covered_ids = covered.select("OrderID_Std").unique().to_series()

    # Update the flag
    df = df.with_columns(
        pl.when(pl.col("OrderID_Std").is_in(covered_ids))
        .then(pl.lit(1))
        .otherwise(pl.col("IsSubscriptionService"))
        .alias("IsSubscriptionService")
    )

    return df


# =====================================================================
# DATE FORMATTING FOR CSV OUTPUT
# =====================================================================

def polars_format_dates_for_csv(df: pl.DataFrame, date_columns: List[str]) -> pl.DataFrame:
    """
    Format datetime columns as ISO 8601 (YYYY-MM-DD) strings for CSV output.
    Nulls become empty strings.
    """
    exprs = []
    for col in date_columns:
        if col in df.columns:
            c = pl.col(col)
            # Cast to datetime if needed, then format
            formatted = (
                pl.when(c.is_not_null())
                .then(c.cast(pl.Datetime("us"), strict=False).dt.strftime("%Y-%m-%d"))
                .otherwise(pl.lit(""))
                .alias(col)
            )
            exprs.append(formatted)

    if exprs:
        df = df.with_columns(exprs)
    return df


# =====================================================================
# DATA INTEGRITY VALIDATION
# =====================================================================

def polars_validate_output(
    df: pl.DataFrame,
    name: str,
    date_col: str = "OrderCohortMonth",
    revenue_col: Optional[str] = None,
    expected_revenue: Optional[float] = None,
) -> Dict[str, Any]:
    """Validate pipeline output for data integrity."""
    issues = []
    results = {"name": name, "rows": df.height, "issues": issues}

    logger.info(f"\n{'=' * 60}")
    logger.info(f"  VALIDATION: {name}")
    logger.info(f"{'=' * 60}")

    # 1. Row count
    logger.info(f"\n  Row count: {df.height:,}")

    # 2. Null key columns
    for key_col in ["CustomerID_Std", "OrderID_Std"]:
        if key_col in df.columns:
            nulls = df[key_col].null_count()
            if nulls > 0:
                issues.append(f"NULL KEYS: {nulls} null values in {key_col}")

    # 3. Revenue check
    if revenue_col and revenue_col in df.columns:
        total_rev = df[revenue_col].sum()
        logger.info(f"\n  Revenue ({revenue_col}): ${total_rev:,.2f}")
        if expected_revenue is not None:
            diff = abs(total_rev - expected_revenue)
            if diff > 1.0:
                issues.append(f"REVENUE MISMATCH: ${total_rev:,.2f} vs expected ${expected_revenue:,.2f}")

    if issues:
        logger.info(f"\n  *** {len(issues)} ISSUE(S) FOUND ***")
        for issue in issues:
            logger.info(f"    - {issue}")
    else:
        logger.info(f"\n  [OK] All checks passed")

    results["passed"] = len(issues) == 0
    return results
