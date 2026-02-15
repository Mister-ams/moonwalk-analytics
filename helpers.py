"""
Helper Functions for PowerQuery to Python Conversion
Contains all shared utility functions used across transform scripts

OPTIMIZED VERSION:
- Added find_cleancloud_file() (was duplicated in 3 scripts)
- Added vectorized bulk operations for date, store, ID, category processing
- Kept scalar functions for edge cases
- Reordered fx_to_date logic (string-first, serial-last) for CSV data
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any, Union


# =====================================================================
# CONFIGURATION (imported from config.py)
# =====================================================================

from config import (
    DOWNLOADS_PATH, LOCAL_STAGING_PATH, ONEDRIVE_SALES_DATA_PATH,
    EXCEL_SERIAL_DATE_BASE, MOONWALK_STORE_ID, HIELO_STORE_ID,
)

from logger_config import setup_logger
logger = setup_logger(__name__)

# DEPRECATED: Use LOCAL_STAGING_PATH from config.py directly.
# Kept for backward compatibility with any external references.
SALES_DATA_PATH = LOCAL_STAGING_PATH


# =====================================================================
# FILE DISCOVERY (was duplicated in 3 scripts)
# =====================================================================

def find_cleancloud_file(pattern: str, downloads_path: Optional[str] = None, required: bool = True) -> Optional[str]:
    """
    Find the most recent CleanCloud file matching pattern in Downloads.
    Consolidated from transform_all_customers, transform_all_sales, transform_all_items.

    IMPORTANT: Excludes Excel exports and requires CC- prefix to avoid confusion.
    """
    downloads = Path(downloads_path or DOWNLOADS_PATH)
    pattern_lower = pattern.lower()
    
    # Only find CleanCloud files (CC- prefix), exclude Excel exports
    matches = [
        f for f in downloads.glob("*.csv") 
        if pattern_lower in f.name.lower()
        and not f.name.startswith('Excel_')  # Exclude Excel pivot exports
        and 'CC-' in f.name  # Must have CleanCloud prefix
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
# VECTORIZED DATE CONVERSION (replaces .apply(fx_to_date))
# =====================================================================

def vectorized_to_date(series: pd.Series, column_name: str = '') -> pd.Series:
    """
    Vectorized date conversion - 5-10x faster than .apply(fx_to_date).
    Handles Excel serial numbers (e.g. 45292) AND text dates (e.g. '2024-01-15').
    Returns pd.Series of datetime64[ns] with NaT for nulls.

    Warns when >5% of non-null values parse to NaT (possible upstream format change).
    """
    s = series.astype(str).str.strip()
    result = pd.Series(pd.NaT, index=series.index, dtype='datetime64[ns]')

    # Step 1: Identify pure-numeric values (Excel serial numbers)
    # These are 1-5 digit numbers like 45292 that pd.to_datetime would misparse
    is_numeric = s.str.match(r'^\d{1,5}(?:\.0*)?$', na=False)
    numeric_vals = pd.to_numeric(s[is_numeric], errors='coerce')
    valid_serials = numeric_vals.between(1, 99999, inclusive='neither')
    if valid_serials.any():
        base = pd.Timestamp(EXCEL_SERIAL_DATE_BASE)
        serial_dates = base + pd.to_timedelta(numeric_vals[valid_serials], unit='D')
        result.loc[serial_dates.index] = serial_dates

    # Step 2: Parse remaining non-numeric values as date strings
    # CRITICAL: Use format='mixed' because sources have different formats:
    #   CC Orders:  '21 Mar 2025 00:39'   (DD Mon YYYY HH:MM)
    #   Invoices:   '2025-03-21 01:29:10' (YYYY-MM-DD HH:MM:SS)
    #   CSV output: '2025-01-15'          (YYYY-MM-DD ISO from format_dates_for_csv)
    # Without 'mixed', pandas infers one format from first values and fails on the rest
    # NOTE: Removed dayfirst=True because it conflicts with ISO format (YYYY-MM-DD)
    #       format='mixed' handles DD Mon YYYY correctly without dayfirst
    non_numeric = ~is_numeric & series.notna() & (s != '') & (s != 'nan') & (s != 'None')
    if non_numeric.any():
        parsed = pd.to_datetime(s[non_numeric], format='mixed', errors='coerce')
        result.loc[parsed.index] = parsed

    # Step 3: Check NaT rate on non-null input values
    non_null_input = series.notna() & (s != '') & (s != 'nan') & (s != 'None')
    non_null_count = non_null_input.sum()
    if non_null_count > 0:
        nat_count = result[non_null_input].isna().sum()
        nat_pct = nat_count / non_null_count * 100
        col_label = f" [{column_name}]" if column_name else ""
        if nat_pct > 5:
            logger.warning(
                f"  [WARN] vectorized_to_date{col_label}: {nat_count:,}/{non_null_count:,} "
                f"({nat_pct:.1f}%) parsed to NaT -- possible format change in source CSV"
            )
        elif nat_count > 0:
            logger.debug(
                f"  vectorized_to_date{col_label}: {nat_count:,}/{non_null_count:,} "
                f"({nat_pct:.1f}%) parsed to NaT"
            )

    return result


# =====================================================================
# SCALAR DATE CONVERSION (kept for edge cases)
# =====================================================================

def fx_to_date(value: Any) -> Optional[date]:
    """Scalar date conversion - use vectorized_to_date for bulk operations."""
    if pd.isna(value):
        return None
    # Try string parsing first (most common for CSV data)
    try:
        if isinstance(value, str):
            return pd.to_datetime(value).date()
    except (ValueError, TypeError):
        pass
    # Try if already datetime
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.date()
    # Try Excel serial number last (rare for CSV)
    try:
        num_val = float(value)
        if 1 < num_val < 99999:
            return (pd.Timestamp(EXCEL_SERIAL_DATE_BASE) + pd.Timedelta(days=num_val)).date()
    except (ValueError, TypeError):
        pass
    return None


# =====================================================================
# DIGIT PADDING
# =====================================================================

def fx_pad_digits(value: Any, width: int) -> Optional[str]:
    """Extract digits from value and pad to fixed width."""
    if pd.isna(value):
        return None
    digits = ''.join(c for c in str(value) if c.isdigit())
    return digits.zfill(width) if digits else None


# =====================================================================
# TEXT STANDARDIZATION (FOR ITEMS/SECTIONS)
# =====================================================================

def fx_standardize_text(text: Any) -> str:
    """Lowercase, removes spaces/hyphens/ampersands/apostrophes."""
    if pd.isna(text):
        return ""
    s = str(text).lower()
    for ch in (' ', '-', '&', "'"):
        s = s.replace(ch, '')
    return s


# =====================================================================
# NAME STANDARDIZATION (FOR CUSTOMER NAMES)
# =====================================================================

def fx_standardize_name(name: Any) -> str:
    """Uppercase, removes spaces/hyphens/dots/apostrophes/commas."""
    if pd.isna(name):
        return ""
    s = str(name).upper().strip()
    for ch in (' ', '-', '.', "'", ','):
        s = s.replace(ch, '')
    return s


def vectorized_name_standardize(series: pd.Series) -> pd.Series:
    """Vectorized name standardization - replaces .apply(fx_standardize_name)."""
    return (
        series.fillna("")
        .astype(str)
        .str.upper()
        .str.strip()
        .str.replace(" ", "", regex=False)
        .str.replace("-", "", regex=False)
        .str.replace(".", "", regex=False)
        .str.replace("'", "", regex=False)
        .str.replace(",", "", regex=False)
    )


# =====================================================================
# VECTORIZED STORE STANDARDIZATION
# =====================================================================

def vectorized_store_std(store_id_series: pd.Series,
                         store_name_series: Optional[pd.Series] = None,
                         source_series: Optional[pd.Series] = None) -> pd.Series:
    """
    Vectorized store standardization - replaces .apply(get_store_std).
    Returns pd.Series of 'Moon Walk', 'Hielo', or None.
    """
    result = pd.Series(np.nan, index=store_id_series.index, dtype='object')

    # Extract digits from Store ID
    digits = store_id_series.astype(str).str.replace(r'\D', '', regex=True)
    result.loc[digits == MOONWALK_STORE_ID] = 'Moon Walk'
    result.loc[digits == HIELO_STORE_ID] = 'Hielo'

    # Fallback to Store Name
    if store_name_series is not None:
        upper_names = store_name_series.astype(str).str.upper()
        still_null = result.isna()
        result.loc[still_null & upper_names.str.contains('MOON', na=False)] = 'Moon Walk'
        result.loc[still_null & upper_names.str.contains('HIELO', na=False)] = 'Hielo'

    # Fallback for Legacy
    if source_series is not None:
        still_null = result.isna()
        result.loc[still_null & (source_series == 'Legacy')] = 'Moon Walk'

    return result


def get_store_std(store_id: Any, store_name: Any, source: Any) -> Optional[str]:
    """Scalar version - kept for edge cases."""
    if pd.notna(store_id):
        digits = ''.join(c for c in str(store_id).strip() if c.isdigit())
        if digits == MOONWALK_STORE_ID:
            return "Moon Walk"
        elif digits == HIELO_STORE_ID:
            return "Hielo"
    if pd.notna(store_name):
        upper = str(store_name).upper().strip()
        if "MOON" in upper:
            return "Moon Walk"
        elif "HIELO" in upper:
            return "Hielo"
    if source == "Legacy":
        return "Moon Walk"
    return None


# =====================================================================
# VECTORIZED CUSTOMER ID STANDARDIZATION
# =====================================================================

def vectorized_customer_id_std(customer_id_series: pd.Series, source_series: pd.Series) -> pd.Series:
    """
    Vectorized CustomerID_Std - replaces .apply(get_customer_id_std).
    Returns pd.Series of 'MW-xxxx' or 'CC-xxxx'.
    """
    result = pd.Series(np.nan, index=customer_id_series.index, dtype='object')

    # Extract and pad digits
    raw = customer_id_series.astype(str).str.strip()

    # Already formatted
    preformatted = raw.str.match(r'^(MW-|CC-)')
    result.loc[preformatted] = raw.loc[preformatted]

    # Needs formatting
    needs_fmt = ~preformatted & customer_id_series.notna()
    digits = raw[needs_fmt].str.replace(r'\D', '', regex=True)
    valid = digits != ''
    padded = digits[valid].str.zfill(4)

    is_legacy = source_series[needs_fmt][valid] == 'Legacy'
    result.loc[padded[is_legacy].index] = 'MW-' + padded[is_legacy]
    result.loc[padded[~is_legacy].index] = 'CC-' + padded[~is_legacy]

    return result


def get_customer_id_std(customer_id: Any, source: Any) -> Optional[str]:
    """Scalar version - kept for edge cases."""
    if pd.isna(customer_id):
        return None
    cust_id_str = str(customer_id).strip()
    if cust_id_str.startswith("MW-") or cust_id_str.startswith("CC-"):
        return cust_id_str
    padded = fx_pad_digits(customer_id, 4)
    if padded is None:
        return None
    return f"MW-{padded}" if source == "Legacy" else f"CC-{padded}"


# =====================================================================
# VECTORIZED ORDER ID STANDARDIZATION
# =====================================================================

def vectorized_order_id_std(order_id_series: pd.Series,
                            store_std_series: pd.Series,
                            source_series: pd.Series,
                            transaction_type_series: pd.Series,
                            row_index_series: pd.Series) -> pd.Series:
    """
    Vectorized OrderID_Std - replaces .apply(get_order_id_std).
    """
    result = pd.Series(np.nan, index=order_id_series.index, dtype='object')
    idx_pad = row_index_series.astype(str).str.zfill(5)

    # Subscription â†’ S-xxxxx
    is_sub = transaction_type_series == 'Subscription'
    result.loc[is_sub] = 'S-' + idx_pad[is_sub]

    # Invoice Payment â†’ I-xxxxx
    is_inv = transaction_type_series == 'Invoice Payment'
    result.loc[is_inv] = 'I-' + idx_pad[is_inv]

    # Orders need parsing
    is_order = result.isna() & order_id_series.notna()
    raw = order_id_series[is_order].astype(str).str.strip()

    # Already R-xxxxx format
    starts_r_dash = raw.str.startswith('R-')
    result.loc[raw[starts_r_dash].index] = raw[starts_r_dash]

    # Legacy R format (no dash)
    remaining = is_order & result.isna()
    raw2 = order_id_series[remaining].astype(str).str.strip()
    starts_r = raw2.str.startswith('R') & ~raw2.str.startswith('R-')
    result.loc[raw2[starts_r].index] = 'R-' + raw2[starts_r].str[1:]

    # Already H- or M- format
    remaining2 = is_order & result.isna()
    raw3 = order_id_series[remaining2].astype(str).str.strip()
    starts_hm = raw3.str.match(r'^[HM]-')
    result.loc[raw3[starts_hm].index] = raw3[starts_hm]

    # Remaining: pad digits and prefix by store
    remaining3 = is_order & result.isna()
    if remaining3.any():
        raw4 = order_id_series[remaining3].astype(str).str.replace(r'\D', '', regex=True)
        valid = raw4 != ''
        padded = raw4[valid].str.zfill(5)
        is_hielo = store_std_series[remaining3][valid] == 'Hielo'
        result.loc[padded[is_hielo].index] = 'H-' + padded[is_hielo]
        result.loc[padded[~is_hielo].index] = 'M-' + padded[~is_hielo]

    return result


def get_order_id_std(order_id: Any,
                     store_std: Any,
                     source: Any,
                     transaction_type: Optional[str] = None,
                     row_index: Optional[int] = None) -> Optional[str]:
    """Scalar version - kept for edge cases."""
    if transaction_type == "Subscription" and row_index:
        return f"S-{str(row_index).zfill(5)}"
    if transaction_type == "Invoice Payment" and row_index:
        return f"I-{str(row_index).zfill(5)}"
    if pd.isna(order_id):
        return None
    oid = str(order_id).strip()
    if oid.startswith("R-"):
        return oid
    if oid.startswith("R"):
        return f"R-{oid[1:]}"
    if oid.startswith("H-") or oid.startswith("M-"):
        return oid
    padded = fx_pad_digits(order_id, 5)
    if padded is None:
        return None
    return f"H-{padded}" if store_std == "Hielo" else f"M-{padded}"


# =====================================================================
# VECTORIZED PAYMENT TYPE STANDARDIZATION
# =====================================================================

def vectorized_payment_type_std(payment_type_series: pd.Series) -> pd.Series:
    """Vectorized payment type - replaces .apply(standardize_payment_type)."""
    pt = payment_type_series.astype(str).str.upper().str.strip()
    result = pd.Series('Other', index=payment_type_series.index, dtype='object')
    result.loc[pt.str.contains('CASH', na=False)] = 'Cash'
    result.loc[pt.str.contains('CARD|TERMINAL', na=False, regex=True)] = 'Terminal'
    result.loc[pt.str.contains('BANK|STRIPE', na=False, regex=True)] = 'Stripe'
    result.loc[pt.str.contains('INVOICE', na=False)] = 'Receivable'
    return result


def standardize_payment_type(payment_type: Any) -> str:
    """Scalar version."""
    if pd.isna(payment_type):
        return "Other"
    pt = str(payment_type).upper().strip()
    if "CASH" in pt:
        return "Cash"
    if "CARD" in pt or "TERMINAL" in pt:
        return "Terminal"
    if "BANK" in pt or "STRIPE" in pt:
        return "Stripe"
    if "INVOICE" in pt:
        return "Receivable"
    return "Other"


# =====================================================================
# VECTORIZED ITEM CATEGORIZATION
# =====================================================================

def vectorized_item_category(item_series: pd.Series, section_series: pd.Series) -> pd.Series:
    """
    Vectorized item categorization - replaces .apply(categorize_item).
    Uses str.contains on the combined standardized text.
    """
    combined = (
        item_series.astype(str).str.lower().str.replace(r"[\s\-&']", '', regex=True)
        + section_series.astype(str).str.lower().str.replace(r"[\s\-&']", '', regex=True)
    )

    result = pd.Series('Others', index=item_series.index, dtype='object')

    # Traditional Wear (check first - most specific)
    trad = combined.str.contains(
        'kandura|kandoora|thobe|abaya|sheyla|shayla|hijab|ghutra|jalabeya',
        na=False, regex=True
    )
    result.loc[trad] = 'Traditional Wear'

    # Home Linens
    linen = combined.str.contains(
        'duvet|comforter|bedsheet|sheet|pillowcase|pillow|towel|curtain|tablecloth',
        na=False, regex=True
    ) & ~trad
    result.loc[linen] = 'Home Linens'

    # Professional Wear
    prof = combined.str.contains(
        'uniform|suit|blazer|jacket|shirt|blouse|top|polo|pant|trouser',
        na=False, regex=True
    ) & ~trad & ~linen
    result.loc[prof] = 'Professional Wear'

    # Extras
    extras = combined.str.contains(
        'shoe|carpet|tailor|alteration',
        na=False, regex=True
    ) & ~trad & ~linen & ~prof
    result.loc[extras] = 'Extras'

    return result


# =====================================================================
# VECTORIZED SERVICE TYPE CATEGORIZATION
# =====================================================================

def vectorized_service_type(section_series: pd.Series) -> pd.Series:
    """Vectorized service type - replaces .apply(categorize_service_type)."""
    s = section_series.astype(str).str.lower().str.replace(r"[\s\-&']", '', regex=True)

    result = pd.Series('Other Service', index=section_series.index, dtype='object')
    result.loc[s.str.contains('drycle|dryclean', na=False, regex=True)] = 'Dry Cleaning'
    result.loc[
        s.str.contains('wash|laund', na=False, regex=True)
        & ~s.str.contains('drycle|dryclean', na=False, regex=True)
    ] = 'Wash & Press'
    result.loc[
        s.str.contains('press|iron', na=False, regex=True)
        & ~s.str.contains('drycle|dryclean', na=False, regex=True)
        & ~s.str.contains('wash|laund', na=False, regex=True)
    ] = 'Press Only'

    return result


# =====================================================================
# ROUTE CATEGORY
# =====================================================================

def vectorized_route_category(route_series: pd.Series) -> pd.Series:
    """Vectorized route category."""
    route = pd.to_numeric(route_series, errors='coerce').fillna(0)
    result = pd.Series('Other', index=route_series.index, dtype='object')
    result.loc[(route >= 1) & (route <= 3)] = 'Inside Abu Dhabi'
    result.loc[route > 3] = 'Outer Abu Dhabi'
    return result


def get_route_category(route_num: Any) -> str:
    """Scalar version."""
    if pd.isna(route_num):
        return "Other"
    try:
        route = float(route_num)
        if route == 0:
            return "Other"
        elif 1 <= route <= 3:
            return "Inside Abu Dhabi"
        else:
            return "Outer Abu Dhabi"
    except (ValueError, TypeError):
        return "Other"


# =====================================================================
# VECTORIZED MONTHS SINCE COHORT
# =====================================================================

def vectorized_months_since_cohort(order_month_series: pd.Series, cohort_month_series: pd.Series) -> pd.Series:
    """Vectorized month difference calculation."""
    om = pd.to_datetime(order_month_series, errors='coerce')
    cm = pd.to_datetime(cohort_month_series, errors='coerce')
    return ((om.dt.year - cm.dt.year) * 12 + (om.dt.month - cm.dt.month)).astype('Int64')


def calculate_months_since_cohort(order_cohort_month: Any, customer_cohort_month: Any) -> Optional[int]:
    """Scalar version."""
    if pd.isna(order_cohort_month) or pd.isna(customer_cohort_month):
        return None
    try:
        om = pd.to_datetime(order_cohort_month)
        cm = pd.to_datetime(customer_cohort_month)
        return int((om.year - cm.year) * 12 + (om.month - cm.month))
    except (ValueError, TypeError):
        return None


# =====================================================================
# SUBSCRIPTION SERVICE CHECK (scalar - used in edge cases)
# =====================================================================

def check_subscription_service(customer_id: Any, earned_date: Any, subscription_dict: Dict[str, List[Dict[str, Any]]]) -> int:
    """Check if an order was placed during active subscription."""
    if pd.isna(customer_id) or pd.isna(earned_date):
        return 0
    periods = subscription_dict.get(customer_id, [])
    for p in periods:
        if p['ValidFrom'] <= earned_date <= p['ValidUntil']:
            return 1
    return 0


# =====================================================================
# VECTORIZED SUBSCRIPTION FLAG (replaces row-by-row check)
# =====================================================================

def vectorized_subscription_flag(df: pd.DataFrame, subscription_dict: Dict[str, List[Dict[str, Any]]]) -> pd.Series:
    """
    Vectorized subscription flag via merge - replaces .apply() loop.
    df must have: CustomerID_Std, Earned_Date, Transaction_Type, OrderID_Std
    Returns Series of 0/1 aligned to df.index.
    """
    result = pd.Series(0, index=df.index, dtype='int64')

    # Only check orders
    orders_mask = (df['Transaction_Type'] == 'Order') & df['Earned_Date'].notna() & df['CustomerID_Std'].notna()
    if not orders_mask.any():
        return result

    # Build subscription periods DataFrame
    sub_records = []
    for cid, periods in subscription_dict.items():
        for p in periods:
            sub_records.append({
                'CustomerID_Std': cid,
                'ValidFrom': p['ValidFrom'],
                'ValidUntil': p['ValidUntil']
            })

    if not sub_records:
        return result

    sub_df = pd.DataFrame(sub_records)
    sub_df['ValidFrom'] = pd.to_datetime(sub_df['ValidFrom'])
    sub_df['ValidUntil'] = pd.to_datetime(sub_df['ValidUntil'])

    # Get orders that could have subscriptions
    orders = df.loc[orders_mask, ['CustomerID_Std', 'Earned_Date', 'OrderID_Std']].copy()
    orders['Earned_Date'] = pd.to_datetime(orders['Earned_Date'])

    # Merge on customer, then filter by date range
    merged = orders.merge(sub_df, on='CustomerID_Std', how='inner')
    covered = merged[
        (merged['Earned_Date'] >= merged['ValidFrom']) &
        (merged['Earned_Date'] <= merged['ValidUntil'])
    ]

    covered_indices = orders.index[orders['OrderID_Std'].isin(covered['OrderID_Std'].unique())]
    result.loc[covered_indices] = 1

    return result


# =====================================================================
# DATE FORMATTING FOR CSV OUTPUT
# =====================================================================

def format_dates_for_csv(df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
    """
    Format datetime columns as ISO 8601 (YYYY-MM-DD) for PowerQuery.
    
    Example: 2025-07-01 â†’ '2025-07-01'
    
    ISO 8601 is unambiguous: year-first eliminates any DD/MM vs MM/DD confusion.
    """
    for col in date_columns:
        if col in df.columns:
            s = df[col]
            if pd.api.types.is_datetime64_any_dtype(s):
                mask = s.notna()
                df[col] = ''  # default empty for NaT
                df.loc[mask, col] = s[mask].dt.strftime('%Y-%m-%d')
            else:
                def _to_text(x):
                    if pd.isna(x) or not hasattr(x, 'strftime'):
                        return ''
                    return pd.Timestamp(x).strftime('%Y-%m-%d')
                df[col] = s.apply(_to_text)
    return df


# =====================================================================
# DATA INTEGRITY VALIDATION
# =====================================================================

def validate_output(df: pd.DataFrame,
                    name: str,
                    raw_counts: Optional[Dict[str, int]] = None,
                    date_col: str = 'OrderCohortMonth',
                    max_expected_date: Optional[str] = None,
                    revenue_col: Optional[str] = None,
                    expected_revenue: Optional[float] = None) -> Dict[str, Any]:
    """
    Validate pipeline output to ensure no data loss or corruption.
    
    Args:
        df: Output DataFrame
        name: Name of the output (e.g. 'All_Sales')
        raw_counts: Dict of {source: expected_row_count} for completeness check
        date_col: Column to check for date range validity
        max_expected_date: Maximum date that should exist (e.g. '2026-02-01')
        revenue_col: Column name for revenue validation
        expected_revenue: Expected total revenue for cross-check
    
    Returns:
        Dict with validation results and any issues found
    """
    issues = []
    results = {'name': name, 'rows': len(df), 'issues': issues}
    
    logger.info(f"\n{'=' * 60}")
    logger.info(f"  VALIDATION: {name}")
    logger.info(f"{'=' * 60}")
    
    # 1. Row count check
    logger.info(f"\n  Row count: {len(df):,}")
    if raw_counts:
        for source, expected in raw_counts.items():
            actual = len(df[df['Source'] == source]) if 'Source' in df.columns else len(df)
            if actual == 0 and expected > 0:
                issues.append(f"MISSING: {source} has 0 rows (expected {expected:,})")
            logger.info(f"    {source}: {actual:,} rows (raw: {expected:,})")
    
    # 2. Date range check
    if date_col in df.columns:
        dates = pd.to_datetime(df[date_col], errors='coerce')
        valid_dates = dates.dropna()
        if len(valid_dates) > 0:
            min_date = valid_dates.min()
            max_date = valid_dates.max()
            logger.info(f"\n  Date range ({date_col}):")
            logger.info(f"    Min: {min_date}")
            logger.info(f"    Max: {max_date}")
            
            if max_expected_date:
                max_dt = pd.Timestamp(max_expected_date)
                future = valid_dates[valid_dates > max_dt]
                if len(future) > 0:
                    issues.append(f"FUTURE DATES: {len(future)} rows have {date_col} > {max_expected_date}")
                    logger.info(f"    *** {len(future)} DATES BEYOND {max_expected_date} ***")
                    # Show details
                    future_df = df[dates > max_dt]
                    for _, row in future_df.head(5).iterrows():
                        logger.info(f"      {row.get('CustomerID_Std','?')} | {row.get('Transaction_Type','?')} | {row[date_col]}")
                else:
                    logger.info(f"    [OK] All dates within expected range")
        
        null_dates = dates.isna().sum()
        if null_dates > 0:
            pct = null_dates / len(df) * 100
            logger.info(f"    Null dates: {null_dates:,} ({pct:.1f}%)")
            if pct > 5:
                issues.append(
                    f"HIGH NaT RATIO: {null_dates:,} ({pct:.1f}%) null dates in {date_col} "
                    f"— possible format change in source CSV"
                )
                logger.info(f"    *** HIGH NaT RATIO — check source date formats ***")

    # 3. Revenue check
    if revenue_col and revenue_col in df.columns:
        total_rev = df[revenue_col].sum()
        logger.info(f"\n  Revenue ({revenue_col}): ${total_rev:,.2f}")
        if expected_revenue is not None:
            diff = abs(total_rev - expected_revenue)
            if diff > 1.0:
                issues.append(f"REVENUE MISMATCH: ${total_rev:,.2f} vs expected ${expected_revenue:,.2f}")
                logger.info(f"    *** MISMATCH: expected ${expected_revenue:,.2f} (diff: ${diff:,.2f}) ***")
            else:
                logger.info(f"    [OK] Matches expected ${expected_revenue:,.2f}")
    
    # 4. Null key columns check
    for key_col in ['CustomerID_Std', 'OrderID_Std']:
        if key_col in df.columns:
            nulls = df[key_col].isna().sum()
            if nulls > 0:
                issues.append(f"NULL KEYS: {nulls} null values in {key_col}")
    
    # Summary
    if issues:
        logger.info(f"\n  *** {len(issues)} ISSUE(S) FOUND ***")
        for issue in issues:
            logger.info(f"    - {issue}")
    else:
        logger.info(f"\n  [OK] All checks passed")
    
    results['passed'] = len(issues) == 0
    return results
