"""
DimPeriod Generator - Standalone Module (Polars)
Generates date dimension with auto 3-month lookahead
Can be imported by master script or run standalone
"""

import polars as pl
from datetime import datetime, date
import calendar
import os
from typing import Tuple, Optional

from logger_config import setup_logger
logger = setup_logger(__name__)


# =====================================================================
# CHECK IF UPDATE NEEDED
# =====================================================================

def check_dimperiod_needs_update(dimperiod_path: str, months_forward: int = 3) -> Tuple[bool, str, Optional[date]]:
    """
    Check if DimPeriod needs regenerating.

    Returns:
        (needs_update: bool, reason: str, current_max_date: date or None)
    """
    today = datetime.now()
    forward_months = today.month + months_forward
    forward_year = today.year + (forward_months - 1) // 12
    forward_month = ((forward_months - 1) % 12) + 1
    last_day = calendar.monthrange(forward_year, forward_month)[1]
    required_max_date = date(forward_year, forward_month, last_day)

    if not os.path.exists(dimperiod_path):
        return True, "File doesn't exist", None

    try:
        df = pl.read_csv(dimperiod_path, try_parse_dates=False)
        max_date_str = df["Date"].tail(1).item()
        max_date = datetime.strptime(max_date_str, "%Y-%m-%d").date()

        if max_date < required_max_date:
            return True, f"Only covers to {max_date}, need {required_max_date}", max_date

        return False, "Up to date", max_date

    except Exception as e:
        return True, f"Error reading file: {str(e)}", None


# =====================================================================
# GENERATE DIMPERIOD
# =====================================================================

def generate_dimperiod(output_path: str, months_forward: int = 3, start_year: int = 2023, verbose: bool = True) -> pl.DataFrame:
    """
    Generate DimPeriod dimension table with correct ISO weeks.
    Auto-extends to 3 months forward from today.

    Returns:
        Polars DataFrame with date dimension
    """
    if verbose:
        logger.info("\n  Generating DimPeriod...")

    # Calculate date range
    start_date = date(start_year, 1, 1)

    today = datetime.now()
    forward_months = today.month + months_forward
    forward_year = today.year + (forward_months - 1) // 12
    forward_month = ((forward_months - 1) % 12) + 1
    last_day = calendar.monthrange(forward_year, forward_month)[1]
    end_date = date(forward_year, forward_month, last_day)

    if verbose:
        logger.info(f"  Date range: {start_date} to {end_date}")

    # Create date range
    date_range = pl.date_range(start_date, end_date, interval="1d", eager=True)
    df = pl.DataFrame({"Date": date_range})

    d = pl.col("Date")

    # Basic date components
    df = df.with_columns([
        d.dt.year().alias("Year"),
        d.dt.quarter().alias("Quarter"),
        d.dt.month().alias("Month"),
        d.dt.day().alias("Day"),
        (d.dt.weekday()).alias("DayOfWeek"),  # Polars: Mon=1..Sun=7
        d.dt.ordinal_day().alias("DayOfYear"),
        d.dt.strftime("%B").alias("MonthName"),
        d.dt.strftime("%b").alias("MonthShort"),
        d.dt.strftime("%A").alias("DayName"),
        d.dt.strftime("%a").alias("DayShort"),
    ])

    # First/last day of month
    df = df.with_columns([
        (pl.col("Day") == 1).cast(pl.Int32).alias("IsFirstDayOfMonth"),
        (d.dt.month_end() == d).cast(pl.Int32).alias("IsLastDayOfMonth"),
    ])

    # ISO week
    df = df.with_columns([
        d.dt.iso_year().alias("ISOYear"),
        d.dt.week().alias("ISOWeek"),
        d.dt.weekday().alias("ISOWeekday"),  # Mon=1..Sun=7
    ])
    df = df.with_columns([
        (pl.col("ISOYear").cast(pl.Utf8) + pl.lit("-W") + pl.col("ISOWeek").cast(pl.Utf8).str.zfill(2))
            .alias("ISOWeekLabel"),
        (pl.col("ISOWeekday") == 1).cast(pl.Int32).alias("IsFirstDayOfISOWeek"),
        (pl.col("ISOWeekday") == 7).cast(pl.Int32).alias("IsLastDayOfISOWeek"),
    ])

    # Period columns
    df = df.with_columns([
        d.dt.strftime("%Y-%m").alias("YearMonth"),
        (pl.col("Year").cast(pl.Utf8) + pl.lit("-Q") + pl.col("Quarter").cast(pl.Utf8))
            .alias("YearQuarter"),
        d.dt.truncate("1mo").alias("MonthStart"),
        d.dt.truncate("1q").alias("QuarterStart"),
    ])

    # Fiscal periods (same as calendar for this business)
    df = df.with_columns([
        pl.col("Year").alias("FiscalYear"),
        pl.col("Quarter").alias("FiscalQuarter"),
    ])

    # Weekend flags
    df = df.with_columns([
        pl.col("DayOfWeek").is_in([6, 7]).cast(pl.Int32).alias("IsWeekend"),
        (~pl.col("DayOfWeek").is_in([6, 7])).cast(pl.Int32).alias("IsWeekday"),
    ])

    # Relative flags
    today_date = date.today()
    current_month_start = date(today_date.year, today_date.month, 1)
    current_quarter = (today_date.month - 1) // 3 + 1
    current_quarter_month = (current_quarter - 1) * 3 + 1
    current_quarter_start = date(today_date.year, current_quarter_month, 1)
    current_iso = today_date.isocalendar()

    df = df.with_columns([
        (pl.col("MonthStart").cast(pl.Date) == pl.lit(current_month_start)).cast(pl.Int32)
            .alias("IsCurrentMonth"),
        (pl.col("QuarterStart").cast(pl.Date) == pl.lit(current_quarter_start)).cast(pl.Int32)
            .alias("IsCurrentQuarter"),
        (pl.col("Year") == today_date.year).cast(pl.Int32).alias("IsCurrentYear"),
        ((pl.col("ISOYear") == current_iso[0]) & (pl.col("ISOWeek") == current_iso[1]))
            .cast(pl.Int32).alias("IsCurrentISOWeek"),
    ])

    # Sort orders
    df = df.with_columns([
        pl.col("Month").alias("MonthSortOrder"),
        pl.col("DayOfWeek").alias("DayOfWeekSortOrder"),
        pl.col("Quarter").alias("QuarterSortOrder"),
    ])

    # Format date columns as YYYY-MM-DD strings
    df = df.with_columns([
        d.dt.strftime("%Y-%m-%d").alias("Date"),
        pl.col("MonthStart").dt.strftime("%Y-%m-%d").alias("MonthStart"),
        pl.col("QuarterStart").dt.strftime("%Y-%m-%d").alias("QuarterStart"),
    ])

    # Column order (matches original)
    column_order = [
        'Date', 'Year', 'Quarter', 'Month', 'Day',
        'YearMonth', 'YearQuarter', 'MonthStart', 'QuarterStart',
        'MonthName', 'MonthShort', 'QuarterSortOrder', 'MonthSortOrder',
        'ISOYear', 'ISOWeek', 'ISOWeekday', 'ISOWeekLabel',
        'IsFirstDayOfISOWeek', 'IsLastDayOfISOWeek',
        'DayOfWeek', 'DayOfYear', 'DayName', 'DayShort', 'DayOfWeekSortOrder',
        'IsFirstDayOfMonth', 'IsLastDayOfMonth',
        'IsWeekend', 'IsWeekday',
        'IsCurrentMonth', 'IsCurrentQuarter', 'IsCurrentYear', 'IsCurrentISOWeek',
        'FiscalYear', 'FiscalQuarter'
    ]

    df_final = df.select(column_order)

    # Save CSV
    df_final.write_csv(output_path)

    if verbose:
        logger.info(f"  Total rows: {df_final.height:,}")
        logger.info(f"  Date format: YYYY-MM-DD (matches All_Sales)")
        logger.info(f"  Saved to: {os.path.basename(output_path)}")

    return df_final


# =====================================================================
# STANDALONE EXECUTION
# =====================================================================

if __name__ == "__main__":
    import sys

    from config import ONEDRIVE_SALES_DATA_PATH
    OUTPUT_PATH = os.path.join(str(ONEDRIVE_SALES_DATA_PATH), "DimPeriod_Python.csv")

    logger.info("=" * 70)
    logger.info("DIMPERIOD GENERATOR - STANDALONE EXECUTION")
    logger.info("=" * 70)
    logger.info("")
    needs_update, reason, current_max = check_dimperiod_needs_update(OUTPUT_PATH, months_forward=3)

    if not needs_update:
        logger.info(f"[OK] DimPeriod is already current (covers to {current_max})")
        response = input("\nForce regeneration anyway? (y/n): ")
        if response.lower() != 'y':
            logger.info("\nExiting without changes.")
            sys.exit(0)
    else:
        logger.info(f"[INFO] DimPeriod needs update: {reason}")

    logger.info("\nRegenerating DimPeriod with 3-month lookahead...")
    df = generate_dimperiod(OUTPUT_PATH, months_forward=3, start_year=2023, verbose=True)

    logger.info("\n" + "=" * 70)
    logger.info("[DONE] DIMPERIOD GENERATION COMPLETE!")
    logger.info("=" * 70)
    logger.info("")
    logger.info("Next Step:")
    logger.info("  1. Refresh Excel (Data -> Refresh All)")
    logger.info("  2. DimPeriod dates will now match All_Sales format")
    logger.info("  3. Relationships will work correctly")
    logger.info("")
    input("\nPress Enter to exit...")
