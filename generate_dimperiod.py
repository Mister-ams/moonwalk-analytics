"""
DimPeriod Generator - Standalone Module
Generates date dimension with auto 3-month lookahead
Can be imported by master script or run standalone

FIXED: Date format now YYYY-MM-DD to match All_Sales
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
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
    Check if DimPeriod needs regenerating

    Args:
        dimperiod_path: Path to DimPeriod_Python.csv
        months_forward: Required months forward coverage

    Returns:
        (needs_update: bool, reason: str, current_max_date: date or None)
    """
    # Calculate required max date (3 months forward)
    today = datetime.now()
    forward_months = today.month + months_forward
    forward_year = today.year + (forward_months - 1) // 12
    forward_month = ((forward_months - 1) % 12) + 1
    last_day = calendar.monthrange(forward_year, forward_month)[1]
    required_max_date = datetime(forward_year, forward_month, last_day).date()
    
    # Check if file exists
    if not os.path.exists(dimperiod_path):
        return True, "File doesn't exist", None
    
    try:
        # Load just the last few rows to check max date
        df_tail = pd.read_csv(dimperiod_path, encoding='utf-8').tail(5)
        
        # Parse max date (now YYYY-MM-DD format)
        max_date_str = df_tail['Date'].max()
        max_date = pd.to_datetime(max_date_str, format='%Y-%m-%d').date()
        
        # Check if it covers required forward period
        if max_date < required_max_date:
            return True, f"Only covers to {max_date}, need {required_max_date}", max_date
        
        return False, "Up to date", max_date
        
    except Exception as e:
        return True, f"Error reading file: {str(e)}", None


# =====================================================================
# GENERATE DIMPERIOD
# =====================================================================

def generate_dimperiod(output_path: str, months_forward: int = 3, start_year: int = 2023, verbose: bool = True) -> pd.DataFrame:
    """
    Generate DimPeriod dimension table with correct ISO weeks
    Auto-extends to 3 months forward from today

    Args:
        output_path: Full path to DimPeriod_Python.csv
        months_forward: How many months to look forward (default 3)
        start_year: Starting year for dimension (default 2023)
        verbose: Print progress messages (default True)

    Returns:
        DataFrame with date dimension
    """
    if verbose:
        logger.info("\n  Generating DimPeriod...")
    
    # Calculate date range
    start_date = f'{start_year}-01-01'
    
    # End date = 3 months forward from today
    today = datetime.now()
    forward_months = today.month + months_forward
    forward_year = today.year + (forward_months - 1) // 12
    forward_month = ((forward_months - 1) % 12) + 1
    
    # Last day of the forward month
    last_day = calendar.monthrange(forward_year, forward_month)[1]
    end_date = f'{forward_year}-{forward_month:02d}-{last_day:02d}'
    
    if verbose:
        logger.info(f"  Date range: {start_date} to {end_date}")
    
    # Create date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    df = pd.DataFrame({'Date': date_range})
    
    # Basic date components
    df['Year'] = df['Date'].dt.year
    df['Quarter'] = df['Date'].dt.quarter
    df['Month'] = df['Date'].dt.month
    df['Day'] = df['Date'].dt.day
    df['DayOfWeek'] = df['Date'].dt.dayofweek + 1
    df['DayOfYear'] = df['Date'].dt.dayofyear
    
    df['MonthName'] = df['Date'].dt.strftime('%B')
    df['MonthShort'] = df['Date'].dt.strftime('%b')
    df['DayName'] = df['Date'].dt.strftime('%A')
    df['DayShort'] = df['Date'].dt.strftime('%a')
    
    df['IsFirstDayOfMonth'] = (df['Date'].dt.day == 1).astype(int)
    df['IsLastDayOfMonth'] = (df['Date'] == df['Date'].dt.to_period('M').dt.to_timestamp('M')).astype(int)
    
    # ISO WEEK - CORRECT!
    iso_cal = df['Date'].dt.isocalendar()
    df['ISOYear'] = iso_cal['year'].astype(int)
    df['ISOWeek'] = iso_cal['week'].astype(int)
    df['ISOWeekday'] = iso_cal['day'].astype(int)
    df['ISOWeekLabel'] = df['ISOYear'].astype(str) + '-W' + df['ISOWeek'].astype(str).str.zfill(2)
    df['IsFirstDayOfISOWeek'] = (df['ISOWeekday'] == 1).astype(int)
    df['IsLastDayOfISOWeek'] = (df['ISOWeekday'] == 7).astype(int)
    
    # Period columns
    df['YearMonth'] = df['Date'].dt.strftime('%Y-%m')
    df['YearQuarter'] = df['Year'].astype(str) + '-Q' + df['Quarter'].astype(str)
    
    # MonthStart = first day of month (simple date arithmetic - no Period objects!)
    df['MonthStart'] = pd.to_datetime(df['Date'].dt.strftime('%Y-%m-01'))
    
    # QuarterStart = first day of quarter (Q1=Jan, Q2=Apr, Q3=Jul, Q4=Oct)
    quarter_start_month = ((df['Quarter'] - 1) * 3 + 1).astype(str).str.zfill(2)
    df['QuarterStart'] = pd.to_datetime(df['Year'].astype(str) + '-' + quarter_start_month + '-01')
    
    
    # Fiscal periods
    df['FiscalYear'] = df['Year']
    df['FiscalQuarter'] = df['Quarter']
    
    # Weekend flags
    df['IsWeekend'] = df['DayOfWeek'].isin([6, 7]).astype(int)
    df['IsWeekday'] = (~df['DayOfWeek'].isin([6, 7])).astype(int)
    
    # Relative flags
    today_normalized = pd.Timestamp.now().normalize()
    
    # Current month start (simple calculation)
    current_month_start = pd.to_datetime(today_normalized.strftime('%Y-%m-01'))
    df['IsCurrentMonth'] = (df['MonthStart'] == current_month_start).astype(int)
    
    # Current quarter start  
    current_quarter = (today_normalized.month - 1) // 3 + 1
    current_quarter_month = (current_quarter - 1) * 3 + 1
    current_quarter_start = pd.to_datetime(f'{today_normalized.year}-{current_quarter_month:02d}-01')
    df['IsCurrentQuarter'] = (df['QuarterStart'] == current_quarter_start).astype(int)
    
    df['IsCurrentYear'] = (df['Year'] == today_normalized.year).astype(int)
    
    current_iso = today_normalized.isocalendar()
    df['IsCurrentISOWeek'] = ((df['ISOYear'] == current_iso[0]) & (df['ISOWeek'] == current_iso[1])).astype(int)
    
    # Sort orders
    df['MonthSortOrder'] = df['Month']
    df['DayOfWeekSortOrder'] = df['DayOfWeek']
    df['QuarterSortOrder'] = df['Quarter']
    
    # Column order
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
    
    df_final = df[column_order].copy()
    
    # =====================================================================
    # CRITICAL FIX: Format dates as YYYY-MM-DD (matches All_Sales format)
    # =====================================================================
    date_columns = ['Date', 'MonthStart', 'QuarterStart']
    for col in date_columns:
        df_final[col] = df_final[col].dt.strftime('%Y-%m-%d')  # ← FIXED!
    
    # Save
    df_final.to_csv(output_path, index=False, encoding='utf-8')
    
    if verbose:
        logger.info(f"  Total rows: {len(df_final):,}")
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
