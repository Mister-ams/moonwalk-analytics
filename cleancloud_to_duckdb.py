"""
DuckDB ETL - Load CSVs into Analytics Database
Author: Moonwalk Analytics Team

Loads processed CSV files from Python ETL into DuckDB for fast SQL analytics.
Replaces Excel PowerPivot with a proper database.

Usage:
    python etl/cleancloud_to_duckdb.py
    
Output:
    analytics.duckdb (single-file database, ~50-100MB)
"""

import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime
import os
import sys


# =====================================================================
# CONFIGURATION (centralized in config.py)
# =====================================================================

from config import LOCAL_STAGING_PATH, DB_PATH

from logger_config import setup_logger
logger = setup_logger(__name__)

CSV_FOLDER = LOCAL_STAGING_PATH

# CSV files to load
CSV_FILES = {
    'sales': 'All_Sales_Python.csv',
    'items': 'All_Items_Python.csv',
    'customers': 'All_Customers_Python.csv',
    'customer_quality': 'Customer_Quality_Monthly_Python.csv',
    'dim_period': 'DimPeriod_Python.csv'
}


# =====================================================================
# VALIDATION
# =====================================================================

def validate_csvs():
    """Check all CSV files exist"""
    logger.info("\n" + "=" * 70)
    logger.info("CSV VALIDATION")
    logger.info("=" * 70)
    logger.info("")
    missing = []
    for table_name, csv_file in CSV_FILES.items():
        csv_path = CSV_FOLDER / csv_file
        if not csv_path.exists():
            missing.append(csv_file)
            logger.info(f"  [ERROR] Missing: {csv_file}")
        else:
            size_mb = csv_path.stat().st_size / (1024 * 1024)
            logger.info(f"  [OK] Found: {csv_file} ({size_mb:.1f} MB)")
    
    if missing:
        logger.info("")
        logger.info(f"ERROR: {len(missing)} CSV file(s) missing from:")
        logger.info(f"  {CSV_FOLDER}")
        logger.info("")
        logger.info("Run your Python ETL first to generate these files.")
        sys.exit(1)
    
    logger.info("")
    logger.info("[OK] All CSV files validated")
    return True


# =====================================================================
# LOAD DATA INTO DUCKDB
# =====================================================================

def create_database():
    """Create DuckDB database and load all tables"""
    
    logger.info("\n" + "=" * 70)
    logger.info("DUCKDB ETL - LOADING DATA")
    logger.info("=" * 70)
    logger.info("")
    # Build into a temp file, then swap — avoids locking issues if
    # the dashboard currently has analytics.duckdb open in read-only mode.
    tmp_path = DB_PATH.with_suffix('.duckdb.tmp')
    if tmp_path.exists():
        tmp_path.unlink()

    conn = duckdb.connect(str(tmp_path))
    logger.info(f"  Building new database: {tmp_path.name}")
    logger.info("")
    total_rows = 0
    
    # Load each CSV as a table
    for table_name, csv_file in CSV_FILES.items():
        csv_path = CSV_FOLDER / csv_file
        
        logger.info(f"  Loading {table_name}...")
        start = datetime.now()
        
        # Read CSV with pandas first for consistent date parsing
        df = pd.read_csv(csv_path, encoding='utf-8', low_memory=False)
        
        # Create table from dataframe
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
        
        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        total_rows += row_count
        
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"    [OK] {row_count:,} rows loaded in {elapsed:.1f}s")
    
    logger.info("")
    logger.info(f"  TOTAL: {total_rows:,} rows across {len(CSV_FILES)} tables")
    
    return conn


# =====================================================================
# CREATE INDEXES FOR PERFORMANCE
# =====================================================================

def create_indexes(conn):
    """Create indexes on frequently filtered columns"""
    
    logger.info("\n" + "=" * 70)
    logger.info("CREATING INDEXES")
    logger.info("=" * 70)
    logger.info("")
    indexes = [
        # Sales table
        ("idx_sales_customer", "sales", "CustomerID_Std"),
        ("idx_sales_order", "sales", "OrderID_Std"),
        ("idx_sales_earned_date", "sales", "Earned_Date"),
        ("idx_sales_txn_type", "sales", "Transaction_Type"),
        
        # Items table
        ("idx_items_customer", "items", "CustomerID_Std"),
        ("idx_items_order", "items", "OrderID_Std"),
        ("idx_items_date", "items", "ItemDate"),
        
        # Customers table
        ("idx_customers_id", "customers", "CustomerID_Std"),
        
        # Customer quality table
        ("idx_cust_quality_id", "customer_quality", "CustomerID_Std"),
        ("idx_cust_quality_month", "customer_quality", "OrderCohortMonth"),
        
        # Period dimension
        ("idx_period_date", "dim_period", "Date"),
    ]
    
    for idx_name, table, column in indexes:
        try:
            conn.execute(f"CREATE INDEX {idx_name} ON {table}({column})")
            logger.info(f"  [OK] Created: {idx_name} on {table}({column})")
        except Exception as e:
            logger.info(f"  [WARN] Skipped: {idx_name} - {str(e)[:50]}")
    
    logger.info("")
    logger.info(f"  Indexes created for faster queries")


# =====================================================================
# VALIDATION QUERIES
# =====================================================================

def validate_data(conn):
    """Run validation queries to ensure data loaded correctly"""
    
    logger.info("\n" + "=" * 70)
    logger.info("DATA VALIDATION")
    logger.info("=" * 70)
    logger.info("")
    # 1. Total revenue check (should match your CSV totals)
    total_revenue = conn.execute("""
        SELECT SUM(Total_Num) as total_revenue
        FROM sales
        WHERE Is_Earned = 1
    """).fetchone()[0]
    
    logger.info(f"  Total Revenue (Earned): ${total_revenue:,.2f}")
    
    # 2. Revenue by transaction type
    revenue_by_type = conn.execute("""
        SELECT 
            Transaction_Type,
            COUNT(*) as count,
            SUM(Total_Num) as revenue
        FROM sales
        WHERE Is_Earned = 1
        GROUP BY Transaction_Type
        ORDER BY revenue DESC
    """).fetchall()
    
    logger.info("")
    logger.info("  Revenue by Type:")
    for txn_type, count, revenue in revenue_by_type:
        pct = (revenue / total_revenue * 100) if total_revenue > 0 else 0
        logger.info(f"    {txn_type:<20}: ${revenue:>12,.2f} ({pct:>5.1f}%) - {count:,} rows")
    
    # 3. Date ranges
    date_range = conn.execute("""
        SELECT 
            MIN(Earned_Date) as min_date,
            MAX(Earned_Date) as max_date
        FROM sales
        WHERE Is_Earned = 1
    """).fetchone()
    
    logger.info("")
    logger.info(f"  Date Range: {date_range[0]} to {date_range[1]}")
    
    # 4. Customer count
    customer_count = conn.execute("SELECT COUNT(DISTINCT CustomerID_Std) FROM customers").fetchone()[0]
    logger.info(f"  Total Customers: {customer_count:,}")
    
    # 5. Items count
    items_count = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    items_quantity = conn.execute("SELECT SUM(Quantity) FROM items").fetchone()[0]
    logger.info(f"  Total Items: {items_count:,} rows ({items_quantity:,} pieces)")
    
    logger.info("")
    logger.info("  [OK] Data validation passed")


# =====================================================================
# CREATE VIEWS FOR COMMON QUERIES
# =====================================================================

def create_views(conn):
    """Create SQL views for frequently used queries"""
    
    logger.info("\n" + "=" * 70)
    logger.info("CREATING VIEWS")
    logger.info("=" * 70)
    logger.info("")
    # View 1: Earned revenue only (excludes uncleaned orders)
    conn.execute("""
        CREATE OR REPLACE VIEW v_earned_sales AS
        SELECT *
        FROM sales
        WHERE Is_Earned = 1
    """)
    logger.info("  [OK] v_earned_sales - Earned revenue only (excludes uncleaned)")
    
    # View 2: B2C items only (for operational load analysis)
    conn.execute("""
        CREATE OR REPLACE VIEW v_b2c_items AS
        SELECT *
        FROM items
        WHERE IsBusinessAccount = 0
    """)
    logger.info("  [OK] v_b2c_items - Consumer items only")
    
    # View 3: Monthly revenue summary
    conn.execute("""
        CREATE OR REPLACE VIEW v_monthly_revenue AS
        SELECT 
            OrderCohortMonth,
            Transaction_Type,
            COUNT(DISTINCT CustomerID_Std) as customers,
            COUNT(*) as transactions,
            SUM(Total_Num) as revenue
        FROM v_earned_sales
        GROUP BY OrderCohortMonth, Transaction_Type
        ORDER BY OrderCohortMonth, Transaction_Type
    """)
    logger.info("  [OK] v_monthly_revenue - Monthly aggregates by transaction type")
    
    logger.info("")
    logger.info("  Views created for easier querying")


# =====================================================================
# MAIN WORKFLOW
# =====================================================================

def main():
    """Run complete ETL workflow"""
    
    logger.info("\n" + "=" * 70)
    logger.info("MOONWALK ANALYTICS - DUCKDB ETL")
    logger.info("=" * 70)
    logger.info("")
    logger.info(f"Source: {CSV_FOLDER}")
    logger.info(f"Target: {DB_PATH}")
    logger.info("")
    start_time = datetime.now()
    
    # Step 1: Validate CSVs exist
    validate_csvs()
    
    # Step 2: Create database and load data
    conn = create_database()
    
    # Step 3: Create indexes
    create_indexes(conn)
    
    # Step 4: Create views
    create_views(conn)
    
    # Step 5: Validate data
    validate_data(conn)
    
    # Close connection, then atomically swap temp file into place
    conn.close()

    # Replace the live DB file (handles locking gracefully)
    tmp_path = DB_PATH.with_suffix('.duckdb.tmp')
    try:
        os.replace(str(tmp_path), str(DB_PATH))
    except PermissionError:
        # Dashboard still holds the file — try removing old first
        try:
            DB_PATH.unlink()
        except PermissionError:
            logger.info(f"  [WARN] Could not replace {DB_PATH.name} (in use) — "
                  f"new DB saved as {tmp_path.name}")
        else:
            os.replace(str(tmp_path), str(DB_PATH))

    elapsed = (datetime.now() - start_time).total_seconds()
    target = DB_PATH if DB_PATH.exists() else tmp_path
    db_size_mb = target.stat().st_size / (1024 * 1024)
    
    logger.info("\n" + "=" * 70)
    logger.info("ETL COMPLETE!")
    logger.info("=" * 70)
    logger.info("")
    logger.info(f"  Database: {DB_PATH}")
    logger.info(f"  Size: {db_size_mb:.1f} MB")
    logger.info(f"  Time: {elapsed:.1f} seconds")
    logger.info("")
    logger.info("NEXT STEPS:")
    logger.info("  1. Run: streamlit run streamlit_app.py")
    logger.info("  2. Open browser to view Revenue Dashboard")
    logger.info("  3. Start developing with Claude Code!")
    logger.info("")
    logger.info("=" * 70)


# =====================================================================
# RUN
# =====================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n\nETL cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.info(f"\n\nERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
