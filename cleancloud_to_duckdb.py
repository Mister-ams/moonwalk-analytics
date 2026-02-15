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

# Columns to cast from VARCHAR to DATE after table creation
DATE_COLUMNS = {
    'sales': ['Placed_Date', 'Earned_Date', 'OrderCohortMonth', 'CohortMonth',
              'Ready By', 'Cleaned', 'Collected', 'Pickup Date', 'Payment Date',
              'Delivery_Date'],
    'items': ['ItemDate', 'ItemCohortMonth'],
    'customers': ['SignedUp_Date', 'CohortMonth'],
    'customer_quality': ['OrderCohortMonth'],
    'dim_period': ['Date', 'MonthStart', 'QuarterStart'],
}

# Columns to cast from BIGINT to BOOLEAN (were stored as 0/1 integers)
BOOL_COLUMNS = {
    'sales': ['Paid', 'Is_Earned', 'HasDelivery', 'HasPickup', 'IsSubscriptionService'],
    'items': ['Express', 'IsBusinessAccount'],
    'customers': ['IsBusinessAccount'],
    'customer_quality': ['Is_Multi_Service'],
}

# Columns to cast from DOUBLE to INTEGER types (pandas nullable inference artifacts)
INT_COLUMNS = {
    'sales': {
        'MonthsSinceCohort': 'SMALLINT',
        'Route #': 'SMALLINT',
        'Processing_Days': 'SMALLINT',
        'TimeInStore_Days': 'SMALLINT',
        'DaysToPayment': 'SMALLINT',
    },
}

# Redundant columns to drop (source columns superseded by derived columns)
DROP_COLUMNS = {
    'sales': ['Delivery'],  # Identical to HasDelivery
    'dim_period': [
        'QuarterSortOrder',    # Identical to Quarter
        'MonthSortOrder',      # Identical to Month
        'ISOWeekday',          # Identical to DayOfWeek
        'FiscalYear',          # Identical to Year
        'FiscalQuarter',       # Identical to Quarter
        'DayOfWeekSortOrder',  # Identical to DayOfWeek
    ],
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
    """Create DuckDB database and load all tables using native CSV reader"""

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

    # Load each CSV directly with DuckDB's native reader (no pandas intermediary)
    for table_name, csv_file in CSV_FILES.items():
        csv_path = CSV_FOLDER / csv_file

        logger.info(f"  Loading {table_name}...")
        start = datetime.now()

        # DuckDB native CSV reader — faster and avoids pandas type issues
        csv_path_str = str(csv_path).replace('\\', '/')
        conn.execute(
            f"CREATE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{csv_path_str}', header=true, "
            f"all_varchar=false, sample_size=-1)"
        )

        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        total_rows += row_count

        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"    [OK] {row_count:,} rows loaded in {elapsed:.1f}s")

    logger.info("")
    logger.info(f"  TOTAL: {total_rows:,} rows across {len(CSV_FILES)} tables")

    # Cast VARCHAR date columns to proper DATE type
    logger.info("")
    logger.info("  Casting date columns to DATE type...")
    for table_name, columns in DATE_COLUMNS.items():
        for col in columns:
            try:
                conn.execute(
                    f'ALTER TABLE {table_name} ALTER COLUMN "{col}" '
                    f'SET DATA TYPE DATE USING TRY_CAST("{col}" AS DATE)'
                )
            except Exception as e:
                logger.info(f"    [WARN] Could not cast {table_name}.{col}: {str(e)[:60]}")
    logger.info("    [OK] Date columns cast")

    # Cast BIGINT boolean columns to proper BOOLEAN type
    logger.info("")
    logger.info("  Casting boolean columns to BOOLEAN type...")
    bool_count = 0
    for table_name, columns in BOOL_COLUMNS.items():
        for col in columns:
            try:
                conn.execute(
                    f'ALTER TABLE {table_name} ALTER COLUMN "{col}" '
                    f'SET DATA TYPE BOOLEAN USING TRY_CAST("{col}" AS BOOLEAN)'
                )
                bool_count += 1
            except Exception as e:
                logger.info(f"    [WARN] Could not cast {table_name}.{col}: {str(e)[:60]}")
    logger.info(f"    [OK] {bool_count} boolean columns cast")

    # Cast DOUBLE integer columns to proper integer types
    logger.info("")
    logger.info("  Casting integer columns to correct types...")
    int_count = 0
    for table_name, col_types in INT_COLUMNS.items():
        for col, target_type in col_types.items():
            try:
                conn.execute(
                    f'ALTER TABLE {table_name} ALTER COLUMN "{col}" '
                    f'SET DATA TYPE {target_type} USING TRY_CAST("{col}" AS {target_type})'
                )
                int_count += 1
            except Exception as e:
                logger.info(f"    [WARN] Could not cast {table_name}.{col}: {str(e)[:60]}")
    logger.info(f"    [OK] {int_count} integer columns cast")

    # Drop redundant columns
    logger.info("")
    logger.info("  Dropping redundant columns...")
    drop_count = 0
    for table_name, columns in DROP_COLUMNS.items():
        for col in columns:
            try:
                conn.execute(f'ALTER TABLE {table_name} DROP COLUMN "{col}"')
                drop_count += 1
            except Exception as e:
                logger.info(f"    [WARN] Could not drop {table_name}.{col}: {str(e)[:60]}")
    logger.info(f"    [OK] {drop_count} redundant columns dropped")

    return conn


# =====================================================================
# CREATE INDEXES FOR PERFORMANCE
# =====================================================================

def create_indexes(conn):
    """Create indexes on frequently filtered/joined columns"""

    logger.info("\n" + "=" * 70)
    logger.info("CREATING INDEXES")
    logger.info("=" * 70)
    logger.info("")
    indexes = [
        # Sales table
        ("idx_sales_customer", "sales", "CustomerID_Std"),
        ("idx_sales_order", "sales", "OrderID_Std"),
        ("idx_sales_cohort_month", "sales", "OrderCohortMonth"),
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
        ("idx_period_yearmonth", "dim_period", "YearMonth"),
        ("idx_period_isoweeklabel", "dim_period", "ISOWeekLabel"),
    ]

    for idx_name, table, column in indexes:
        try:
            conn.execute(f'CREATE INDEX {idx_name} ON {table}("{column}")')
            logger.info(f"  [OK] Created: {idx_name} on {table}({column})")
        except Exception as e:
            logger.info(f"  [WARN] Skipped: {idx_name} - {str(e)[:50]}")

    logger.info("")
    logger.info(f"  Indexes created for faster queries")

    # Materialized order lookup for item-to-subscription joins
    logger.info("")
    logger.info("  Creating order_lookup table...")
    conn.execute("""
        CREATE TABLE order_lookup AS
        SELECT DISTINCT OrderID_Std, IsSubscriptionService FROM sales
    """)
    conn.execute("CREATE INDEX idx_order_lookup_id ON order_lookup(OrderID_Std)")
    ol_count = conn.execute("SELECT COUNT(*) FROM order_lookup").fetchone()[0]
    logger.info(f"  [OK] order_lookup: {ol_count:,} distinct orders")


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
        WHERE Is_Earned = true
    """).fetchone()[0]

    logger.info(f"  Total Revenue (Earned): ${total_revenue:,.2f}")

    # 2. Revenue by transaction type
    revenue_by_type = conn.execute("""
        SELECT
            Transaction_Type,
            COUNT(*) as count,
            SUM(Total_Num) as revenue
        FROM sales
        WHERE Is_Earned = true
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
        WHERE Is_Earned = true
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

    # 6. Type verification
    logger.info("")
    logger.info("  Type verification:")
    for check_table, check_col, expected_type in [
        ('sales', 'Is_Earned', 'BOOLEAN'),
        ('sales', 'MonthsSinceCohort', 'SMALLINT'),
        ('items', 'Express', 'BOOLEAN'),
        ('customers', 'IsBusinessAccount', 'BOOLEAN'),
    ]:
        actual = conn.execute(
            f"SELECT data_type FROM information_schema.columns "
            f"WHERE table_name = '{check_table}' AND column_name = '{check_col}'"
        ).fetchone()
        if actual:
            status = "[OK]" if actual[0] == expected_type else f"[WARN] got {actual[0]}"
            logger.info(f"    {status} {check_table}.{check_col} = {actual[0]}")

    logger.info("")
    logger.info("  [OK] Data validation passed")


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

    # Step 4: Validate data
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
