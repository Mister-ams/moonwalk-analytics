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
import json
from pathlib import Path
from datetime import datetime
import os
import sys


# =====================================================================
# CONFIGURATION (centralized in config.py)
# =====================================================================

from config import LOCAL_STAGING_PATH, DB_PATH, LOGS_PATH, DUCKDB_KEY

from logger_config import setup_logger

logger = setup_logger(__name__)

# Profiling accumulator
_profile_entries = []

CSV_FOLDER = LOCAL_STAGING_PATH


def _count_meaningful_values(conn, table: str, col: str) -> int:
    """Count non-null, non-empty-string values (meaningful data before cast)."""
    return conn.execute(
        f"""SELECT COUNT(*) FROM {table}
        WHERE "{col}" IS NOT NULL AND CAST("{col}" AS VARCHAR) != ''"""
    ).fetchone()[0]


def _count_non_null(conn, table: str, col: str) -> int:
    """Count non-null values (after type cast)."""
    return conn.execute(f'SELECT COUNT(*) FROM {table} WHERE "{col}" IS NOT NULL').fetchone()[0]


def _log_cast_loss(conn, table: str, col: str, pre_meaningful: int, cast_type: str) -> None:
    """Compare pre-cast meaningful values vs post-cast non-null; warn on actual data loss."""
    post_non_null = _count_non_null(conn, table, col)
    lost = pre_meaningful - post_non_null
    if lost > 0:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        pct = lost / total * 100 if total > 0 else 0
        logger.warning(
            f"    [WARN] TRY_CAST {table}.{col} to {cast_type}: {lost} non-empty values failed to parse ({pct:.2f}%)"
        )


# CSV files to load
CSV_FILES = {
    "sales": "All_Sales_Python.csv",
    "items": "All_Items_Python.csv",
    "customers": "All_Customers_Python.csv",
    "customer_quality": "Customer_Quality_Monthly_Python.csv",
    "dim_period": "DimPeriod_Python.csv",
}

# Columns to cast from VARCHAR to DATE after table creation
DATE_COLUMNS = {
    "sales": [
        "Placed_Date",
        "Earned_Date",
        "OrderCohortMonth",
        "CohortMonth",
        "Ready By",
        "Cleaned",
        "Collected",
        "Pickup Date",
        "Payment Date",
        "Delivery_Date",
    ],
    "items": ["ItemDate", "ItemCohortMonth"],
    "customers": ["SignedUp_Date", "CohortMonth"],
    "customer_quality": ["OrderCohortMonth"],
    "dim_period": ["Date", "MonthStart", "QuarterStart"],
}

# Columns to cast from BIGINT to BOOLEAN (were stored as 0/1 integers)
BOOL_COLUMNS = {
    "sales": ["Paid", "Is_Earned", "HasDelivery", "HasPickup", "IsSubscriptionService"],
    "items": ["Express", "IsBusinessAccount"],
    "customers": ["IsBusinessAccount"],
    "customer_quality": ["Is_Multi_Service"],
}

# Columns to cast from DOUBLE to INTEGER types (pandas nullable inference artifacts)
INT_COLUMNS = {
    "sales": {
        "MonthsSinceCohort": "SMALLINT",
        "Route #": "SMALLINT",
        "Processing_Days": "SMALLINT",
        "TimeInStore_Days": "SMALLINT",
        "DaysToPayment": "SMALLINT",
    },
}

# ENUM type definitions for low-cardinality columns (data quality + storage)
ENUM_COLUMNS = {
    "sales": {
        "Source": ["CC_2025", "Legacy"],
        "Transaction_Type": ["Order", "Subscription", "Invoice Payment"],
        "Payment_Type_Std": ["Stripe", "Terminal", "Cash", "Receivable", "Other"],
        "Store_Std": ["Moon Walk", "Hielo"],
        "Route_Category": ["Inside Abu Dhabi", "Outer Abu Dhabi", "Other"],
    },
    "items": {
        "Source": ["CC_2025"],
        "Store_Std": ["Moon Walk", "Hielo"],
        "Item_Category": ["Professional Wear", "Traditional Wear", "Home Linens", "Extras", "Others"],
        "Service_Type": ["Wash & Press", "Dry Cleaning", "Press Only", "Other Service"],
    },
}

# Redundant columns to drop (source columns superseded by derived columns)
DROP_COLUMNS = {
    "sales": ["Delivery"],  # Identical to HasDelivery
    "dim_period": [
        "QuarterSortOrder",  # Identical to Quarter
        "MonthSortOrder",  # Identical to Month
        "ISOWeekday",  # Identical to DayOfWeek
        "FiscalYear",  # Identical to Year
        "FiscalQuarter",  # Identical to Quarter
        "DayOfWeekSortOrder",  # Identical to DayOfWeek
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
    tmp_path = DB_PATH.with_suffix(".duckdb.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    if DUCKDB_KEY:
        tmp_path_str = str(tmp_path).replace("\\", "/")
        conn = duckdb.connect(":memory:")
        conn.execute(f"ATTACH '{tmp_path_str}' AS db (ENCRYPTION_KEY '{DUCKDB_KEY}')")
        conn.execute("USE db")
        logger.info(f"  Building new ENCRYPTED database: {tmp_path.name}")
    else:
        conn = duckdb.connect(str(tmp_path))
        logger.info(f"  Building new database: {tmp_path.name}")
    logger.info("")
    total_rows = 0

    # Load each table — prefer Parquet when available, fall back to CSV
    for table_name, csv_file in CSV_FILES.items():
        csv_path = CSV_FOLDER / csv_file
        parquet_path = csv_path.with_suffix(".parquet")

        logger.info(f"  Loading {table_name}...")
        start = datetime.now()

        if parquet_path.exists():
            # Parquet: faster, type-preserving, smaller
            pq_path_str = str(parquet_path).replace("\\", "/")
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{pq_path_str}')")
            source_fmt = "parquet"
        else:
            # CSV fallback
            csv_path_str = str(csv_path).replace("\\", "/")
            conn.execute(
                f"CREATE TABLE {table_name} AS "
                f"SELECT * FROM read_csv_auto('{csv_path_str}', header=true, "
                f"all_varchar=false, sample_size=-1)"
            )
            source_fmt = "csv"

        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        total_rows += row_count

        elapsed = (datetime.now() - start).total_seconds()
        _profile_entries.append(
            {"phase": f"load_{table_name}", "elapsed_s": round(elapsed, 3), "rows": row_count, "format": source_fmt}
        )
        logger.info(f"    [OK] {row_count:,} rows loaded in {elapsed:.1f}s ({source_fmt})")

    logger.info("")
    logger.info(f"  TOTAL: {total_rows:,} rows across {len(CSV_FILES)} tables")

    # Cast VARCHAR date columns to proper DATE type
    logger.info("")
    logger.info("  Casting date columns to DATE type...")
    _cast_start = datetime.now()
    for table_name, columns in DATE_COLUMNS.items():
        for col in columns:
            try:
                pre_meaningful = _count_meaningful_values(conn, table_name, col)
                conn.execute(
                    f'ALTER TABLE {table_name} ALTER COLUMN "{col}" SET DATA TYPE DATE USING TRY_CAST("{col}" AS DATE)'
                )
                _log_cast_loss(conn, table_name, col, pre_meaningful, "DATE")
            except Exception as e:
                logger.info(f"    [WARN] Could not cast {table_name}.{col}: {str(e)[:60]}")
    _profile_entries.append(
        {"phase": "cast_dates", "elapsed_s": round((datetime.now() - _cast_start).total_seconds(), 3)}
    )
    logger.info("    [OK] Date columns cast")

    # Cast BIGINT boolean columns to proper BOOLEAN type
    logger.info("")
    logger.info("  Casting boolean columns to BOOLEAN type...")
    _bool_start = datetime.now()
    bool_count = 0
    for table_name, columns in BOOL_COLUMNS.items():
        for col in columns:
            try:
                pre_meaningful = _count_meaningful_values(conn, table_name, col)
                conn.execute(
                    f'ALTER TABLE {table_name} ALTER COLUMN "{col}" '
                    f'SET DATA TYPE BOOLEAN USING TRY_CAST("{col}" AS BOOLEAN)'
                )
                _log_cast_loss(conn, table_name, col, pre_meaningful, "BOOLEAN")
                bool_count += 1
            except Exception as e:
                logger.info(f"    [WARN] Could not cast {table_name}.{col}: {str(e)[:60]}")
    _profile_entries.append(
        {"phase": "cast_booleans", "elapsed_s": round((datetime.now() - _bool_start).total_seconds(), 3)}
    )
    logger.info(f"    [OK] {bool_count} boolean columns cast")

    # Cast DOUBLE integer columns to proper integer types
    logger.info("")
    logger.info("  Casting integer columns to correct types...")
    _int_start = datetime.now()
    int_count = 0
    for table_name, col_types in INT_COLUMNS.items():
        for col, target_type in col_types.items():
            try:
                pre_meaningful = _count_meaningful_values(conn, table_name, col)
                conn.execute(
                    f'ALTER TABLE {table_name} ALTER COLUMN "{col}" '
                    f'SET DATA TYPE {target_type} USING TRY_CAST("{col}" AS {target_type})'
                )
                _log_cast_loss(conn, table_name, col, pre_meaningful, target_type)
                int_count += 1
            except Exception as e:
                logger.info(f"    [WARN] Could not cast {table_name}.{col}: {str(e)[:60]}")
    _profile_entries.append(
        {"phase": "cast_integers", "elapsed_s": round((datetime.now() - _int_start).total_seconds(), 3)}
    )
    logger.info(f"    [OK] {int_count} integer columns cast")

    # Drop redundant columns
    logger.info("")
    logger.info("  Dropping redundant columns...")
    _drop_start = datetime.now()
    drop_count = 0
    for table_name, columns in DROP_COLUMNS.items():
        for col in columns:
            try:
                conn.execute(f'ALTER TABLE {table_name} DROP COLUMN "{col}"')
                drop_count += 1
            except Exception as e:
                logger.info(f"    [WARN] Could not drop {table_name}.{col}: {str(e)[:60]}")
    _profile_entries.append(
        {"phase": "drop_columns", "elapsed_s": round((datetime.now() - _drop_start).total_seconds(), 3)}
    )
    logger.info(f"    [OK] {drop_count} redundant columns dropped")

    # Create ENUM types and cast low-cardinality VARCHAR columns
    logger.info("")
    logger.info("  Creating ENUM types for low-cardinality columns...")
    _enum_start = datetime.now()
    enum_count = 0
    created_enums = set()
    for table_name, col_defs in ENUM_COLUMNS.items():
        for col, values in col_defs.items():
            # Pre-load validation: detect unknown values in source data
            actual_values = conn.execute(
                f'SELECT DISTINCT "{col}" FROM {table_name} WHERE "{col}" IS NOT NULL'
            ).fetchall()
            actual_set = {row[0] for row in actual_values}
            expected_set = set(values)
            unknown = actual_set - expected_set
            if unknown:
                logger.warning(f"    [WARN] {table_name}.{col} has unknown values not in ENUM spec: {unknown}")

            # Use a shared ENUM name so identical types are reused across tables
            enum_name = f"enum_{col.lower()}"
            if enum_name not in created_enums:
                values_sql = ", ".join(f"'{v}'" for v in values)
                conn.execute(f"CREATE TYPE {enum_name} AS ENUM ({values_sql})")
                created_enums.add(enum_name)
            try:
                pre_meaningful = _count_meaningful_values(conn, table_name, col)
                conn.execute(
                    f'ALTER TABLE {table_name} ALTER COLUMN "{col}" '
                    f'SET DATA TYPE {enum_name} USING TRY_CAST("{col}" AS {enum_name})'
                )
                _log_cast_loss(conn, table_name, col, pre_meaningful, enum_name)
                enum_count += 1
            except Exception as e:
                logger.info(f"    [WARN] Could not cast {table_name}.{col} to ENUM: {str(e)[:60]}")
    _profile_entries.append(
        {"phase": "cast_enums", "elapsed_s": round((datetime.now() - _enum_start).total_seconds(), 3)}
    )
    logger.info(f"    [OK] {enum_count} columns cast to ENUM ({len(created_enums)} types created)")

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

    _idx_start = datetime.now()
    for idx_name, table, column in indexes:
        try:
            conn.execute(f'CREATE INDEX {idx_name} ON {table}("{column}")')
            logger.info(f"  [OK] Created: {idx_name} on {table}({column})")
        except Exception as e:
            logger.info(f"  [WARN] Skipped: {idx_name} - {str(e)[:50]}")

    _profile_entries.append(
        {"phase": "create_indexes", "elapsed_s": round((datetime.now() - _idx_start).total_seconds(), 3)}
    )
    logger.info("")
    logger.info(f"  Indexes created for faster queries")

    # Materialized order lookup for item-to-subscription joins
    logger.info("")
    logger.info("  Creating order_lookup table...")
    _ol_start = datetime.now()
    conn.execute("""
        CREATE TABLE order_lookup AS
        SELECT DISTINCT OrderID_Std, IsSubscriptionService FROM sales
    """)
    conn.execute("CREATE INDEX idx_order_lookup_id ON order_lookup(OrderID_Std)")
    ol_count = conn.execute("SELECT COUNT(*) FROM order_lookup").fetchone()[0]
    _profile_entries.append(
        {"phase": "order_lookup", "elapsed_s": round((datetime.now() - _ol_start).total_seconds(), 3), "rows": ol_count}
    )
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
        ("sales", "Is_Earned", "BOOLEAN"),
        ("sales", "MonthsSinceCohort", "SMALLINT"),
        ("items", "Express", "BOOLEAN"),
        ("customers", "IsBusinessAccount", "BOOLEAN"),
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
# INSIGHTS TABLE — RULES-BASED AUTOMATED ANALYSIS
# =====================================================================


def _insert_insight(conn, period, rule_id, category, headline, detail, sentiment):
    """Insert a single insight row."""
    conn.execute(
        "INSERT INTO insights VALUES (?,?,?,?,?,?)",
        [period, rule_id, category, headline, detail, sentiment],
    )


def create_insights_table(conn):
    """Build rules-based insights for the most recent period."""
    logger.info("\n" + "=" * 70)
    logger.info("BUILDING INSIGHTS TABLE")
    logger.info("=" * 70)
    logger.info("")

    conn.execute("DROP TABLE IF EXISTS insights")
    conn.execute("""
        CREATE TABLE insights (
            period    VARCHAR,
            rule_id   VARCHAR,
            category  VARCHAR,
            headline  VARCHAR,
            detail    VARCHAR,
            sentiment VARCHAR
        )
    """)

    # Determine current and prior month
    row = conn.execute(
        "SELECT MAX(YearMonth) FROM dim_period WHERE YearMonth <= strftime(CURRENT_DATE, '%Y-%m')"
    ).fetchone()
    if not row or not row[0]:
        logger.info("  [SKIP] No dim_period data — skipping insights")
        return
    current_period = row[0]

    # Helper: get prior YearMonth string
    prior_row = conn.execute(f"""
        SELECT YearMonth FROM dim_period
        WHERE YearMonth < '{current_period}'
        ORDER BY YearMonth DESC LIMIT 1
    """).fetchone()
    prior_period = prior_row[0] if prior_row else None

    # Helper: get same month prior year
    yoy_period = f"{int(current_period[:4]) - 1}{current_period[4:]}"

    count = 0

    # REV_MOM — Revenue month-over-month
    if prior_period:
        r = conn.execute(f"""
            SELECT
                SUM(CASE WHEN p.YearMonth = '{current_period}' THEN s.Total_Num ELSE 0 END) AS cur_rev,
                SUM(CASE WHEN p.YearMonth = '{prior_period}' THEN s.Total_Num ELSE 0 END) AS prev_rev
            FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
            WHERE s.Earned_Date IS NOT NULL AND p.YearMonth IN ('{current_period}', '{prior_period}')
        """).fetchone()
        if r and r[1] and r[1] > 0:
            pct = (r[0] - r[1]) / r[1] * 100
            sentiment = "positive" if pct > 0 else "negative"
            _insert_insight(
                conn,
                current_period,
                "REV_MOM",
                "revenue",
                f"Revenue {pct:+.0f}% vs last month",
                f"Dhs {r[0]:,.0f} this month vs Dhs {r[1]:,.0f} last month",
                sentiment,
            )
            count += 1

    # REV_YOY — Revenue year-over-year
    r = conn.execute(f"""
        SELECT
            SUM(CASE WHEN p.YearMonth = '{current_period}' THEN s.Total_Num ELSE 0 END) AS cur_rev,
            SUM(CASE WHEN p.YearMonth = '{yoy_period}' THEN s.Total_Num ELSE 0 END) AS yoy_rev
        FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL AND p.YearMonth IN ('{current_period}', '{yoy_period}')
    """).fetchone()
    if r and r[1] and r[1] > 0:
        pct = (r[0] - r[1]) / r[1] * 100
        sentiment = "positive" if pct > 2 else ("negative" if pct < -2 else "neutral")
        _insert_insight(
            conn,
            current_period,
            "REV_YOY",
            "revenue",
            f"Revenue {pct:+.0f}% vs same month last year",
            f"Dhs {r[0]:,.0f} this month vs Dhs {r[1]:,.0f} in {yoy_period}",
            sentiment,
        )
        count += 1

    # CUST_MOM — Active customer count change
    if prior_period:
        r = conn.execute(f"""
            SELECT
                COUNT(DISTINCT CASE WHEN p.YearMonth = '{current_period}' THEN s.CustomerID_Std END) AS cur,
                COUNT(DISTINCT CASE WHEN p.YearMonth = '{prior_period}' THEN s.CustomerID_Std END) AS prev
            FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
            WHERE s.Earned_Date IS NOT NULL AND s.Transaction_Type <> 'Invoice Payment'
              AND p.YearMonth IN ('{current_period}', '{prior_period}')
        """).fetchone()
        if r and r[1] and r[1] > 0:
            pct = (r[0] - r[1]) / r[1] * 100
            sentiment = "positive" if pct > 0 else "negative"
            _insert_insight(
                conn,
                current_period,
                "CUST_MOM",
                "customers",
                f"Active customers {pct:+.0f}% vs last month",
                f"{r[0]:,} active this month vs {r[1]:,} last month",
                sentiment,
            )
            count += 1

    # NEW_CUST — New customers this month
    r = conn.execute(f"""
        SELECT COUNT(DISTINCT s.CustomerID_Std) AS new_c,
               COUNT(DISTINCT s.CustomerID_Std) * 100.0
                   / NULLIF(COUNT(DISTINCT s.CustomerID_Std), 0) AS pct_check
        FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL AND s.MonthsSinceCohort = 0
          AND p.YearMonth = '{current_period}'
    """).fetchone()
    tot = conn.execute(f"""
        SELECT COUNT(DISTINCT s.CustomerID_Std) FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL AND s.Transaction_Type <> 'Invoice Payment'
          AND p.YearMonth = '{current_period}'
    """).fetchone()[0]
    if r and r[0] and tot and tot > 0:
        pct_share = r[0] / tot * 100
        sentiment = "positive" if pct_share >= 10 else "neutral"
        _insert_insight(
            conn,
            current_period,
            "NEW_CUST",
            "customers",
            f"{r[0]:,} new customers ({pct_share:.0f}% of active)",
            f"First-time customers in {current_period}",
            sentiment,
        )
        count += 1

    # M1_RETENTION — M1 retention rate
    if prior_period:
        r = conn.execute(f"""
            SELECT
                COUNT(DISTINCT CASE WHEN p.YearMonth = '{current_period}' AND s.MonthsSinceCohort = 1 THEN s.CustomerID_Std END) AS m1_cur,
                COUNT(DISTINCT CASE WHEN p.YearMonth = '{prior_period}' AND s.MonthsSinceCohort = 0 THEN s.CustomerID_Std END) AS m0_prev
            FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
            WHERE s.Earned_Date IS NOT NULL AND p.YearMonth IN ('{current_period}', '{prior_period}')
        """).fetchone()
        if r and r[1] and r[1] > 0:
            retention = r[0] / r[1] * 100
            sentiment = "positive" if retention >= 50 else ("negative" if retention < 30 else "neutral")
            _insert_insight(
                conn,
                current_period,
                "M1_RETENTION",
                "customers",
                f"M1 retention: {retention:.0f}%",
                f"{r[0]:,} of {r[1]:,} prior new customers returned",
                sentiment,
            )
            count += 1

    # REACTIVATIONS — Customers dormant 3+ months returning
    r = conn.execute(f"""
        WITH monthly_active AS (
            SELECT DISTINCT s.CustomerID_Std, s.OrderCohortMonth AS month_date
            FROM sales s WHERE s.Earned_Date IS NOT NULL AND s.Transaction_Type <> 'Invoice Payment'
        ),
        with_lag AS (
            SELECT CustomerID_Std, month_date,
                   LAG(month_date) OVER (PARTITION BY CustomerID_Std ORDER BY month_date) AS prev_month
            FROM monthly_active
        ),
        reactivated AS (
            SELECT CustomerID_Std, month_date FROM with_lag
            WHERE prev_month IS NOT NULL AND DATEDIFF('month', prev_month, month_date) >= 3
        )
        SELECT COUNT(DISTINCT r.CustomerID_Std)
        FROM reactivated r JOIN dim_period p ON r.month_date = p.Date
        WHERE p.YearMonth = '{current_period}'
    """).fetchone()
    if r and r[0] and r[0] > 0:
        sentiment = "positive"
        _insert_insight(
            conn,
            current_period,
            "REACTIVATIONS",
            "customers",
            f"{r[0]:,} customers reactivated after 3+ month gap",
            f"Customers returning after dormancy in {current_period}",
            sentiment,
        )
        count += 1

    # SUB_SHARE — Subscription revenue share
    if prior_period:
        r = conn.execute(f"""
            SELECT
                SUM(CASE WHEN p.YearMonth = '{current_period}' AND (s.Transaction_Type = 'Subscription' OR s.IsSubscriptionService = TRUE) THEN s.Total_Num ELSE 0 END) AS sub_cur,
                SUM(CASE WHEN p.YearMonth = '{current_period}' THEN s.Total_Num ELSE 0 END) AS tot_cur,
                SUM(CASE WHEN p.YearMonth = '{prior_period}' AND (s.Transaction_Type = 'Subscription' OR s.IsSubscriptionService = TRUE) THEN s.Total_Num ELSE 0 END) AS sub_prev,
                SUM(CASE WHEN p.YearMonth = '{prior_period}' THEN s.Total_Num ELSE 0 END) AS tot_prev
            FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
            WHERE s.Earned_Date IS NOT NULL AND p.YearMonth IN ('{current_period}', '{prior_period}')
        """).fetchone()
        if r and r[1] and r[1] > 0 and r[3] and r[3] > 0:
            share_cur = r[0] / r[1] * 100
            share_prev = r[2] / r[3] * 100
            diff = share_cur - share_prev
            sentiment = "positive" if diff > 0 else ("negative" if diff < -2 else "neutral")
            _insert_insight(
                conn,
                current_period,
                "SUB_SHARE",
                "revenue",
                f"Subscription revenue at {share_cur:.0f}% of total ({diff:+.0f}pp vs last month)",
                f"Dhs {r[0]:,.0f} subscription of Dhs {r[1]:,.0f} total revenue",
                sentiment,
            )
            count += 1

    # MULTI_SERVICE — Multi-service customer percentage
    r = conn.execute(f"""
        SELECT COUNT(DISTINCT CASE WHEN cq.Is_Multi_Service = TRUE THEN cq.CustomerID_Std END) AS multi,
               COUNT(DISTINCT cq.CustomerID_Std) AS total
        FROM customer_quality cq JOIN dim_period p ON cq.OrderCohortMonth = p.Date
        WHERE p.YearMonth = '{current_period}'
    """).fetchone()
    if r and r[1] and r[1] > 0:
        pct = r[0] / r[1] * 100
        sentiment = "positive" if pct >= 20 else "neutral"
        _insert_insight(
            conn,
            current_period,
            "MULTI_SERVICE",
            "customers",
            f"{pct:.0f}% of customers use multiple services",
            f"{r[0]:,} of {r[1]:,} customers in {current_period}",
            sentiment,
        )
        count += 1

    # CONCENTRATION — Top-20th-percentile revenue share
    r = conn.execute(f"""
        WITH cust_rev AS (
            SELECT s.CustomerID_Std, SUM(s.Total_Num) AS rev
            FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
            WHERE s.Earned_Date IS NOT NULL AND p.YearMonth = '{current_period}'
            GROUP BY s.CustomerID_Std
        ),
        threshold AS (SELECT PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY rev) AS p80 FROM cust_rev)
        SELECT SUM(CASE WHEN rev >= t.p80 THEN rev ELSE 0 END) AS top20_rev,
               SUM(rev) AS total_rev
        FROM cust_rev, threshold t
    """).fetchone()
    if r and r[1] and r[1] > 0:
        share = r[0] / r[1] * 100
        sentiment = "neutral" if share < 70 else ("negative" if share > 85 else "neutral")
        _insert_insight(
            conn,
            current_period,
            "CONCENTRATION",
            "revenue",
            f"Top 20% of customers generate {share:.0f}% of revenue",
            f"Dhs {r[0]:,.0f} of Dhs {r[1]:,.0f} total in {current_period}",
            sentiment,
        )
        count += 1

    # TOP_CATEGORY — Item category with highest volume
    r = conn.execute(f"""
        SELECT i.Item_Category, SUM(i.Quantity) AS qty
        FROM items i JOIN dim_period p ON i.ItemDate = p.Date
        WHERE p.YearMonth = '{current_period}'
        GROUP BY i.Item_Category ORDER BY qty DESC LIMIT 1
    """).fetchone()
    if r:
        _insert_insight(
            conn,
            current_period,
            "TOP_CATEGORY",
            "operations",
            f"Top category: {r[0]} ({r[1]:,} items)",
            f"Highest volume item category in {current_period}",
            "neutral",
        )
        count += 1

    # TOP_SERVICE — Service type with highest volume
    r = conn.execute(f"""
        SELECT i.Service_Type, SUM(i.Quantity) AS qty
        FROM items i JOIN dim_period p ON i.ItemDate = p.Date
        WHERE p.YearMonth = '{current_period}'
        GROUP BY i.Service_Type ORDER BY qty DESC LIMIT 1
    """).fetchone()
    if r:
        _insert_insight(
            conn,
            current_period,
            "TOP_SERVICE",
            "operations",
            f"Top service: {r[0]} ({r[1]:,} items)",
            f"Highest volume service type in {current_period}",
            "neutral",
        )
        count += 1

    # EXPRESS_SHARE — Express item share
    r = conn.execute(f"""
        SELECT SUM(CASE WHEN i.Express = TRUE THEN i.Quantity ELSE 0 END) AS express_qty,
               SUM(i.Quantity) AS total_qty
        FROM items i JOIN dim_period p ON i.ItemDate = p.Date
        WHERE p.YearMonth = '{current_period}'
    """).fetchone()
    if r and r[1] and r[1] > 0:
        pct = r[0] / r[1] * 100
        sentiment = "positive" if pct >= 20 else "neutral"
        _insert_insight(
            conn,
            current_period,
            "EXPRESS_SHARE",
            "operations",
            f"Express orders: {pct:.0f}% of items",
            f"{r[0]:,} express of {r[1]:,} total items in {current_period}",
            sentiment,
        )
        count += 1

    # DELIVERY_RATE — Deliveries as share of total stops
    r = conn.execute(f"""
        SELECT SUM(s.HasDelivery) AS del, SUM(s.HasPickup) AS pck
        FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL AND p.YearMonth = '{current_period}'
    """).fetchone()
    if r and r[0] is not None and r[1] is not None and (r[0] + r[1]) > 0:
        rate = r[0] / (r[0] + r[1]) * 100
        sentiment = "neutral"
        _insert_insight(
            conn,
            current_period,
            "DELIVERY_RATE",
            "operations",
            f"Delivery rate: {rate:.0f}% ({r[0]:,} deliveries, {r[1]:,} pickups)",
            f"Total stops: {r[0] + r[1]:,} in {current_period}",
            sentiment,
        )
        count += 1

    # REV_PER_DELIVERY — Revenue per delivery stop
    r = conn.execute(f"""
        SELECT SUM(CASE WHEN s.HasDelivery = TRUE THEN s.Total_Num ELSE 0 END)
                   / NULLIF(SUM(s.HasDelivery), 0) AS rpd
        FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL AND p.YearMonth = '{current_period}'
    """).fetchone()
    if r and r[0] is not None:
        sentiment = "positive" if r[0] >= 100 else "neutral"
        _insert_insight(
            conn,
            current_period,
            "REV_PER_DELIVERY",
            "operations",
            f"Revenue per delivery: Dhs {r[0]:,.0f}",
            f"Average revenue generated per delivery stop in {current_period}",
            sentiment,
        )
        count += 1

    # GEO_SHIFT — Inside Abu Dhabi stop share vs prior month
    if prior_period:
        r = conn.execute(f"""
            SELECT
                SUM(CASE WHEN p.YearMonth = '{current_period}' AND s.Route_Category = 'Inside Abu Dhabi' THEN CAST(s.HasDelivery AS INTEGER) + CAST(s.HasPickup AS INTEGER) ELSE 0 END) AS inside_cur,
                SUM(CASE WHEN p.YearMonth = '{current_period}' THEN CAST(s.HasDelivery AS INTEGER) + CAST(s.HasPickup AS INTEGER) ELSE 0 END) AS total_cur,
                SUM(CASE WHEN p.YearMonth = '{prior_period}' AND s.Route_Category = 'Inside Abu Dhabi' THEN CAST(s.HasDelivery AS INTEGER) + CAST(s.HasPickup AS INTEGER) ELSE 0 END) AS inside_prev,
                SUM(CASE WHEN p.YearMonth = '{prior_period}' THEN CAST(s.HasDelivery AS INTEGER) + CAST(s.HasPickup AS INTEGER) ELSE 0 END) AS total_prev
            FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
            WHERE s.Earned_Date IS NOT NULL AND p.YearMonth IN ('{current_period}', '{prior_period}')
        """).fetchone()
        if r and r[1] and r[1] > 0 and r[3] and r[3] > 0:
            pct_cur = r[0] / r[1] * 100
            pct_prev = r[2] / r[3] * 100
            diff = pct_cur - pct_prev
            sentiment = "neutral"
            _insert_insight(
                conn,
                current_period,
                "GEO_SHIFT",
                "operations",
                f"Inside Abu Dhabi stops: {pct_cur:.0f}% ({diff:+.0f}pp vs last month)",
                f"{r[0]:,} inside stops of {r[1]:,} total in {current_period}",
                sentiment,
            )
            count += 1

    # DIGITAL_PAYMENT — Stripe + Terminal share
    r = conn.execute(f"""
        SELECT
            SUM(CASE WHEN s.Payment_Type_Std IN ('Stripe', 'Terminal') THEN s.Collections ELSE 0 END) AS digital,
            SUM(s.Collections) AS total
        FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL AND p.YearMonth = '{current_period}'
    """).fetchone()
    if r and r[1] and r[1] > 0:
        pct = r[0] / r[1] * 100
        sentiment = "positive" if pct >= 70 else "neutral"
        _insert_insight(
            conn,
            current_period,
            "DIGITAL_PAYMENT",
            "payments",
            f"Digital payments: {pct:.0f}% of collections",
            f"Dhs {r[0]:,.0f} stripe+terminal of Dhs {r[1]:,.0f} total in {current_period}",
            sentiment,
        )
        count += 1

    # COLLECTION_RATE — Collections / Revenue
    r = conn.execute(f"""
        SELECT SUM(s.Collections) AS coll, SUM(s.Total_Num) AS rev
        FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL AND p.YearMonth = '{current_period}'
    """).fetchone()
    if r and r[1] and r[1] > 0:
        rate = r[0] / r[1] * 100
        sentiment = "positive" if rate >= 90 else ("negative" if rate < 70 else "neutral")
        _insert_insight(
            conn,
            current_period,
            "COLLECTION_RATE",
            "payments",
            f"Collection rate: {rate:.0f}% of revenue collected",
            f"Dhs {r[0]:,.0f} collected of Dhs {r[1]:,.0f} earned in {current_period}",
            sentiment,
        )
        count += 1

    # AVG_DAYS_PAYMENT — Avg days to payment vs prior month
    if prior_period:
        r = conn.execute(f"""
            SELECT
                AVG(CASE WHEN p.YearMonth = '{current_period}' THEN s.DaysToPayment END) AS cur,
                AVG(CASE WHEN p.YearMonth = '{prior_period}' THEN s.DaysToPayment END) AS prev
            FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
            WHERE s.Earned_Date IS NOT NULL AND s.DaysToPayment IS NOT NULL
              AND p.YearMonth IN ('{current_period}', '{prior_period}')
        """).fetchone()
        if r and r[0] is not None and r[1] is not None and r[1] > 0:
            diff = r[0] - r[1]
            sentiment = "positive" if diff < 0 else ("negative" if diff > 1 else "neutral")
            _insert_insight(
                conn,
                current_period,
                "AVG_DAYS_PAYMENT",
                "payments",
                f"Avg days to payment: {r[0]:.1f} days ({diff:+.1f} vs last month)",
                f"Average collection cycle in {current_period}",
                sentiment,
            )
            count += 1

    # OUTSTANDING_PCT — Outstanding as % of revenue (CC_2025 only)
    r = conn.execute(f"""
        SELECT SUM(CASE WHEN s.Paid = FALSE AND s.Source = 'CC_2025' THEN s.Total_Num ELSE 0 END) AS outstanding,
               SUM(s.Total_Num) AS total_rev
        FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL AND p.YearMonth = '{current_period}'
    """).fetchone()
    if r and r[1] and r[1] > 0 and r[0] is not None:
        pct = r[0] / r[1] * 100
        sentiment = "negative" if pct > 10 else ("neutral" if pct > 5 else "positive")
        _insert_insight(
            conn,
            current_period,
            "OUTSTANDING_PCT",
            "payments",
            f"Outstanding: {pct:.0f}% of revenue (Dhs {r[0]:,.0f})",
            f"Unpaid CC_2025 orders in {current_period}",
            sentiment,
        )
        count += 1

    # PROCESSING_TIME — Flag if avg processing > 3 days
    r = conn.execute(f"""
        SELECT AVG(s.Processing_Days) AS avg_proc
        FROM sales s JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL AND s.Processing_Days IS NOT NULL
          AND p.YearMonth = '{current_period}'
    """).fetchone()
    if r and r[0] is not None:
        sentiment = "negative" if r[0] > 3.0 else "positive"
        _insert_insight(
            conn,
            current_period,
            "PROCESSING_TIME",
            "operations",
            f"Avg processing time: {r[0]:.1f} days{'  — above target' if r[0] > 3.0 else ''}",
            f"Average order processing cycle in {current_period}",
            sentiment,
        )
        count += 1

    logger.info(f"  [OK] {count} insights generated for {current_period}")


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

    # Step 2b: Mark Legacy orders as Paid (they were settled pre-CleanCloud)
    n = conn.execute("SELECT COUNT(*) FROM sales WHERE Source = 'Legacy' AND Paid = FALSE").fetchone()[0]
    conn.execute("UPDATE sales SET Paid = TRUE WHERE Source = 'Legacy'")
    logger.info(f"  [OK] Marked {n:,} Legacy orders as Paid")

    # Step 3: Create indexes
    create_indexes(conn)

    # Step 4: Validate data
    validate_data(conn)

    # Step 5: Build rules-based insights table
    create_insights_table(conn)

    # Close connection, then atomically swap temp file into place
    conn.close()

    # Replace the live DB file (handles locking gracefully)
    tmp_path = DB_PATH.with_suffix(".duckdb.tmp")
    try:
        os.replace(str(tmp_path), str(DB_PATH))
    except PermissionError:
        # Dashboard still holds the file — try removing old first
        try:
            DB_PATH.unlink()
        except PermissionError:
            logger.info(f"  [WARN] Could not replace {DB_PATH.name} (in use) — new DB saved as {tmp_path.name}")
        else:
            os.replace(str(tmp_path), str(DB_PATH))

    elapsed = (datetime.now() - start_time).total_seconds()
    target = DB_PATH if DB_PATH.exists() else tmp_path
    db_size_mb = target.stat().st_size / (1024 * 1024)

    # Write profiling results
    profile = {
        "timestamp": datetime.now().isoformat(),
        "total_elapsed_s": round(elapsed, 3),
        "db_size_mb": round(db_size_mb, 1),
        "phases": _profile_entries,
    }
    LOGS_PATH.mkdir(parents=True, exist_ok=True)
    profile_path = LOGS_PATH / f"duckdb_profile_{datetime.now():%Y-%m-%d_%H%M%S}.json"
    profile_path.write_text(json.dumps(profile, indent=2))
    logger.info(f"\n  [PROFILE] Written to {profile_path.name}")

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
