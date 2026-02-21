"""Postgres ETL — Sync Moonwalk analytics tables from Parquet to Railway Postgres.

Reads the same Parquet files produced by cleancloud_to_excel_MASTER.py and
bulk-loads them into the analytics schema in Railway Postgres.  Runs in
parallel with the DuckDB rebuild during the Tock M migration period.

Key differences from cleancloud_to_duckdb.py:
  - Schema already exists (migration 001 created it); only TRUNCATE + reload.
  - Booleans stored as Int32 in Parquet → cast to Python bool before insert.
  - Date strings ("YYYY-MM-DD") accepted directly by Postgres via psycopg2.
  - Phone + Email encrypted with pgp_sym_encrypt (Postgres server-side).
  - order_lookup derived via SQL INSERT ... SELECT after sales is loaded.
  - Insights SQL ported to Postgres dialect (quoted identifiers, TO_CHAR,
    AGE() instead of DATEDIFF).
  - Skips silently when ANALYTICS_DATABASE_URL is not configured.

Usage:
    python cleancloud_to_postgres.py
    python cleancloud_to_postgres.py --skip-insights   # faster, data only
"""

import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl
import psycopg2
import psycopg2.extras

from config import ANALYTICS_DATABASE_URL, ENCRYPTION_KEY, LOCAL_STAGING_PATH
from logger_config import setup_logger

logger = setup_logger(__name__)

# =====================================================================
# FILE MAP  (mirrors cleancloud_to_duckdb.py CSV_FILES)
# =====================================================================

PARQUET_FILES = {
    "sales": "All_Sales_Python.parquet",
    "items": "All_Items_Python.parquet",
    "customers": "All_Customers_Python.parquet",
    "customer_quality": "Customer_Quality_Monthly_Python.parquet",
    "dim_period": "DimPeriod_Python.parquet",
}

# Tables with SERIAL PKs need RESTART IDENTITY on truncate
SERIAL_PK_TABLES = {"sales", "items"}

# Columns to exclude before inserting (mirrors cleancloud_to_duckdb.py DROP_COLUMNS)
_DROP_COLUMNS = {
    "sales": {"Delivery"},
    "dim_period": {
        "QuarterSortOrder",
        "MonthSortOrder",
        "ISOWeekday",
        "FiscalYear",
        "FiscalQuarter",
        "DayOfWeekSortOrder",
    },
}

# Boolean columns stored as Int32/Int64 in Parquet — need Python bool cast
_BOOL_COLUMNS = {
    "sales": {"Paid", "Is_Earned", "HasDelivery", "HasPickup", "IsSubscriptionService"},
    "items": {"Express", "IsBusinessAccount"},
    "customers": {"IsBusinessAccount"},
    "customer_quality": {"Is_Multi_Service"},
    "dim_period": {
        "IsFirstDayOfISOWeek",
        "IsLastDayOfISOWeek",
        "IsCurrentMonth",
        "IsCurrentQuarter",
        "IsCurrentYear",
        "IsCurrentISOWeek",
        "IsFirstDayOfMonth",
        "IsLastDayOfMonth",
        "IsWeekend",
        "IsWeekday",
    },
}

# PII columns encrypted with pgcrypto in the customers table
_PII_COLUMNS = {"Phone", "Email"}


# =====================================================================
# CONNECTION
# =====================================================================


def _connect():
    """Open a psycopg2 connection with analytics as the default schema.

    Uses PostgreSQL startup options to set search_path before any transaction
    begins.  This avoids the SQLAlchemy autobegin trap (see alembic/env.py).
    """
    return psycopg2.connect(
        ANALYTICS_DATABASE_URL,
        options="-c search_path=analytics,public",
    )


# =====================================================================
# POLARS PREPROCESSING — normalise types before inserting
# =====================================================================


def _prepare_df(table_name: str, df: pl.DataFrame) -> pl.DataFrame:
    """Cast Parquet types to the shapes psycopg2 expects for Postgres.

    - Drop excluded columns.
    - Cast Int32/Int64 boolean 0/1 columns to pl.Boolean (→ Python bool).
    - Float64 NaN → null so NUMERIC columns receive NULL not NaN.
    - Empty-string "" in String columns → null so DATE/VARCHAR columns
      receive NULL rather than an invalid date literal.
    """
    # Drop excluded columns
    drop = _DROP_COLUMNS.get(table_name, set())
    to_drop = [c for c in drop if c in df.columns]
    if to_drop:
        df = df.drop(to_drop)

    # Cast boolean int columns to proper Python bool
    for col in _BOOL_COLUMNS.get(table_name, set()):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Boolean))

    # Eliminate NaN from float columns so Postgres receives NULL
    float_cols = [c for c, d in zip(df.columns, df.dtypes) if d == pl.Float64]
    if float_cols:
        df = df.with_columns([pl.col(c).fill_nan(None) for c in float_cols])

    # Replace empty strings with null in all String columns so that date
    # columns (stored as "YYYY-MM-DD" strings in Parquet) don't send "" to
    # Postgres DATE fields, which would raise InvalidDatetimeFormat.
    str_cols = [c for c, d in zip(df.columns, df.dtypes) if d == pl.String]
    if str_cols:
        df = df.with_columns([pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c) for c in str_cols])

    return df


# =====================================================================
# BULK LOAD HELPERS
# =====================================================================


def _truncate(cur, table_name: str) -> None:
    restart = "RESTART IDENTITY" if table_name in SERIAL_PK_TABLES else ""
    cur.execute(f"TRUNCATE analytics.{table_name} {restart} CASCADE")


def _bulk_insert(cur, table_name: str, df: pl.DataFrame) -> int:
    """Bulk-insert all rows from a prepared DataFrame using execute_values."""
    col_sql = ", ".join(f'"{c}"' for c in df.columns)
    rows = df.rows()
    psycopg2.extras.execute_values(
        cur,
        f"INSERT INTO analytics.{table_name} ({col_sql}) VALUES %s",
        rows,
        page_size=2000,
    )
    return len(rows)


def _load_customers(conn, df: pl.DataFrame) -> int:
    """Load customers with pgcrypto encryption on Phone and Email.

    pgp_sym_encrypt() is called server-side; plaintext travels over TLS
    (sslmode=require is set in ANALYTICS_DATABASE_URL).
    """
    schema_cols = [
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
    present = [c for c in schema_cols if c in df.columns]
    df = _prepare_df("customers", df.select(present))

    # Build per-column SQL fragments; PII columns use pgp_sym_encrypt
    col_sql = ", ".join(f'"{c}"' for c in df.columns)
    template_parts = [f"pgp_sym_encrypt(%s, %s)" if c in _PII_COLUMNS else "%s" for c in df.columns]
    template = "(" + ", ".join(template_parts) + ")"

    # For PII columns interleave (value, encryption_key) in the row tuple
    rows = []
    for row in df.rows(named=True):
        values: list = []
        for col in df.columns:
            val = row[col]
            if col in _PII_COLUMNS:
                values.append(str(val) if val is not None else None)
                values.append(ENCRYPTION_KEY)
            else:
                values.append(val)
        rows.append(tuple(values))

    with conn.cursor() as cur:
        _truncate(cur, "customers")
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO analytics.customers ({col_sql}) VALUES %s",
            rows,
            template=template,
            page_size=2000,
        )
    conn.commit()
    return len(rows)


# =====================================================================
# ORDER LOOKUP — derived from sales
# =====================================================================


def _load_order_lookup(conn) -> int:
    """Truncate + fill order_lookup from the just-loaded sales table."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE analytics.order_lookup")
        cur.execute("""
            INSERT INTO analytics.order_lookup ("OrderID_Std", "IsSubscriptionService")
            SELECT DISTINCT "OrderID_Std", "IsSubscriptionService"
            FROM analytics.sales
            WHERE "OrderID_Std" IS NOT NULL
        """)
        cur.execute("SELECT COUNT(*) FROM analytics.order_lookup")
        n = cur.fetchone()[0]
    conn.commit()
    return n


# =====================================================================
# INSIGHTS — ported from cleancloud_to_duckdb.py to Postgres SQL
#
# Key dialect changes:
#   strftime(CURRENT_DATE, '%Y-%m')  →  TO_CHAR(CURRENT_DATE, 'YYYY-MM')
#   DATEDIFF('month', d1, d2) >= 3  →  d2 >= d1 + INTERVAL '3 months'
#   All column references quoted with double-quotes
#   ? placeholders                   →  %s
# =====================================================================


def _insert_insight(cur, period, rule_id, category, headline, detail, sentiment, granularity="monthly"):
    cur.execute(
        """INSERT INTO analytics.insights
           (period, rule_id, category, headline, detail, sentiment, granularity)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (period, rule_id, category, headline, detail, sentiment, granularity),
    )


def _create_insights(conn) -> int:
    """Build rules-based insights for the most recent period.

    Truncates the insights table then regenerates all monthly + weekly rules.
    Returns the total number of insight rows inserted.
    """
    with conn.cursor() as cur:
        cur.execute("TRUNCATE analytics.insights")

        # Current and prior month
        cur.execute("""
            SELECT MAX("YearMonth") FROM analytics.dim_period
            WHERE "YearMonth" < TO_CHAR(CURRENT_DATE, 'YYYY-MM')
        """)
        row = cur.fetchone()
        if not row or not row[0]:
            logger.info("  [SKIP] No dim_period data — skipping insights")
            conn.commit()
            return 0
        current_period = row[0]

        cur.execute(
            """
            SELECT "YearMonth" FROM analytics.dim_period
            WHERE "YearMonth" < %s
            ORDER BY "YearMonth" DESC LIMIT 1
        """,
            (current_period,),
        )
        row = cur.fetchone()
        prior_period = row[0] if row else None

        yoy_period = f"{int(current_period[:4]) - 1}{current_period[4:]}"

        count = 0

        # REV_MOM
        if prior_period:
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN p."YearMonth" = %s THEN s."Total_Num" ELSE 0 END),
                    SUM(CASE WHEN p."YearMonth" = %s THEN s."Total_Num" ELSE 0 END)
                FROM analytics.sales s
                JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
                WHERE s."Earned_Date" IS NOT NULL
                  AND p."YearMonth" IN (%s, %s)
            """,
                (current_period, prior_period, current_period, prior_period),
            )
            r = cur.fetchone()
            if r and r[1] and r[1] > 0:
                pct = (r[0] - r[1]) / r[1] * 100
                _insert_insight(
                    cur,
                    current_period,
                    "REV_MOM",
                    "revenue",
                    f"Revenue {pct:+.0f}% vs last month",
                    f"Dhs {r[0]:,.0f} this month vs Dhs {r[1]:,.0f} last month",
                    "positive" if pct > 0 else "negative",
                )
                count += 1

        # REV_YOY
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN p."YearMonth" = %s THEN s."Total_Num" ELSE 0 END),
                SUM(CASE WHEN p."YearMonth" = %s THEN s."Total_Num" ELSE 0 END)
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
            WHERE s."Earned_Date" IS NOT NULL
              AND p."YearMonth" IN (%s, %s)
        """,
            (current_period, yoy_period, current_period, yoy_period),
        )
        r = cur.fetchone()
        if r and r[1] and r[1] > 0:
            pct = (r[0] - r[1]) / r[1] * 100
            sent = "positive" if pct > 2 else ("negative" if pct < -2 else "neutral")
            _insert_insight(
                cur,
                current_period,
                "REV_YOY",
                "revenue",
                f"Revenue {pct:+.0f}% vs same month last year",
                f"Dhs {r[0]:,.0f} this month vs Dhs {r[1]:,.0f} in {yoy_period}",
                sent,
            )
            count += 1

        # CUST_MOM
        if prior_period:
            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT CASE WHEN p."YearMonth" = %s THEN s."CustomerID_Std" END),
                    COUNT(DISTINCT CASE WHEN p."YearMonth" = %s THEN s."CustomerID_Std" END)
                FROM analytics.sales s
                JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
                WHERE s."Earned_Date" IS NOT NULL
                  AND s."Transaction_Type" <> 'Invoice Payment'
                  AND p."YearMonth" IN (%s, %s)
            """,
                (current_period, prior_period, current_period, prior_period),
            )
            r = cur.fetchone()
            if r and r[1] and r[1] > 0:
                pct = (r[0] - r[1]) / r[1] * 100
                _insert_insight(
                    cur,
                    current_period,
                    "CUST_MOM",
                    "customers",
                    f"Active customers {pct:+.0f}% vs last month",
                    f"{r[0]:,} active this month vs {r[1]:,} last month",
                    "positive" if pct > 0 else "negative",
                )
                count += 1

        # NEW_CUST
        cur.execute(
            """
            SELECT COUNT(DISTINCT s."CustomerID_Std")
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
            WHERE s."Earned_Date" IS NOT NULL AND s."MonthsSinceCohort" = 0
              AND p."YearMonth" = %s
        """,
            (current_period,),
        )
        new_c = cur.fetchone()[0] or 0
        cur.execute(
            """
            SELECT COUNT(DISTINCT s."CustomerID_Std")
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
            WHERE s."Earned_Date" IS NOT NULL
              AND s."Transaction_Type" <> 'Invoice Payment'
              AND p."YearMonth" = %s
        """,
            (current_period,),
        )
        tot = cur.fetchone()[0] or 0
        if new_c and tot and tot > 0:
            pct_share = new_c / tot * 100
            _insert_insight(
                cur,
                current_period,
                "NEW_CUST",
                "customers",
                f"{new_c:,} new customers ({pct_share:.0f}% of active)",
                f"First-time customers in {current_period}",
                "positive" if pct_share >= 10 else "neutral",
            )
            count += 1

        # M1_RETENTION
        if prior_period:
            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT CASE WHEN p."YearMonth" = %s AND s."MonthsSinceCohort" = 1
                                        THEN s."CustomerID_Std" END),
                    COUNT(DISTINCT CASE WHEN p."YearMonth" = %s AND s."MonthsSinceCohort" = 0
                                        THEN s."CustomerID_Std" END)
                FROM analytics.sales s
                JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
                WHERE s."Earned_Date" IS NOT NULL
                  AND p."YearMonth" IN (%s, %s)
            """,
                (current_period, prior_period, current_period, prior_period),
            )
            r = cur.fetchone()
            if r and r[1] and r[1] > 0:
                ret = r[0] / r[1] * 100
                sent = "positive" if ret >= 50 else ("negative" if ret < 30 else "neutral")
                _insert_insight(
                    cur,
                    current_period,
                    "M1_RETENTION",
                    "customers",
                    f"M1 retention: {ret:.0f}%",
                    f"{r[0]:,} of {r[1]:,} prior new customers returned",
                    sent,
                )
                count += 1

        # REACTIVATIONS  (DATEDIFF → AGE interval)
        cur.execute(f"""
            WITH monthly_active AS (
                SELECT DISTINCT s."CustomerID_Std",
                       s."OrderCohortMonth" AS month_date
                FROM analytics.sales s
                WHERE s."Earned_Date" IS NOT NULL
                  AND s."Transaction_Type" <> 'Invoice Payment'
            ),
            with_lag AS (
                SELECT "CustomerID_Std", month_date,
                       LAG(month_date) OVER (PARTITION BY "CustomerID_Std"
                                             ORDER BY month_date) AS prev_month
                FROM monthly_active
            ),
            reactivated AS (
                SELECT "CustomerID_Std", month_date
                FROM with_lag
                WHERE prev_month IS NOT NULL
                  AND month_date >= prev_month + INTERVAL '3 months'
            )
            SELECT COUNT(DISTINCT r."CustomerID_Std")
            FROM reactivated r
            JOIN analytics.dim_period p ON r.month_date = p."Date"
            WHERE p."YearMonth" = '{current_period}'
        """)
        r = cur.fetchone()
        if r and r[0] and r[0] > 0:
            _insert_insight(
                cur,
                current_period,
                "REACTIVATIONS",
                "customers",
                f"{r[0]:,} customers reactivated after 3+ month gap",
                f"Customers returning after dormancy in {current_period}",
                "positive",
            )
            count += 1

        # SUB_SHARE
        if prior_period:
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN p."YearMonth" = %s
                              AND (s."Transaction_Type" = 'Subscription'
                                   OR s."IsSubscriptionService" = TRUE)
                             THEN s."Total_Num" ELSE 0 END),
                    SUM(CASE WHEN p."YearMonth" = %s THEN s."Total_Num" ELSE 0 END),
                    SUM(CASE WHEN p."YearMonth" = %s
                              AND (s."Transaction_Type" = 'Subscription'
                                   OR s."IsSubscriptionService" = TRUE)
                             THEN s."Total_Num" ELSE 0 END),
                    SUM(CASE WHEN p."YearMonth" = %s THEN s."Total_Num" ELSE 0 END)
                FROM analytics.sales s
                JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
                WHERE s."Earned_Date" IS NOT NULL
                  AND p."YearMonth" IN (%s, %s)
            """,
                (current_period, current_period, prior_period, prior_period, current_period, prior_period),
            )
            r = cur.fetchone()
            if r and r[1] and r[1] > 0 and r[3] and r[3] > 0:
                sc = r[0] / r[1] * 100
                sp = r[2] / r[3] * 100
                diff = sc - sp
                _insert_insight(
                    cur,
                    current_period,
                    "SUB_SHARE",
                    "revenue",
                    f"Subscription revenue at {sc:.0f}% of total ({diff:+.0f}pp vs last month)",
                    f"Dhs {r[0]:,.0f} subscription of Dhs {r[1]:,.0f} total revenue",
                    "positive" if diff > 0 else ("negative" if diff < -2 else "neutral"),
                )
                count += 1

        # MULTI_SERVICE
        cur.execute(
            """
            SELECT COUNT(DISTINCT CASE WHEN cq."Is_Multi_Service" = TRUE
                                        THEN cq."CustomerID_Std" END),
                   COUNT(DISTINCT cq."CustomerID_Std")
            FROM analytics.customer_quality cq
            JOIN analytics.dim_period p ON cq."OrderCohortMonth" = p."Date"
            WHERE p."YearMonth" = %s
        """,
            (current_period,),
        )
        r = cur.fetchone()
        if r and r[1] and r[1] > 0:
            pct = r[0] / r[1] * 100
            _insert_insight(
                cur,
                current_period,
                "MULTI_SERVICE",
                "customers",
                f"{pct:.0f}% of customers use multiple services",
                f"{r[0]:,} of {r[1]:,} customers in {current_period}",
                "positive" if pct >= 20 else "neutral",
            )
            count += 1

        # CONCENTRATION
        cur.execute(f"""
            WITH cust_rev AS (
                SELECT s."CustomerID_Std", SUM(s."Total_Num") AS rev
                FROM analytics.sales s
                JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
                WHERE s."Earned_Date" IS NOT NULL AND p."YearMonth" = '{current_period}'
                GROUP BY s."CustomerID_Std"
            ),
            threshold AS (
                SELECT PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY rev) AS p80
                FROM cust_rev
            )
            SELECT SUM(CASE WHEN rev >= t.p80 THEN rev ELSE 0 END),
                   SUM(rev)
            FROM cust_rev, threshold t
        """)
        r = cur.fetchone()
        if r and r[1] and r[1] > 0:
            share = r[0] / r[1] * 100
            _insert_insight(
                cur,
                current_period,
                "CONCENTRATION",
                "revenue",
                f"Top 20% of customers generate {share:.0f}% of revenue",
                f"Dhs {r[0]:,.0f} of Dhs {r[1]:,.0f} total in {current_period}",
                "negative" if share > 85 else "neutral",
            )
            count += 1

        # TOP_CATEGORY
        cur.execute(f"""
            SELECT i."Item_Category", SUM(i."Quantity") AS qty
            FROM analytics.items i
            JOIN analytics.dim_period p ON i."ItemDate" = p."Date"
            WHERE p."YearMonth" = '{current_period}'
            GROUP BY i."Item_Category" ORDER BY qty DESC LIMIT 1
        """)
        r = cur.fetchone()
        if r:
            _insert_insight(
                cur,
                current_period,
                "TOP_CATEGORY",
                "operations",
                f"Top category: {r[0]} ({r[1]:,} items)",
                f"Highest volume item category in {current_period}",
                "neutral",
            )
            count += 1

        # TOP_SERVICE
        cur.execute(f"""
            SELECT i."Service_Type", SUM(i."Quantity") AS qty
            FROM analytics.items i
            JOIN analytics.dim_period p ON i."ItemDate" = p."Date"
            WHERE p."YearMonth" = '{current_period}'
            GROUP BY i."Service_Type" ORDER BY qty DESC LIMIT 1
        """)
        r = cur.fetchone()
        if r:
            _insert_insight(
                cur,
                current_period,
                "TOP_SERVICE",
                "operations",
                f"Top service: {r[0]} ({r[1]:,} items)",
                f"Highest volume service type in {current_period}",
                "neutral",
            )
            count += 1

        # EXPRESS_SHARE
        cur.execute(f"""
            SELECT SUM(CASE WHEN i."Express" = TRUE THEN i."Quantity" ELSE 0 END),
                   SUM(i."Quantity")
            FROM analytics.items i
            JOIN analytics.dim_period p ON i."ItemDate" = p."Date"
            WHERE p."YearMonth" = '{current_period}'
        """)
        r = cur.fetchone()
        if r and r[1] and r[1] > 0:
            pct = r[0] / r[1] * 100
            _insert_insight(
                cur,
                current_period,
                "EXPRESS_SHARE",
                "operations",
                f"Express orders: {pct:.0f}% of items",
                f"{r[0]:,} express of {r[1]:,} total items in {current_period}",
                "positive" if pct >= 20 else "neutral",
            )
            count += 1

        # DELIVERY_RATE
        cur.execute(f"""
            SELECT SUM(CAST(s."HasDelivery" AS INTEGER)),
                   SUM(CAST(s."HasPickup" AS INTEGER))
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
            WHERE s."Earned_Date" IS NOT NULL AND p."YearMonth" = '{current_period}'
        """)
        r = cur.fetchone()
        if r and r[0] is not None and r[1] is not None and (r[0] + r[1]) > 0:
            rate = r[0] / (r[0] + r[1]) * 100
            _insert_insight(
                cur,
                current_period,
                "DELIVERY_RATE",
                "operations",
                f"Delivery rate: {rate:.0f}% ({r[0]:,} deliveries, {r[1]:,} pickups)",
                f"Total stops: {r[0] + r[1]:,} in {current_period}",
                "neutral",
            )
            count += 1

        # REV_PER_DELIVERY
        cur.execute(f"""
            SELECT SUM(CASE WHEN s."HasDelivery" = TRUE THEN s."Total_Num" ELSE 0 END)
                       / NULLIF(SUM(CAST(s."HasDelivery" AS INTEGER)), 0)
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
            WHERE s."Earned_Date" IS NOT NULL AND p."YearMonth" = '{current_period}'
        """)
        r = cur.fetchone()
        if r and r[0] is not None:
            _insert_insight(
                cur,
                current_period,
                "REV_PER_DELIVERY",
                "operations",
                f"Revenue per delivery: Dhs {r[0]:,.0f}",
                f"Average revenue generated per delivery stop in {current_period}",
                "positive" if r[0] >= 100 else "neutral",
            )
            count += 1

        # GEO_SHIFT
        if prior_period:
            cur.execute(
                """
                SELECT
                    SUM(CASE WHEN p."YearMonth" = %s AND s."Route_Category" = 'Inside Abu Dhabi'
                             THEN CAST(s."HasDelivery" AS INTEGER) + CAST(s."HasPickup" AS INTEGER)
                             ELSE 0 END),
                    SUM(CASE WHEN p."YearMonth" = %s
                             THEN CAST(s."HasDelivery" AS INTEGER) + CAST(s."HasPickup" AS INTEGER)
                             ELSE 0 END),
                    SUM(CASE WHEN p."YearMonth" = %s AND s."Route_Category" = 'Inside Abu Dhabi'
                             THEN CAST(s."HasDelivery" AS INTEGER) + CAST(s."HasPickup" AS INTEGER)
                             ELSE 0 END),
                    SUM(CASE WHEN p."YearMonth" = %s
                             THEN CAST(s."HasDelivery" AS INTEGER) + CAST(s."HasPickup" AS INTEGER)
                             ELSE 0 END)
                FROM analytics.sales s
                JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
                WHERE s."Earned_Date" IS NOT NULL
                  AND p."YearMonth" IN (%s, %s)
            """,
                (current_period, current_period, prior_period, prior_period, current_period, prior_period),
            )
            r = cur.fetchone()
            if r and r[1] and r[1] > 0 and r[3] and r[3] > 0:
                pc = r[0] / r[1] * 100
                pp = r[2] / r[3] * 100
                diff = pc - pp
                _insert_insight(
                    cur,
                    current_period,
                    "GEO_SHIFT",
                    "operations",
                    f"Inside Abu Dhabi stops: {pc:.0f}% ({diff:+.0f}pp vs last month)",
                    f"{r[0]:,} inside stops of {r[1]:,} total in {current_period}",
                    "neutral",
                )
                count += 1

        # DIGITAL_PAYMENT
        cur.execute(f"""
            SELECT SUM(CASE WHEN s."Payment_Type_Std" IN ('Stripe', 'Terminal')
                            THEN s."Collections" ELSE 0 END),
                   SUM(s."Collections")
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
            WHERE s."Earned_Date" IS NOT NULL AND p."YearMonth" = '{current_period}'
        """)
        r = cur.fetchone()
        if r and r[1] and r[1] > 0:
            pct = r[0] / r[1] * 100
            _insert_insight(
                cur,
                current_period,
                "DIGITAL_PAYMENT",
                "payments",
                f"Digital payments: {pct:.0f}% of collections",
                f"Dhs {r[0]:,.0f} stripe+terminal of Dhs {r[1]:,.0f} total",
                "positive" if pct >= 70 else "neutral",
            )
            count += 1

        # COLLECTION_RATE
        cur.execute(f"""
            SELECT SUM(s."Collections"), SUM(s."Total_Num")
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
            WHERE s."Earned_Date" IS NOT NULL AND p."YearMonth" = '{current_period}'
        """)
        r = cur.fetchone()
        if r and r[1] and r[1] > 0:
            rate = r[0] / r[1] * 100
            sent = "positive" if rate >= 90 else ("negative" if rate < 70 else "neutral")
            _insert_insight(
                cur,
                current_period,
                "COLLECTION_RATE",
                "payments",
                f"Collection rate: {rate:.0f}% of revenue collected",
                f"Dhs {r[0]:,.0f} collected of Dhs {r[1]:,.0f} earned",
                sent,
            )
            count += 1

        # AVG_DAYS_PAYMENT
        if prior_period:
            cur.execute(
                """
                SELECT
                    AVG(CASE WHEN p."YearMonth" = %s THEN s."DaysToPayment" END),
                    AVG(CASE WHEN p."YearMonth" = %s THEN s."DaysToPayment" END)
                FROM analytics.sales s
                JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
                WHERE s."Earned_Date" IS NOT NULL AND s."DaysToPayment" IS NOT NULL
                  AND p."YearMonth" IN (%s, %s)
            """,
                (current_period, prior_period, current_period, prior_period),
            )
            r = cur.fetchone()
            if r and r[0] is not None and r[1] is not None and r[1] > 0:
                diff = r[0] - r[1]
                sent = "positive" if diff < 0 else ("negative" if diff > 1 else "neutral")
                _insert_insight(
                    cur,
                    current_period,
                    "AVG_DAYS_PAYMENT",
                    "payments",
                    f"Avg days to payment: {r[0]:.1f} days ({diff:+.1f} vs last month)",
                    f"Average collection cycle in {current_period}",
                    sent,
                )
                count += 1

        # OUTSTANDING_PCT
        cur.execute(f"""
            SELECT SUM(CASE WHEN s."Paid" = FALSE AND s."Source" = 'CC_2025'
                            THEN s."Total_Num" ELSE 0 END),
                   SUM(s."Total_Num")
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
            WHERE s."Earned_Date" IS NOT NULL AND p."YearMonth" = '{current_period}'
        """)
        r = cur.fetchone()
        if r and r[1] and r[1] > 0 and r[0] is not None:
            pct = r[0] / r[1] * 100
            sent = "negative" if pct > 10 else ("neutral" if pct > 5 else "positive")
            _insert_insight(
                cur,
                current_period,
                "OUTSTANDING_PCT",
                "payments",
                f"Outstanding: {pct:.0f}% of revenue (Dhs {r[0]:,.0f})",
                f"Unpaid CC_2025 orders in {current_period}",
                sent,
            )
            count += 1

        # PROCESSING_TIME
        cur.execute(f"""
            SELECT AVG(s."Processing_Days")
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."OrderCohortMonth" = p."Date"
            WHERE s."Earned_Date" IS NOT NULL AND s."Processing_Days" IS NOT NULL
              AND p."YearMonth" = '{current_period}'
        """)
        r = cur.fetchone()
        if r and r[0] is not None:
            sent = "negative" if r[0] > 3.0 else "positive"
            _insert_insight(
                cur,
                current_period,
                "PROCESSING_TIME",
                "operations",
                f"Avg processing time: {r[0]:.1f} days" + ("  - above target" if r[0] > 3.0 else ""),
                f"Average order processing cycle in {current_period}",
                sent,
            )
            count += 1

        logger.info(f"  [OK] {count} monthly insights for {current_period}")
        count += _create_weekly_insights(cur)

    conn.commit()
    return count


def _create_weekly_insights(cur) -> int:
    """Generate WoW insight rules for the last completed ISO week."""
    cur.execute("""
        SELECT "ISOWeekLabel" FROM analytics.dim_period
        WHERE "IsCurrentISOWeek" = FALSE AND "Date" <= CURRENT_DATE
        ORDER BY "ISOWeekLabel" DESC LIMIT 1
    """)
    row = cur.fetchone()
    if not row:
        logger.info("  [SKIP] No completed ISO weeks — skipping weekly insights")
        return 0
    cur_week = row[0]

    cur.execute(
        """
        SELECT "ISOWeekLabel" FROM analytics.dim_period
        WHERE "IsCurrentISOWeek" = FALSE AND "ISOWeekLabel" < %s
        ORDER BY "ISOWeekLabel" DESC LIMIT 1
    """,
        (cur_week,),
    )
    row = cur.fetchone()
    prior_week = row[0] if row else None

    count = 0

    # WRev_WOW
    if prior_week:
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN p."ISOWeekLabel" = %s THEN s."Total_Num" ELSE 0 END),
                SUM(CASE WHEN p."ISOWeekLabel" = %s THEN s."Total_Num" ELSE 0 END)
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."Earned_Date" = p."Date"
            WHERE s."Is_Earned" = TRUE
              AND p."ISOWeekLabel" IN (%s, %s)
        """,
            (cur_week, prior_week, cur_week, prior_week),
        )
        r = cur.fetchone()
        if r and r[1] and r[1] > 0:
            pct = (r[0] - r[1]) / r[1] * 100
            sent = "positive" if pct > 2 else ("negative" if pct < -2 else "neutral")
            _insert_insight(
                cur,
                cur_week,
                "WRev_WOW",
                "revenue",
                f"Revenue {pct:+.0f}% vs last week",
                f"Dhs {r[0]:,.0f} this week vs Dhs {r[1]:,.0f} last week",
                sent,
                "weekly",
            )
            count += 1

    # WRev_TREND (vs 4-week avg)
    cur.execute(f"""
        WITH wk AS (
            SELECT p."ISOWeekLabel", SUM(s."Total_Num") AS rev
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."Earned_Date" = p."Date"
            WHERE s."Is_Earned" = TRUE AND p."IsCurrentISOWeek" = FALSE
            GROUP BY p."ISOWeekLabel"
            ORDER BY p."ISOWeekLabel" DESC LIMIT 5
        )
        SELECT
            MAX(CASE WHEN "ISOWeekLabel" = '{cur_week}' THEN rev END),
            AVG(CASE WHEN "ISOWeekLabel" != '{cur_week}' THEN rev END)
        FROM wk
    """)
    r = cur.fetchone()
    if r and r[0] is not None and r[1] is not None and r[1] > 0:
        pct = (r[0] - r[1]) / r[1] * 100
        sent = "positive" if pct > 5 else ("negative" if pct < -5 else "neutral")
        _insert_insight(
            cur,
            cur_week,
            "WRev_TREND",
            "revenue",
            f"Revenue {pct:+.0f}% vs 4-week average",
            f"Dhs {r[0]:,.0f} this week vs Dhs {r[1]:,.0f} avg",
            sent,
            "weekly",
        )
        count += 1

    # WCust_WOW
    if prior_week:
        cur.execute(
            """
            SELECT
                COUNT(DISTINCT CASE WHEN p."ISOWeekLabel" = %s THEN s."CustomerID_Std" END),
                COUNT(DISTINCT CASE WHEN p."ISOWeekLabel" = %s THEN s."CustomerID_Std" END)
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."Earned_Date" = p."Date"
            WHERE s."Is_Earned" = TRUE AND p."ISOWeekLabel" IN (%s, %s)
        """,
            (cur_week, prior_week, cur_week, prior_week),
        )
        r = cur.fetchone()
        if r and r[1] and r[1] > 0:
            pct = (r[0] - r[1]) / r[1] * 100
            _insert_insight(
                cur,
                cur_week,
                "WCust_WOW",
                "customers",
                f"Active customers {pct:+.0f}% vs last week",
                f"{r[0]:,} customers this week vs {r[1]:,} last week",
                "positive" if pct > 0 else "negative",
                "weekly",
            )
            count += 1

    # WStops_WOW
    if prior_week:
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN p."ISOWeekLabel" = %s
                         THEN CAST(s."HasDelivery" AS INTEGER) + CAST(s."HasPickup" AS INTEGER)
                         ELSE 0 END),
                SUM(CASE WHEN p."ISOWeekLabel" = %s
                         THEN CAST(s."HasDelivery" AS INTEGER) + CAST(s."HasPickup" AS INTEGER)
                         ELSE 0 END)
            FROM analytics.sales s
            JOIN analytics.dim_period p ON s."Earned_Date" = p."Date"
            WHERE s."Is_Earned" = TRUE AND p."ISOWeekLabel" IN (%s, %s)
        """,
            (cur_week, prior_week, cur_week, prior_week),
        )
        r = cur.fetchone()
        if r and r[1] and r[1] > 0:
            pct = (r[0] - r[1]) / r[1] * 100
            _insert_insight(
                cur,
                cur_week,
                "WStops_WOW",
                "operations",
                f"Stops {pct:+.0f}% vs last week",
                f"{r[0]:,} stops this week vs {r[1]:,} last week",
                "positive" if pct > 0 else "negative",
                "weekly",
            )
            count += 1

    # WItems_WOW
    if prior_week:
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN p."ISOWeekLabel" = %s THEN i."Quantity" ELSE 0 END),
                SUM(CASE WHEN p."ISOWeekLabel" = %s THEN i."Quantity" ELSE 0 END)
            FROM analytics.items i
            JOIN analytics.dim_period p ON i."ItemDate" = p."Date"
            WHERE p."ISOWeekLabel" IN (%s, %s)
        """,
            (cur_week, prior_week, cur_week, prior_week),
        )
        r = cur.fetchone()
        if r and r[1] and r[1] > 0:
            pct = (r[0] - r[1]) / r[1] * 100
            _insert_insight(
                cur,
                cur_week,
                "WItems_WOW",
                "operations",
                f"Items {pct:+.0f}% vs last week",
                f"{r[0]:,} items this week vs {r[1]:,} last week",
                "positive" if pct > 0 else "negative",
                "weekly",
            )
            count += 1

    # WProcessing
    cur.execute(
        """
        SELECT AVG(s."Processing_Days")
        FROM analytics.sales s
        JOIN analytics.dim_period p ON s."Earned_Date" = p."Date"
        WHERE s."Is_Earned" = TRUE AND s."Processing_Days" IS NOT NULL
          AND p."ISOWeekLabel" = %s
    """,
        (cur_week,),
    )
    r = cur.fetchone()
    if r and r[0] is not None:
        _insert_insight(
            cur,
            cur_week,
            "WProcessing",
            "operations",
            f"Avg processing: {r[0]:.1f} days" + ("  - above target" if r[0] > 3.0 else ""),
            f"Average order processing time in {cur_week}",
            "negative" if r[0] > 3.0 else "positive",
            "weekly",
        )
        count += 1

    # WCollection_Rate
    cur.execute(
        """
        SELECT SUM(s."Collections"), SUM(s."Total_Num")
        FROM analytics.sales s
        JOIN analytics.dim_period p ON s."Earned_Date" = p."Date"
        WHERE s."Is_Earned" = TRUE AND p."ISOWeekLabel" = %s
    """,
        (cur_week,),
    )
    r = cur.fetchone()
    if r and r[1] and r[1] > 0:
        rate = r[0] / r[1] * 100
        sent = "positive" if rate >= 90 else ("negative" if rate < 70 else "neutral")
        _insert_insight(
            cur,
            cur_week,
            "WCollection_Rate",
            "payments",
            f"Collection rate: {rate:.0f}% of revenue collected",
            f"Dhs {r[0]:,.0f} collected of Dhs {r[1]:,.0f} earned in {cur_week}",
            sent,
            "weekly",
        )
        count += 1

    # WDelivery_Rate
    cur.execute(
        """
        SELECT SUM(CAST(s."HasDelivery" AS INTEGER)),
               SUM(CAST(s."HasPickup" AS INTEGER))
        FROM analytics.sales s
        JOIN analytics.dim_period p ON s."Earned_Date" = p."Date"
        WHERE s."Is_Earned" = TRUE AND p."ISOWeekLabel" = %s
    """,
        (cur_week,),
    )
    r = cur.fetchone()
    if r and r[0] is not None and r[1] is not None and (r[0] + r[1]) > 0:
        rate = r[0] / (r[0] + r[1]) * 100
        _insert_insight(
            cur,
            cur_week,
            "WDelivery_Rate",
            "operations",
            f"Delivery rate: {rate:.0f}% ({r[0]:,} deliveries, {r[1]:,} pickups)",
            f"Total stops: {r[0] + r[1]:,} in {cur_week}",
            "neutral",
            "weekly",
        )
        count += 1

    logger.info(f"  [OK] {count} weekly insights for {cur_week}")
    return count


# =====================================================================
# MAIN WORKFLOW
# =====================================================================


def main(skip_insights: bool = False) -> dict:
    """Sync all Parquet files to Postgres analytics schema.

    Returns a summary dict with row counts and elapsed time.
    Raises RuntimeError if ANALYTICS_DATABASE_URL is not configured.
    """
    if not ANALYTICS_DATABASE_URL:
        raise RuntimeError("ANALYTICS_DATABASE_URL not configured — skipping Postgres sync")

    logger.info("")
    logger.info("=" * 70)
    logger.info("POSTGRES ETL - LOADING DATA")
    logger.info("=" * 70)
    logger.info("")

    start = datetime.now()
    summary: dict = {}

    conn = _connect()
    try:
        # ── 1. Load primary tables ──────────────────────────────────────
        for table_name, filename in PARQUET_FILES.items():
            pq_path = LOCAL_STAGING_PATH / filename
            csv_path = pq_path.with_suffix(".csv")

            if pq_path.exists():
                df = pl.read_parquet(pq_path)
                src = "parquet"
            elif csv_path.exists():
                df = pl.read_csv(csv_path, infer_schema_length=0)
                src = "csv"
            else:
                logger.warning(f"  [WARN] {filename} not found — skipping {table_name}")
                continue

            t0 = time.time()

            if table_name == "customers":
                n = _load_customers(conn, df)
            else:
                df = _prepare_df(table_name, df)
                with conn.cursor() as cur:
                    _truncate(cur, table_name)
                    n = _bulk_insert(cur, table_name, df)
                conn.commit()

            elapsed = time.time() - t0
            summary[table_name] = n
            logger.info(f"  [OK] {table_name}: {n:,} rows in {elapsed:.1f}s ({src})")

        # ── 2. Mark Legacy orders as Paid (same logic as DuckDB loader) ─
        with conn.cursor() as cur:
            cur.execute("""UPDATE analytics.sales SET "Paid" = TRUE
                           WHERE "Source" = 'Legacy' AND "Paid" = FALSE""")
            n_legacy = cur.rowcount
        conn.commit()
        logger.info(f"  [OK] Marked {n_legacy:,} Legacy orders as Paid")

        # ── 3. Derive order_lookup ──────────────────────────────────────
        t0 = time.time()
        n_ol = _load_order_lookup(conn)
        summary["order_lookup"] = n_ol
        logger.info(f"  [OK] order_lookup: {n_ol:,} distinct orders in {time.time() - t0:.1f}s")

        # ── 4. Build insights ───────────────────────────────────────────
        if not skip_insights:
            t0 = time.time()
            n_ins = _create_insights(conn)
            summary["insights"] = n_ins
            logger.info(f"  [OK] insights: {n_ins} rules in {time.time() - t0:.1f}s")

    finally:
        conn.close()

    elapsed = (datetime.now() - start).total_seconds()
    summary["elapsed_s"] = round(elapsed, 1)

    logger.info("")
    logger.info(f"  Postgres sync complete in {elapsed:.1f}s")
    logger.info("=" * 70)
    return summary


if __name__ == "__main__":
    skip = "--skip-insights" in sys.argv
    try:
        result = main(skip_insights=skip)
        print("Summary:", result)
    except RuntimeError as e:
        print(f"Skipped: {e}")
    except Exception as e:
        import traceback

        traceback.print_exc()
        sys.exit(1)
