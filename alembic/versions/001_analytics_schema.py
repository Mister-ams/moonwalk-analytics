"""Create analytics schema with all Moonwalk tables.

Creates:
  - 7 ENUM types in analytics schema
  - 7 tables: sales, customers, items, customer_quality, dim_period, order_lookup, insights
  - 13 indexes (mirrors DuckDB cleancloud_to_duckdb.py)

Schema and pgcrypto extension are bootstrapped in alembic/env.py before this
migration runs (required so alembic_version can be written to analytics schema).

Excluded columns (per DROP_COLUMNS in cleancloud_to_duckdb.py):
  - sales.Delivery          (identical to HasDelivery)
  - dim_period.*SortOrder   (identical to base columns)
  - dim_period.ISOWeekday   (identical to DayOfWeek)
  - dim_period.FiscalYear   (identical to Year)
  - dim_period.FiscalQuarter (identical to Quarter)

PII encryption:
  - customers.Phone and customers.Email stored as BYTEA.
  - Encrypted at write time:  pgp_sym_encrypt(value, ENCRYPTION_KEY)
  - Decrypted at read time:   pgp_sym_decrypt(column, ENCRYPTION_KEY)
  - CustomerName is plaintext — required for fuzzy name search in Invoice Automation.

Revision ID: 001
Revises: None
Create Date: 2026-02-21
"""

from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # 1. ENUM types
    # ------------------------------------------------------------------
    op.execute("CREATE TYPE analytics.source_enum AS ENUM ('CC_2025', 'Legacy')")
    op.execute("CREATE TYPE analytics.transaction_type_enum AS ENUM ('Order', 'Subscription', 'Invoice Payment')")
    op.execute("CREATE TYPE analytics.payment_type_enum AS ENUM ('Stripe', 'Terminal', 'Cash', 'Receivable', 'Other')")
    op.execute("CREATE TYPE analytics.store_enum AS ENUM ('Moon Walk', 'Hielo')")
    op.execute("CREATE TYPE analytics.route_category_enum AS ENUM ('Inside Abu Dhabi', 'Outer Abu Dhabi', 'Other')")
    op.execute(
        "CREATE TYPE analytics.item_category_enum AS ENUM ('Professional Wear', 'Traditional Wear', 'Home Linens', 'Extras', 'Others')"
    )
    op.execute(
        "CREATE TYPE analytics.service_type_enum AS ENUM ('Wash & Press', 'Dry Cleaning', 'Press Only', 'Other Service')"
    )

    # ------------------------------------------------------------------
    # 2. Tables (raw SQL — bypasses SQLAlchemy ENUM auto-create)
    # ------------------------------------------------------------------

    # sales: one row per order/subscription/invoice payment.
    # Surrogate PK — no natural unique key across the full history.
    op.execute("""
        CREATE TABLE IF NOT EXISTS analytics.sales (
            id                      SERIAL PRIMARY KEY,
            "Source"                analytics.source_enum,
            "Transaction_Type"      analytics.transaction_type_enum,
            "Payment_Type_Std"      analytics.payment_type_enum,
            "Collections"           NUMERIC(12,2),
            "Paid"                  BOOLEAN,
            "Store_Std"             analytics.store_enum,
            "CustomerID_Std"        VARCHAR(50),
            "OrderID_Std"           VARCHAR(50),
            "Placed_Date"           DATE,
            "Earned_Date"           DATE,
            "OrderCohortMonth"      DATE,
            "CohortMonth"           DATE,
            "MonthsSinceCohort"     SMALLINT,
            "Total_Num"             NUMERIC(12,2),
            "Is_Earned"             BOOLEAN,
            "Ready By"              DATE,
            "Cleaned"               DATE,
            "Collected"             DATE,
            "Pickup Date"           DATE,
            "Payment Date"          DATE,
            "Delivery_Date"         DATE,
            "Pieces"                INTEGER,
            "HasDelivery"           BOOLEAN,
            "HasPickup"             BOOLEAN,
            "Route #"               SMALLINT,
            "Route_Category"        analytics.route_category_enum,
            "IsSubscriptionService" BOOLEAN,
            "Processing_Days"       SMALLINT,
            "TimeInStore_Days"      SMALLINT,
            "DaysToPayment"         SMALLINT
        )
    """)

    # customers: one row per unique customer.
    # Phone + Email stored as BYTEA (pgp_sym_encrypt). CustomerName is plaintext.
    op.execute("""
        CREATE TABLE IF NOT EXISTS analytics.customers (
            "CustomerID_Std"    VARCHAR(50) PRIMARY KEY,
            "CustomerID_Raw"    INTEGER,
            "CustomerName"      VARCHAR(200),
            "Store_Std"         analytics.store_enum,
            "SignedUp_Date"     DATE,
            "CohortMonth"       DATE,
            "Route #"           SMALLINT,
            "IsBusinessAccount" BOOLEAN,
            "Source_System"     VARCHAR(50),
            "Phone"             BYTEA,
            "Email"             BYTEA
        )
    """)

    # items: one row per line item on an order.
    op.execute("""
        CREATE TABLE IF NOT EXISTS analytics.items (
            id                  SERIAL PRIMARY KEY,
            "Source"            analytics.source_enum,
            "Store_Std"         analytics.store_enum,
            "CustomerID_Std"    VARCHAR(50),
            "OrderID_Std"       VARCHAR(50),
            "ItemDate"          DATE,
            "ItemCohortMonth"   DATE,
            "Item"              VARCHAR(200),
            "Section"           VARCHAR(100),
            "Quantity"          INTEGER,
            "Total"             NUMERIC(12,2),
            "Express"           BOOLEAN,
            "Item_Category"     analytics.item_category_enum,
            "Service_Type"      analytics.service_type_enum,
            "IsBusinessAccount" BOOLEAN
        )
    """)

    # customer_quality: one row per customer per month.
    op.execute("""
        CREATE TABLE IF NOT EXISTS analytics.customer_quality (
            "CustomerID_Std"         VARCHAR(50)    NOT NULL,
            "OrderCohortMonth"       DATE           NOT NULL,
            "Order_Revenue"          NUMERIC(12,2),
            "Subscription_Revenue"   NUMERIC(12,2),
            "Monthly_Revenue"        NUMERIC(12,2),
            "Monthly_Items"          INTEGER,
            "Services_Used_10pct"    SMALLINT,
            "Is_Multi_Service"       BOOLEAN,
            PRIMARY KEY ("CustomerID_Std", "OrderCohortMonth")
        )
    """)

    # dim_period: date dimension with 3-month lookahead.
    # Redundant columns excluded: QuarterSortOrder, MonthSortOrder, ISOWeekday,
    # FiscalYear, FiscalQuarter, DayOfWeekSortOrder.
    op.execute("""
        CREATE TABLE IF NOT EXISTS analytics.dim_period (
            "Date"                  DATE PRIMARY KEY,
            "Year"                  SMALLINT,
            "Quarter"               SMALLINT,
            "Month"                 SMALLINT,
            "Day"                   SMALLINT,
            "YearMonth"             VARCHAR(7),
            "YearQuarter"           VARCHAR(7),
            "MonthStart"            DATE,
            "QuarterStart"          DATE,
            "MonthName"             VARCHAR(20),
            "MonthShort"            VARCHAR(5),
            "ISOYear"               SMALLINT,
            "ISOWeek"               SMALLINT,
            "ISOWeekLabel"          VARCHAR(10),
            "IsFirstDayOfISOWeek"   BOOLEAN,
            "IsLastDayOfISOWeek"    BOOLEAN,
            "DayOfWeek"             SMALLINT,
            "DayOfYear"             SMALLINT,
            "DayName"               VARCHAR(15),
            "DayShort"              VARCHAR(5),
            "IsFirstDayOfMonth"     BOOLEAN,
            "IsLastDayOfMonth"      BOOLEAN,
            "IsWeekend"             BOOLEAN,
            "IsWeekday"             BOOLEAN,
            "IsCurrentMonth"        BOOLEAN,
            "IsCurrentQuarter"      BOOLEAN,
            "IsCurrentYear"         BOOLEAN,
            "IsCurrentISOWeek"      BOOLEAN
        )
    """)

    # order_lookup: OrderID -> IsSubscriptionService, used to avoid full sales scans.
    op.execute("""
        CREATE TABLE IF NOT EXISTS analytics.order_lookup (
            "OrderID_Std"           VARCHAR(50) PRIMARY KEY,
            "IsSubscriptionService" BOOLEAN
        )
    """)

    # insights: rules-based analysis computed during each ETL run.
    # period is YYYY-MM for monthly rules, YYYY-Www for weekly rules.
    op.execute("""
        CREATE TABLE IF NOT EXISTS analytics.insights (
            period      VARCHAR(20)  NOT NULL,
            rule_id     VARCHAR(50)  NOT NULL,
            category    VARCHAR(50),
            headline    VARCHAR(500),
            detail      VARCHAR(500),
            sentiment   VARCHAR(20),
            granularity VARCHAR(20)  DEFAULT 'monthly',
            PRIMARY KEY (period, rule_id)
        )
    """)

    # ------------------------------------------------------------------
    # 3. Indexes (mirrors cleancloud_to_duckdb.py create_indexes())
    # ------------------------------------------------------------------
    op.execute('CREATE INDEX IF NOT EXISTS idx_sales_customer      ON analytics.sales ("CustomerID_Std")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_sales_order         ON analytics.sales ("OrderID_Std")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_sales_cohort_month  ON analytics.sales ("OrderCohortMonth")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_sales_earned_date   ON analytics.sales ("Earned_Date")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_sales_txn_type      ON analytics.sales ("Transaction_Type")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_items_customer      ON analytics.items ("CustomerID_Std")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_items_order         ON analytics.items ("OrderID_Std")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_items_date          ON analytics.items ("ItemDate")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_customers_raw_id    ON analytics.customers ("CustomerID_Raw")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_cust_quality_id     ON analytics.customer_quality ("CustomerID_Std")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_cust_quality_month  ON analytics.customer_quality ("OrderCohortMonth")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_period_yearmonth    ON analytics.dim_period ("YearMonth")')
    op.execute('CREATE INDEX IF NOT EXISTS idx_period_isoweeklabel ON analytics.dim_period ("ISOWeekLabel")')


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS analytics.insights")
    op.execute("DROP TABLE IF EXISTS analytics.order_lookup")
    op.execute("DROP TABLE IF EXISTS analytics.dim_period")
    op.execute("DROP TABLE IF EXISTS analytics.customer_quality")
    op.execute("DROP TABLE IF EXISTS analytics.items")
    op.execute("DROP TABLE IF EXISTS analytics.customers")
    op.execute("DROP TABLE IF EXISTS analytics.sales")

    op.execute("DROP TYPE IF EXISTS analytics.service_type_enum")
    op.execute("DROP TYPE IF EXISTS analytics.item_category_enum")
    op.execute("DROP TYPE IF EXISTS analytics.route_category_enum")
    op.execute("DROP TYPE IF EXISTS analytics.store_enum")
    op.execute("DROP TYPE IF EXISTS analytics.payment_type_enum")
    op.execute("DROP TYPE IF EXISTS analytics.transaction_type_enum")
    op.execute("DROP TYPE IF EXISTS analytics.source_enum")

    # Note: analytics schema is not dropped here — it is owned by env.py bootstrap.
