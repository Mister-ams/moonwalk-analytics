"""Shared fixtures for Moonwalk Analytics test suite."""

import pytest
import polars as pl
from datetime import date, datetime
from pathlib import Path

import duckdb
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import LOCAL_STAGING_PATH


# ── Bare DuckDB (for TRY_CAST edge cases) ────────────────────────────

@pytest.fixture
def raw_duckdb():
    """Bare in-memory DuckDB for TRY_CAST edge case testing."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


# ── Sample DataFrames ────────────────────────────────────────────────

@pytest.fixture
def sample_customers_df() -> pl.DataFrame:
    """Minimal customers DataFrame matching transform_all_customers output schema."""
    return pl.DataFrame({
        "CustomerID_Std": ["MW-0001", "CC-0042", "CC-0100"],
        "Customer Name": ["JOHN DOE", "JANE SMITH", "ALI AHMED"],
        "Store_Std": ["Moon Walk", "Moon Walk", "Hielo"],
        "Source": ["Legacy", "CleanCloud", "CleanCloud"],
        "Business ID": ["36319", "36319", "38516"],
        "CohortMonth": [
            datetime(2024, 1, 1),
            datetime(2024, 6, 1),
            datetime(2025, 1, 1),
        ],
    })


@pytest.fixture
def sample_orders_df() -> pl.DataFrame:
    """Minimal orders DataFrame matching transform_all_sales output schema."""
    return pl.DataFrame({
        "OrderID_Std": ["M-00101", "M-00102", "H-00201", "S-00001"],
        "CustomerID_Std": ["MW-0001", "MW-0001", "CC-0100", "CC-0042"],
        "Store_Std": ["Moon Walk", "Moon Walk", "Hielo", "Moon Walk"],
        "Transaction_Type": ["Order", "Order", "Order", "Subscription"],
        "Total_Num": [150.0, 200.0, 75.0, 300.0],
        "Is_Earned": [1, 1, 1, 1],
        "IsSubscriptionService": [0, 1, 0, 0],
        "Earned_Date": [
            datetime(2025, 1, 15),
            datetime(2025, 1, 20),
            datetime(2025, 2, 5),
            datetime(2025, 1, 10),
        ],
        "OrderCohortMonth": [
            datetime(2025, 1, 1),
            datetime(2025, 1, 1),
            datetime(2025, 2, 1),
            datetime(2025, 1, 1),
        ],
        "Payment_Type_Std": ["Cash", "Terminal", "Stripe", "Stripe"],
        "Route_Category": ["Inside Abu Dhabi", "Inside Abu Dhabi", "Outer Abu Dhabi", "Other"],
    })


@pytest.fixture
def sample_items_df() -> pl.DataFrame:
    """Minimal items DataFrame matching transform_all_items output schema."""
    return pl.DataFrame({
        "OrderID_Std": ["M-00101", "M-00101", "M-00102", "H-00201"],
        "CustomerID_Std": ["MW-0001", "MW-0001", "MW-0001", "CC-0100"],
        "Store_Std": ["Moon Walk", "Moon Walk", "Moon Walk", "Hielo"],
        "Item": ["Kandura", "Shirt", "Abaya", "Duvet Cover"],
        "Section": ["Dry Cleaning", "Wash & Press", "Dry Cleaning", "Laundry"],
        "Quantity": [1, 2, 1, 1],
        "Item_Category": ["Traditional Wear", "Professional Wear", "Traditional Wear", "Home Linens"],
        "Service_Type": ["Dry Cleaning", "Wash & Press", "Dry Cleaning", "Wash & Press"],
        "OrderCohortMonth": [
            datetime(2025, 1, 1),
            datetime(2025, 1, 1),
            datetime(2025, 1, 1),
            datetime(2025, 2, 1),
        ],
    })


# ── Golden baselines ─────────────────────────────────────────────────

@pytest.fixture
def golden_baselines_path():
    """Path to golden baselines directory. Skips test if not present."""
    baselines = LOCAL_STAGING_PATH / "golden_baselines"
    if not baselines.exists():
        pytest.skip("Golden baselines not found (run verify_migration.py first)")
    return baselines


# ── DuckDB in-memory connection ──────────────────────────────────────

@pytest.fixture
def test_duckdb_connection(sample_orders_df, sample_items_df, sample_customers_df):
    """In-memory DuckDB loaded with sample fixture data."""
    import duckdb

    con = duckdb.connect(":memory:")

    # Register DataFrames as tables (convert to pandas for DuckDB compatibility)
    orders_pd = sample_orders_df.to_pandas()
    items_pd = sample_items_df.to_pandas()
    customers_pd = sample_customers_df.to_pandas()

    con.execute("CREATE TABLE All_Sales AS SELECT * FROM orders_pd")
    con.execute("CREATE TABLE All_Items AS SELECT * FROM items_pd")
    con.execute("CREATE TABLE All_Customers AS SELECT * FROM customers_pd")

    # Create order_lookup materialized table (mirrors cleancloud_to_duckdb.py)
    con.execute("""
        CREATE TABLE order_lookup AS
        SELECT DISTINCT OrderID_Std, IsSubscriptionService
        FROM All_Sales
        WHERE OrderID_Std IS NOT NULL
    """)

    # Create DimPeriod with minimal data
    con.execute("""
        CREATE TABLE DimPeriod AS
        SELECT
            CAST('2025-01-01' AS DATE) AS DateValue,
            '2025-01' AS YearMonth,
            '2025-W03' AS ISOYearWeek,
            'W03' AS ISOWeekLabel,
            2025 AS Year,
            1 AS Month,
            3 AS ISOWeek
        UNION ALL
        SELECT
            CAST('2025-02-01' AS DATE),
            '2025-02',
            '2025-W06',
            'W06',
            2025,
            2,
            6
    """)

    yield con
    con.close()
