"""SQL validation tests using in-memory DuckDB with fixture data.

Tests the key SQL patterns used by the Streamlit dashboard.
"""

import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.mark.integration
class TestGrainContext:
    """Test get_grain_context returns correct SQL fragments."""

    def test_monthly_grain(self):
        from dashboard_shared import get_grain_context
        ctx = get_grain_context("2025-01")
        assert "YearMonth" in ctx["period_col"]
        assert "OrderCohortMonth" in ctx["sales_join"]

    def test_weekly_grain(self):
        from dashboard_shared import get_grain_context
        ctx = get_grain_context("2025-W03")
        assert "ISOWeekLabel" in ctx["period_col"]
        assert "Earned_Date" in ctx["sales_join"]

    def test_monthly_list(self):
        from dashboard_shared import get_grain_context
        ctx = get_grain_context(["2025-01", "2025-02"])
        assert "YearMonth" in ctx["period_col"]

    def test_weekly_list(self):
        from dashboard_shared import get_grain_context
        ctx = get_grain_context(["2025-W03", "2025-W04"])
        assert "ISOWeekLabel" in ctx["period_col"]


@pytest.mark.integration
class TestDuckDBQueries:
    """Test SQL queries against in-memory DuckDB with fixture data."""

    def test_sales_table_exists(self, test_duckdb_connection):
        con = test_duckdb_connection
        result = con.execute("SELECT COUNT(*) FROM All_Sales").fetchone()
        assert result[0] > 0

    def test_items_table_exists(self, test_duckdb_connection):
        con = test_duckdb_connection
        result = con.execute("SELECT COUNT(*) FROM All_Items").fetchone()
        assert result[0] > 0

    def test_order_lookup_coverage(self, test_duckdb_connection):
        """order_lookup should cover all distinct OrderID_Std from sales."""
        con = test_duckdb_connection
        sales_orders = con.execute(
            "SELECT COUNT(DISTINCT OrderID_Std) FROM All_Sales WHERE OrderID_Std IS NOT NULL"
        ).fetchone()[0]
        lookup_orders = con.execute(
            "SELECT COUNT(DISTINCT OrderID_Std) FROM order_lookup"
        ).fetchone()[0]
        assert lookup_orders == sales_orders

    def test_revenue_query(self, test_duckdb_connection):
        """Total revenue = SUM(Total_Num) WHERE Is_Earned = 1."""
        con = test_duckdb_connection
        result = con.execute(
            "SELECT SUM(Total_Num) FROM All_Sales WHERE Is_Earned = 1"
        ).fetchone()
        assert result[0] > 0

    def test_customer_count_query(self, test_duckdb_connection):
        """Active customers = COUNT(DISTINCT CustomerID_Std) WHERE Is_Earned = 1."""
        con = test_duckdb_connection
        result = con.execute(
            "SELECT COUNT(DISTINCT CustomerID_Std) FROM All_Sales WHERE Is_Earned = 1"
        ).fetchone()
        assert result[0] > 0

    def test_dimperiod_exists(self, test_duckdb_connection):
        con = test_duckdb_connection
        result = con.execute("SELECT COUNT(*) FROM DimPeriod").fetchone()
        assert result[0] > 0
