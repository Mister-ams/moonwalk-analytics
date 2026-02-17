"""SQL validation tests using in-memory DuckDB with fixture data.

Tests the key SQL patterns used by the Streamlit dashboard.
"""

import pytest
from pathlib import Path
from unittest.mock import patch

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


@pytest.mark.integration
class TestTryCastEdgeCases:
    """Test DuckDB TRY_CAST behavior for types used in cleancloud_to_duckdb.py."""

    # ── DATE casts ──────────────────────────────────────────────────

    def test_date_empty_string(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST('' AS DATE)").fetchone()
        assert result[0] is None

    def test_date_invalid_format(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST('13-45-2026' AS DATE)").fetchone()
        assert result[0] is None

    def test_date_partial(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST('2026-02' AS DATE)").fetchone()
        assert result[0] is None

    def test_date_valid(self, raw_duckdb):
        from datetime import date
        result = raw_duckdb.execute("SELECT TRY_CAST('2025-06-15' AS DATE)").fetchone()
        assert result[0] == date(2025, 6, 15)

    def test_date_null(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST(NULL AS DATE)").fetchone()
        assert result[0] is None

    # ── BOOLEAN casts ───────────────────────────────────────────────

    def test_bool_zero_one(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST(0 AS BOOLEAN), TRY_CAST(1 AS BOOLEAN)").fetchone()
        assert result[0] is False
        assert result[1] is True

    def test_bool_string_true_false(self, raw_duckdb):
        result = raw_duckdb.execute(
            "SELECT TRY_CAST('true' AS BOOLEAN), TRY_CAST('false' AS BOOLEAN)"
        ).fetchone()
        assert result[0] is True
        assert result[1] is False

    def test_bool_empty_string(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST('' AS BOOLEAN)").fetchone()
        assert result[0] is None

    def test_bool_invalid(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST('maybe' AS BOOLEAN)").fetchone()
        assert result[0] is None

    # ── SMALLINT casts ──────────────────────────────────────────────

    def test_smallint_valid(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST('42' AS SMALLINT)").fetchone()
        assert result[0] == 42

    def test_smallint_negative(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST('-5' AS SMALLINT)").fetchone()
        assert result[0] == -5

    def test_smallint_overflow(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST('99999' AS SMALLINT)").fetchone()
        assert result[0] is None

    def test_smallint_empty(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST('' AS SMALLINT)").fetchone()
        assert result[0] is None

    def test_smallint_float_string(self, raw_duckdb):
        result = raw_duckdb.execute("SELECT TRY_CAST('1.5' AS SMALLINT)").fetchone()
        # DuckDB rounds to nearest integer (banker's rounding)
        assert result[0] == 2

    # ── ENUM casts ──────────────────────────────────────────────────

    def test_enum_exact_match(self, raw_duckdb):
        raw_duckdb.execute("CREATE TYPE store_t AS ENUM ('Moon Walk', 'Hielo')")
        result = raw_duckdb.execute("SELECT TRY_CAST('Moon Walk' AS store_t)").fetchone()
        assert result[0] == "Moon Walk"

    def test_enum_case_mismatch(self, raw_duckdb):
        raw_duckdb.execute("CREATE TYPE store_t AS ENUM ('Moon Walk', 'Hielo')")
        result = raw_duckdb.execute("SELECT TRY_CAST('moon walk' AS store_t)").fetchone()
        assert result[0] is None

    def test_enum_unknown(self, raw_duckdb):
        raw_duckdb.execute("CREATE TYPE store_t AS ENUM ('Moon Walk', 'Hielo')")
        result = raw_duckdb.execute("SELECT TRY_CAST('Unknown Store' AS store_t)").fetchone()
        assert result[0] is None

    def test_enum_null(self, raw_duckdb):
        raw_duckdb.execute("CREATE TYPE store_t AS ENUM ('Moon Walk', 'Hielo')")
        result = raw_duckdb.execute("SELECT TRY_CAST(NULL AS store_t)").fetchone()
        assert result[0] is None

    def test_enum_empty_string(self, raw_duckdb):
        raw_duckdb.execute("CREATE TYPE store_t AS ENUM ('Moon Walk', 'Hielo')")
        result = raw_duckdb.execute("SELECT TRY_CAST('' AS store_t)").fetchone()
        assert result[0] is None


@pytest.mark.integration
class TestCastLossDetection:
    """Test the cast-loss detection helpers in cleancloud_to_duckdb.py."""

    def test_count_meaningful_values(self, raw_duckdb):
        from cleancloud_to_duckdb import _count_meaningful_values
        raw_duckdb.execute("""
            CREATE TABLE test_t AS
            SELECT * FROM (VALUES ('hello'), (NULL), (''), ('world')) AS t(col)
        """)
        count = _count_meaningful_values(raw_duckdb, "test_t", "col")
        assert count == 2  # 'hello' and 'world' — excludes NULL and ''

    def test_log_cast_loss_warns(self, raw_duckdb):
        from cleancloud_to_duckdb import _log_cast_loss
        # Create table with a column that will lose values on SMALLINT cast
        raw_duckdb.execute("""
            CREATE TABLE cast_t(val VARCHAR);
            INSERT INTO cast_t VALUES ('42'), ('99999'), ('hello'), (NULL), ('');
        """)
        pre_meaningful = 3  # '42', '99999', 'hello'
        raw_duckdb.execute("ALTER TABLE cast_t ALTER val TYPE SMALLINT USING TRY_CAST(val AS SMALLINT)")
        with patch("cleancloud_to_duckdb.logger") as mock_logger:
            _log_cast_loss(raw_duckdb, "cast_t", "val", pre_meaningful, "SMALLINT")
            mock_logger.warning.assert_called_once()
            call_args = mock_logger.warning.call_args[0][0]
            assert "2 non-empty values failed" in call_args


@pytest.mark.integration
class TestOrderLookup:
    """Test order_lookup table integrity against All_Sales source."""

    def test_values_match_source(self, test_duckdb_connection):
        con = test_duckdb_connection
        mismatches = con.execute("""
            SELECT COUNT(*) FROM order_lookup ol
            JOIN All_Sales s ON ol.OrderID_Std = s.OrderID_Std
            WHERE ol.IsSubscriptionService != s.IsSubscriptionService
        """).fetchone()[0]
        assert mismatches == 0

    def test_unique_ids(self, test_duckdb_connection):
        con = test_duckdb_connection
        result = con.execute("""
            SELECT
                COUNT(DISTINCT OrderID_Std) = COUNT(*)
            FROM order_lookup
        """).fetchone()[0]
        assert result is True

    def test_no_null_ids(self, test_duckdb_connection):
        con = test_duckdb_connection
        nulls = con.execute(
            "SELECT COUNT(*) FROM order_lookup WHERE OrderID_Std IS NULL"
        ).fetchone()[0]
        assert nulls == 0
