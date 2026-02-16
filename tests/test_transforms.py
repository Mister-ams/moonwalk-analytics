"""Integration tests for ETL transform output validation.

Validates the existing output CSVs in LOCAL_STAGING_PATH have correct
schemas and data integrity. These tests verify transform output quality
without re-running transforms (which need real CleanCloud CSVs).

Requires: ETL pipeline to have been run at least once.
"""

import pytest
import polars as pl
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import LOCAL_STAGING_PATH


def _load_if_exists(name: str) -> pl.DataFrame:
    """Load a CSV from staging, skip test if not present."""
    path = LOCAL_STAGING_PATH / name
    if not path.exists():
        pytest.skip(f"{name} not found — run ETL first")
    return pl.read_csv(path, infer_schema_length=10000, try_parse_dates=False)


@pytest.mark.integration
class TestAllSalesOutput:
    """Validate All_Sales_Python.csv output schema and integrity."""

    def test_schema(self):
        df = _load_if_exists("All_Sales_Python.csv")
        expected = {
            "OrderID_Std", "CustomerID_Std", "Store_Std",
            "Transaction_Type", "Total_Num", "Is_Earned",
            "Payment_Type_Std", "Route_Category", "IsSubscriptionService",
            "Earned_Date", "OrderCohortMonth",
        }
        assert expected.issubset(set(df.columns))

    def test_rows_positive(self):
        df = _load_if_exists("All_Sales_Python.csv")
        assert df.height > 0

    def test_key_columns_non_null(self):
        df = _load_if_exists("All_Sales_Python.csv")
        for col in ["CustomerID_Std", "OrderID_Std", "Store_Std"]:
            null_count = df[col].null_count()
            assert null_count == 0, f"{col} has {null_count} nulls"

    def test_transaction_types(self):
        df = _load_if_exists("All_Sales_Python.csv")
        valid = {"Order", "Subscription", "Invoice Payment"}
        actual = set(df["Transaction_Type"].unique().to_list())
        assert actual.issubset(valid), f"Unexpected types: {actual - valid}"

    def test_store_values(self):
        df = _load_if_exists("All_Sales_Python.csv")
        valid = {"Moon Walk", "Hielo"}
        actual = set(df["Store_Std"].unique().to_list())
        assert actual.issubset(valid), f"Unexpected stores: {actual - valid}"


@pytest.mark.integration
class TestAllCustomersOutput:
    """Validate All_Customers_Python.csv output schema and integrity."""

    def test_schema(self):
        df = _load_if_exists("All_Customers_Python.csv")
        expected = {"CustomerID_Std", "CustomerName", "Store_Std", "CohortMonth"}
        assert expected.issubset(set(df.columns))

    def test_rows_positive(self):
        df = _load_if_exists("All_Customers_Python.csv")
        assert df.height > 0

    def test_customer_id_format(self):
        df = _load_if_exists("All_Customers_Python.csv")
        ids = df["CustomerID_Std"].to_list()
        for cid in ids:
            assert cid.startswith("CC-") or cid.startswith("MW-"), f"Bad ID: {cid}"


@pytest.mark.integration
class TestAllItemsOutput:
    """Validate All_Items_Python.csv output schema and integrity."""

    def test_schema(self):
        df = _load_if_exists("All_Items_Python.csv")
        expected = {
            "OrderID_Std", "CustomerID_Std", "Store_Std",
            "Item", "Section", "Quantity", "Item_Category", "Service_Type",
        }
        assert expected.issubset(set(df.columns))

    def test_rows_positive(self):
        df = _load_if_exists("All_Items_Python.csv")
        assert df.height > 0

    def test_item_categories(self):
        df = _load_if_exists("All_Items_Python.csv")
        valid = {"Traditional Wear", "Home Linens", "Professional Wear", "Extras", "Others"}
        actual = set(df["Item_Category"].unique().to_list())
        assert actual.issubset(valid), f"Unexpected categories: {actual - valid}"

    def test_service_types(self):
        df = _load_if_exists("All_Items_Python.csv")
        valid = {"Dry Cleaning", "Wash & Press", "Press Only", "Other Service"}
        actual = set(df["Service_Type"].unique().to_list())
        assert actual.issubset(valid), f"Unexpected types: {actual - valid}"


@pytest.mark.integration
class TestCustomerQualityOutput:
    """Validate Customer_Quality_Monthly_Python.csv output."""

    def test_schema(self):
        df = _load_if_exists("Customer_Quality_Monthly_Python.csv")
        expected = {"CustomerID_Std", "OrderCohortMonth", "Monthly_Revenue", "Monthly_Items"}
        assert expected.issubset(set(df.columns))

    def test_rows_positive(self):
        df = _load_if_exists("Customer_Quality_Monthly_Python.csv")
        assert df.height > 0


@pytest.mark.integration
class TestDimPeriodOutput:
    """Validate DimPeriod_Python.csv output."""

    def test_schema(self):
        df = _load_if_exists("DimPeriod_Python.csv")
        expected = {"Date", "YearMonth", "Year", "Month"}
        assert expected.issubset(set(df.columns))

    def test_rows_positive(self):
        df = _load_if_exists("DimPeriod_Python.csv")
        assert df.height > 0

    def test_date_range(self):
        df = _load_if_exists("DimPeriod_Python.csv")
        years = df["Year"].cast(pl.Int32).unique().to_list()
        assert 2025 in years or 2024 in years, "Expected recent years in DimPeriod"
