"""Unit tests for helpers.py — Polars vectorized ETL functions."""

import pytest
import polars as pl
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from helpers import (
    find_cleancloud_file,
    polars_to_date,
    polars_name_standardize,
    polars_store_std,
    polars_customer_id_std,
    polars_order_id_std,
    polars_payment_type_std,
    polars_item_category,
    polars_service_type,
    polars_route_category,
    polars_months_since_cohort,
    polars_subscription_flag,
    _merge_overlapping_periods,
    polars_format_dates_for_csv,
    polars_validate_output,
)


# =====================================================================
# polars_to_date
# =====================================================================

class TestPolarsToDate:
    def test_iso_date(self):
        df = pl.DataFrame({"d": ["2025-01-15"]})
        result = polars_to_date(df, "d")
        assert result["d"][0].date() == date(2025, 1, 15)

    def test_iso_datetime(self):
        df = pl.DataFrame({"d": ["2025-03-21 01:29:10"]})
        result = polars_to_date(df, "d")
        assert result["d"][0].date() == date(2025, 3, 21)

    def test_human_date(self):
        df = pl.DataFrame({"d": ["21 Mar 2025 00:39"]})
        result = polars_to_date(df, "d")
        assert result["d"][0].date() == date(2025, 3, 21)

    def test_excel_serial(self):
        # 45292 = 2024-01-01 in Excel serial date system
        df = pl.DataFrame({"d": ["45292"]})
        result = polars_to_date(df, "d")
        assert result["d"][0].date() == date(2024, 1, 1)

    def test_null_values(self):
        df = pl.DataFrame({"d": [None, "", "nan"]})
        result = polars_to_date(df, "d")
        assert result["d"].null_count() == 3

    def test_mixed_formats(self):
        df = pl.DataFrame({"d": ["2025-01-15", "21 Mar 2025 00:39", "45292", None]})
        result = polars_to_date(df, "d")
        assert result["d"][0].date() == date(2025, 1, 15)
        assert result["d"][2].date() == date(2024, 1, 1)
        assert result["d"][3] is None

    def test_alias(self):
        df = pl.DataFrame({"raw_date": ["2025-06-01"]})
        result = polars_to_date(df, "raw_date", alias="ParsedDate")
        assert "ParsedDate" in result.columns


# =====================================================================
# polars_store_std
# =====================================================================

class TestPolarsStoreStd:
    def test_moonwalk_id(self):
        df = pl.DataFrame({"sid": ["36319"]})
        result = df.select(polars_store_std("sid").alias("store"))
        assert result["store"][0] == "Moon Walk"

    def test_hielo_id(self):
        df = pl.DataFrame({"sid": ["38516"]})
        result = df.select(polars_store_std("sid").alias("store"))
        assert result["store"][0] == "Hielo"

    def test_unknown_id(self):
        df = pl.DataFrame({"sid": ["99999"]})
        result = df.select(polars_store_std("sid").alias("store"))
        assert result["store"][0] is None


# =====================================================================
# polars_customer_id_std
# =====================================================================

class TestPolarsCustomerIdStd:
    def test_legacy_customer(self):
        df = pl.DataFrame({"cid": ["123"], "Source": ["Legacy"]})
        result = df.select(polars_customer_id_std("cid", "Source").alias("std"))
        assert result["std"][0] == "MW-0123"

    def test_cleancloud_customer(self):
        df = pl.DataFrame({"cid": ["42"], "Source": ["CleanCloud"]})
        result = df.select(polars_customer_id_std("cid", "Source").alias("std"))
        assert result["std"][0] == "CC-0042"

    def test_preformatted(self):
        df = pl.DataFrame({"cid": ["MW-0001"], "Source": ["Legacy"]})
        result = df.select(polars_customer_id_std("cid", "Source").alias("std"))
        assert result["std"][0] == "MW-0001"

    def test_null_customer(self):
        df = pl.DataFrame({"cid": [None], "Source": ["CleanCloud"]})
        result = df.select(polars_customer_id_std("cid", "Source").alias("std"))
        assert result["std"][0] is None


# =====================================================================
# polars_order_id_std
# =====================================================================

class TestPolarsOrderIdStd:
    def _make_df(self, order_id, store="Moon Walk", source="CleanCloud", txn_type="Order"):
        return pl.DataFrame({
            "Order ID": [order_id],
            "Store_Std": [store],
            "Source": [source],
            "Transaction_Type": [txn_type],
        })

    def test_subscription_prefix(self):
        df = self._make_df("12345", txn_type="Subscription")
        result = polars_order_id_std(df)
        assert result["OrderID_Std"][0].startswith("S-")

    def test_invoice_prefix(self):
        df = self._make_df("12345", txn_type="Invoice Payment")
        result = polars_order_id_std(df)
        assert result["OrderID_Std"][0].startswith("I-")

    def test_receipt_prefix_kept(self):
        df = self._make_df("R-12345")
        result = polars_order_id_std(df)
        assert result["OrderID_Std"][0] == "R-12345"

    def test_hielo_prefix(self):
        df = self._make_df("99", store="Hielo")
        result = polars_order_id_std(df)
        assert result["OrderID_Std"][0] == "H-00099"

    def test_moonwalk_prefix(self):
        df = self._make_df("99", store="Moon Walk")
        result = polars_order_id_std(df)
        assert result["OrderID_Std"][0] == "M-00099"

    def test_preformatted_h_or_m(self):
        df = self._make_df("H-00123")
        result = polars_order_id_std(df)
        assert result["OrderID_Std"][0] == "H-00123"


# =====================================================================
# polars_payment_type_std
# =====================================================================

class TestPolarsPaymentTypeStd:
    def _eval(self, payment_type):
        df = pl.DataFrame({"Payment Type": [payment_type]})
        return df.select(polars_payment_type_std().alias("pt"))["pt"][0]

    def test_cash(self):
        assert self._eval("Cash") == "Cash"

    def test_terminal(self):
        assert self._eval("Card Terminal") == "Terminal"

    def test_stripe(self):
        assert self._eval("Bank Transfer / Stripe") == "Stripe"

    def test_receivable(self):
        assert self._eval("Invoice") == "Receivable"

    def test_other(self):
        assert self._eval("Cheque") == "Other"


# =====================================================================
# polars_item_category
# =====================================================================

class TestPolarsItemCategory:
    def _eval(self, item, section=""):
        df = pl.DataFrame({"Item": [item], "Section": [section]})
        return df.select(polars_item_category().alias("cat"))["cat"][0]

    def test_traditional_wear(self):
        assert self._eval("Kandura") == "Traditional Wear"

    def test_home_linens(self):
        assert self._eval("Duvet Cover") == "Home Linens"

    def test_professional_wear(self):
        assert self._eval("Suit Jacket") == "Professional Wear"

    def test_extras(self):
        assert self._eval("Shoe Cleaning") == "Extras"

    def test_others(self):
        assert self._eval("Random Item") == "Others"


# =====================================================================
# polars_service_type
# =====================================================================

class TestPolarsServiceType:
    def _eval(self, section):
        df = pl.DataFrame({"Section": [section]})
        return df.select(polars_service_type().alias("svc"))["svc"][0]

    def test_dry_cleaning(self):
        assert self._eval("Dry Cleaning") == "Dry Cleaning"

    def test_wash_and_press(self):
        assert self._eval("Wash & Press") == "Wash & Press"

    def test_press_only(self):
        assert self._eval("Press Only") == "Press Only"

    def test_other_service(self):
        assert self._eval("Alterations") == "Other Service"


# =====================================================================
# polars_route_category
# =====================================================================

class TestPolarsRouteCategory:
    def _eval(self, route):
        df = pl.DataFrame({"Route #": [route]})
        return df.select(polars_route_category().alias("rc"))["rc"][0]

    def test_inside_abu_dhabi(self):
        assert self._eval("2") == "Inside Abu Dhabi"

    def test_outer_abu_dhabi(self):
        assert self._eval("5") == "Outer Abu Dhabi"

    def test_other(self):
        assert self._eval("0") == "Other"


# =====================================================================
# polars_months_since_cohort
# =====================================================================

class TestPolarsMonthsSinceCohort:
    def test_same_year(self):
        df = pl.DataFrame({
            "order": [datetime(2025, 6, 1)],
            "cohort": [datetime(2025, 1, 1)],
        })
        result = df.select(polars_months_since_cohort("order", "cohort").alias("m"))
        assert result["m"][0] == 5

    def test_year_boundary(self):
        df = pl.DataFrame({
            "order": [datetime(2025, 2, 1)],
            "cohort": [datetime(2024, 11, 1)],
        })
        result = df.select(polars_months_since_cohort("order", "cohort").alias("m"))
        assert result["m"][0] == 3


# =====================================================================
# polars_subscription_flag
# =====================================================================

class TestPolarsSubscriptionFlag:
    def test_covered_order(self):
        df = pl.DataFrame({
            "CustomerID_Std": ["CC-0001"],
            "Earned_Date": [datetime(2025, 1, 15)],
            "Transaction_Type": ["Order"],
            "OrderID_Std": ["M-00001"],
        })
        sub_dict = {
            "CC-0001": [{"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 31)}],
        }
        result = polars_subscription_flag(df, sub_dict)
        assert result["IsSubscriptionService"][0] == 1

    def test_uncovered_order(self):
        df = pl.DataFrame({
            "CustomerID_Std": ["CC-0001"],
            "Earned_Date": [datetime(2025, 3, 15)],
            "Transaction_Type": ["Order"],
            "OrderID_Std": ["M-00002"],
        })
        sub_dict = {
            "CC-0001": [{"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 31)}],
        }
        result = polars_subscription_flag(df, sub_dict)
        assert result["IsSubscriptionService"][0] == 0

    def test_overlapping_periods_still_covered(self):
        """Order during overlapping subscription periods should be flagged."""
        df = pl.DataFrame({
            "CustomerID_Std": ["CC-0001"],
            "Earned_Date": [datetime(2025, 1, 20)],
            "Transaction_Type": ["Order"],
            "OrderID_Std": ["M-00003"],
        })
        sub_dict = {
            "CC-0001": [
                {"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 25)},
                {"ValidFrom": datetime(2025, 1, 10), "ValidUntil": datetime(2025, 2, 10)},
            ],
        }
        result = polars_subscription_flag(df, sub_dict)
        assert result["IsSubscriptionService"][0] == 1

    def test_adjacent_periods_not_merged(self):
        """Non-overlapping periods should remain separate; gap order is uncovered."""
        df = pl.DataFrame({
            "CustomerID_Std": ["CC-0001"],
            "Earned_Date": [datetime(2025, 2, 5)],
            "Transaction_Type": ["Order"],
            "OrderID_Std": ["M-00004"],
        })
        sub_dict = {
            "CC-0001": [
                {"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 31)},
                {"ValidFrom": datetime(2025, 3, 1), "ValidUntil": datetime(2025, 3, 31)},
            ],
        }
        result = polars_subscription_flag(df, sub_dict)
        assert result["IsSubscriptionService"][0] == 0

    def test_empty_subscription_dict(self):
        df = pl.DataFrame({
            "CustomerID_Std": ["CC-0001"],
            "Earned_Date": [datetime(2025, 1, 15)],
            "Transaction_Type": ["Order"],
            "OrderID_Std": ["M-00005"],
        })
        result = polars_subscription_flag(df, {})
        assert result["IsSubscriptionService"][0] == 0

    def test_subscription_ignores_non_orders(self):
        """Subscription and Invoice Payment rows should not get flagged."""
        df = pl.DataFrame({
            "CustomerID_Std": ["CC-0001"],
            "Earned_Date": [datetime(2025, 1, 15)],
            "Transaction_Type": ["Subscription"],
            "OrderID_Std": ["S-00001"],
        })
        sub_dict = {
            "CC-0001": [{"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 31)}],
        }
        result = polars_subscription_flag(df, sub_dict)
        assert result["IsSubscriptionService"][0] == 0


# =====================================================================
# _merge_overlapping_periods
# =====================================================================

class TestMergeOverlappingPeriods:
    def test_no_overlap(self):
        periods = [
            {"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 31)},
            {"ValidFrom": datetime(2025, 3, 1), "ValidUntil": datetime(2025, 3, 31)},
        ]
        result = _merge_overlapping_periods(periods)
        assert len(result) == 2

    def test_full_overlap(self):
        periods = [
            {"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 31)},
            {"ValidFrom": datetime(2025, 1, 10), "ValidUntil": datetime(2025, 1, 20)},
        ]
        result = _merge_overlapping_periods(periods)
        assert len(result) == 1
        assert result[0]["ValidFrom"] == datetime(2025, 1, 1)
        assert result[0]["ValidUntil"] == datetime(2025, 1, 31)

    def test_partial_overlap(self):
        periods = [
            {"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 20)},
            {"ValidFrom": datetime(2025, 1, 15), "ValidUntil": datetime(2025, 2, 15)},
        ]
        result = _merge_overlapping_periods(periods)
        assert len(result) == 1
        assert result[0]["ValidUntil"] == datetime(2025, 2, 15)

    def test_boundary_touch(self):
        """Periods that touch at the boundary should merge."""
        periods = [
            {"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 31)},
            {"ValidFrom": datetime(2025, 1, 31), "ValidUntil": datetime(2025, 2, 28)},
        ]
        result = _merge_overlapping_periods(periods)
        assert len(result) == 1

    def test_single_period(self):
        periods = [{"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 31)}]
        result = _merge_overlapping_periods(periods)
        assert len(result) == 1

    def test_empty(self):
        assert _merge_overlapping_periods([]) == []

    def test_three_way_chain(self):
        """Three overlapping periods should merge into one."""
        periods = [
            {"ValidFrom": datetime(2025, 1, 1), "ValidUntil": datetime(2025, 1, 20)},
            {"ValidFrom": datetime(2025, 1, 15), "ValidUntil": datetime(2025, 2, 10)},
            {"ValidFrom": datetime(2025, 2, 5), "ValidUntil": datetime(2025, 3, 1)},
        ]
        result = _merge_overlapping_periods(periods)
        assert len(result) == 1
        assert result[0]["ValidFrom"] == datetime(2025, 1, 1)
        assert result[0]["ValidUntil"] == datetime(2025, 3, 1)


# =====================================================================
# polars_name_standardize
# =====================================================================

class TestPolarsNameStandardize:
    def test_normal_name(self):
        df = pl.DataFrame({"n": ["John Doe-Smith"]})
        result = df.select(polars_name_standardize(pl.col("n")).alias("s"))
        assert result["s"][0] == "JOHNDOESMITH"

    def test_special_chars(self):
        df = pl.DataFrame({"n": ["O'Brien, Jr."]})
        result = df.select(polars_name_standardize(pl.col("n")).alias("s"))
        assert result["s"][0] == "OBRIENJR"


# =====================================================================
# polars_format_dates_for_csv
# =====================================================================

class TestPolarsFormatDatesForCsv:
    def test_valid_dates(self):
        df = pl.DataFrame({"d": [datetime(2025, 1, 15), datetime(2025, 6, 30)]})
        result = polars_format_dates_for_csv(df, ["d"])
        assert result["d"][0] == "2025-01-15"
        assert result["d"][1] == "2025-06-30"

    def test_null_dates(self):
        df = pl.DataFrame({"d": [None, datetime(2025, 1, 1)]}).cast({"d": pl.Datetime("us")})
        result = polars_format_dates_for_csv(df, ["d"])
        assert result["d"][0] == ""
        assert result["d"][1] == "2025-01-01"


# =====================================================================
# polars_validate_output
# =====================================================================

class TestPolarsValidateOutput:
    def test_passes(self):
        df = pl.DataFrame({
            "CustomerID_Std": ["MW-0001"],
            "OrderCohortMonth": [datetime(2025, 1, 1)],
        })
        result = polars_validate_output(df, "test_output")
        assert result["passed"] is True

    def test_null_keys_flagged(self):
        df = pl.DataFrame({
            "CustomerID_Std": [None],
            "OrderCohortMonth": [datetime(2025, 1, 1)],
        })
        result = polars_validate_output(df, "test_output")
        assert result["passed"] is False
        assert any("NULL KEYS" in i for i in result["issues"])


# =====================================================================
# find_cleancloud_file
# =====================================================================

class TestFindCleancloudFile:
    def test_found(self, tmp_path):
        # Create a matching file
        f = tmp_path / "CC-Orders-2025.csv"
        f.write_text("header\n")
        result = find_cleancloud_file("orders", downloads_path=str(tmp_path))
        assert result == str(f)

    def test_missing_required(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            find_cleancloud_file("orders", downloads_path=str(tmp_path), required=True)

    def test_missing_optional(self, tmp_path):
        result = find_cleancloud_file("orders", downloads_path=str(tmp_path), required=False)
        assert result is None
