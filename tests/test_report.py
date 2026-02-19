"""Tests for the monthly PDF report generator.

Tests cover: return type, PDF validity, section resilience, and graceful
handling when a requested period has no data.

Fetch functions from section_data are mocked to avoid requiring a full
database schema and Streamlit session context.
"""

from __future__ import annotations

import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── Helpers ──────────────────────────────────────────────────────────

def _mock_measures(period: str) -> dict:
    return {
        period: {
            "customers": 42, "clients": 35, "subscribers": 7,
            "items": 210, "items_client": 180, "items_sub": 30,
            "revenues": 12500.0, "rev_client": 9800.0, "rev_sub": 2700.0,
            "deliveries": 38, "pickups": 38, "stops": 76,
        }
    }

def _mock_yoy(period: str) -> dict:
    return {period: {"customers": 38, "items": 190, "revenues": 11000.0, "stops": 70}}

def _mock_acq(period: str) -> dict:
    return {period: {"new_customers": 8, "existing_customers": 34, "new_revenue": 2100.0, "existing_revenue": 10400.0}}

def _mock_rfm() -> pd.DataFrame:
    return pd.DataFrame()  # triggers "Not enough data" guard path

def _mock_logistics(period: str) -> dict:
    return {period: {
        "lg_total_stops": 76, "lg_deliveries": 38, "lg_pickups": 38,
        "lg_items_delivered": 180, "lg_rev_per_delivery": 160.0,
        "geo": {
            "Inside Abu Dhabi": {"customers": 30, "stops": 55, "items": 140, "revenue": 8500.0},
            "Outer Abu Dhabi":  {"customers": 12, "stops": 21, "items": 70,  "revenue": 4000.0},
        },
    }}

def _mock_operations(period: str) -> dict:
    return {period: {
        "categories": {
            "Traditional Wear":  {"items": 80, "revenue": 5000.0},
            "Professional Wear": {"items": 50, "revenue": 3500.0},
        },
        "services": {},
        "express_share": 0.25,
        "ops_avg_processing_time": 1.2,
        "ops_avg_time_in_store": 0.5,
    }}

def _mock_payments(period: str) -> dict:
    return {period: {
        "pm_revenue": 12500.0, "pm_total_collections": 11800.0,
        "pm_stripe": 7000.0, "pm_terminal": 3500.0, "pm_cash": 1300.0,
        "pm_avg_days_to_payment": 2.4,
    }}

def _mock_outstanding() -> dict:
    return {"total_outstanding": 3200.0, "order_count": 8, "aging": None, "top20": None}


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture
def patched_report():
    """Patch all fetch functions so generate_report needs no real database."""
    period = "2025-01"
    prev = "2024-12"

    with (
        patch("dashboard_shared.fetch_measures_batch", return_value={**_mock_measures(period), **_mock_measures(prev)}),
        patch("section_data.fetch_yoy_batch", return_value=_mock_yoy(period)),
        patch("customer_report_shared.fetch_new_customer_detail_batch", return_value=_mock_acq(period)),
        patch("section_data.fetch_rfm_snapshot", return_value=_mock_rfm()),
        patch("section_data.fetch_logistics_batch", return_value={**_mock_logistics(period), **_mock_logistics(prev)}),
        patch("section_data.fetch_operations_batch", return_value={**_mock_operations(period), **_mock_operations(prev)}),
        patch("section_data.fetch_payments_batch", return_value={**_mock_payments(period), **_mock_payments(prev)}),
        patch("section_data.fetch_outstanding", return_value=_mock_outstanding()),
    ):
        yield period, [prev, period]


# ── Tests ─────────────────────────────────────────────────────────────

@pytest.mark.integration
def test_report_returns_bytes(patched_report):
    """generate_monthly_report returns non-empty bytes."""
    from generate_report import generate_monthly_report

    period, available = patched_report
    result = generate_monthly_report(MagicMock(), period, available)
    assert isinstance(result, bytes)
    assert len(result) > 0


@pytest.mark.integration
def test_report_is_valid_pdf(patched_report):
    """PDF output starts with the %%PDF magic bytes."""
    from generate_report import generate_monthly_report

    period, available = patched_report
    result = generate_monthly_report(MagicMock(), period, available)
    assert result[:4] == b"%PDF"


@pytest.mark.integration
def test_report_all_sections_present(patched_report):
    """Report includes at least 5 pages (cover + 4 sections)."""
    from generate_report import generate_monthly_report

    period, available = patched_report
    result = generate_monthly_report(MagicMock(), period, available)
    # Each page adds a /Page entry in the PDF cross-reference
    assert result.count(b"/Page") >= 5


@pytest.mark.integration
def test_report_bad_period_graceful(patched_report):
    """generate_monthly_report handles a period with no matching data without raising."""
    from generate_report import generate_monthly_report

    # Measures return empty dicts for a period with no data
    with patch("dashboard_shared.fetch_measures_batch", return_value={}):
        _, available = patched_report
        result = generate_monthly_report(MagicMock(), "2020-01", ["2019-12", "2020-01"])

    assert isinstance(result, bytes)
    assert result[:4] == b"%PDF"
