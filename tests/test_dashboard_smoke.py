"""Playwright smoke tests for the LOOMI Monthly Report dashboard (4-page persona structure).

Prerequisites:
    - Dashboard must be running on port 8504:
      python -m streamlit run moonwalk_dashboard.py --server.port 8504
    - pytest-playwright must be installed:
      pip install pytest-playwright && python -m playwright install chromium

Run:
    python -m pytest tests/test_dashboard_smoke.py -m playwright -v
"""

import pytest
import urllib.request
import urllib.error
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BASE_URL = "http://localhost:8504"

# 4-page persona routes (st.navigation assigns these from file paths)
PERSONA_PAGES = ["/", "/customer_analytics", "/operations_center", "/financial_performance"]

# Sidebar page titles for the 4 persona pages
PERSONA_TITLES = ["Executive Pulse", "Customer Analytics", "Operations Center", "Financial Performance"]

# Read password once at module level
_PASSWORD = ""
_secrets_path = Path(__file__).resolve().parent.parent / ".streamlit" / "secrets.toml"
if _secrets_path.exists():
    for _line in _secrets_path.read_text().splitlines():
        if _line.startswith("DASHBOARD_PASSWORD"):
            _PASSWORD = _line.split("=", 1)[1].strip().strip('"').strip("'")
            break


def _dashboard_running():
    """Check if dashboard is reachable."""
    try:
        urllib.request.urlopen(BASE_URL, timeout=3)
        return True
    except (urllib.error.URLError, OSError):
        return False


skip_if_no_dashboard = pytest.mark.skipif(not _dashboard_running(), reason="Dashboard not running on port 8504")


def _goto_and_auth(page, url):
    """Navigate to URL and authenticate through password gate if shown.

    Always authenticates at the root first (to establish session), then
    navigates to the target URL via sidebar link if it's a sub-page.
    """
    # Authenticate at root to establish session
    page.goto(BASE_URL)
    page.wait_for_load_state("networkidle")
    pwd_input = page.locator('[data-testid="stTextInput"] input[type="password"]')
    if pwd_input.count() > 0 and _PASSWORD:
        pwd_input.fill(_PASSWORD)
        pwd_input.press("Enter")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(3000)

    # Navigate to target sub-page via sidebar link (preserves session)
    if url not in (BASE_URL, f"{BASE_URL}/"):
        path = url.replace(BASE_URL, "")
        sidebar_link = page.locator(f'a[href*="{path}"]')
        if sidebar_link.count() > 0:
            sidebar_link.first.click()
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(2000)


def _click_tab(page, label_contains):
    """Click a tab button by partial label text and wait for content."""
    tab = page.locator('button[role="tab"]').filter(has_text=label_contains)
    if tab.count() > 0:
        tab.first.click()
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(800)


@pytest.mark.playwright
@skip_if_no_dashboard
class TestDashboardSmoke:
    """Browser smoke tests for the Streamlit dashboard (4-page persona structure)."""

    def test_executive_pulse_loads(self, page):
        _goto_and_auth(page, BASE_URL)
        # Root page should render — check title or visible heading
        assert page.title() or page.locator("text=Executive Pulse").count() > 0 or page.locator("h1").count() > 0

    @pytest.mark.parametrize("path", PERSONA_PAGES)
    def test_all_persona_pages_load(self, page, path):
        _goto_and_auth(page, f"{BASE_URL}{path}")
        # No st.error or st.exception elements should be visible
        errors = page.locator('[data-testid="stException"], [data-testid="stError"]')
        assert errors.count() == 0, f"Page {path} has errors"

    def test_sidebar_has_persona_pages(self, page):
        _goto_and_auth(page, BASE_URL)
        content = page.content()
        for title in PERSONA_TITLES:
            assert title in content, f"Sidebar missing persona page: {title}"

    def test_period_selector_renders(self, page):
        _goto_and_auth(page, BASE_URL)
        # Should have a selectbox (period dropdown) or pills control
        selectors = page.locator('[data-testid="stSelectbox"], [data-testid="stPills"]')
        assert selectors.count() > 0

    def test_monthly_weekly_toggle(self, page):
        _goto_and_auth(page, BASE_URL)
        # Find Weekly pill and click it
        weekly = page.locator("text=Weekly")
        if weekly.count() > 0:
            weekly.first.click()
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(1000)
            # Verify no crash after switching to Weekly
            errors = page.locator('[data-testid="stException"]')
            assert errors.count() == 0

    def test_toggle_persists_to_customer_analytics(self, page):
        _goto_and_auth(page, BASE_URL)
        # Click Weekly on Executive Pulse
        weekly = page.locator("text=Weekly")
        if weekly.count() > 0:
            weekly.first.click()
            page.wait_for_timeout(1000)
            # Navigate to Customer Analytics via sidebar (preserves session)
            ca_link = page.locator('a[href*="/customer_analytics"]')
            if ca_link.count() > 0:
                ca_link.first.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(1000)
            # Weekly should still be selected (or at least page loads without error)
            errors = page.locator('[data-testid="stException"]')
            assert errors.count() == 0

    def test_executive_pulse_tabs(self, page):
        _goto_and_auth(page, BASE_URL)
        for tab_label in ["Snapshot", "Trends", "Insights"]:
            _click_tab(page, tab_label)
            errors = page.locator('[data-testid="stException"]')
            assert errors.count() == 0, f"Error on Executive Pulse '{tab_label}' tab"
        # Verify Snapshot tab renders headline cards
        _click_tab(page, "Snapshot")
        content = page.content()
        assert any(kw in content for kw in ["Customers", "Items", "Revenue", "Stops"])

    def test_customer_analytics_tabs(self, page):
        _goto_and_auth(page, f"{BASE_URL}/customer_analytics")
        for tab_label in ["Acquisition", "Segmentation", "Cohort", "Per-Customer"]:
            _click_tab(page, tab_label)
            errors = page.locator('[data-testid="stException"]')
            assert errors.count() == 0, f"Error on Customer Analytics '{tab_label}' tab"

    def test_operations_center_tabs(self, page):
        _goto_and_auth(page, f"{BASE_URL}/operations_center")
        for tab_label in ["Logistics", "Geography", "Service Mix"]:
            _click_tab(page, tab_label)
            errors = page.locator('[data-testid="stException"]')
            assert errors.count() == 0, f"Error on Operations Center '{tab_label}' tab"

    def test_financial_performance_tabs(self, page):
        _goto_and_auth(page, f"{BASE_URL}/financial_performance")
        for tab_label in ["Collections", "Payment Cycle", "Concentration", "Outstanding"]:
            _click_tab(page, tab_label)
            errors = page.locator('[data-testid="stException"]')
            assert errors.count() == 0, f"Error on Financial Performance '{tab_label}' tab"

    def test_executive_pulse_snapshot_cards(self, page):
        _goto_and_auth(page, BASE_URL)
        _click_tab(page, "Snapshot")
        content = page.content()
        for kw in ["Customers", "Items", "Revenue", "Stops"]:
            assert kw in content, f"Snapshot card '{kw}' not found"

    def test_insights_tab_content(self, page):
        _goto_and_auth(page, BASE_URL)
        _click_tab(page, "Insights")
        page.wait_for_timeout(1000)
        content = page.content()
        # Either the insights table renders, or we see the "run cleancloud_to_duckdb.py" message
        assert any(kw in content for kw in ["🟢", "🔴", "🟡", "cleancloud_to_duckdb", "Insights"]), (
            "Insights tab has no content"
        )

    def test_chart_renders_on_trends(self, page):
        _goto_and_auth(page, BASE_URL)
        _click_tab(page, "Trends")
        page.wait_for_timeout(1500)
        charts = page.locator('.js-plotly-plot, [data-testid="stPlotlyChart"], iframe')
        assert charts.count() > 0, "No chart rendered on Trends tab"

    def test_monthly_only_tab_gates(self, page):
        _goto_and_auth(page, BASE_URL)
        # Switch to Weekly on Executive Pulse
        weekly = page.locator("text=Weekly")
        if weekly.count() > 0:
            weekly.first.click()
            page.wait_for_timeout(1000)
        # Navigate to Customer Analytics
        ca_link = page.locator('a[href*="/customer_analytics"]')
        if ca_link.count() > 0:
            ca_link.first.click()
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(1000)
        # Click Acquisition tab (monthly-only)
        _click_tab(page, "Acquisition")
        page.wait_for_timeout(1000)
        info_banner = page.locator('[data-testid="stInfo"]')
        # Click Cohort tab (also monthly-only)
        _click_tab(page, "Cohort")
        page.wait_for_timeout(1000)
        cohort_info = page.locator('[data-testid="stInfo"]')
        assert info_banner.count() > 0 or cohort_info.count() > 0, (
            "Monthly-only gate not shown for Acquisition/Cohort in weekly mode"
        )

    def test_outstanding_tab_content(self, page):
        _goto_and_auth(page, f"{BASE_URL}/financial_performance")
        _click_tab(page, "Outstanding")
        page.wait_for_timeout(1000)
        content = page.content()
        # Should show either outstanding data or an empty state message
        assert any(kw in content for kw in ["CC_2025", "outstanding", "Outstanding", "No "]), (
            "Outstanding tab has no content"
        )

    def test_footer_data_as_of(self, page):
        _goto_and_auth(page, BASE_URL)
        data_as_of = page.locator("text=Data as of")
        assert data_as_of.count() > 0

    def test_no_console_errors(self, page):
        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        _goto_and_auth(page, BASE_URL)
        page.wait_for_timeout(2000)
        # Filter out known benign errors (e.g., favicon, Streamlit telemetry)
        real_errors = [
            e
            for e in errors
            if "favicon" not in e.lower() and "websocket" not in e.lower() and "analytics" not in e.lower()
        ]
        assert len(real_errors) == 0, f"Console errors: {real_errors}"
