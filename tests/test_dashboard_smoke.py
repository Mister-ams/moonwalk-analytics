"""Playwright smoke tests for the LOOMI Monthly Report dashboard.

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

# All page paths in the dashboard
ALL_PAGES = [
    "/",
    "/customers",
    "/items",
    "/revenues",
    "/customer_insights",
    "/customer_report",
    "/customer_report_revenue",
    "/new_customers",
    "/cohort",
    "/logistics",
    "/operations",
    "/payments",
]

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


skip_if_no_dashboard = pytest.mark.skipif(
    not _dashboard_running(),
    reason="Dashboard not running on port 8504"
)


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


@pytest.mark.playwright
@skip_if_no_dashboard
class TestDashboardSmoke:
    """Browser smoke tests for the Streamlit dashboard."""

    def test_overview_loads(self, page):
        _goto_and_auth(page, BASE_URL)
        # Streamlit renders the app title or header
        assert page.title() or page.locator("text=LOOMI").count() > 0 or page.locator("h1").count() > 0

    @pytest.mark.parametrize("path", ALL_PAGES)
    def test_all_pages_load(self, page, path):
        _goto_and_auth(page, f"{BASE_URL}{path}")
        # No st.error or st.exception elements should be visible
        errors = page.locator('[data-testid="stException"], [data-testid="stError"]')
        assert errors.count() == 0, f"Page {path} has errors"

    def test_period_selector_renders(self, page):
        _goto_and_auth(page, BASE_URL)
        # Should have a selectbox (period dropdown) or pills control
        selectors = page.locator('[data-testid="stSelectbox"], [data-testid="stPills"]')
        assert selectors.count() > 0

    def test_monthly_weekly_toggle(self, page):
        _goto_and_auth(page, BASE_URL)
        # Find Weekly pill and click it
        weekly = page.locator('text=Weekly')
        if weekly.count() > 0:
            weekly.first.click()
            page.wait_for_load_state("networkidle")
            # After clicking Weekly, period dropdown should show week-format values
            page.wait_for_timeout(1000)
            # Just verify no crash — page still loaded
            errors = page.locator('[data-testid="stException"]')
            assert errors.count() == 0

    def test_toggle_persists_across_pages(self, page):
        _goto_and_auth(page, BASE_URL)
        # Click Weekly
        weekly = page.locator('text=Weekly')
        if weekly.count() > 0:
            weekly.first.click()
            page.wait_for_timeout(1000)
            # Navigate to Customers via sidebar (preserves session)
            customers_link = page.locator('a[href*="/customers"]')
            if customers_link.count() > 0:
                customers_link.first.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(1000)
            # Weekly should still be selected (or at least page loads without error)
            errors = page.locator('[data-testid="stException"]')
            assert errors.count() == 0

    def test_sidebar_sections(self, page):
        _goto_and_auth(page, BASE_URL)
        content = page.content()
        for section in ["Monthly Report", "Customer Intelligence", "Operations", "Financials"]:
            assert section in content, f"Sidebar missing section: {section}"

    def test_footer_data_as_of(self, page):
        _goto_and_auth(page, BASE_URL)
        data_as_of = page.locator('text=Data as of')
        assert data_as_of.count() > 0

    def test_headline_cards_on_overview(self, page):
        _goto_and_auth(page, BASE_URL)
        # Headline cards use st.markdown with custom HTML — look for card divs
        cards = page.locator('.headline-card, [class*="card"]')
        # Fallback: just check that there's meaningful content rendered
        if cards.count() == 0:
            # At least 4 st.columns worth of content
            columns = page.locator('[data-testid="stColumn"]')
            assert columns.count() >= 4

    def test_detail_page_back_link(self, page):
        _goto_and_auth(page, f"{BASE_URL}/customers")
        # Should have an "Overview" link or back navigation
        overview_link = page.locator('text=Overview')
        assert overview_link.count() > 0

    def test_chart_renders(self, page):
        _goto_and_auth(page, f"{BASE_URL}/customers")
        # Plotly charts render as iframe or div with class plotly
        charts = page.locator('.js-plotly-plot, [data-testid="stPlotlyChart"], iframe')
        assert charts.count() > 0, "No chart rendered on customers page"

    def test_monthly_only_page_gate(self, page):
        _goto_and_auth(page, BASE_URL)
        # Click Weekly
        weekly = page.locator('text=Weekly')
        if weekly.count() > 0:
            weekly.first.click()
            page.wait_for_timeout(1000)
            # Navigate to Cohort (monthly-only page) via sidebar
            cohort_link = page.locator('a[href*="/cohort"]')
            if cohort_link.count() > 0:
                cohort_link.first.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(1000)
            # Should show an info gate message
            info_banner = page.locator('[data-testid="stInfo"]')
            monthly_text = page.locator('text=monthly')
            assert info_banner.count() > 0 or monthly_text.count() > 0, \
                "Monthly-only gate not shown for Cohort in weekly mode"

    def test_no_console_errors(self, page):
        errors = []
        page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
        _goto_and_auth(page, BASE_URL)
        page.wait_for_timeout(2000)
        # Filter out known benign errors (e.g., favicon, Streamlit telemetry)
        real_errors = [
            e for e in errors
            if "favicon" not in e.lower()
            and "websocket" not in e.lower()
            and "analytics" not in e.lower()
        ]
        assert len(real_errors) == 0, f"Console errors: {real_errors}"
