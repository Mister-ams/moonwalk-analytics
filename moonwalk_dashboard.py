"""
LOOMI Monthly Report — Multi-page Streamlit + DuckDB dashboard.

Entrypoint: configures the app and routes to pages.
Run with: python -m streamlit run moonwalk_dashboard.py
"""

import hmac
import streamlit as st

st.set_page_config(
    page_title="LOOMI Monthly Report",
    page_icon="\U0001f9fc",
    layout="wide",
)


def _check_password():
    """Gate dashboard behind a shared password from st.secrets."""
    if st.session_state.get("_auth_ok"):
        return True
    pwd = st.secrets.get("DASHBOARD_PASSWORD", "")
    if not pwd:
        return True

    def _on_submit():
        if hmac.compare_digest(st.session_state.get("_pwd_input", ""), pwd):
            st.session_state["_auth_ok"] = True
        else:
            st.session_state["_auth_fail"] = True

    st.markdown("### LOOMI Monthly Report")
    st.text_input("Password", type="password", key="_pwd_input", on_change=_on_submit)
    if st.session_state.get("_auth_fail"):
        st.error("Incorrect password")
    st.stop()


if not _check_password():
    st.stop()

# Business Performance
overview = st.Page("pages/overview.py", title="Overview", default=True)
customers = st.Page("pages/customers.py", title="Customers")
items_page = st.Page("pages/items.py", title="Items")
revenues = st.Page("pages/revenues.py", title="Revenues")
ci_overview = st.Page("pages/customer_insights.py", title="Customer Insights")
customer_report = st.Page("pages/customer_report.py", title="Items per Customer")
customer_report_revenue = st.Page("pages/customer_report_revenue.py", title="Revenue per Customer")
new_customers_page = st.Page("pages/new_customers.py", title="New Customers")
cohort_page = st.Page("pages/cohort.py", title="Cohort Analysis")

# Operations
operations_page = st.Page("pages/operations.py", title="Operations")
logistics_page = st.Page("pages/logistics.py", title="Logistics")

# Financials
payments_page = st.Page("pages/payments.py", title="Payments")

page = st.navigation(
    {
        "Monthly Report": [overview, customers, items_page, revenues],
        "Customer Intelligence": [
            ci_overview, customer_report, customer_report_revenue,
            new_customers_page, cohort_page,
        ],
        "Operations": [operations_page, logistics_page],
        "Financials": [payments_page],
    },
    position="sidebar",
)
page.run()
