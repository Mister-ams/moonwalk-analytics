"""
LOOMI Monthly Report — Multi-page Streamlit + DuckDB dashboard.

Entrypoint: configures the app and routes to pages.
Run with: python -m streamlit run moonwalk_dashboard.py
"""

import streamlit as st

st.set_page_config(
    page_title="LOOMI Monthly Report",
    page_icon="\U0001f9fc",
    layout="wide",
)

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
