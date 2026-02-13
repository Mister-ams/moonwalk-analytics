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

overview = st.Page("pages/overview.py", title="Overview", default=True)
customers = st.Page("pages/customers.py", title="Customers")
items_page = st.Page("pages/items.py", title="Items")
revenues = st.Page("pages/revenues.py", title="Revenues")
stops = st.Page("pages/stops.py", title="Stops")

customer_report = st.Page("pages/customer_report.py", title="Customer Report")

page = st.navigation(
    {
        "01 Monthly Report": [overview, customers, items_page, revenues, stops],
        "02 Customer Report": [customer_report],
    },
    position="sidebar",
)
page.run()
