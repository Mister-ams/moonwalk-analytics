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

page = st.navigation(
    [overview, customers, items_page, revenues, stops],
    position="hidden",
)
page.run()
