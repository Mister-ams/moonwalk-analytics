"""
Overview page — 4 headline KPI cards + 8 sub-cards with page navigation links.
"""

import streamlit as st
from dashboard_shared import (
    inject_global_styles, month_selector, get_connection,
    fetch_measures_batch, change_html, headline_card, sub_card,
    fmt_count, fmt_dhs, fmt_dhs_sub,
    COLORS, SALES_CSV,
)
from datetime import datetime

inject_global_styles()
con = get_connection()

selected_month, available_months = month_selector(con, show_title=True)
st.markdown("---")

# ─── Fetch current + previous month via cached batch call ────────────
idx = available_months.index(selected_month)
fetch_months = [selected_month]
if idx > 0:
    fetch_months.insert(0, available_months[idx - 1])
batch = fetch_measures_batch(con, tuple(fetch_months))
cur = batch.get(selected_month, {})
prev = batch.get(available_months[idx - 1]) if idx > 0 else None


def _chg(key):
    return change_html(cur[key], prev[key] if prev else None)


# ─── Headline cards (4 columns) ──────────────────────────────────────
_HEADLINE = [
    ("Customers", fmt_count(cur["customers"]), _chg("customers"),
     COLORS["customers"]["header"]),
    ("Items", fmt_count(cur["items"]), _chg("items"),
     COLORS["items"]["header"]),
    ("Revenues", fmt_dhs(cur["revenues"]), _chg("revenues"),
     COLORS["revenues"]["header"]),
    ("Stops", fmt_count(cur["stops"]), _chg("stops"),
     COLORS["stops"]["header"]),
]

_PAGES = ["pages/customers.py", "pages/items.py", "pages/revenues.py", "pages/stops.py"]

hcols = st.columns(4)
for i, (label, val_html, chg_html, hdr_color) in enumerate(_HEADLINE):
    with hcols[i]:
        st.markdown(
            headline_card(label, val_html, chg_html, hdr_color),
            unsafe_allow_html=True,
        )
        st.page_link(_PAGES[i], label="View Details \u2192", icon="\U0001f4ca")

# ─── Sub-cards (2 per column, below headline row) ────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    c = COLORS["customers"]["sub"]
    st.markdown(
        sub_card("Clients", fmt_count(cur["clients"]), _chg("clients"), c),
        unsafe_allow_html=True,
    )
    st.markdown(
        sub_card("Subscribers", fmt_count(cur["subscribers"]), _chg("subscribers"), c),
        unsafe_allow_html=True,
    )

with col2:
    c = COLORS["items"]["sub"]
    st.markdown(
        sub_card("Client Items", fmt_count(cur["items_client"]), _chg("items_client"), c),
        unsafe_allow_html=True,
    )
    st.markdown(
        sub_card("Subscriber Items", fmt_count(cur["items_sub"]), _chg("items_sub"), c),
        unsafe_allow_html=True,
    )

with col3:
    c = COLORS["revenues"]["sub"]
    st.markdown(
        sub_card("Client Revenue", fmt_dhs_sub(cur["rev_client"]), _chg("rev_client"), c),
        unsafe_allow_html=True,
    )
    st.markdown(
        sub_card("Subscriber Revenue", fmt_dhs_sub(cur["rev_sub"]), _chg("rev_sub"), c),
        unsafe_allow_html=True,
    )

with col4:
    c = COLORS["stops"]["sub"]
    st.markdown(
        sub_card("Deliveries", fmt_count(cur["deliveries"]), _chg("deliveries"), c),
        unsafe_allow_html=True,
    )
    st.markdown(
        sub_card("Pickups", fmt_count(cur["pickups"]), _chg("pickups"), c),
        unsafe_allow_html=True,
    )

# ─── Footer ──────────────────────────────────────────────────────────
st.markdown("---")
st.caption(f"Data: {SALES_CSV}")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.caption("Powered by Streamlit + DuckDB")
