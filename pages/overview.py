"""
Overview page — 4 headline KPI cards + 8 sub-cards with page navigation links.
"""

import streamlit as st
from dashboard_shared import (
    inject_global_styles, period_selector, get_connection, is_weekly,
    fetch_measures_batch, change_html, headline_card, sub_card,
    get_display_window, render_metric_selector,
    render_footer, render_section_heading,
    fmt_count, fmt_dhs, fmt_dhs_sub,
    COLORS,
)
from customer_report_shared import fetch_customer_measures_batch

inject_global_styles()
con = get_connection()

selected_period, available_periods = period_selector(con, show_title=True)
st.markdown("---")

weekly_mode = is_weekly(selected_period)

# ─── Fetch current + previous period via cached batch call ───────────
idx = available_periods.index(selected_period)
fetch_periods = [selected_period]
if idx > 0:
    fetch_periods.insert(0, available_periods[idx - 1])
batch = fetch_measures_batch(con, tuple(fetch_periods))
cur = batch.get(selected_period, {})
prev = batch.get(available_periods[idx - 1]) if idx > 0 else None


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

_PAGES = ["pages/customers.py", "pages/items.py", "pages/revenues.py", "pages/logistics.py"]

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

# ─── Customer Insights ───────────────────────────────────────────────
render_section_heading("Customer Insights", "#BF360C")

# Fetch display window of customer measures
ci_window = get_display_window(selected_period, available_periods)
ci_first_idx = available_periods.index(ci_window[0])
ci_fetch = (
    [available_periods[ci_first_idx - 1]] if ci_first_idx > 0 else []
) + ci_window
ci_data = fetch_customer_measures_batch(con, tuple(ci_fetch))

ci_hdr = COLORS["customer_report"]["header"]

# Row 1: New Customers (hidden in weekly mode — monthly concept)
if not weekly_mode:
    render_metric_selector(
        metrics=[("New Customers", "new_customers")],
        trend_data=ci_data, window=ci_window,
        available_periods=available_periods, selected_period=selected_period,
        state_key="overview_new_customers", header_color=ci_hdr,
        chart_height=380,
        detail_link={"page": "pages/new_customers.py", "label": "View Details \u2192"},
    )

    st.markdown('<div style="height:0.8rem;"></div>', unsafe_allow_html=True)

# Row 2: Items per Customer
render_metric_selector(
    metrics=[("Items per Customer", "items_per_customer")],
    trend_data=ci_data, window=ci_window,
    available_periods=available_periods, selected_period=selected_period,
    state_key="overview_items_per_cust", header_color=ci_hdr,
    chart_height=380,
    detail_link={"page": "pages/customer_report.py", "label": "View Details \u2192"},
)

st.markdown('<div style="height:0.8rem;"></div>', unsafe_allow_html=True)

# Row 3: Revenue per Customer
render_metric_selector(
    metrics=[("Revenue per Customer", "rev_per_customer")],
    trend_data=ci_data, window=ci_window,
    available_periods=available_periods, selected_period=selected_period,
    state_key="overview_rev_per_cust", header_color=ci_hdr,
    chart_height=380,
    detail_link={"page": "pages/customer_report_revenue.py", "label": "View Details \u2192"},
)

# ─── Footer ──────────────────────────────────────────────────────────
render_footer()
