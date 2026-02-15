"""
Customer Insights overview page (Section 02).

Shows active/multi-service customers, top 20% analysis by spend and volume,
and trend charts with links to detail pages.
"""

import streamlit as st
from dashboard_shared import (
    inject_global_styles, get_connection, is_weekly,
    change_html, headline_card, sub_card,
    compute_fetch_periods, render_metric_selector,
    render_page_header, render_page_title, render_section_heading, render_footer,
    fmt_count, fmt_dhs, fmt_dhs_sub, fmt_pct,
    COLORS,
)
from customer_report_shared import fetch_customer_measures_batch
from section_data import fetch_customer_insights_batch

inject_global_styles()
con = get_connection()
selected_period, available_periods = render_page_header(con)

hdr = COLORS["customer_insights"]["header"]
sub_bg = COLORS["customer_insights"]["sub"]

render_page_title("Customer Insights", hdr)

# Weekly gate — customer_quality table is monthly only
if is_weekly(selected_period):
    st.info("Customer Insights is available in monthly view only. Toggle back to monthly to view data.")
    render_footer()
    st.stop()

# Fetch data (display window + one extra for period-over-period)
window, fetch_periods = compute_fetch_periods(selected_period, available_periods)

ci_data = fetch_customer_insights_batch(con, tuple(fetch_periods))
cur = ci_data.get(selected_period, {})

idx = available_periods.index(selected_period)
prev = ci_data.get(available_periods[idx - 1]) if idx > 0 else None


def _chg(key):
    return change_html(cur.get(key, 0), prev.get(key, 0) if prev else None)


# ─── Row 1: 2 headline cards ──────────────────────────────────────────
h1, h2 = st.columns(2)
with h1:
    st.markdown(
        headline_card("Active Customers", fmt_count(cur.get("ci_active_customers", 0)),
                       _chg("ci_active_customers"), hdr),
        unsafe_allow_html=True,
    )
with h2:
    st.markdown(
        headline_card("Multi Service Customers", fmt_count(cur.get("ci_multi_service", 0)),
                       _chg("ci_multi_service"), hdr),
        unsafe_allow_html=True,
    )

# ─── Top 20% by Spend ─────────────────────────────────────────────────
render_section_heading("Top 20% by Spend", hdr)
s1, s2, s3 = st.columns(3)
with s1:
    st.markdown(
        sub_card("Threshold", fmt_dhs_sub(cur.get("ci_spend_threshold", 0)),
                  _chg("ci_spend_threshold"), sub_bg),
        unsafe_allow_html=True,
    )
with s2:
    st.markdown(
        sub_card("Revenue", fmt_dhs_sub(cur.get("ci_top20_spend_rev", 0)),
                  _chg("ci_top20_spend_rev"), sub_bg),
        unsafe_allow_html=True,
    )
with s3:
    st.markdown(
        sub_card("Revenue Share", fmt_pct(cur.get("ci_spend_share", 0)),
                  _chg("ci_spend_share"), sub_bg),
        unsafe_allow_html=True,
    )

# ─── Top 20% by Volume ────────────────────────────────────────────────
render_section_heading("Top 20% by Volume", hdr)
v1, v2, v3 = st.columns(3)
with v1:
    st.markdown(
        sub_card("Threshold", fmt_count(int(cur.get("ci_volume_threshold", 0))),
                  _chg("ci_volume_threshold"), sub_bg),
        unsafe_allow_html=True,
    )
with v2:
    st.markdown(
        sub_card("Revenue", fmt_dhs_sub(cur.get("ci_top20_vol_rev", 0)),
                  _chg("ci_top20_vol_rev"), sub_bg),
        unsafe_allow_html=True,
    )
with v3:
    st.markdown(
        sub_card("Revenue Share", fmt_pct(cur.get("ci_volume_share", 0)),
                  _chg("ci_volume_share"), sub_bg),
        unsafe_allow_html=True,
    )

# ─── Metric selectors with detail links ────────────────────────────────
st.markdown('<div style="height:1rem;"></div>', unsafe_allow_html=True)

# Fetch customer measures for trend charts (reuse existing batch)
ci_measures = fetch_customer_measures_batch(con, tuple(fetch_periods))

render_metric_selector(
    metrics=[("Active Customers", "active_customers")],
    trend_data=ci_measures, window=window,
    available_periods=available_periods, selected_period=selected_period,
    state_key="ci_active_customers", header_color=hdr,
    chart_height=380,
    detail_link={"page": "pages/customer_report.py", "label": "View Details"},
)

st.markdown('<div style="height:0.8rem;"></div>', unsafe_allow_html=True)

render_metric_selector(
    metrics=[("Revenue per Customer", "rev_per_customer")],
    trend_data=ci_measures, window=window,
    available_periods=available_periods, selected_period=selected_period,
    state_key="ci_rev_per_customer", header_color=hdr,
    chart_height=380,
    detail_link={"page": "pages/customer_report_revenue.py", "label": "View Details"},
)

# ─── Footer ────────────────────────────────────────────────────────────
render_footer()
