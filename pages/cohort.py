"""
Cohort Analysis page (Section 03).

Shows M0 (new customers) and M1 (first month retention) metrics
with trend charts and retention rate sub-cards.
"""

import streamlit as st
from dashboard_shared import (
    inject_global_styles, get_connection, is_weekly,
    change_html, sub_card, get_display_window, render_metric_selector,
    render_page_header, render_page_title, render_section_heading, render_footer,
    fmt_count, fmt_dhs_sub, fmt_ratio, fmt_pct,
    COLORS,
)
from section_data import fetch_cohort_batch, compute_cohort_retention

inject_global_styles()
con = get_connection()
selected_period, available_periods = render_page_header(con)

hdr = COLORS["cohort"]["header"]
sub_bg = COLORS["cohort"]["sub"]

render_page_title("Cohort Analysis", hdr)

# Weekly gate — cohort uses MonthsSinceCohort (monthly concept)
if is_weekly(selected_period):
    st.info("Cohort Analysis is available in monthly view only. Toggle back to monthly to view data.")
    render_footer()
    st.stop()

# Fetch data
window = get_display_window(selected_period, available_periods)
first_idx = available_periods.index(window[0])
fetch_periods = (
    [available_periods[first_idx - 1]] if first_idx > 0 else []
) + window
cohort_data = fetch_cohort_batch(con, tuple(fetch_periods))

cur = cohort_data.get(selected_period, {})
idx = available_periods.index(selected_period)
prev = cohort_data.get(available_periods[idx - 1]) if idx > 0 else None


def _chg(key):
    return change_html(cur.get(key, 0), prev.get(key, 0) if prev else None)


# ─── M0 — New Customers ───────────────────────────────────────────────
render_section_heading("M0 -- New Customers", hdr)

render_metric_selector(
    metrics=[
        ("M0 Customers", "m0_customers"),
        ("M0 Items", "m0_items"),
        ("M0 Revenue", "m0_revenue"),
    ],
    trend_data=cohort_data, window=window,
    available_periods=available_periods, selected_period=selected_period,
    state_key="cohort_m0_metric", header_color=hdr,
)

m0_s1, m0_s2 = st.columns(2)
with m0_s1:
    st.markdown(
        sub_card("M0 Rev/Customer", fmt_dhs_sub(cur.get("m0_rev_per_customer", 0)),
                  _chg("m0_rev_per_customer"), sub_bg),
        unsafe_allow_html=True,
    )
with m0_s2:
    st.markdown(
        sub_card("M0 Items/Customer", fmt_ratio(cur.get("m0_items_per_customer", 0)),
                  _chg("m0_items_per_customer"), sub_bg),
        unsafe_allow_html=True,
    )

# ─── M1 — First Month Retention ───────────────────────────────────────
render_section_heading("M1 -- First Month Retention", hdr)

render_metric_selector(
    metrics=[
        ("M1 Customers", "m1_customers"),
        ("M1 Items", "m1_items"),
        ("M1 Revenue", "m1_revenue"),
    ],
    trend_data=cohort_data, window=window,
    available_periods=available_periods, selected_period=selected_period,
    state_key="cohort_m1_metric", header_color=hdr,
)

# Retention rates
retention = compute_cohort_retention(cohort_data, available_periods, selected_period)

# MoM for retention: compute previous month's retention too
prev_retention = None
if idx > 1:
    prev_retention = compute_cohort_retention(cohort_data, available_periods, available_periods[idx - 1])


def _ret_chg(key):
    cur_val = retention.get(key)
    prev_val = prev_retention.get(key) if prev_retention else None
    if cur_val is None:
        return '<span style="font-size:0.8rem;color:#aaa;">&mdash;</span>'
    return change_html(cur_val, prev_val)


def _ret_fmt(key):
    val = retention.get(key)
    if val is None:
        return "--"
    return fmt_pct(val)


r1, r2, r3 = st.columns(3)
with r1:
    st.markdown(
        sub_card("Customer Retention", _ret_fmt("customer_retention"),
                  _ret_chg("customer_retention"), sub_bg),
        unsafe_allow_html=True,
    )
with r2:
    st.markdown(
        sub_card("Item Retention", _ret_fmt("item_retention"),
                  _ret_chg("item_retention"), sub_bg),
        unsafe_allow_html=True,
    )
with r3:
    st.markdown(
        sub_card("Revenue Retention", _ret_fmt("revenue_retention"),
                  _ret_chg("revenue_retention"), sub_bg),
        unsafe_allow_html=True,
    )

m1_s1, m1_s2 = st.columns(2)
with m1_s1:
    st.markdown(
        sub_card("M1 Rev/Customer", fmt_dhs_sub(cur.get("m1_rev_per_customer", 0)),
                  _chg("m1_rev_per_customer"), sub_bg),
        unsafe_allow_html=True,
    )
with m1_s2:
    st.markdown(
        sub_card("M1 Items/Customer", fmt_ratio(cur.get("m1_items_per_customer", 0)),
                  _chg("m1_items_per_customer"), sub_bg),
        unsafe_allow_html=True,
    )

# ─── Footer ────────────────────────────────────────────────────────────
render_footer()
