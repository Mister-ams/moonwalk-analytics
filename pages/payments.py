"""
Payments page (Section 06).

Shows revenue, collections by method, processing metrics,
and trend charts for collections.
"""

import streamlit as st
from dashboard_shared import (
    inject_global_styles, get_connection,
    change_html, headline_card, sub_card,
    compute_fetch_periods, render_metric_selector,
    render_page_header, render_page_title, render_section_heading, render_footer,
    fmt_count, fmt_dhs, fmt_dhs_sub, fmt_days,
    COLORS,
)
from section_data import fetch_payments_batch

inject_global_styles()
con = get_connection()
selected_period, available_periods = render_page_header(con)

hdr = COLORS["payments"]["header"]
sub_bg = COLORS["payments"]["sub"]

render_page_title("Payments", hdr)

# Fetch data
window, fetch_periods = compute_fetch_periods(selected_period, available_periods)
pm_data = fetch_payments_batch(con, tuple(fetch_periods))

cur = pm_data.get(selected_period, {})
idx = available_periods.index(selected_period)
prev = pm_data.get(available_periods[idx - 1]) if idx > 0 else None


def _chg(key):
    return change_html(cur.get(key, 0), prev.get(key, 0) if prev else None)


# ─── Row 1: 2 headline cards ──────────────────────────────────────────
h1, h2 = st.columns(2)
with h1:
    st.markdown(
        headline_card("Revenues", fmt_dhs(cur.get("pm_revenue", 0)),
                       _chg("pm_revenue"), hdr),
        unsafe_allow_html=True,
    )
with h2:
    st.markdown(
        headline_card("Total Collections", fmt_dhs(cur.get("pm_total_collections", 0)),
                       _chg("pm_total_collections"), hdr),
        unsafe_allow_html=True,
    )

# ─── Collection Methods ───────────────────────────────────────────────
render_section_heading("Collection Methods", hdr)
c1, c2, c3 = st.columns(3)
with c1:
    st.markdown(
        sub_card("Stripe", fmt_dhs_sub(cur.get("pm_stripe", 0)),
                  _chg("pm_stripe"), sub_bg),
        unsafe_allow_html=True,
    )
with c2:
    st.markdown(
        sub_card("Terminal", fmt_dhs_sub(cur.get("pm_terminal", 0)),
                  _chg("pm_terminal"), sub_bg),
        unsafe_allow_html=True,
    )
with c3:
    st.markdown(
        sub_card("Cash", fmt_dhs_sub(cur.get("pm_cash", 0)),
                  _chg("pm_cash"), sub_bg),
        unsafe_allow_html=True,
    )

# ─── Payment Timing ──────────────────────────────────────────────────
render_section_heading("Payment Timing", hdr)
_, p_center, _ = st.columns([1, 2, 1])
with p_center:
    st.markdown(
        sub_card("Avg Days To Payment", fmt_days(cur.get("pm_avg_days_to_payment", 0)),
                  _chg("pm_avg_days_to_payment"), sub_bg),
        unsafe_allow_html=True,
    )

# ─── Trend chart ──────────────────────────────────────────────────────
st.markdown('<div style="height:1rem;"></div>', unsafe_allow_html=True)

render_metric_selector(
    metrics=[
        ("Collections", "pm_total_collections"),
        ("Stripe", "pm_stripe"),
        ("Terminal", "pm_terminal"),
    ],
    trend_data=pm_data, window=window,
    available_periods=available_periods, selected_period=selected_period,
    state_key="payments_trend", header_color=hdr,
)

# ─── Footer ────────────────────────────────────────────────────────────
render_footer()
