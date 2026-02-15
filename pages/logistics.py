"""
Logistics page (Section 04).

Shows delivery/pickup activity, geographic distribution,
and trend charts for Total Stops and Items Delivered.
"""

import streamlit as st
from dashboard_shared import (
    inject_global_styles, get_connection,
    change_html, headline_card, sub_card, detail_card,
    get_display_window, render_metric_selector,
    render_page_header, render_page_title, render_section_heading, render_footer,
    fmt_count, fmt_dhs_sub, fmt_pct,
    COLORS,
)
from section_data import fetch_logistics_batch

inject_global_styles()
con = get_connection()
selected_period, available_periods = render_page_header(con)

hdr = COLORS["logistics"]["header"]
sub_bg = COLORS["logistics"]["sub"]

render_page_title("Logistics", hdr)

# Fetch data
window = get_display_window(selected_period, available_periods)
first_idx = available_periods.index(window[0])
fetch_periods = (
    [available_periods[first_idx - 1]] if first_idx > 0 else []
) + window
lg_data = fetch_logistics_batch(con, tuple(fetch_periods))

cur = lg_data.get(selected_period, {})
idx = available_periods.index(selected_period)
prev = lg_data.get(available_periods[idx - 1]) if idx > 0 else None


def _chg(key):
    return change_html(cur.get(key, 0), prev.get(key, 0) if prev else None)


# ─── Row 1: 4 headline cards ──────────────────────────────────────────
h1, h2, h3, h4 = st.columns(4)
with h1:
    st.markdown(
        headline_card("Total Stops", fmt_count(cur.get("lg_total_stops", 0)),
                       _chg("lg_total_stops"), hdr),
        unsafe_allow_html=True,
    )
with h2:
    st.markdown(
        headline_card("Items Delivered", fmt_count(cur.get("lg_items_delivered", 0)),
                       _chg("lg_items_delivered"), hdr),
        unsafe_allow_html=True,
    )
with h3:
    st.markdown(
        headline_card("Delivery Rev %", fmt_pct(cur.get("lg_delivery_rev_pct", 0)),
                       _chg("lg_delivery_rev_pct"), hdr),
        unsafe_allow_html=True,
    )
with h4:
    st.markdown(
        headline_card("Delivery Rate %", fmt_pct(cur.get("lg_delivery_rate", 0)),
                       _chg("lg_delivery_rate"), hdr),
        unsafe_allow_html=True,
    )

# ─── Row 2: 2 sub-cards ───────────────────────────────────────────────
s1, s2 = st.columns(2)
with s1:
    st.markdown(
        sub_card("Deliveries", fmt_count(cur.get("lg_deliveries", 0)),
                  _chg("lg_deliveries"), sub_bg),
        unsafe_allow_html=True,
    )
with s2:
    st.markdown(
        sub_card("Pickups", fmt_count(cur.get("lg_pickups", 0)),
                  _chg("lg_pickups"), sub_bg),
        unsafe_allow_html=True,
    )

# ─── Geographic Distribution ──────────────────────────────────────────
render_section_heading("Geographic Distribution", hdr)

geo = cur.get("geo", {})
inside = geo.get("Inside Abu Dhabi", {})
outer = geo.get("Outer Abu Dhabi", {})

prev_geo = prev.get("geo", {}) if prev else {}
prev_inside = prev_geo.get("Inside Abu Dhabi", {})
prev_outer = prev_geo.get("Outer Abu Dhabi", {})

def _geo_rows(data, prev_data):
    """Build detail_card rows for a geographic region."""
    rows = []
    for label, key in [("Customers", "customers"), ("Items Delivered", "items"),
                        ("Stops", "stops"), ("Revenue", "revenue")]:
        val = data.get(key, 0)
        prev_val = prev_data.get(key, 0) if prev_data else None
        display = fmt_dhs_sub(val) if key == "revenue" else fmt_count(val)
        rows.append((label, display, change_html(val, prev_val)))
    return rows

g1, g2 = st.columns(2)
with g1:
    st.markdown(
        detail_card("Inside Abu Dhabi", _geo_rows(inside, prev_inside), hdr, sub_bg),
        unsafe_allow_html=True,
    )
with g2:
    st.markdown(
        detail_card("Outer Abu Dhabi", _geo_rows(outer, prev_outer), hdr, sub_bg),
        unsafe_allow_html=True,
    )

# ─── Trend chart: Total Stops | Items Delivered ───────────────────────
st.markdown('<div style="height:1rem;"></div>', unsafe_allow_html=True)

render_metric_selector(
    metrics=[
        ("Total Stops", "lg_total_stops"),
        ("Items Delivered", "lg_items_delivered"),
    ],
    trend_data=lg_data, window=window,
    available_periods=available_periods, selected_period=selected_period,
    state_key="logistics_trend", header_color=hdr,
)

# ─── Footer ────────────────────────────────────────────────────────────
render_footer()
