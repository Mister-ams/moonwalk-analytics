"""
Operations page (Section 05).

Shows items and revenue breakdowns by Item Category and Service Type
with interactive button selectors and trend charts.
"""

import streamlit as st
from dashboard_shared import (
    inject_global_styles, get_connection,
    change_html, sub_card, get_display_window,
    render_trend_chart_v2,
    render_page_header, render_page_title, render_section_heading, render_footer,
    fmt_count, fmt_dhs_sub, fmt_days,
    COLORS, METRIC_CONFIG,
)
from section_data import fetch_operations_batch

inject_global_styles()
con = get_connection()
selected_period, available_periods = render_page_header(con)

hdr = COLORS["operations"]["header"]
sub_bg = COLORS["operations"]["sub"]

render_page_title("Operations", hdr)

# Fetch data
window = get_display_window(selected_period, available_periods)
first_idx = available_periods.index(window[0])
fetch_periods = (
    [available_periods[first_idx - 1]] if first_idx > 0 else []
) + window
ops_data = fetch_operations_batch(con, tuple(fetch_periods))

cur = ops_data.get(selected_period, {})
idx = available_periods.index(selected_period)
prev = ops_data.get(available_periods[idx - 1]) if idx > 0 else None

CATEGORIES = ["Professional Wear", "Traditional Wear", "Home Linens", "Extras", "Others"]
SERVICES = ["Wash & Press", "Dry Cleaning", "Press Only", "Other Service"]


def _safe_key(name):
    return name.lower().replace(" ", "_").replace("&", "and")


# ─── By Item Category ─────────────────────────────────────────────────
render_section_heading("By Item Category", hdr)

if "operations_category" not in st.session_state:
    st.session_state["operations_category"] = CATEGORIES[0]

cat_cols = st.columns(len(CATEGORIES))
for i, cat in enumerate(CATEGORIES):
    with cat_cols[i]:
        is_active = st.session_state["operations_category"] == cat
        if st.button(cat, key=f"ops_cat_btn_{i}",
                      use_container_width=True,
                      type="primary" if is_active else "secondary"):
            st.session_state["operations_category"] = cat
            st.rerun()

sel_cat = st.session_state["operations_category"]
cat_data = cur.get("categories", {}).get(sel_cat, {})
prev_cat = prev.get("categories", {}).get(sel_cat, {}) if prev else {}

sc1, sc2 = st.columns(2)
with sc1:
    st.markdown(
        sub_card("Items", fmt_count(cat_data.get("items", 0)),
                  change_html(cat_data.get("items", 0), prev_cat.get("items")),
                  sub_bg),
        unsafe_allow_html=True,
    )
with sc2:
    st.markdown(
        sub_card("Revenue", fmt_dhs_sub(cat_data.get("revenue", 0)),
                  change_html(cat_data.get("revenue", 0), prev_cat.get("revenue")),
                  sub_bg),
        unsafe_allow_html=True,
    )

# Build trend data keyed by metric for the chart
safe_cat = _safe_key(sel_cat)
items_key = f"cat_{safe_cat}_items"
rev_key = f"cat_{safe_cat}_rev"

# Trend chart for selected category — items
_, center, _ = st.columns([1, 18, 1])
with center:
    render_trend_chart_v2(
        f"ops_cat_{safe_cat}_items", ops_data, window, available_periods,
        METRIC_CONFIG[items_key], hdr, show_title=True, height=350,
    )

# ─── By Service Type ──────────────────────────────────────────────────
render_section_heading("By Service Type", hdr)

if "operations_service" not in st.session_state:
    st.session_state["operations_service"] = SERVICES[0]

svc_cols = st.columns(len(SERVICES))
for i, svc in enumerate(SERVICES):
    with svc_cols[i]:
        is_active = st.session_state["operations_service"] == svc
        if st.button(svc, key=f"ops_svc_btn_{i}",
                      use_container_width=True,
                      type="primary" if is_active else "secondary"):
            st.session_state["operations_service"] = svc
            st.rerun()

sel_svc = st.session_state["operations_service"]
svc_data = cur.get("services", {}).get(sel_svc, {})
prev_svc = prev.get("services", {}).get(sel_svc, {}) if prev else {}

ss1, ss2 = st.columns(2)
with ss1:
    st.markdown(
        sub_card("Items", fmt_count(svc_data.get("items", 0)),
                  change_html(svc_data.get("items", 0), prev_svc.get("items")),
                  sub_bg),
        unsafe_allow_html=True,
    )
with ss2:
    st.markdown(
        sub_card("Revenue", fmt_dhs_sub(svc_data.get("revenue", 0)),
                  change_html(svc_data.get("revenue", 0), prev_svc.get("revenue")),
                  sub_bg),
        unsafe_allow_html=True,
    )

safe_svc = _safe_key(sel_svc)
svc_items_key = f"svc_{safe_svc}_items"
svc_rev_key = f"svc_{safe_svc}_rev"

_, center, _ = st.columns([1, 18, 1])
with center:
    render_trend_chart_v2(
        f"ops_svc_{safe_svc}_items", ops_data, window, available_periods,
        METRIC_CONFIG[svc_items_key], hdr, show_title=True, height=350,
    )

# ─── Processing Efficiency ────────────────────────────────────────────
render_section_heading("Processing Efficiency", hdr)

pe1, pe2 = st.columns(2)
with pe1:
    st.markdown(
        sub_card("Avg Processing Time", fmt_days(cur.get("ops_avg_processing_time", 0)),
                  change_html(cur.get("ops_avg_processing_time", 0),
                              prev.get("ops_avg_processing_time", 0) if prev else None),
                  sub_bg),
        unsafe_allow_html=True,
    )
with pe2:
    st.markdown(
        sub_card("Avg Time In Store", fmt_days(cur.get("ops_avg_time_in_store", 0)),
                  change_html(cur.get("ops_avg_time_in_store", 0),
                              prev.get("ops_avg_time_in_store", 0) if prev else None),
                  sub_bg),
        unsafe_allow_html=True,
    )

# ─── Footer ────────────────────────────────────────────────────────────
render_footer()
