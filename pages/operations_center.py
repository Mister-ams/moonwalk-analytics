"""
Operations Center — Logistics, Geography, and Service Mix.

Tabs: Logistics | Geography | Service Mix
"""

import plotly.graph_objects as go
import streamlit as st

from dashboard_shared import (
    COLORS,
    METRIC_CONFIG,
    activate_tab_from_url,
    change_html,
    compute_fetch_periods,
    detail_card,
    dirham_html,
    fmt_count,
    fmt_days,
    fmt_dhs_sub,
    fmt_pct,
    get_connection,
    headline_card,
    inject_global_styles,
    is_weekly,
    period_selector,
    render_footer,
    render_metric_selector,
    render_page_title,
    render_section_heading,
    render_trend_chart_v2,
    sub_card,
)
from section_data import fetch_logistics_batch, fetch_operations_batch

inject_global_styles()
con = get_connection()

hdr = COLORS["operations"]["header"]
sub_bg = COLORS["operations"]["sub"]
lg_hdr = COLORS["logistics"]["header"]
lg_sub = COLORS["logistics"]["sub"]

render_page_title("Operations Center", hdr)
selected_period, available_periods = period_selector(con, show_title=False)
window, fetch_periods = compute_fetch_periods(selected_period, available_periods)

lg_data = fetch_logistics_batch(con, tuple(fetch_periods))
ops_data = fetch_operations_batch(con, tuple(fetch_periods))

lg_cur = lg_data.get(selected_period, {})
lg_idx = available_periods.index(selected_period)
lg_prev = lg_data.get(available_periods[lg_idx - 1]) if lg_idx > 0 else None

ops_cur = ops_data.get(selected_period, {})
ops_idx = available_periods.index(selected_period)
ops_prev = ops_data.get(available_periods[ops_idx - 1]) if ops_idx > 0 else None

_OC_TABS = ["Logistics", "Geography", "Service Mix"]
tab1, tab2, tab3 = st.tabs(["🚚 Logistics", "🗺️ Geography", "🧺 Service Mix"])
activate_tab_from_url(_OC_TABS)

# ─── LOGISTICS TAB ────────────────────────────────────────────────────
with tab1:

    def _lg_chg(key):
        return change_html(lg_cur.get(key, 0), lg_prev.get(key, 0) if lg_prev else None)

    h1, h2, h3, h4 = st.columns(4)
    with h1:
        st.markdown(
            headline_card(
                "Total Stops",
                fmt_count(lg_cur.get("lg_total_stops", 0)),
                _lg_chg("lg_total_stops"),
                lg_hdr,
            ),
            unsafe_allow_html=True,
        )
    with h2:
        rpd = lg_cur.get("lg_rev_per_delivery", 0) or 0
        prev_rpd = (lg_prev.get("lg_rev_per_delivery", 0) or 0) if lg_prev else None
        st.markdown(
            headline_card(
                "Rev per Delivery",
                fmt_dhs_sub(rpd),
                change_html(rpd, prev_rpd),
                lg_hdr,
            ),
            unsafe_allow_html=True,
        )
    with h3:
        st.markdown(
            headline_card(
                "Delivery Rate",
                fmt_pct(lg_cur.get("lg_delivery_rate", 0)),
                _lg_chg("lg_delivery_rate"),
                lg_hdr,
            ),
            unsafe_allow_html=True,
        )
    with h4:
        st.markdown(
            headline_card(
                "Items Delivered",
                fmt_count(lg_cur.get("lg_items_delivered", 0)),
                _lg_chg("lg_items_delivered"),
                lg_hdr,
            ),
            unsafe_allow_html=True,
        )

    s1, s2 = st.columns(2)
    with s1:
        st.markdown(
            sub_card(
                "Deliveries",
                fmt_count(lg_cur.get("lg_deliveries", 0)),
                _lg_chg("lg_deliveries"),
                lg_sub,
            ),
            unsafe_allow_html=True,
        )
    with s2:
        st.markdown(
            sub_card(
                "Pickups",
                fmt_count(lg_cur.get("lg_pickups", 0)),
                _lg_chg("lg_pickups"),
                lg_sub,
            ),
            unsafe_allow_html=True,
        )

    st.markdown("")
    render_metric_selector(
        [("Total Stops", "lg_total_stops"), ("Items Delivered", "lg_items_delivered")],
        lg_data,
        window,
        available_periods,
        selected_period,
        "oc_logistics",
        lg_hdr,
    )

    render_footer()

    with st.expander("Download Data"):
        import pandas as pd

        df_export = pd.DataFrame([{"period": p, **v} for p, v in lg_data.items() if p in window])
        st.download_button(
            "Download CSV",
            df_export.to_csv(index=False).encode("utf-8"),
            f"oc_logistics_{selected_period}.csv",
            "text/csv",
        )

# ─── GEOGRAPHY TAB ───────────────────────────────────────────────────
with tab2:
    geo = lg_cur.get("geo", {})
    inside = geo.get("Inside Abu Dhabi", {})
    outer = geo.get("Outer Abu Dhabi", {})

    prev_geo = lg_prev.get("geo", {}) if lg_prev else {}
    prev_inside = prev_geo.get("Inside Abu Dhabi", {})
    prev_outer = prev_geo.get("Outer Abu Dhabi", {})

    def _geo_rows(data, prev_data):
        rows = []
        for label, key in [
            ("Customers", "customers"),
            ("Items Delivered", "items"),
            ("Stops", "stops"),
            ("Revenue", "revenue"),
        ]:
            val = data.get(key, 0)
            prev_val = prev_data.get(key, 0) if prev_data else None
            display = fmt_dhs_sub(val) if key == "revenue" else fmt_count(val)
            rows.append((label, display, change_html(val, prev_val)))
        return rows

    _geo_metrics = [
        ("Customers", "customers"),
        ("Items", "items"),
        ("Stops", "stops"),
        ("Revenue", "revenue"),
    ]
    _inside_vals = [inside.get(k, 0) for _, k in _geo_metrics]
    _outer_vals = [outer.get(k, 0) for _, k in _geo_metrics]
    _totals = [i + o if (i + o) > 0 else 1 for i, o in zip(_inside_vals, _outer_vals)]
    _inside_pcts = [i / t * 100 for i, t in zip(_inside_vals, _totals)]
    _outer_pcts = [o / t * 100 for o, t in zip(_outer_vals, _totals)]
    _geo_labels = [lbl for lbl, _ in _geo_metrics]

    geo_fig = go.Figure()
    geo_fig.add_trace(
        go.Bar(
            y=_geo_labels,
            x=_inside_pcts,
            orientation="h",
            name="Inside Abu Dhabi",
            marker_color="#1565C0",
            text=[f"{v:.0f}%" for v in _inside_pcts],
            textposition="inside",
            textfont=dict(color="#fff", size=12, weight=700),
            hovertemplate="%{y}: %{x:.0f}% Inside<extra></extra>",
        )
    )
    geo_fig.add_trace(
        go.Bar(
            y=_geo_labels,
            x=_outer_pcts,
            orientation="h",
            name="Outer Abu Dhabi",
            marker_color="#90CAF9",
            text=[f"{v:.0f}%" for v in _outer_pcts],
            textposition="inside",
            textfont=dict(color="#333", size=12, weight=700),
            hovertemplate="%{y}: %{x:.0f}% Outer<extra></extra>",
        )
    )
    geo_fig.update_layout(
        barmode="stack",
        height=250,
        margin=dict(t=30, b=20, l=80, r=20),
        paper_bgcolor="#ffffff",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showticklabels=False, showgrid=False, fixedrange=True, range=[0, 100]),
        yaxis=dict(tickfont=dict(size=12), fixedrange=True, autorange="reversed"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
            font=dict(size=11),
        ),
        dragmode=False,
    )
    _, geo_center, _ = st.columns([1, 18, 1])
    with geo_center:
        st.plotly_chart(
            geo_fig,
            key="oc_geo_split_chart",
            use_container_width=True,
            config={"displayModeBar": False, "scrollZoom": False, "staticPlot": False},
        )

    g1, g2 = st.columns(2)
    with g1:
        st.markdown(
            detail_card("Inside Abu Dhabi", _geo_rows(inside, prev_inside), lg_hdr, lg_sub),
            unsafe_allow_html=True,
        )
    with g2:
        st.markdown(
            detail_card("Outer Abu Dhabi", _geo_rows(outer, prev_outer), lg_hdr, lg_sub),
            unsafe_allow_html=True,
        )

    st.markdown("")
    render_metric_selector(
        [("Total Stops", "lg_total_stops"), ("Items Delivered", "lg_items_delivered")],
        lg_data,
        window,
        available_periods,
        selected_period,
        "oc_geo",
        lg_hdr,
    )

    render_footer()

# ─── SERVICE MIX TAB ─────────────────────────────────────────────────
with tab3:
    CATEGORIES = ["Professional Wear", "Traditional Wear", "Home Linens", "Extras", "Others"]
    SERVICES = ["Wash & Press", "Dry Cleaning", "Press Only", "Other Service"]

    def _safe_key(name):
        return name.lower().replace(" ", "_").replace("&", "and")

    render_section_heading("By Item Category", hdr)

    if "oc_category" not in st.session_state:
        st.session_state["oc_category"] = CATEGORIES[0]

    cat_cols = st.columns(len(CATEGORIES))
    for i, cat in enumerate(CATEGORIES):
        with cat_cols[i]:
            is_active = st.session_state["oc_category"] == cat
            if st.button(
                cat,
                key=f"oc_cat_btn_{i}",
                use_container_width=True,
                type="primary" if is_active else "secondary",
            ):
                st.session_state["oc_category"] = cat
                st.rerun()

    sel_cat = st.session_state["oc_category"]
    cat_data = ops_cur.get("categories", {}).get(sel_cat, {})
    prev_cat = ops_prev.get("categories", {}).get(sel_cat, {}) if ops_prev else {}

    sc1, sc2 = st.columns(2)
    with sc1:
        st.markdown(
            sub_card(
                "Items",
                fmt_count(cat_data.get("items", 0)),
                change_html(cat_data.get("items", 0), prev_cat.get("items")),
                sub_bg,
            ),
            unsafe_allow_html=True,
        )
    with sc2:
        st.markdown(
            sub_card(
                "Revenue",
                fmt_dhs_sub(cat_data.get("revenue", 0)),
                change_html(cat_data.get("revenue", 0), prev_cat.get("revenue")),
                sub_bg,
            ),
            unsafe_allow_html=True,
        )

    safe_cat = _safe_key(sel_cat)
    items_key = f"cat_{safe_cat}_items"
    _, center, _ = st.columns([1, 18, 1])
    with center:
        render_trend_chart_v2(
            f"oc_cat_{safe_cat}_items",
            ops_data,
            window,
            available_periods,
            METRIC_CONFIG[items_key],
            hdr,
            show_title=True,
            height=300,
        )

    render_section_heading("By Service Type", hdr)

    if "oc_service" not in st.session_state:
        st.session_state["oc_service"] = SERVICES[0]

    svc_cols = st.columns(len(SERVICES))
    for i, svc in enumerate(SERVICES):
        with svc_cols[i]:
            is_active = st.session_state["oc_service"] == svc
            if st.button(
                svc,
                key=f"oc_svc_btn_{i}",
                use_container_width=True,
                type="primary" if is_active else "secondary",
            ):
                st.session_state["oc_service"] = svc
                st.rerun()

    sel_svc = st.session_state["oc_service"]
    svc_data = ops_cur.get("services", {}).get(sel_svc, {})
    prev_svc = ops_prev.get("services", {}).get(sel_svc, {}) if ops_prev else {}

    ss1, ss2 = st.columns(2)
    with ss1:
        st.markdown(
            sub_card(
                "Items",
                fmt_count(svc_data.get("items", 0)),
                change_html(svc_data.get("items", 0), prev_svc.get("items")),
                sub_bg,
            ),
            unsafe_allow_html=True,
        )
    with ss2:
        st.markdown(
            sub_card(
                "Revenue",
                fmt_dhs_sub(svc_data.get("revenue", 0)),
                change_html(svc_data.get("revenue", 0), prev_svc.get("revenue")),
                sub_bg,
            ),
            unsafe_allow_html=True,
        )

    safe_svc = _safe_key(sel_svc)
    svc_items_key = f"svc_{safe_svc}_items"
    _, svc_center, _ = st.columns([1, 18, 1])
    with svc_center:
        render_trend_chart_v2(
            f"oc_svc_{safe_svc}_items",
            ops_data,
            window,
            available_periods,
            METRIC_CONFIG[svc_items_key],
            hdr,
            show_title=True,
            height=300,
        )

    st.markdown("---")
    render_section_heading("Express & Efficiency", hdr)

    express_val = ops_cur.get("express_share", 0) or 0
    prev_express = (ops_prev.get("express_share", 0) or 0) if ops_prev else None
    _, ex_center, _ = st.columns([3, 2, 3])
    with ex_center:
        st.markdown(
            sub_card(
                "Express Order Share",
                fmt_pct(express_val),
                change_html(express_val, prev_express),
                sub_bg,
            ),
            unsafe_allow_html=True,
        )

    pe1, pe2 = st.columns(2)
    with pe1:
        st.markdown(
            sub_card(
                "Avg Processing Time",
                fmt_days(ops_cur.get("ops_avg_processing_time", 0)),
                change_html(
                    ops_cur.get("ops_avg_processing_time", 0),
                    ops_prev.get("ops_avg_processing_time", 0) if ops_prev else None,
                ),
                sub_bg,
            ),
            unsafe_allow_html=True,
        )
    with pe2:
        st.markdown(
            sub_card(
                "Avg Time In Store",
                fmt_days(ops_cur.get("ops_avg_time_in_store", 0)),
                change_html(
                    ops_cur.get("ops_avg_time_in_store", 0),
                    ops_prev.get("ops_avg_time_in_store", 0) if ops_prev else None,
                ),
                sub_bg,
            ),
            unsafe_allow_html=True,
        )

    render_footer()
