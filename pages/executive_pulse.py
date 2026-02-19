"""
Executive Pulse — High-level business snapshot with YoY and insights.

Tabs: Snapshot | Trends | Insights
"""

import pandas as pd
import streamlit as st

from dashboard_shared import (
    COLORS,
    METRIC_CONFIG,
    activate_tab_from_url,
    change_html,
    compute_fetch_periods,
    dirham_html,
    fmt_count,
    fmt_dirham,
    fmt_pct,
    get_connection,
    headline_card_with_subs,
    inject_global_styles,
    is_weekly,
    period_selector,
    render_footer,
    render_trend_chart_v3,
    sub_card,
)
from section_data import fetch_yoy_batch
from dashboard_shared import fetch_measures_batch

inject_global_styles()
con = get_connection()

hdr = COLORS["revenues"]["header"]
sub_bg = COLORS["revenues"]["sub"]
cust_hdr = COLORS["customers"]["header"]
item_hdr = COLORS["items"]["header"]
stop_hdr = COLORS["stops"]["header"]

selected_period, available_periods = period_selector(con, show_title=True)
window, fetch_periods = compute_fetch_periods(selected_period, available_periods)

_EP_TABS = ["Snapshot", "Trends", "Insights"]
tab1, tab2, tab3 = st.tabs(["📊 Snapshot", "📈 Trends", "💡 Insights"])
activate_tab_from_url(_EP_TABS)

# ─── SNAPSHOT TAB ────────────────────────────────────────────────────
with tab1:
    _, _dl_col, _ = st.columns([1, 18, 1])
    with _dl_col:
        if st.button("Download Monthly Report (PDF)", key="ep_pdf_btn", type="secondary"):
            from generate_report import generate_monthly_report

            pdf_bytes = generate_monthly_report(con, selected_period, available_periods)
            st.download_button(
                label="Save PDF",
                data=pdf_bytes,
                file_name=f"moonwalk-report-{selected_period}.pdf",
                mime="application/pdf",
                key="ep_pdf_dl",
            )

    trend_data = fetch_measures_batch(con, tuple(fetch_periods))
    yoy_data = fetch_yoy_batch(con, tuple(fetch_periods))

    cur = trend_data.get(selected_period, {})
    p_idx = available_periods.index(selected_period)
    prev = trend_data.get(available_periods[p_idx - 1], {}) if p_idx > 0 else {}
    yoy_cur = yoy_data.get(selected_period, {})

    def _sub_row(label, key, cur_dict, prev_dict, yoy_dict):
        val = cur_dict.get(key, 0)
        prev_val = prev_dict.get(key)
        yoy_val = yoy_dict.get(key)
        cfg = METRIC_CONFIG.get(key, {})
        is_cur = cfg.get("is_currency", False)
        is_pct = cfg.get("is_percentage", False)
        if is_cur:
            fmted = fmt_dirham(val)
        elif is_pct:
            fmted = fmt_pct(val)
        else:
            fmted = fmt_count(val)
        mom = change_html(val, prev_val, size="compact")
        yoy = (
            change_html(val, yoy_val, size="compact")
            if yoy_val
            else '<span style="color:#aaa;font-size:0.65rem;">—</span>'
        )
        return (label, fmted + " " + mom, f"YoY: {yoy}")

    cols = st.columns(4)

    # Customers card
    with cols[0]:
        val = cur.get("customers", 0)
        subs = [
            _sub_row("Clients", "clients", cur, prev, yoy_cur),
            _sub_row("Subscribers", "subscribers", cur, prev, yoy_cur),
            (
                "vs Prior Year",
                fmt_count(yoy_cur.get("customers", 0)) if yoy_cur.get("customers") else "—",
                change_html(val, yoy_cur.get("customers"), size="compact"),
            ),
        ]
        st.markdown(
            headline_card_with_subs(
                "Customers",
                fmt_count(val),
                change_html(val, prev.get("customers")),
                cust_hdr,
                subs,
            ),
            unsafe_allow_html=True,
        )

    # Items card
    with cols[1]:
        val = cur.get("items", 0)
        subs = [
            _sub_row("Client Items", "items_client", cur, prev, yoy_cur),
            _sub_row("Sub Items", "items_sub", cur, prev, yoy_cur),
            (
                "vs Prior Year",
                fmt_count(yoy_cur.get("items", 0)) if yoy_cur.get("items") else "—",
                change_html(val, yoy_cur.get("items"), size="compact"),
            ),
        ]
        st.markdown(
            headline_card_with_subs(
                "Items",
                fmt_count(val),
                change_html(val, prev.get("items")),
                item_hdr,
                subs,
            ),
            unsafe_allow_html=True,
        )

    # Revenue card
    with cols[2]:
        val = cur.get("revenues", 0)
        subs = [
            _sub_row("Client Revenue", "rev_client", cur, prev, yoy_cur),
            _sub_row("Sub Revenue", "rev_sub", cur, prev, yoy_cur),
            (
                "vs Prior Year",
                dirham_html(yoy_cur.get("revenues", 0), size=14) if yoy_cur.get("revenues") else "—",
                change_html(val, yoy_cur.get("revenues"), size="compact"),
            ),
        ]
        st.markdown(
            headline_card_with_subs(
                "Revenue",
                dirham_html(val),
                change_html(val, prev.get("revenues")),
                hdr,
                subs,
            ),
            unsafe_allow_html=True,
        )

    # Stops card
    with cols[3]:
        val = cur.get("stops", 0)
        subs = [
            _sub_row("Deliveries", "deliveries", cur, prev, yoy_cur),
            _sub_row("Pickups", "pickups", cur, prev, yoy_cur),
            (
                "vs Prior Year",
                fmt_count(yoy_cur.get("stops", 0)) if yoy_cur.get("stops") else "—",
                change_html(val, yoy_cur.get("stops"), size="compact"),
            ),
        ]
        st.markdown(
            headline_card_with_subs(
                "Stops",
                fmt_count(val),
                change_html(val, prev.get("stops")),
                stop_hdr,
                subs,
            ),
            unsafe_allow_html=True,
        )

    st.markdown("")

    # AOV sub-card (centered)
    rev = cur.get("revenues", 0)
    items_total = cur.get("items", 0)
    aov = rev / items_total if items_total > 0 else 0
    prev_rev = prev.get("revenues", 0)
    prev_items = prev.get("items", 0)
    prev_aov = prev_rev / prev_items if prev_items > 0 else 0
    _, c, _ = st.columns([3, 2, 3])
    with c:
        st.markdown(
            sub_card(
                "Avg Order Value (Rev / Items)",
                dirham_html(aov, size=22),
                change_html(aov, prev_aov),
                sub_bg,
            ),
            unsafe_allow_html=True,
        )

    render_footer()

    with st.expander("Download Data"):
        df_export = pd.DataFrame([{"period": p, **v} for p, v in trend_data.items() if p in window])
        st.download_button(
            "Download CSV",
            df_export.to_csv(index=False).encode("utf-8"),
            f"executive_pulse_snapshot_{selected_period}.csv",
            "text/csv",
        )

# ─── TRENDS TAB ──────────────────────────────────────────────────────
with tab2:
    trend_data = fetch_measures_batch(con, tuple(fetch_periods))
    yoy_trend = fetch_yoy_batch(con, tuple(fetch_periods))

    row1 = st.columns(2)
    row2 = st.columns(2)

    with row1[0]:
        render_trend_chart_v3(
            "ep_customers",
            trend_data,
            window,
            available_periods,
            METRIC_CONFIG["customers"],
            cust_hdr,
            show_title=True,
            height=380,
            yoy_data=yoy_trend,
            moving_avg_periods=3,
        )
    with row1[1]:
        render_trend_chart_v3(
            "ep_items",
            trend_data,
            window,
            available_periods,
            METRIC_CONFIG["items"],
            item_hdr,
            show_title=True,
            height=380,
            yoy_data=yoy_trend,
            moving_avg_periods=3,
        )
    with row2[0]:
        render_trend_chart_v3(
            "ep_revenues",
            trend_data,
            window,
            available_periods,
            METRIC_CONFIG["revenues"],
            hdr,
            show_title=True,
            height=380,
            yoy_data=yoy_trend,
            moving_avg_periods=3,
        )
    with row2[1]:
        render_trend_chart_v3(
            "ep_stops",
            trend_data,
            window,
            available_periods,
            METRIC_CONFIG["stops"],
            stop_hdr,
            show_title=True,
            height=380,
            yoy_data=yoy_trend,
            moving_avg_periods=3,
        )

    render_footer()

    with st.expander("Download Data"):
        df_export = pd.DataFrame([{"period": p, **v} for p, v in trend_data.items() if p in window])
        st.download_button(
            "Download CSV",
            df_export.to_csv(index=False).encode("utf-8"),
            f"executive_pulse_trends_{selected_period}.csv",
            "text/csv",
        )

# ─── INSIGHTS TAB ────────────────────────────────────────────────────
with tab3:
    try:
        insights_df = con.execute("SELECT * FROM insights ORDER BY category, rule_id").df()
    except Exception:
        insights_df = None
    if insights_df is None:
        st.info("Run `python cleancloud_to_duckdb.py` to build the insights table.")
    else:
        if len(insights_df) == 0:
            st.info("No insights generated yet. Check your data and re-run DuckDB ETL.")
        else:
            DOT = {"positive": "🟢", "negative": "🔴", "neutral": "🟡"}
            categories = insights_df["category"].unique()
            for cat in categories:
                st.subheader(cat.title())
                cat_df = insights_df[insights_df["category"] == cat]
                for _, row in cat_df.iterrows():
                    dot = DOT.get(row["sentiment"], ":white_circle:")
                    st.markdown(f"{dot} **{row['headline']}**  \n{row['detail']}")
                st.markdown("")

    render_footer()
