"""
Financial Performance — Collections, Payment Cycle, Concentration, Outstanding.

Tabs: Collections | Payment Cycle | Concentration | Outstanding
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard_shared import (
    COLORS,
    change_html,
    compute_fetch_periods,
    dirham_html,
    fmt_count,
    fmt_days,
    fmt_dhs,
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
    sub_card,
)
from section_data import fetch_operations_batch, fetch_outstanding, fetch_pareto_data, fetch_payments_batch

inject_global_styles()
con = get_connection()

hdr = COLORS["payments"]["header"]
sub_bg = COLORS["payments"]["sub"]

render_page_title("Financial Performance", hdr)
selected_period, available_periods = period_selector(con, show_title=False)
window, fetch_periods = compute_fetch_periods(selected_period, available_periods)

pm_data = fetch_payments_batch(con, tuple(fetch_periods))
ops_data = fetch_operations_batch(con, tuple(fetch_periods))

pm_cur = pm_data.get(selected_period, {})
pm_idx = available_periods.index(selected_period)
pm_prev = pm_data.get(available_periods[pm_idx - 1]) if pm_idx > 0 else None

ops_cur = ops_data.get(selected_period, {})
ops_prev = ops_data.get(available_periods[pm_idx - 1]) if pm_idx > 0 else None

tab1, tab2, tab3, tab4 = st.tabs(["💳 Collections", "⏱️ Payment Cycle", "📊 Concentration", "📋 Outstanding"])


def _pm_chg(key):
    return change_html(pm_cur.get(key, 0), pm_prev.get(key, 0) if pm_prev else None)


# ─── COLLECTIONS TAB ─────────────────────────────────────────────────
with tab1:
    h1, h2 = st.columns(2)
    with h1:
        st.markdown(
            headline_card(
                "Revenue",
                fmt_dhs(pm_cur.get("pm_revenue", 0)),
                _pm_chg("pm_revenue"),
                hdr,
            ),
            unsafe_allow_html=True,
        )
    with h2:
        st.markdown(
            headline_card(
                "Total Collections",
                fmt_dhs(pm_cur.get("pm_total_collections", 0)),
                _pm_chg("pm_total_collections"),
                hdr,
            ),
            unsafe_allow_html=True,
        )

    render_section_heading("Collection Methods", hdr)
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(
            sub_card("Stripe", fmt_dhs_sub(pm_cur.get("pm_stripe", 0)), _pm_chg("pm_stripe"), sub_bg),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            sub_card("Terminal", fmt_dhs_sub(pm_cur.get("pm_terminal", 0)), _pm_chg("pm_terminal"), sub_bg),
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            sub_card("Cash", fmt_dhs_sub(pm_cur.get("pm_cash", 0)), _pm_chg("pm_cash"), sub_bg),
            unsafe_allow_html=True,
        )

    # Collection rate sub-card
    rev = pm_cur.get("pm_revenue", 0) or 0
    coll = pm_cur.get("pm_total_collections", 0) or 0
    coll_rate = coll / rev if rev > 0 else 0
    prev_rev = (pm_prev.get("pm_revenue", 0) or 0) if pm_prev else 0
    prev_coll = (pm_prev.get("pm_total_collections", 0) or 0) if pm_prev else 0
    prev_coll_rate = prev_coll / prev_rev if prev_rev > 0 else None

    st.markdown("")
    _, cr_center, _ = st.columns([3, 2, 3])
    with cr_center:
        st.markdown(
            sub_card(
                "Collection Rate",
                fmt_pct(coll_rate),
                change_html(coll_rate, prev_coll_rate),
                sub_bg,
            ),
            unsafe_allow_html=True,
        )

    st.markdown("")
    render_metric_selector(
        [
            ("Collections", "pm_total_collections"),
            ("Stripe", "pm_stripe"),
            ("Terminal", "pm_terminal"),
        ],
        pm_data,
        window,
        available_periods,
        selected_period,
        "fp_collections",
        hdr,
    )

    render_footer()

    with st.expander("Download Data"):
        df_export = pd.DataFrame([{"period": p, **v} for p, v in pm_data.items() if p in window])
        st.download_button(
            "Download CSV",
            df_export.to_csv(index=False).encode("utf-8"),
            f"fp_collections_{selected_period}.csv",
            "text/csv",
        )

# ─── PAYMENT CYCLE TAB ───────────────────────────────────────────────
with tab2:
    st.info("Avg Days to Payment is based on CC_2025 orders only.")

    pc1, pc2, pc3 = st.columns(3)
    with pc1:
        st.markdown(
            sub_card(
                "Avg Days to Payment",
                fmt_days(pm_cur.get("pm_avg_days_to_payment", 0)),
                _pm_chg("pm_avg_days_to_payment"),
                sub_bg,
            ),
            unsafe_allow_html=True,
        )
    with pc2:
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
    with pc3:
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

    st.markdown("")
    render_metric_selector(
        [("Avg Days to Payment", "pm_avg_days_to_payment")],
        pm_data,
        window,
        available_periods,
        selected_period,
        "fp_payment_cycle",
        hdr,
    )

    render_footer()

# ─── CONCENTRATION TAB ───────────────────────────────────────────────
with tab3:
    if is_weekly(selected_period):
        st.info("This view is available in monthly mode only.")
    else:
        pareto_df = fetch_pareto_data(con, selected_period)
        if len(pareto_df) == 0:
            st.info("No revenue data for this period.")
        else:
            pareto_df = pareto_df.copy().reset_index(drop=True)
            total_rev = pareto_df["revenue"].sum()
            pareto_df["cumulative_pct"] = pareto_df["revenue"].cumsum() / total_rev * 100

            # Dual-axis chart
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=list(range(1, len(pareto_df) + 1)),
                    y=pareto_df["revenue"].tolist(),
                    name="Revenue",
                    marker_color=hdr,
                    hovertemplate="Customer #%{x}<br>Revenue: Dhs %{y:,.0f}<extra></extra>",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=list(range(1, len(pareto_df) + 1)),
                    y=pareto_df["cumulative_pct"].tolist(),
                    name="Cumulative %",
                    yaxis="y2",
                    mode="lines",
                    line=dict(color="#FF8F00", width=2),
                    hovertemplate="Customer #%{x}: %{y:.1f}% cumulative<extra></extra>",
                )
            )
            fig.update_layout(
                height=380,
                margin=dict(t=30, b=40, l=60, r=60),
                paper_bgcolor="#fff",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(
                    title="Customer Rank",
                    showgrid=False,
                    fixedrange=True,
                ),
                yaxis=dict(
                    title="Revenue (Dhs)",
                    showgrid=True,
                    gridcolor="rgba(0,0,0,0.06)",
                    fixedrange=True,
                ),
                yaxis2=dict(
                    title="Cumulative %",
                    overlaying="y",
                    side="right",
                    range=[0, 105],
                    ticksuffix="%",
                    fixedrange=True,
                    showgrid=False,
                ),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                dragmode=False,
            )
            st.plotly_chart(fig, key="fp_pareto_chart", use_container_width=True, config={"displayModeBar": False})

            # Top 20 table
            top20 = pareto_df.head(20).copy()
            top20.index = range(1, len(top20) + 1)
            top20["revenue"] = top20["revenue"].apply(lambda v: f"Dhs {v:,.0f}")
            top20["cumulative_pct"] = top20["cumulative_pct"].apply(lambda v: f"{v:.1f}%")
            st.dataframe(
                top20[["CustomerName", "revenue", "cumulative_pct"]].rename(
                    columns={
                        "CustomerName": "Customer",
                        "revenue": "Revenue",
                        "cumulative_pct": "Cumulative %",
                    }
                ),
                use_container_width=True,
            )

        render_footer()

        with st.expander("Download Data"):
            if len(pareto_df) > 0:
                st.download_button(
                    "Download CSV",
                    pareto_df.to_csv(index=False).encode("utf-8"),
                    f"fp_concentration_{selected_period}.csv",
                    "text/csv",
                )

# ─── OUTSTANDING TAB ─────────────────────────────────────────────────
with tab4:
    outstanding = fetch_outstanding(con)
    order_count = outstanding.get("order_count", 0)

    if order_count == 0:
        st.success("No outstanding orders.")
    else:
        total_out = outstanding.get("total_outstanding", 0)
        st.markdown(
            f"<div style='font-size:1.4rem;font-weight:700;margin-bottom:0.25rem;'>"
            f"{dirham_html(total_out, size=28)} outstanding</div>"
            f"<div style='color:#888;font-size:0.85rem;margin-bottom:1rem;'>{order_count:,} orders</div>",
            unsafe_allow_html=True,
        )
        st.caption("CC_2025 orders with Paid = FALSE. Legacy orders excluded (assumed paid).")

        # Aging buckets
        aging_df = outstanding.get("aging")
        if aging_df is not None and len(aging_df) > 0:
            st.markdown("")
            render_section_heading("Aging Buckets", hdr)
            bucket_cols = st.columns(len(aging_df))
            for i, row in aging_df.iterrows():
                with bucket_cols[i]:
                    st.markdown(
                        sub_card(
                            row["bucket"],
                            f"Dhs {row['amount']:,.0f}",
                            f'<span style="font-size:0.75rem;color:#555;">{int(row["orders"])} orders</span>',
                            sub_bg,
                        ),
                        unsafe_allow_html=True,
                    )

        # Top 20 outstanding orders
        top20_df = outstanding.get("top20")
        if top20_df is not None and len(top20_df) > 0:
            st.markdown("")
            render_section_heading("Longest Outstanding Orders", hdr)
            display_df = top20_df[
                ["CustomerName", "OrderID_Std", "Placed_Date", "Total_Num", "days_outstanding"]
            ].copy()
            display_df["Total_Num"] = display_df["Total_Num"].apply(lambda v: f"Dhs {v:,.0f}")
            st.dataframe(
                display_df.rename(
                    columns={
                        "CustomerName": "Customer",
                        "OrderID_Std": "Order",
                        "Placed_Date": "Placed",
                        "Total_Num": "Amount",
                        "days_outstanding": "Days Outstanding",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

    render_footer()

    if order_count > 0:
        with st.expander("Download Data"):
            top20_df = outstanding.get("top20")
            if top20_df is not None:
                st.download_button(
                    "Download CSV",
                    top20_df.to_csv(index=False).encode("utf-8"),
                    "fp_outstanding.csv",
                    "text/csv",
                )
