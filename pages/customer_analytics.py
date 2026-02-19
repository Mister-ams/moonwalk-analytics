"""
Customer Analytics — Acquisition, Segmentation, Cohort, Per-Customer.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from customer_report_shared import fetch_customer_measures_batch, fetch_new_customer_detail_batch
from dashboard_shared import (
    COLORS,
    METRIC_CONFIG,
    change_html,
    compute_fetch_periods,
    dirham_html,
    fmt_count,
    fmt_dirham,
    fmt_pct,
    fmt_ratio,
    get_connection,
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
from section_data import (
    compute_cohort_retention,
    fetch_clv_estimate,
    fetch_customer_insights_batch,
    fetch_extended_cohort_batch,
    fetch_reactivation_batch,
    fetch_retention_heatmap,
    fetch_rfm_snapshot,
)

inject_global_styles()
con = get_connection()

hdr = COLORS["customers"]["header"]
sub_bg = COLORS["customers"]["sub"]

render_page_title("Customer Analytics", hdr)
selected_period, available_periods = period_selector(con, show_title=False)
window, fetch_periods = compute_fetch_periods(selected_period, available_periods)

tab1, tab2, tab3, tab4 = st.tabs(["👥 Acquisition", "🎯 Segmentation", "📅 Cohort", "📊 Per-Customer"])

# ─── ACQUISITION TAB ─────────────────────────────────────────────────
with tab1:
    if is_weekly(selected_period):
        st.info("This view is available in monthly mode only.")
    else:
        new_data = fetch_new_customer_detail_batch(con, tuple(fetch_periods))
        react_data = fetch_reactivation_batch(con, tuple(fetch_periods))

        # Merge reactivation into new_data
        for m in new_data:
            new_data[m]["reactivated_customers"] = react_data.get(m, {}).get("reactivated_customers", 0)

        cur = new_data.get(selected_period, {})
        p_idx = available_periods.index(selected_period)
        prev = new_data.get(available_periods[p_idx - 1], {}) if p_idx > 0 else {}

        cols = st.columns(3)
        for i, (label, key) in enumerate(
            [
                ("New Customers", "new_customers"),
                ("Existing Customers", "existing_customers"),
                ("Reactivated", "reactivated_customers"),
            ]
        ):
            val = cur.get(key, 0)
            prev_val = prev.get(key)
            with cols[i]:
                st.markdown(
                    sub_card(
                        label,
                        fmt_count(val),
                        change_html(val, prev_val),
                        sub_bg,
                    ),
                    unsafe_allow_html=True,
                )

        st.markdown("")
        render_section_heading("Items Split", hdr)
        render_metric_selector(
            [("New Customer Items", "new_items"), ("Existing Customer Items", "existing_items")],
            new_data,
            window,
            available_periods,
            selected_period,
            "ca_acq_items",
            hdr,
        )
        render_section_heading("Revenue Split", hdr)
        render_metric_selector(
            [("New Customer Revenue", "new_revenue"), ("Existing Customer Revenue", "existing_revenue")],
            new_data,
            window,
            available_periods,
            selected_period,
            "ca_acq_revenue",
            hdr,
        )

        render_footer()

        with st.expander("Download Data"):
            df_export = pd.DataFrame([{"period": p, **v} for p, v in new_data.items() if p in window])
            st.download_button(
                "Download CSV",
                df_export.to_csv(index=False).encode("utf-8"),
                f"ca_acquisition_{selected_period}.csv",
                "text/csv",
            )

# ─── SEGMENTATION TAB ────────────────────────────────────────────────
with tab2:
    if is_weekly(selected_period):
        st.info("This view is available in monthly mode only.")
    else:
        ci_data = fetch_customer_insights_batch(con, tuple(fetch_periods))
        cur = ci_data.get(selected_period, {})
        p_idx = available_periods.index(selected_period)
        prev_ci = ci_data.get(available_periods[p_idx - 1], {}) if p_idx > 0 else {}

        # Active / Multi-service row
        cols = st.columns(2)
        for i, (label, key) in enumerate(
            [
                ("Active Customers", "ci_active_customers"),
                ("Multi-Service Customers", "ci_multi_service"),
            ]
        ):
            val = cur.get(key, 0)
            with cols[i]:
                st.markdown(
                    sub_card(
                        label,
                        fmt_count(val),
                        change_html(val, prev_ci.get(key)),
                        sub_bg,
                    ),
                    unsafe_allow_html=True,
                )

        st.markdown("")
        # Top 20% analysis
        cols2 = st.columns(2)
        for i, (label, rev_key, thresh_key, share_key) in enumerate(
            [
                ("Top 20% by Spend", "ci_top20_spend_rev", "ci_spend_threshold", "ci_spend_share"),
                ("Top 20% by Volume", "ci_top20_vol_rev", "ci_volume_threshold", "ci_volume_share"),
            ]
        ):
            rev = cur.get(rev_key, 0)
            thresh = cur.get(thresh_key, 0)
            share = cur.get(share_key, 0)
            with cols2[i]:
                st.markdown(
                    sub_card(
                        label,
                        dirham_html(rev, size=20),
                        f'<span style="font-size:0.75rem;color:#555;">Share: {fmt_pct(share)} | Threshold: {fmt_dirham(thresh)}</span>',
                        sub_bg,
                    ),
                    unsafe_allow_html=True,
                )

        st.markdown("---")
        render_section_heading("RFM Segmentation", hdr)

        rfm_df = fetch_rfm_snapshot(con, selected_period)
        if len(rfm_df) < 5:
            st.info("Not enough data for RFM segmentation this period.")
        else:
            # Compute quintiles
            rfm_df = rfm_df.copy()
            rfm_df["r_score"] = pd.qcut(rfm_df["recency"], 5, labels=[5, 4, 3, 2, 1]).astype(int)
            rfm_df["f_score"] = pd.qcut(rfm_df["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]).astype(int)
            rfm_df["m_score"] = pd.qcut(rfm_df["monetary"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]).astype(int)
            rfm_df["rfm"] = (
                rfm_df["r_score"].astype(str) + rfm_df["f_score"].astype(str) + rfm_df["m_score"].astype(str)
            )

            def _segment(row):
                r, f = row["r_score"], row["f_score"]
                if r >= 4 and f >= 4:
                    return "Champions"
                elif r >= 3 and f >= 3:
                    return "Loyal"
                elif r >= 4 and f <= 2:
                    return "Recent"
                elif r <= 2 and f >= 3:
                    return "At Risk"
                elif r >= 3 and f <= 2:
                    return "Frequent"
                else:
                    return "Other"

            rfm_df["segment"] = rfm_df.apply(_segment, axis=1)
            seg_counts = (
                rfm_df.groupby("segment")
                .agg(customers=("CustomerID_Std", "count"), revenue=("monetary", "sum"))
                .reset_index()
                .sort_values("customers", ascending=True)
            )

            fig = go.Figure(
                go.Bar(
                    x=seg_counts["customers"],
                    y=seg_counts["segment"],
                    orientation="h",
                    marker_color=hdr,
                    text=[f"{c:,}" for c in seg_counts["customers"]],
                    textposition="outside",
                    hovertemplate="<b>%{y}</b><br>Customers: %{x}<extra></extra>",
                )
            )
            fig.update_layout(
                height=280,
                margin=dict(t=20, b=20, l=120, r=60),
                paper_bgcolor="#fff",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.06)", fixedrange=True),
                yaxis=dict(fixedrange=True),
                dragmode=False,
            )
            st.plotly_chart(fig, key="ca_rfm_bar", use_container_width=True, config={"displayModeBar": False})

            seg_table = seg_counts[["segment", "customers", "revenue"]].sort_values("revenue", ascending=False).copy()
            seg_table["revenue"] = seg_table["revenue"].apply(lambda v: f"Dhs {v:,.0f}")
            st.dataframe(
                seg_table.rename(columns={"segment": "Segment", "customers": "Customers", "revenue": "Revenue"}),
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("---")
        render_section_heading("Customer Lifetime Value (Simple)", hdr)
        clv = fetch_clv_estimate(con)
        clv_cols = st.columns(3)
        for i, (label, val) in enumerate(
            [
                ("Avg Monthly Revenue / Customer", clv["avg_monthly_rev"]),
                ("Avg Active Lifespan (months)", clv["avg_lifespan"]),
                ("Simple CLV", clv["simple_clv"]),
            ]
        ):
            with clv_cols[i]:
                is_rev = i != 1
                fmted = dirham_html(val, size=22) if is_rev else fmt_ratio(val)
                st.markdown(sub_card(label, fmted, "", sub_bg), unsafe_allow_html=True)

        render_footer()

# ─── COHORT TAB ──────────────────────────────────────────────────────
with tab3:
    if is_weekly(selected_period):
        st.info("This view is available in monthly mode only.")
    else:
        cohort_data = fetch_extended_cohort_batch(con, tuple(fetch_periods))
        heatmap_df = fetch_retention_heatmap(con)

        cur = cohort_data.get(selected_period, {})
        p_idx = available_periods.index(selected_period)
        prev_c = cohort_data.get(available_periods[p_idx - 1], {}) if p_idx > 0 else {}

        # M0/M1 sections with retention
        for cm in (0, 1):
            prefix = f"m{cm}"
            render_section_heading(f"M{cm} — {'New' if cm == 0 else 'Returning'} Customers", hdr)
            render_metric_selector(
                [
                    (f"M{cm} Customers", f"{prefix}_customers"),
                    (f"M{cm} Items", f"{prefix}_items"),
                    (f"M{cm} Revenue", f"{prefix}_revenue"),
                ],
                cohort_data,
                window,
                available_periods,
                selected_period,
                f"ca_cohort_m{cm}",
                hdr,
            )
            if cm == 1:
                # Retention sub-cards
                ret = compute_cohort_retention(cohort_data, list(available_periods), selected_period)
                r_cols = st.columns(3)
                for i, (label, key) in enumerate(
                    [
                        ("M1 Customer Retention", "customer_retention"),
                        ("M1 Item Retention", "item_retention"),
                        ("M1 Revenue Retention", "revenue_retention"),
                    ]
                ):
                    val = ret.get(key)
                    with r_cols[i]:
                        fmted = fmt_pct(val) if val is not None else "—"
                        st.markdown(sub_card(label, fmted, "", sub_bg), unsafe_allow_html=True)

        # M2 / M3
        for cm in (2, 3):
            prefix = f"m{cm}"
            render_section_heading(f"M{cm} Cohort", hdr)
            render_metric_selector(
                [
                    (f"M{cm} Customers", f"{prefix}_customers"),
                    (f"M{cm} Items", f"{prefix}_items"),
                    (f"M{cm} Revenue", f"{prefix}_revenue"),
                ],
                cohort_data,
                window,
                available_periods,
                selected_period,
                f"ca_cohort_m{cm}",
                hdr,
            )

        # Retention heatmap
        st.markdown("---")
        render_section_heading("Retention Heatmap (All-Time)", hdr)
        if len(heatmap_df) == 0:
            st.info("No cohort data available for heatmap.")
        else:
            heatmap_df["CohortMonth"] = pd.to_datetime(heatmap_df["CohortMonth"]).dt.strftime("%Y-%m")
            pivot = heatmap_df.pivot_table(index="CohortMonth", columns="month_num", values="customers", fill_value=0)
            m0 = pivot.get(0, pd.Series(1, index=pivot.index))
            retention_pct = pivot.div(m0.replace(0, 1), axis=0)

            cols_list = sorted(pivot.columns.tolist())
            rows_list = pivot.index.tolist()
            z = [[retention_pct.get(c, pd.Series([0])).get(r, 0) for c in cols_list] for r in rows_list]
            text = [[f"{v * 100:.0f}%" for v in row] for row in z]

            fig = go.Figure(
                go.Heatmap(
                    z=z,
                    x=[f"M{c}" for c in cols_list],
                    y=rows_list,
                    text=text,
                    texttemplate="%{text}",
                    colorscale="RdYlGn",
                    zmin=0,
                    zmax=1,
                    hovertemplate="Cohort: %{y}<br>%{x}: %{text}<extra></extra>",
                )
            )
            h = max(300, len(rows_list) * 28 + 100)
            fig.update_layout(
                height=h,
                margin=dict(t=30, b=40, l=80, r=30),
                paper_bgcolor="#fff",
                yaxis=dict(autorange="reversed", fixedrange=True),
                xaxis=dict(fixedrange=True),
                dragmode=False,
            )
            st.plotly_chart(fig, key="ca_heatmap", use_container_width=True, config={"displayModeBar": False})

        render_footer()

        with st.expander("Download Data"):
            df_export = pd.DataFrame([{"period": p, **v} for p, v in cohort_data.items() if p in window])
            st.download_button(
                "Download CSV",
                df_export.to_csv(index=False).encode("utf-8"),
                f"ca_cohort_{selected_period}.csv",
                "text/csv",
            )

# ─── PER-CUSTOMER TAB ────────────────────────────────────────────────
with tab4:
    cm_data = fetch_customer_measures_batch(con, tuple(fetch_periods))

    render_section_heading("Items per Customer", hdr)
    render_metric_selector(
        [("Items per Client", "client_items_per_customer"), ("Items per Subscriber", "sub_items_per_customer")],
        cm_data,
        window,
        available_periods,
        selected_period,
        "ca_per_cust_items",
        hdr,
    )
    render_section_heading("Revenue per Customer", hdr)
    render_metric_selector(
        [("Revenue per Client", "rev_per_client"), ("Revenue per Subscriber", "rev_per_subscriber")],
        cm_data,
        window,
        available_periods,
        selected_period,
        "ca_per_cust_rev",
        hdr,
    )

    render_footer()

    with st.expander("Download Data"):
        df_export = pd.DataFrame([{"period": p, **v} for p, v in cm_data.items() if p in window])
        st.download_button(
            "Download CSV",
            df_export.to_csv(index=False).encode("utf-8"),
            f"ca_per_customer_{selected_period}.csv",
            "text/csv",
        )
