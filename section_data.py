"""
Shared data layer for new dashboard sections (02-06).

Provides cached batch SQL fetch functions for:
- Customer Insights (02) — active/multi-service customers, top 20% analysis
- Cohort Analysis (03) — M0/M1 customers, items, revenue, retention
- Logistics (04) — stops, deliveries, pickups, geographic split
- Operations (05) — items/revenue by category and service type
- Payments (06) — collections by method, processing metrics
"""

import streamlit as st
from dashboard_shared import get_grain_context


# =====================================================================
# 02 — CUSTOMER INSIGHTS
# =====================================================================

@st.cache_data(ttl=300)
def fetch_customer_insights_batch(_con, months_tuple):
    """Fetch active customers, multi-service, and top-20% analysis."""
    months = list(months_tuple)
    placeholders = ", ".join(f"'{m}'" for m in months)

    # Query 1: Active customers + multi-service count
    cust_df = _con.execute(f"""
        SELECT p.YearMonth,
            COUNT(DISTINCT cq.CustomerID_Std) AS active_customers,
            COUNT(DISTINCT CASE WHEN cq.Is_Multi_Service = 1
                  THEN cq.CustomerID_Std END) AS multi_service
        FROM customer_quality cq
        JOIN dim_period p ON cq.OrderCohortMonth = p.Date
        WHERE p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth
    """).df()

    # Query 2: Top 20% by spend and volume
    top20_df = _con.execute(f"""
        WITH thresholds AS (
            SELECT p.YearMonth,
                PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY cq.Monthly_Revenue) AS p80_rev,
                PERCENTILE_CONT(0.8) WITHIN GROUP (ORDER BY cq.Monthly_Items) AS p80_items
            FROM customer_quality cq
            JOIN dim_period p ON cq.OrderCohortMonth = p.Date
            WHERE p.YearMonth IN ({placeholders})
            GROUP BY p.YearMonth
        )
        SELECT p.YearMonth,
            t.p80_rev AS spend_threshold,
            SUM(CASE WHEN cq.Monthly_Revenue >= t.p80_rev THEN cq.Monthly_Revenue ELSE 0 END) AS top20_spend_rev,
            SUM(cq.Monthly_Revenue) AS total_rev,
            t.p80_items AS volume_threshold,
            SUM(CASE WHEN cq.Monthly_Items >= t.p80_items THEN cq.Monthly_Revenue ELSE 0 END) AS top20_vol_rev
        FROM customer_quality cq
        JOIN dim_period p ON cq.OrderCohortMonth = p.Date
        JOIN thresholds t ON p.YearMonth = t.YearMonth
        WHERE p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth, t.p80_rev, t.p80_items
    """).df()

    result = {}
    for m in months:
        c_row = cust_df[cust_df["YearMonth"] == m]
        active = int(c_row["active_customers"].iloc[0]) if len(c_row) else 0
        multi = int(c_row["multi_service"].iloc[0]) if len(c_row) else 0

        t_row = top20_df[top20_df["YearMonth"] == m]
        spend_thresh = float(t_row["spend_threshold"].iloc[0]) if len(t_row) else 0.0
        top20_spend_rev = float(t_row["top20_spend_rev"].iloc[0]) if len(t_row) else 0.0
        total_rev = float(t_row["total_rev"].iloc[0]) if len(t_row) else 0.0
        vol_thresh = float(t_row["volume_threshold"].iloc[0]) if len(t_row) else 0.0
        top20_vol_rev = float(t_row["top20_vol_rev"].iloc[0]) if len(t_row) else 0.0

        result[m] = {
            "ci_active_customers": active,
            "ci_multi_service": multi,
            "ci_spend_threshold": spend_thresh,
            "ci_top20_spend_rev": top20_spend_rev,
            "ci_spend_share": top20_spend_rev / total_rev if total_rev > 0 else 0.0,
            "ci_volume_threshold": vol_thresh,
            "ci_top20_vol_rev": top20_vol_rev,
            "ci_volume_share": top20_vol_rev / total_rev if total_rev > 0 else 0.0,
        }
    return result


# =====================================================================
# 03 — COHORT ANALYSIS
# =====================================================================

@st.cache_data(ttl=300)
def fetch_cohort_batch(_con, months_tuple):
    """Fetch M0/M1 customer counts, items, revenue, and retention metrics."""
    months = list(months_tuple)
    placeholders = ", ".join(f"'{m}'" for m in months)

    # Query 1: M0/M1 customer counts + revenue
    sales_df = _con.execute(f"""
        SELECT p.YearMonth, CAST(s.MonthsSinceCohort AS INTEGER) AS cohort_month,
            COUNT(DISTINCT s.CustomerID_Std) AS customers,
            SUM(s.Total_Num) AS revenue
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL
          AND s.MonthsSinceCohort IN (0, 1)
          AND p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth, CAST(s.MonthsSinceCohort AS INTEGER)
    """).df()

    # Query 2: M0/M1 items
    items_df = _con.execute(f"""
        SELECT p.YearMonth, CAST(sc.MonthsSinceCohort AS INTEGER) AS cohort_month,
            SUM(i.Quantity) AS items
        FROM items i
        JOIN dim_period p ON i.ItemDate = p.Date
        LEFT JOIN (
            SELECT DISTINCT OrderID_Std, MonthsSinceCohort FROM sales
        ) sc ON i.OrderID_Std = sc.OrderID_Std
        WHERE sc.MonthsSinceCohort IN (0, 1)
          AND p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth, CAST(sc.MonthsSinceCohort AS INTEGER)
    """).df()

    result = {}
    for m in months:
        row = {}
        for cm in (0, 1):
            prefix = f"m{cm}"
            s_row = sales_df[(sales_df["YearMonth"] == m) & (sales_df["cohort_month"] == cm)]
            i_row = items_df[(items_df["YearMonth"] == m) & (items_df["cohort_month"] == cm)]

            customers = int(s_row["customers"].iloc[0]) if len(s_row) else 0
            revenue = float(s_row["revenue"].iloc[0]) if len(s_row) else 0.0
            items = int(i_row["items"].iloc[0]) if len(i_row) else 0

            row[f"{prefix}_customers"] = customers
            row[f"{prefix}_revenue"] = revenue
            row[f"{prefix}_items"] = items
            row[f"{prefix}_rev_per_customer"] = revenue / customers if customers > 0 else 0.0
            row[f"{prefix}_items_per_customer"] = items / customers if customers > 0 else 0.0

        result[m] = row
    return result


def compute_cohort_retention(trend_data, available_months, selected_month):
    """Compute M1 retention rates: M1[month] / M0[previous_month].

    Returns dict with customer_retention, item_retention, revenue_retention
    for the selected month, or None values if previous month unavailable.
    """
    idx = available_months.index(selected_month) if selected_month in available_months else -1
    if idx <= 0:
        return {"customer_retention": None, "item_retention": None, "revenue_retention": None}

    prev_month = available_months[idx - 1]
    cur = trend_data.get(selected_month, {})
    prev = trend_data.get(prev_month, {})

    m1_cust = cur.get("m1_customers", 0)
    m0_cust_prev = prev.get("m0_customers", 0)
    m1_items = cur.get("m1_items", 0)
    m0_items_prev = prev.get("m0_items", 0)
    m1_rev = cur.get("m1_revenue", 0)
    m0_rev_prev = prev.get("m0_revenue", 0)

    return {
        "customer_retention": m1_cust / m0_cust_prev if m0_cust_prev > 0 else None,
        "item_retention": m1_items / m0_items_prev if m0_items_prev > 0 else None,
        "revenue_retention": m1_rev / m0_rev_prev if m0_rev_prev > 0 else None,
    }


# =====================================================================
# 04 — LOGISTICS
# =====================================================================

@st.cache_data(ttl=300)
def fetch_logistics_batch(_con, periods_tuple):
    """Fetch stops, delivery metrics, and geographic distribution."""
    periods = list(periods_tuple)
    ctx = get_grain_context(periods)
    period_col, sales_join = ctx["period_col"], ctx["sales_join"]
    placeholders = ", ".join(f"'{p}'" for p in periods)

    # Query 1: Headlines
    head_df = _con.execute(f"""
        SELECT {period_col} AS period,
            SUM(s.HasDelivery) + SUM(s.HasPickup) AS total_stops,
            SUM(CASE WHEN s.HasDelivery = 1 THEN s.Pieces ELSE 0 END) AS items_delivered,
            SUM(CASE WHEN s.HasDelivery = 1 THEN s.Total_Num ELSE 0 END) AS delivery_revenue,
            SUM(s.Total_Num) AS total_revenue,
            SUM(s.HasDelivery) AS deliveries,
            SUM(s.HasPickup) AS pickups
        FROM sales s
        JOIN dim_period p ON {sales_join}
        WHERE s.Earned_Date IS NOT NULL
          AND {period_col} IN ({placeholders})
        GROUP BY {period_col}
    """).df()

    # Query 2: Geographic split
    geo_df = _con.execute(f"""
        SELECT {period_col} AS period, s.Route_Category,
            COUNT(DISTINCT s.CustomerID_Std) AS customers,
            SUM(CASE WHEN s.HasDelivery = 1 THEN s.Pieces ELSE 0 END) AS items,
            SUM(s.HasDelivery) + SUM(s.HasPickup) AS stops,
            SUM(s.Total_Num) AS revenue
        FROM sales s
        JOIN dim_period p ON {sales_join}
        WHERE s.Earned_Date IS NOT NULL
          AND s.Route_Category IN ('Inside Abu Dhabi', 'Outer Abu Dhabi')
          AND {period_col} IN ({placeholders})
        GROUP BY {period_col}, s.Route_Category
    """).df()

    result = {}
    for p in periods:
        h_row = head_df[head_df["period"] == p]
        total_stops = int(h_row["total_stops"].iloc[0]) if len(h_row) else 0
        items_delivered = int(h_row["items_delivered"].iloc[0]) if len(h_row) else 0
        delivery_rev = float(h_row["delivery_revenue"].iloc[0]) if len(h_row) else 0.0
        total_rev = float(h_row["total_revenue"].iloc[0]) if len(h_row) else 0.0
        deliveries = int(h_row["deliveries"].iloc[0]) if len(h_row) else 0
        pickups = int(h_row["pickups"].iloc[0]) if len(h_row) else 0

        geo = {}
        for cat in ("Inside Abu Dhabi", "Outer Abu Dhabi"):
            g_row = geo_df[(geo_df["period"] == p) & (geo_df["Route_Category"] == cat)]
            geo[cat] = {
                "customers": int(g_row["customers"].iloc[0]) if len(g_row) else 0,
                "items": int(g_row["items"].iloc[0]) if len(g_row) else 0,
                "stops": int(g_row["stops"].iloc[0]) if len(g_row) else 0,
                "revenue": float(g_row["revenue"].iloc[0]) if len(g_row) else 0.0,
            }

        result[p] = {
            "lg_total_stops": total_stops,
            "lg_items_delivered": items_delivered,
            "lg_delivery_rev_pct": delivery_rev / total_rev if total_rev > 0 else 0.0,
            "lg_delivery_rate": deliveries / total_stops if total_stops > 0 else 0.0,
            "lg_deliveries": deliveries,
            "lg_pickups": pickups,
            "geo": geo,
        }
    return result


# =====================================================================
# 05 — OPERATIONS
# =====================================================================

@st.cache_data(ttl=300)
def fetch_operations_batch(_con, periods_tuple):
    """Fetch items and revenue by Item_Category and Service_Type."""
    periods = list(periods_tuple)
    ctx = get_grain_context(periods)
    period_col, sales_join = ctx["period_col"], ctx["sales_join"]
    placeholders = ", ".join(f"'{p}'" for p in periods)

    # Query 1: By Item_Category
    cat_df = _con.execute(f"""
        SELECT {period_col} AS period, i.Item_Category,
            SUM(i.Quantity) AS items, SUM(i.Total) AS revenue
        FROM items i
        JOIN dim_period p ON i.ItemDate = p.Date
        WHERE {period_col} IN ({placeholders})
        GROUP BY {period_col}, i.Item_Category
    """).df()

    # Query 2: By Service_Type
    svc_df = _con.execute(f"""
        SELECT {period_col} AS period, i.Service_Type,
            SUM(i.Quantity) AS items, SUM(i.Total) AS revenue
        FROM items i
        JOIN dim_period p ON i.ItemDate = p.Date
        WHERE {period_col} IN ({placeholders})
        GROUP BY {period_col}, i.Service_Type
    """).df()

    # Query 3: Processing efficiency metrics
    proc_df = _con.execute(f"""
        SELECT {period_col} AS period,
            AVG(s.Processing_Days) AS avg_processing_time,
            AVG(s.TimeInStore_Days) AS avg_time_in_store
        FROM sales s
        JOIN dim_period p ON {sales_join}
        WHERE s.Earned_Date IS NOT NULL
          AND {period_col} IN ({placeholders})
        GROUP BY {period_col}
    """).df()

    categories = ["Professional Wear", "Traditional Wear", "Home Linens", "Extras", "Others"]
    services = ["Wash & Press", "Dry Cleaning", "Press Only", "Other Service"]

    result = {}
    for pd in periods:
        row = {"categories": {}, "services": {}}

        for cat in categories:
            c_row = cat_df[(cat_df["period"] == pd) & (cat_df["Item_Category"] == cat)]
            items = int(c_row["items"].iloc[0]) if len(c_row) else 0
            revenue = float(c_row["revenue"].iloc[0]) if len(c_row) else 0.0
            row["categories"][cat] = {"items": items, "revenue": revenue}
            safe = cat.lower().replace(" ", "_").replace("&", "and")
            row[f"cat_{safe}_items"] = items
            row[f"cat_{safe}_rev"] = revenue

        for svc in services:
            s_row = svc_df[(svc_df["period"] == pd) & (svc_df["Service_Type"] == svc)]
            items = int(s_row["items"].iloc[0]) if len(s_row) else 0
            revenue = float(s_row["revenue"].iloc[0]) if len(s_row) else 0.0
            row["services"][svc] = {"items": items, "revenue": revenue}
            safe = svc.lower().replace(" ", "_").replace("&", "and")
            row[f"svc_{safe}_items"] = items
            row[f"svc_{safe}_rev"] = revenue

        # Processing efficiency
        p_row = proc_df[proc_df["period"] == pd]
        row["ops_avg_processing_time"] = float(p_row["avg_processing_time"].iloc[0]) if len(p_row) else 0.0
        row["ops_avg_time_in_store"] = float(p_row["avg_time_in_store"].iloc[0]) if len(p_row) else 0.0

        result[pd] = row
    return result


# =====================================================================
# 06 — PAYMENTS
# =====================================================================

@st.cache_data(ttl=300)
def fetch_payments_batch(_con, periods_tuple):
    """Fetch revenue, collections by method, and processing metrics."""
    periods = list(periods_tuple)
    ctx = get_grain_context(periods)
    period_col, sales_join = ctx["period_col"], ctx["sales_join"]
    placeholders = ", ".join(f"'{p}'" for p in periods)

    df = _con.execute(f"""
        SELECT {period_col} AS period,
            SUM(s.Total_Num) AS revenue,
            SUM(s.Collections) AS total_collections,
            SUM(CASE WHEN s.Payment_Type_Std = 'Stripe' THEN s.Collections ELSE 0 END) AS stripe,
            SUM(CASE WHEN s.Payment_Type_Std = 'Terminal' THEN s.Collections ELSE 0 END) AS terminal,
            SUM(CASE WHEN s.Payment_Type_Std = 'Cash' THEN s.Collections ELSE 0 END) AS cash,
            AVG(s.DaysToPayment) AS avg_days_to_payment
        FROM sales s
        JOIN dim_period p ON {sales_join}
        WHERE s.Earned_Date IS NOT NULL
          AND {period_col} IN ({placeholders})
        GROUP BY {period_col}
    """).df()

    result = {}
    for p in periods:
        r = df[df["period"] == p]
        result[p] = {
            "pm_revenue": float(r["revenue"].iloc[0]) if len(r) else 0.0,
            "pm_total_collections": float(r["total_collections"].iloc[0]) if len(r) else 0.0,
            "pm_stripe": float(r["stripe"].iloc[0]) if len(r) else 0.0,
            "pm_terminal": float(r["terminal"].iloc[0]) if len(r) else 0.0,
            "pm_cash": float(r["cash"].iloc[0]) if len(r) else 0.0,
            "pm_avg_days_to_payment": float(r["avg_days_to_payment"].iloc[0]) if len(r) else 0.0,
        }
    return result
