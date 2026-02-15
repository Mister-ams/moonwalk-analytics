"""
Shared data layer for the Customer Report.

Provides batch SQL fetch for customer acquisition and per-customer item measures:
- active_customers / active_subscribers / active_clients
- new_customers / new_customer_pct
- items / subscriber items / client items (and per-customer ratios)
- subscriber items %
- revenue per customer / per client / per subscriber
"""

import streamlit as st
from dashboard_shared import get_grain_context


@st.cache_data(ttl=300)
def fetch_customer_measures_batch(_con, periods_tuple):
    """Fetch customer, item, and revenue measures for multiple periods in three queries."""
    periods = list(periods_tuple)
    ctx = get_grain_context(periods)
    period_col, sales_join = ctx["period_col"], ctx["sales_join"]
    placeholders = ", ".join(f"'{p}'" for p in periods)

    # Query 1: customer counts
    cust_df = _con.execute(f"""
        SELECT {period_col} AS period,
               COUNT(DISTINCT s.CustomerID_Std) AS active_customers,
               COUNT(DISTINCT CASE
                   WHEN s.MonthsSinceCohort = 0 THEN s.CustomerID_Std
               END) AS new_customers,
               COUNT(DISTINCT CASE
                   WHEN s.Transaction_Type = 'Subscription' THEN s.CustomerID_Std
               END) AS active_subscribers
        FROM sales s
        JOIN dim_period p ON {sales_join}
        WHERE s.Transaction_Type <> 'Invoice Payment'
          AND s.Earned_Date IS NOT NULL
          AND {period_col} IN ({placeholders})
        GROUP BY {period_col}
    """).df()

    # Query 2: item counts
    items_df = _con.execute(f"""
        SELECT sub.period,
               COALESCE(SUM(sub.qty), 0) AS items_total,
               COALESCE(SUM(CASE WHEN sub.iss = 0 THEN sub.qty END), 0) AS items_client,
               COALESCE(SUM(CASE WHEN sub.iss = 1 THEN sub.qty END), 0) AS items_sub
        FROM (
            SELECT {period_col} AS period, i.Quantity AS qty,
                   COALESCE(ol.IsSubscriptionService, 0) AS iss
            FROM items i
            JOIN dim_period p ON i.ItemDate = p.Date
            LEFT JOIN order_lookup ol ON i.OrderID_Std = ol.OrderID_Std
            WHERE {period_col} IN ({placeholders})
        ) sub
        GROUP BY sub.period
    """).df()

    # Query 3: revenue totals
    rev_df = _con.execute(f"""
        SELECT {period_col} AS period,
               COALESCE(SUM(s.Total_Num), 0) AS rev_total,
               COALESCE(SUM(CASE
                   WHEN s.Transaction_Type = 'Order' AND s.IsSubscriptionService = 0
                   THEN s.Total_Num END), 0) AS rev_client,
               COALESCE(SUM(CASE
                   WHEN s.Transaction_Type = 'Subscription' THEN s.Total_Num
                   WHEN s.Transaction_Type = 'Order' AND s.IsSubscriptionService = 1
                   THEN s.Total_Num END), 0) AS rev_sub
        FROM sales s
        JOIN dim_period p ON {sales_join}
        WHERE s.Earned_Date IS NOT NULL
          AND {period_col} IN ({placeholders})
        GROUP BY {period_col}
    """).df()

    result = {}
    for pd in periods:
        c_row = cust_df[cust_df["period"] == pd]
        active = int(c_row["active_customers"].iloc[0]) if len(c_row) else 0
        new = int(c_row["new_customers"].iloc[0]) if len(c_row) else 0
        subscribers = int(c_row["active_subscribers"].iloc[0]) if len(c_row) else 0
        clients = active - subscribers

        i_row = items_df[items_df["period"] == pd]
        items_total = int(i_row["items_total"].iloc[0]) if len(i_row) else 0
        items_sub = int(i_row["items_sub"].iloc[0]) if len(i_row) else 0
        items_client = int(i_row["items_client"].iloc[0]) if len(i_row) else 0

        r_row = rev_df[rev_df["period"] == pd]
        rev_total = float(r_row["rev_total"].iloc[0]) if len(r_row) else 0.0
        rev_client = float(r_row["rev_client"].iloc[0]) if len(r_row) else 0.0
        rev_sub = float(r_row["rev_sub"].iloc[0]) if len(r_row) else 0.0

        result[pd] = {
            # Customer counts
            "active_customers": active,
            "active_subscribers": subscribers,
            "active_clients": clients,
            "new_customers": new,
            "new_customer_pct": new / active if active > 0 else 0.0,
            # Item counts
            "cr_items_sub": items_sub,
            "cr_items_client": items_client,
            # Per-customer ratios
            "items_per_customer": items_total / active if active > 0 else 0.0,
            "sub_items_per_customer": items_sub / subscribers if subscribers > 0 else 0.0,
            "client_items_per_customer": items_client / clients if clients > 0 else 0.0,
            # Subscriber items share
            "sub_items_pct": items_sub / items_total if items_total > 0 else 0.0,
            # Revenue per head
            "rev_per_customer": rev_total / active if active > 0 else 0.0,
            "rev_per_client": rev_client / clients if clients > 0 else 0.0,
            "rev_per_subscriber": rev_sub / subscribers if subscribers > 0 else 0.0,
        }
    return result


@st.cache_data(ttl=300)
def fetch_new_customer_detail_batch(_con, months_tuple):
    """Fetch new vs existing customer splits for items and revenue."""
    months = list(months_tuple)
    placeholders = ", ".join(f"'{m}'" for m in months)

    # Query 1: customer counts (reuse same logic as above)
    cust_df = _con.execute(f"""
        SELECT p.YearMonth,
               COUNT(DISTINCT s.CustomerID_Std) AS active_customers,
               COUNT(DISTINCT CASE
                   WHEN s.MonthsSinceCohort = 0 THEN s.CustomerID_Std
               END) AS new_customers
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Transaction_Type <> 'Invoice Payment'
          AND s.Earned_Date IS NOT NULL
          AND p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth
    """).df()

    # Query 2: revenue split by new/existing
    rev_df = _con.execute(f"""
        SELECT p.YearMonth,
               COALESCE(SUM(CASE WHEN s.MonthsSinceCohort = 0 THEN s.Total_Num END), 0) AS rev_new,
               COALESCE(SUM(CASE WHEN s.MonthsSinceCohort > 0 THEN s.Total_Num END), 0) AS rev_existing
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL
          AND p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth
    """).df()

    # Query 3: items split by new/existing
    items_df = _con.execute(f"""
        SELECT p.YearMonth AS ym,
               COALESCE(SUM(CASE WHEN sc.MonthsSinceCohort = 0 THEN i.Quantity END), 0) AS items_new,
               COALESCE(SUM(CASE WHEN COALESCE(sc.MonthsSinceCohort, 1) > 0 THEN i.Quantity END), 0) AS items_existing
        FROM items i
        JOIN dim_period p ON i.ItemDate = p.Date
        LEFT JOIN (
            SELECT DISTINCT OrderID_Std, MonthsSinceCohort FROM sales
        ) sc ON i.OrderID_Std = sc.OrderID_Std
        WHERE p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth
    """).df()

    result = {}
    for m in months:
        c_row = cust_df[cust_df["YearMonth"] == m]
        active = int(c_row["active_customers"].iloc[0]) if len(c_row) else 0
        new = int(c_row["new_customers"].iloc[0]) if len(c_row) else 0

        r_row = rev_df[rev_df["YearMonth"] == m]
        rev_new = float(r_row["rev_new"].iloc[0]) if len(r_row) else 0.0
        rev_existing = float(r_row["rev_existing"].iloc[0]) if len(r_row) else 0.0

        i_row = items_df[items_df["ym"] == m]
        items_new = int(i_row["items_new"].iloc[0]) if len(i_row) else 0
        items_existing = int(i_row["items_existing"].iloc[0]) if len(i_row) else 0

        result[m] = {
            "new_customers": new,
            "existing_customers": active - new,
            "new_items": items_new,
            "existing_items": items_existing,
            "new_revenue": rev_new,
            "existing_revenue": rev_existing,
        }
    return result
