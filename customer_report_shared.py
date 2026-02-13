"""
Shared data layer for the Customer Report.

Provides batch SQL fetch for customer acquisition and per-customer item measures:
- active_customers / active_subscribers / active_clients
- new_customers / new_customer_pct
- items / subscriber items / client items (and per-customer ratios)
- subscriber items %
"""

import streamlit as st


@st.cache_data(ttl=300)
def fetch_customer_measures_batch(_con, months_tuple):
    """Fetch customer and item measures for multiple months in two queries."""
    months = list(months_tuple)
    placeholders = ", ".join(f"'{m}'" for m in months)

    # Query 1: customer counts
    cust_df = _con.execute(f"""
        SELECT p.YearMonth,
               COUNT(DISTINCT s.CustomerID_Std) AS active_customers,
               COUNT(DISTINCT CASE
                   WHEN s.MonthsSinceCohort = 0 THEN s.CustomerID_Std
               END) AS new_customers,
               COUNT(DISTINCT CASE
                   WHEN s.Transaction_Type = 'Subscription' THEN s.CustomerID_Std
               END) AS active_subscribers
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Transaction_Type <> 'Invoice Payment'
          AND s.Earned_Date IS NOT NULL
          AND p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth
    """).df()

    # Query 2: item counts (mirrors fetch_measures_batch items query)
    items_df = _con.execute(f"""
        SELECT sub.ym,
               COALESCE(SUM(sub.qty), 0) AS items_total,
               COALESCE(SUM(CASE WHEN sub.iss = 0 THEN sub.qty END), 0) AS items_client,
               COALESCE(SUM(CASE WHEN sub.iss = 1 THEN sub.qty END), 0) AS items_sub
        FROM (
            SELECT p.YearMonth AS ym, i.Quantity AS qty,
                   COALESCE(sd.IsSubscriptionService, 0) AS iss
            FROM items i
            JOIN dim_period p ON i.ItemDate = p.Date
            LEFT JOIN (
                SELECT DISTINCT OrderID_Std, IsSubscriptionService FROM sales
            ) sd ON i.OrderID_Std = sd.OrderID_Std
            WHERE p.YearMonth IN ({placeholders})
        ) sub
        GROUP BY sub.ym
    """).df()

    result = {}
    for m in months:
        c_row = cust_df[cust_df["YearMonth"] == m]
        active = int(c_row["active_customers"].iloc[0]) if len(c_row) else 0
        new = int(c_row["new_customers"].iloc[0]) if len(c_row) else 0
        subscribers = int(c_row["active_subscribers"].iloc[0]) if len(c_row) else 0
        clients = active - subscribers

        i_row = items_df[items_df["ym"] == m]
        items_total = int(i_row["items_total"].iloc[0]) if len(i_row) else 0
        items_sub = int(i_row["items_sub"].iloc[0]) if len(i_row) else 0
        items_client = int(i_row["items_client"].iloc[0]) if len(i_row) else 0

        result[m] = {
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
        }
    return result
