"""Push structured KPI rows to a Notion database after each refresh.

Queries DuckDB for the last 6 closed months and last 13 closed ISO weeks,
then upserts one row per period into a Notion database.

On first run with an empty NOTION_KPI_DB_ID, the database is auto-created
as a child of NOTION_PAGE_ID.  The new DB ID is logged — save it to
.streamlit/secrets.toml as NOTION_KPI_DB_ID to enable upsert mode.

Safe to skip: if NOTION_API_KEY or NOTION_PAGE_ID is missing, logs a
warning and returns without raising.

Usage (standalone):
    python notion_kpi_push.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import DB_PATH, DUCKDB_KEY, NOTION_API_KEY, NOTION_KPI_DB_ID, NOTION_PAGE_ID, NOTION_TOKEN

_BASE_URL = "https://loomi-performance-analytics.streamlit.app"
_DB_TITLE = "Moonwalk KPI Tracker"

# =====================================================================
# DuckDB helpers
# =====================================================================


def _open_db():
    """Open encrypted DuckDB connection (read-only) using the ATTACH pattern."""
    import duckdb

    con = duckdb.connect(":memory:")
    if DUCKDB_KEY:
        con.execute(f"ATTACH '{DB_PATH}' AS db (ENCRYPTION_KEY '{DUCKDB_KEY}', READ_ONLY)")
    else:
        con.execute(f"ATTACH '{DB_PATH}' AS db (READ_ONLY)")
    con.execute("USE db")
    return con


def _fetch_monthly(con) -> list[dict]:
    """Return last 6 closed months of core KPIs, newest first."""
    rows = con.execute("""
        WITH base AS (
            SELECT
                strftime(s.OrderCohortMonth, '%b %Y')          AS period_label,
                s.OrderCohortMonth                              AS period_date,
                COUNT(DISTINCT s.CustomerID_Std)                AS customers,
                SUM(s.Total_Num)                                AS revenue,
                SUM(s.HasDelivery::INT + s.HasPickup::INT)      AS stops,
                COUNT(DISTINCT CASE WHEN s.IsSubscriptionService THEN s.CustomerID_Std END)
                                                                AS subscriptions,
                SUM(s.Collections)                              AS collections,
                AVG(s.Processing_Days)                          AS avg_processing_days
            FROM sales s
            WHERE s.Is_Earned = TRUE
              AND s.OrderCohortMonth < date_trunc('month', current_date)
            GROUP BY s.OrderCohortMonth
            ORDER BY s.OrderCohortMonth DESC
            LIMIT 7
        ),
        new_customers AS (
            SELECT
                strftime(c.CohortMonth, '%b %Y') AS period_label,
                COUNT(*)                          AS new_customers
            FROM customers c
            WHERE c.CohortMonth IN (SELECT DISTINCT period_date FROM base)
            GROUP BY c.CohortMonth
        ),
        items_agg AS (
            SELECT
                strftime(i.ItemCohortMonth, '%b %Y') AS period_label,
                SUM(i.Quantity)                       AS items
            FROM items i
            WHERE i.ItemCohortMonth IN (SELECT DISTINCT period_date FROM base)
            GROUP BY i.ItemCohortMonth
        ),
        insights_latest AS (
            SELECT ins.period, ins.category, ins.sentiment
            FROM insights ins
            WHERE ins.period IN (
                SELECT strftime(period_date, '%Y-%m') FROM base
            )
              AND ins.category = 'revenue'
            ORDER BY ins.period DESC, ins.rule_id
            LIMIT 7
        )
        SELECT
            b.period_label,
            b.period_date,
            b.revenue,
            b.customers,
            COALESCE(nc.new_customers, 0)   AS new_customers,
            COALESCE(ia.items, 0)           AS items,
            b.stops,
            b.subscriptions,
            b.collections,
            b.avg_processing_days,
            il.sentiment                    AS revenue_trend
        FROM base b
        LEFT JOIN new_customers nc ON nc.period_label = b.period_label
        LEFT JOIN items_agg ia     ON ia.period_label = b.period_label
        LEFT JOIN insights_latest il ON strftime(b.period_date, '%Y-%m') = il.period
        ORDER BY b.period_date DESC
        LIMIT 7
    """).fetchall()

    cols = [
        "period_label",
        "period_date",
        "revenue",
        "customers",
        "new_customers",
        "items",
        "stops",
        "subscriptions",
        "collections",
        "avg_processing_days",
        "revenue_trend",
    ]
    # Process oldest-first so vs_prior correctly compares each period to the one before it.
    # rows is newest-first (DESC); reverse, compute, take last 6, reverse back.
    rows_asc = list(reversed(rows))
    result = []
    prev_revenue = None
    prev_customers = None
    for row in rows_asc:
        d = dict(zip(cols, row))
        d["granularity"] = "Monthly"
        d["revenue_vs_prior"] = (
            round((d["revenue"] / prev_revenue - 1) * 100, 1) if prev_revenue and prev_revenue > 0 else None
        )
        d["customers_vs_prior"] = (
            round((d["customers"] / prev_customers - 1) * 100, 1) if prev_customers and prev_customers > 0 else None
        )
        d["collection_rate"] = (
            round(d["collections"] / d["revenue"] * 100, 1) if d["revenue"] and d["revenue"] > 0 else None
        )
        prev_revenue = d["revenue"]
        prev_customers = d["customers"]
        result.append(d)

    result = result[-6:]  # drop oldest anchor month, keep 6 most recent
    result.reverse()  # newest-first for Notion display
    return result


def _fetch_weekly(con) -> list[dict]:
    """Return last 13 closed ISO weeks of core KPIs, newest first."""
    rows = con.execute("""
        WITH sales_weekly AS (
            SELECT
                p.ISOWeekLabel                                  AS period_label,
                p.ISOYearWeek                                   AS iso_year_week,
                COUNT(DISTINCT s.CustomerID_Std)                AS customers,
                SUM(s.Total_Num)                                AS revenue,
                SUM(s.HasDelivery::INT + s.HasPickup::INT)      AS stops,
                COUNT(DISTINCT CASE WHEN s.IsSubscriptionService THEN s.CustomerID_Std END)
                                                                AS subscriptions,
                SUM(s.Collections)                              AS collections,
                AVG(s.Processing_Days)                          AS avg_processing_days
            FROM sales s
            JOIN dim_period p ON s.Earned_Date = p.Date
            WHERE s.Is_Earned = TRUE AND p.IsCurrentISOWeek = FALSE
            GROUP BY p.ISOWeekLabel, p.ISOYearWeek
        ),
        items_weekly AS (
            SELECT p.ISOYearWeek, SUM(i.Quantity) AS items
            FROM items i
            JOIN dim_period p ON i.ItemDate = p.Date
            WHERE p.IsCurrentISOWeek = FALSE
            GROUP BY p.ISOYearWeek
        )
        SELECT sw.period_label, sw.iso_year_week, sw.customers, sw.revenue,
               sw.stops, sw.subscriptions, sw.collections, sw.avg_processing_days,
               COALESCE(iw.items, 0) AS items
        FROM sales_weekly sw
        LEFT JOIN items_weekly iw ON sw.iso_year_week = iw.ISOYearWeek
        ORDER BY sw.iso_year_week DESC
        LIMIT 14
    """).fetchall()

    cols = [
        "period_label",
        "iso_year_week",
        "customers",
        "revenue",
        "stops",
        "subscriptions",
        "collections",
        "avg_processing_days",
        "items",
    ]
    # Process oldest-first so vs_prior correctly compares each week to the one before it.
    rows_asc = list(reversed(rows))
    result = []
    prev_revenue = None
    prev_customers = None
    for row in rows_asc:
        d = dict(zip(cols, row))
        d["granularity"] = "Weekly"
        d["new_customers"] = None
        d["revenue_vs_prior"] = (
            round((d["revenue"] / prev_revenue - 1) * 100, 1) if prev_revenue and prev_revenue > 0 else None
        )
        d["customers_vs_prior"] = (
            round((d["customers"] / prev_customers - 1) * 100, 1) if prev_customers and prev_customers > 0 else None
        )
        d["collection_rate"] = (
            round(d["collections"] / d["revenue"] * 100, 1) if d["revenue"] and d["revenue"] > 0 else None
        )
        # Derive revenue_trend from week-over-week change (no monthly insights table for weekly)
        rvp = d["revenue_vs_prior"]
        d["revenue_trend"] = (
            "positive"
            if rvp is not None and rvp > 2
            else "negative"
            if rvp is not None and rvp < -2
            else "neutral"
            if rvp is not None
            else None
        )
        prev_revenue = d["revenue"]
        prev_customers = d["customers"]
        result.append(d)

    result = result[-13:]  # drop oldest anchor week, keep 13 most recent
    result.reverse()  # newest-first for Notion display
    return result


# =====================================================================
# Notion schema helpers
# =====================================================================

_DB_PROPERTIES = {
    "Period": {"title": {}},
    "Granularity": {"select": {"options": [{"name": "Monthly"}, {"name": "Weekly"}]}},
    "Revenue (Dhs)": {"number": {"format": "number"}},
    "Revenue vs Prior (%)": {"number": {"format": "number"}},
    "Customers": {"number": {"format": "number"}},
    "Customers vs Prior (%)": {"number": {"format": "number"}},
    "New Customers": {"number": {"format": "number"}},
    "Items": {"number": {"format": "number"}},
    "Stops": {"number": {"format": "number"}},
    "Subscriptions": {"number": {"format": "number"}},
    "Collection Rate (%)": {"number": {"format": "number"}},
    "Avg Processing Days": {"number": {"format": "number"}},
    "Revenue Trend": {
        "select": {
            "options": [
                {"name": "positive"},
                {"name": "negative"},
                {"name": "neutral"},
            ]
        }
    },
    "Open Dashboard": {"url": {}},
}


def _ensure_database(notion_client, log) -> str | None:
    """Return existing NOTION_KPI_DB_ID or create + log the new one."""
    if NOTION_KPI_DB_ID:
        return NOTION_KPI_DB_ID

    log("Notion KPI: no NOTION_KPI_DB_ID — auto-creating database...")
    try:
        result = notion_client.databases.create(
            parent={"type": "page_id", "page_id": NOTION_PAGE_ID},
            title=[{"type": "text", "text": {"content": _DB_TITLE}}],
            properties=_DB_PROPERTIES,
        )
        new_id = result["id"]
        log(
            f"Notion KPI: created database '{_DB_TITLE}' (id={new_id}). "
            f'Add NOTION_KPI_DB_ID = "{new_id}" to .streamlit/secrets.toml.'
        )
        return new_id
    except Exception as exc:
        log(f"Notion KPI: could not create database: {exc}")
        return None


def _build_properties(row: dict, notion_token: str) -> dict:
    """Build Notion page properties dict from a KPI row."""
    url = _BASE_URL
    params = []
    if notion_token:
        params.append(f"token={notion_token}")
    params.append("tab=snapshot")
    url += "?" + "&".join(params)

    def _num(val):
        if val is None:
            return {"number": None}
        try:
            return {"number": round(float(val), 2)}
        except (TypeError, ValueError):
            return {"number": None}

    def _select(val):
        if not val:
            return {"select": None}
        return {"select": {"name": str(val)}}

    return {
        "Period": {"title": [{"type": "text", "text": {"content": row["period_label"]}}]},
        "Granularity": _select(row.get("granularity")),
        "Revenue (Dhs)": _num(row.get("revenue")),
        "Revenue vs Prior (%)": _num(row.get("revenue_vs_prior")),
        "Customers": _num(row.get("customers")),
        "Customers vs Prior (%)": _num(row.get("customers_vs_prior")),
        "New Customers": _num(row.get("new_customers")),
        "Items": _num(row.get("items")),
        "Stops": _num(row.get("stops")),
        "Subscriptions": _num(row.get("subscriptions")),
        "Collection Rate (%)": _num(row.get("collection_rate")),
        "Avg Processing Days": _num(row.get("avg_processing_days")),
        "Revenue Trend": _select(row.get("revenue_trend")),
        "Open Dashboard": {"url": url},
    }


def _upsert_row(notion_client, db_id: str, row: dict, log):
    """Query for an existing row; update if found, create if not."""
    period = row["period_label"]
    granularity = row["granularity"]

    response = notion_client.databases.query(
        database_id=db_id,
        filter={
            "and": [
                {"property": "Period", "title": {"equals": period}},
                {"property": "Granularity", "select": {"equals": granularity}},
            ]
        },
    )
    props = _build_properties(row, NOTION_TOKEN)

    existing = response.get("results", [])
    if existing:
        page_id = existing[0]["id"]
        notion_client.pages.update(page_id=page_id, properties=props)
        log(f"  Updated: {granularity} / {period}")
    else:
        notion_client.pages.create(
            parent={"database_id": db_id},
            properties=props,
        )
        log(f"  Created: {granularity} / {period}")


# =====================================================================
# Entry point
# =====================================================================


def run(log=print):
    """Upsert KPI rows into Notion database. Called by moonwalk_flow.py."""
    if not NOTION_API_KEY:
        log("Notion KPI push skipped - NOTION_API_KEY not configured")
        return
    if not NOTION_PAGE_ID:
        log("Notion KPI push skipped - NOTION_PAGE_ID not configured")
        return

    from notion_client import Client

    notion_client = Client(auth=NOTION_API_KEY)

    db_id = _ensure_database(notion_client, log)
    if not db_id:
        log("Notion KPI push skipped - could not resolve database ID")
        return

    log("Notion KPI: fetching KPIs from DuckDB...")
    con = _open_db()
    try:
        monthly_rows = _fetch_monthly(con)
        weekly_rows = _fetch_weekly(con)
    finally:
        con.close()

    log(f"Notion KPI: upserting {len(monthly_rows)} months + {len(weekly_rows)} weeks...")
    for row in monthly_rows:
        _upsert_row(notion_client, db_id, row, log)
    for row in weekly_rows:
        _upsert_row(notion_client, db_id, row, log)

    log(f"Notion KPI: done - {len(monthly_rows) + len(weekly_rows)} rows upserted")


if __name__ == "__main__":
    run()
