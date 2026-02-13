"""
Shared utilities for the LOOMI Monthly Report multi-page dashboard.

Provides: DB connection, Dirham formatting, card rendering, SQL measures,
trend chart rendering, global CSS injection, and month selector.
"""

import streamlit as st
import duckdb
import plotly.graph_objects as go
from datetime import datetime
from pathlib import Path

# =====================================================================
# DATA PATHS (centralized in config.py)
# =====================================================================

from config import SALES_CSV, ITEMS_CSV, DIMPERIOD_CSV, DB_PATH

# =====================================================================
# DIRHAM SYMBOL (CBUAE official SVG, base64-encoded for inline use)
# =====================================================================

_DIRHAM_B64 = (
    "PHN2ZyB2ZXJzaW9uPSIxLjIiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIg"
    "dmlld0JveD0iMCAwIDEwMDAgODcwIiB3aWR0aD0iMTAwMCIgaGVpZ2h0PSI4NzAiPgoJPHRp"
    "dGxlPkxheWVyIGNvcHk8L3RpdGxlPgoJPHN0eWxlPgoJCS5zMCB7IGZpbGw6ICMwMDAwMDAg"
    "fSAKCTwvc3R5bGU+Cgk8cGF0aCBpZD0iTGF5ZXIgY29weSIgY2xhc3M9InMwIiBkPSJtODgu"
    "MyAxYzAuNCAwLjYgMi42IDMuMyA0LjcgNS45IDE1LjMgMTguMiAyNi44IDQ3LjggMzMgODUu"
    "MSA0LjEgMjQuNSA0LjMgMzIuMiA0LjMgMTI1LjZ2ODdoLTQxLjhjLTM4LjIgMC00Mi42LTAu"
    "Mi01MC4xLTEuNy0xMS44LTIuNS0yNC05LjItMzIuMi0xNy44LTYuNS02LjktNi4zLTcuMy01"
    "LjkgMTMuNiAwLjUgMTcuMyAwLjcgMTkuMiAzLjIgMjguNiA0IDE0LjkgOS41IDI2IDE3Ljgg"
    "MzUuOSAxMS4zIDEzLjYgMjIuOCAyMS4yIDM5LjIgMjYuMyAzLjUgMSAxMC45IDEuNCAzNy4x"
    "IDEuNmwzMi43IDAuNXY0My4zIDQzLjRsLTQ2LjEtMC4zLTQ2LjMtMC4zLTgtMy4yYy05LjUt"
    "My44LTEzLjgtNi42LTIzLjEtMTQuOWwtNi44LTYuMSAwLjQgMTkuMWMwLjUgMTcuNyAwLjYg"
    "MTkuNyAzLjEgMjguNyA4LjcgMzEuOCAyOS43IDU0LjUgNTcuNCA2MS45IDYuOSAxLjkgOS42"
    "IDIgMzguNSAyLjRsMzAuOSAwLjR2ODkuNmMwIDU0LjEtMC4zIDk0LTAuOCAxMDAuOC0wLjUg"
    "Ni4yLTIuMSAxNy44LTMuNSAyNS45LTYuNSAzNy4zLTE4LjIgNjUuNC0zNSA4My42bC0zLjQg"
    "My43aDE2OS4xYzEwMS4xIDAgMTc2LjctMC40IDE4Ny44LTAuOSAxOS41LTEgNjMtNS4zIDcy"
    "LjgtNy40IDMuMS0wLjYgOC45LTEuNSAxMi43LTIuMSA4LjEtMS4yIDIxLjUtNCA0MC44LTgu"
    "OSAyNy4yLTYuOCA1Mi0xNS4zIDc2LjMtMjYuMSA3LjYtMy40IDI5LjQtMTQuNSAzNS4yLTE4"
    "IDMuMS0xLjggNi44LTQgOC4yLTQuNyAzLjktMi4xIDEwLjQtNi4zIDE5LjktMTMuMSA0Ljct"
    "My40IDkuNC02LjcgMTAuNC03LjQgNC4yLTIuOCAxOC43LTE0LjkgMjUuMy0yMSAyNS4xLTIz"
    "LjEgNDYuMS00OC44IDYyLjQtNzYuMyAyLjMtNCA1LjMtOSA2LjYtMTEuMSAzLjMtNS42IDE2"
    "LjktMzMuNiAxOC4yLTM3LjggMC42LTEuOSAxLjQtMy45IDEuOC00LjMgMi42LTMuNCAxNy42"
    "LTUwLjYgMTkuNC02MC45IDAuNi0zLjMgMC45LTMuOCAzLjQtNC4zIDEuNi0wLjMgMjQuOS0w"
    "LjMgNTEuOC0wLjEgNTMuOCAwLjQgNTMuOCAwLjQgNjUuNyA1LjkgNi43IDMuMSA4LjcgNC41"
    "IDE2LjEgMTEuMiA5LjcgOC43IDguOCAxMC4xIDguMi0xMS43LTAuNC0xMi44LTAuOS0yMC43"
    "LTEuOC0yMy45LTMuNC0xMi4zLTQuMi0xNC45LTcuMi0yMS4xLTkuOC0yMS40LTI2LjItMzYu"
    "Ny00Ny4yLTQ0bC04LjItMy0zMy40LTAuNC0zMy4zLTAuNSAwLjQtMTEuN2MwLjQtMTUuNCAw"
    "LjQtNDUuOS0wLjEtNjEuNmwtMC40LTEyLjYgNDQuNi0wLjJjMzguMi0wLjIgNDUuMyAwIDQ5"
    "LjUgMS4xIDEyLjYgMy41IDIxLjEgOC4zIDMxLjUgMTcuOGw1LjggNS40di0xNC44YzAtMTcu"
    "Ni0wLjktMjUuNC00LjUtMzctNy4xLTIzLjUtMjEuMS00MS00MS4xLTUxLjgtMTMtNy0xMy44"
    "LTcuMi01OC41LTcuNS0yNi4yLTAuMi0zOS45LTAuNi00MC42LTEuMi0wLjYtMC42LTEuMS0x"
    "LjYtMS4xLTIuNCAwLTAuOC0xLjUtNy4xLTMuNS0xMy45LTIzLjQtODIuNy02Ny4xLTE0OC40"
    "LTEzMS0xOTcuMS04LjctNi43LTMwLTIwLjgtMzguNi0yNS42LTMuMy0xLjktNi45LTMuOS03"
    "LjgtNC41LTQuMi0yLjMtMjguMy0xNC4xLTM0LjMtMTYuNi0zLjYtMS42LTguMy0zLjYtMTAu"
    "NC00LjQtMzUuMy0xNS4zLTk0LjUtMjkuOC0xMzkuNy0zNC4zLTcuNC0wLjctMTcuMi0xLjgt"
    "MjEuNy0yLjItMjAuNC0yLjMtNDguNy0yLjYtMjA5LjQtMi42LTEzNS44IDAtMTY5LjkgMC4z"
    "LTE2OS40IDF6bTMzMC43IDQzLjNjMzMuOCAyIDU0LjYgNC42IDc4LjkgMTAuNSA3NC4yIDE3"
    "LjYgMTI2LjQgNTQuOCAxNjQuMyAxMTcgMy41IDUuOCAxOC4zIDM2IDIwLjUgNDIuMSAxMC41"
    "IDI4LjMgMTUuNiA0NS4xIDIwLjEgNjcuMyAxLjEgNS40IDIuNiAxMi42IDMuMyAxNiAwLjcg"
    "My4zIDEgNi40IDAuNyA2LjctMC41IDAuNC0xMDAuOSAwLjYtMjIzLjMgMC41bC0yMjIuNS0w"
    "LjItMC4zLTEyOC41Yy0wLjEtNzAuNiAwLTEyOS4zIDAuMy0xMzAuNGwwLjQtMS45aDcxLjFj"
    "MzkgMCA3OCAwLjQgODYuNSAwLjl6bTI5Ny41IDM1MC4zYzAuNyA0LjMgMC43IDc3LjMgMCA4"
    "MC45bC0wLjYgMi43LTIyNy41LTAuMi0yMjcuNC0wLjMtMC4yLTQyLjRjLTAuMi0yMy4zIDAg"
    "LTQyLjcgMC4yLTQzLjEgMC4zLTAuNSA5Ny4yLTAuOCAyMjcuNy0wLjhoMjI3LjJ6bS0xMC4y"
    "IDE3MS43YzAuNSAxLjUtMS45IDEzLjgtNi44IDMzLjgtNS42IDIyLjUtMTMuMiA0NS4yLTIwLjkg"
    "NjItMy44IDguNi0xMy4zIDI3LjItMTUuNiAzMC43LTEuMSAxLjYtNC4zIDYuNy03LjEgMTEu"
    "Mi0xOCAyOC4yLTQzLjcgNTMuOS03MyA3Mi45LTEwLjcgNi44LTMyLjcgMTguNC0zOC42IDIw"
    "LjItMS4yIDAuMy0yLjUgMC45LTMgMS4zLTAuNyAwLjYtOS44IDQtMjAuNCA3LjgtMTkuNSA2"
    "LjktNTYuNiAxNC40LTg2LjQgMTcuNS0xOS4zIDEuOS0yMi40IDItOTYuNyAyaC03Ni45di0x"
    "MjkuNy0xMjkuOGwyMjAuOS0wLjRjMTIxLjUtMC4yIDIyMS42LTAuNSAyMjIuNC0wLjcgMC45"
    "LTAuMSAxLjggMC41IDIuMSAxLjJ6Ii8+Cjwvc3ZnPg=="
)


def fmt_dirham(value, decimals=0):
    """Plain-text fallback using 'Dhs' prefix (for chart axes, tables, etc.)."""
    return f"Dhs {value:,.{decimals}f}"


def dirham_html(value, decimals=0, size=18):
    """Render a value with the CBUAE Dirham symbol as an inline SVG image."""
    formatted = f"{value:,.{decimals}f}"
    return (
        f'<img src="data:image/svg+xml;base64,{_DIRHAM_B64}" '
        f'style="height:{size}px;vertical-align:middle;margin-right:4px;" '
        f'alt="Dhs" />'
        f'<span style="vertical-align:middle;">{formatted}</span>'
    )


# =====================================================================
# CARD RENDERING
# =====================================================================

def change_html(current, previous):
    """Return HTML for a MoM change pill with colored background."""
    if previous is None or previous == 0:
        return '<span style="font-size:0.8rem;color:#aaa;">&mdash;</span>'
    pct = (current - previous) / abs(previous) * 100
    if pct > 0.5:
        arrow, bg, fg = "\u25b2", "#a5d6a7", "#1b5e20"
    elif pct < -0.5:
        arrow, bg, fg = "\u25bc", "#ef9a9a", "#b71c1c"
    else:
        arrow, bg, fg = "\u25a0", "#fff176", "#f57f17"
    return (
        f'<span style="display:inline-block;background:{bg};color:{fg};'
        f'font-size:0.8rem;font-weight:700;padding:0.2rem 0.6rem;'
        f'border-radius:0.75rem;letter-spacing:0.02em;">'
        f'{arrow} {pct:+.0f}%</span>'
    )


def headline_card(label, value_html, change, header_color):
    """Display-only headline card with colored header banner."""
    return (
        f'<div style="border-radius:0.75rem;overflow:hidden;background:#fff;'
        f'box-shadow:0 4px 16px rgba(0,0,0,0.18);">'
        f'<div style="background:{header_color};padding:0.5rem 0;text-align:center;">'
        f'<span style="color:#fff;font-weight:700;font-size:0.95rem;'
        f'letter-spacing:0.04em;">{label}</span></div>'
        f'<div style="padding:0.5rem 0.5rem 0.4rem;text-align:center;">'
        f'<div style="font-size:2rem;font-weight:700;color:#0e1117;'
        f'line-height:1.3;">{value_html}</div>'
        f'<div style="margin-top:0.2rem;">{change}</div>'
        f'</div></div>'
    )


def sub_card(label, value_html, change, bg_color):
    """Display-only sub-card with tinted background."""
    return (
        f'<div style="background:{bg_color};border-radius:0.4rem;'
        f'padding:0.45rem 0.4rem;text-align:center;height:110px;'
        f'box-shadow:0 2px 6px rgba(0,0,0,0.13);'
        f'display:flex;flex-direction:column;justify-content:center;">'
        f'<div style="font-size:0.8rem;color:#555;font-weight:600;">{label}</div>'
        f'<div style="font-size:1.4rem;font-weight:700;color:#0e1117;'
        f'line-height:1.3;">{value_html}</div>'
        f'<div style="margin-top:0.15rem;">{change}</div>'
        '</div>'
    )


# =====================================================================
# COLORS & METRIC CONFIG
# =====================================================================

COLORS = {
    "customers": {"header": "#004D40", "sub": "#E0F2F1"},
    "items":     {"header": "#4E342E", "sub": "#F5EBE6"},
    "revenues":  {"header": "#4A148C", "sub": "#EDE7F6"},
    "stops":     {"header": "#1A5276", "sub": "#E3EEF6"},
}

METRIC_CONFIG = {
    "customers":    {"key": "customers",    "label": "Total Customers",     "category": "customers", "is_currency": False},
    "items":        {"key": "items",        "label": "Total Items",         "category": "items",     "is_currency": False},
    "revenues":     {"key": "revenues",     "label": "Total Revenue",       "category": "revenues",  "is_currency": True},
    "stops":        {"key": "stops",        "label": "Total Stops",         "category": "stops",     "is_currency": False},
    "clients":      {"key": "clients",      "label": "Clients",            "category": "customers", "is_currency": False},
    "subscribers":  {"key": "subscribers",  "label": "Subscribers",         "category": "customers", "is_currency": False},
    "items_client": {"key": "items_client", "label": "Client Items",        "category": "items",     "is_currency": False},
    "items_sub":    {"key": "items_sub",    "label": "Subscriber Items",    "category": "items",     "is_currency": False},
    "rev_client":   {"key": "rev_client",   "label": "Client Revenue",      "category": "revenues",  "is_currency": True},
    "rev_sub":      {"key": "rev_sub",      "label": "Subscriber Revenue",  "category": "revenues",  "is_currency": True},
    "deliveries":   {"key": "deliveries",   "label": "Deliveries",          "category": "stops",     "is_currency": False},
    "pickups":      {"key": "pickups",      "label": "Pickups",             "category": "stops",     "is_currency": False},
}


# =====================================================================
# FORMAT HELPERS
# =====================================================================

def fmt_count(v):
    return f"{v:,}"


def fmt_dhs(v):
    """Main card: Dirham SVG sized to match 2rem text (~28px)."""
    return (
        f'<img src="data:image/svg+xml;base64,{_DIRHAM_B64}" '
        f'style="height:1.6rem;vertical-align:baseline;margin-right:0.25rem;" '
        f'alt="Dhs" />{v:,.0f}'
    )


def fmt_dhs_sub(v):
    """Sub-card: Dirham SVG sized to match 1.4rem text (~20px)."""
    return (
        f'<img src="data:image/svg+xml;base64,{_DIRHAM_B64}" '
        f'style="height:1.15rem;vertical-align:baseline;margin-right:0.2rem;" '
        f'alt="Dhs" />{v:,.0f}'
    )


# =====================================================================
# DATABASE CONNECTION
# =====================================================================

@st.cache_resource
def get_connection():
    """Open the file-based analytics DuckDB (with indexes & views).

    Falls back to in-memory CSV ingestion if the .duckdb file is missing
    or corrupted.  Shows st.error + st.stop if no data source is available.
    """
    db_tmp = DB_PATH.with_suffix('.duckdb.tmp')
    db_file = None

    if DB_PATH.exists() and db_tmp.exists():
        # Use whichever is newer
        db_file = db_tmp if db_tmp.stat().st_mtime > DB_PATH.stat().st_mtime else DB_PATH
    elif DB_PATH.exists():
        db_file = DB_PATH
    elif db_tmp.exists():
        db_file = db_tmp

    if db_file:
        try:
            con = duckdb.connect(str(db_file), read_only=True)
            tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
            for required in ("sales", "dim_period"):
                if required not in tables:
                    raise RuntimeError(f"Table '{required}' missing from {db_file.name}")
            return con
        except Exception as e:
            st.warning(f"Could not open {db_file.name}: {e}. Falling back to CSV.")

    # CSV fallback — validate files exist
    missing = [p for p in [SALES_CSV, ITEMS_CSV, DIMPERIOD_CSV] if not Path(p).exists()]
    if missing:
        st.error(
            "Required data files not found. Run the ETL pipeline first.\n\nMissing:\n"
            + "\n".join(f"- {m}" for m in missing)
        )
        st.stop()

    con = duckdb.connect()
    con.execute(f"CREATE TABLE sales AS SELECT * FROM read_csv_auto('{SALES_CSV}')")
    con.execute(f"CREATE TABLE items AS SELECT * FROM read_csv_auto('{ITEMS_CSV}')")
    con.execute(f"CREATE TABLE dim_period AS SELECT * FROM read_csv_auto('{DIMPERIOD_CSV}')")
    return con


# =====================================================================
# MEASURES
# =====================================================================

def fetch_measures(con, month):
    """Return all snapshot measures for a given YearMonth string."""
    cust_row = con.execute("""
        SELECT
            COUNT(DISTINCT s.CustomerID_Std),
            COUNT(DISTINCT CASE
                WHEN s.Transaction_Type = 'Subscription' THEN s.CustomerID_Std
            END)
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Transaction_Type <> 'Invoice Payment'
          AND s.Earned_Date IS NOT NULL
          AND p.YearMonth = $1
    """, [month]).fetchone()
    customers = int(cust_row[0])
    subscribers = int(cust_row[1])
    clients = customers - subscribers

    items_row = con.execute("""
        SELECT
            COALESCE(SUM(sub.qty), 0),
            COALESCE(SUM(CASE WHEN sub.iss = 0 THEN sub.qty END), 0),
            COALESCE(SUM(CASE WHEN sub.iss = 1 THEN sub.qty END), 0)
        FROM (
            SELECT i.Quantity AS qty,
                   COALESCE(sd.IsSubscriptionService, 0) AS iss
            FROM items i
            JOIN dim_period p ON i.ItemDate = p.Date
            LEFT JOIN (
                SELECT DISTINCT OrderID_Std, IsSubscriptionService FROM sales
            ) sd ON i.OrderID_Std = sd.OrderID_Std
            WHERE p.YearMonth = $1
        ) sub
    """, [month]).fetchone()
    items_total = int(items_row[0])
    items_client = int(items_row[1])
    items_sub = int(items_row[2])

    rev_row = con.execute("""
        SELECT
            COALESCE(SUM(s.Total_Num), 0),
            COALESCE(SUM(CASE
                WHEN s.Transaction_Type = 'Order' AND s.IsSubscriptionService = 0
                THEN s.Total_Num END), 0),
            COALESCE(SUM(CASE
                WHEN s.Transaction_Type = 'Subscription' THEN s.Total_Num
                WHEN s.Transaction_Type = 'Order' AND s.IsSubscriptionService = 1
                THEN s.Total_Num END), 0)
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL
          AND p.YearMonth = $1
    """, [month]).fetchone()
    rev_total = float(rev_row[0])
    rev_client = float(rev_row[1])
    rev_sub = float(rev_row[2])

    stops_row = con.execute("""
        SELECT
            COALESCE(SUM(s.HasDelivery), 0),
            COALESCE(SUM(s.HasPickup), 0)
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL
          AND p.YearMonth = $1
    """, [month]).fetchone()
    deliveries = int(stops_row[0])
    pickups = int(stops_row[1])

    return {
        "customers": customers, "clients": clients, "subscribers": subscribers,
        "items": items_total, "items_client": items_client, "items_sub": items_sub,
        "revenues": rev_total, "rev_client": rev_client, "rev_sub": rev_sub,
        "deliveries": deliveries, "pickups": pickups, "stops": deliveries + pickups,
    }


@st.cache_data(ttl=300)
def fetch_measures_batch(_con, months_tuple):
    """Fetch all measures for multiple months in 4 batched SQL queries."""
    months = list(months_tuple)
    placeholders = ", ".join(f"'{m}'" for m in months)

    cust_df = _con.execute(f"""
        SELECT p.YearMonth,
               COUNT(DISTINCT s.CustomerID_Std) AS customers,
               COUNT(DISTINCT CASE
                   WHEN s.Transaction_Type = 'Subscription' THEN s.CustomerID_Std
               END) AS subscribers
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Transaction_Type <> 'Invoice Payment'
          AND s.Earned_Date IS NOT NULL
          AND p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth
    """).df()

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

    rev_df = _con.execute(f"""
        SELECT p.YearMonth,
               COALESCE(SUM(s.Total_Num), 0) AS rev_total,
               COALESCE(SUM(CASE
                   WHEN s.Transaction_Type = 'Order' AND s.IsSubscriptionService = 0
                   THEN s.Total_Num END), 0) AS rev_client,
               COALESCE(SUM(CASE
                   WHEN s.Transaction_Type = 'Subscription' THEN s.Total_Num
                   WHEN s.Transaction_Type = 'Order' AND s.IsSubscriptionService = 1
                   THEN s.Total_Num END), 0) AS rev_sub
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL
          AND p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth
    """).df()

    stops_df = _con.execute(f"""
        SELECT p.YearMonth,
               COALESCE(SUM(s.HasDelivery), 0) AS deliveries,
               COALESCE(SUM(s.HasPickup), 0) AS pickups
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL
          AND p.YearMonth IN ({placeholders})
        GROUP BY p.YearMonth
    """).df()

    result = {}
    for m in months:
        c_row = cust_df[cust_df["YearMonth"] == m]
        customers = int(c_row["customers"].iloc[0]) if len(c_row) else 0
        subscribers = int(c_row["subscribers"].iloc[0]) if len(c_row) else 0

        i_row = items_df[items_df["ym"] == m]
        items_total = int(i_row["items_total"].iloc[0]) if len(i_row) else 0
        items_client = int(i_row["items_client"].iloc[0]) if len(i_row) else 0
        items_sub = int(i_row["items_sub"].iloc[0]) if len(i_row) else 0

        r_row = rev_df[rev_df["YearMonth"] == m]
        rev_total = float(r_row["rev_total"].iloc[0]) if len(r_row) else 0.0
        rev_client = float(r_row["rev_client"].iloc[0]) if len(r_row) else 0.0
        rev_sub = float(r_row["rev_sub"].iloc[0]) if len(r_row) else 0.0

        s_row = stops_df[stops_df["YearMonth"] == m]
        deliveries = int(s_row["deliveries"].iloc[0]) if len(s_row) else 0
        pickups = int(s_row["pickups"].iloc[0]) if len(s_row) else 0

        result[m] = {
            "customers": customers, "clients": customers - subscribers,
            "subscribers": subscribers,
            "items": items_total, "items_client": items_client,
            "items_sub": items_sub,
            "revenues": rev_total, "rev_client": rev_client, "rev_sub": rev_sub,
            "deliveries": deliveries, "pickups": pickups,
            "stops": deliveries + pickups,
        }
    return result


# =====================================================================
# TREND CHART
# =====================================================================

def format_month_label(ym):
    y, m = ym.split("-")
    return datetime(int(y), int(m), 1).strftime("%b %Y")


def get_6_month_window(selected_month, available_months):
    """Return up to 6 months ending at selected_month."""
    idx = available_months.index(selected_month)
    start = max(0, idx - 5)
    return available_months[start:idx + 1]



def render_trend_chart_v2(active_key, trend_data, display_months,
                          available_months, config, bar_color,
                          show_title=True):
    """V2 chart: MoM annotations below date labels, taller, more bottom margin."""
    metric_key = config["key"]
    is_currency = config["is_currency"]

    labels = [format_month_label(m) for m in display_months]
    values = [trend_data.get(m, {}).get(metric_key, 0) for m in display_months]

    if is_currency:
        text_labels = [fmt_dirham(v) for v in values]
    else:
        text_labels = [f"{v:,}" for v in values]

    fig = go.Figure(go.Bar(
        x=labels, y=values,
        text=text_labels, textposition="outside",
        textfont=dict(size=13, weight=700),
        marker_color=bar_color, marker_line=dict(width=0),
        cliponaxis=False,
    ))

    for i, m in enumerate(display_months):
        val = values[i]
        m_idx = available_months.index(m) if m in available_months else -1
        prev_m = available_months[m_idx - 1] if m_idx > 0 else None
        prev_val = trend_data.get(prev_m, {}).get(metric_key) if prev_m else None

        if prev_val is not None and prev_val != 0:
            pct = (val - prev_val) / abs(prev_val) * 100
            if pct > 0.5:
                arrow, fg = "\u25b2", "#1b5e20"
            elif pct < -0.5:
                arrow, fg = "\u25bc", "#b71c1c"
            else:
                arrow, fg = "\u25a0", "#f57f17"
            ann_text = f"<b>{arrow} {pct:+.0f}%</b>"
        else:
            ann_text = "\u2014"
            fg = "#999"

        fig.add_annotation(
            x=labels[i], y=-0.22, text=ann_text, showarrow=False,
            font=dict(size=13, color=fg),
            xref="x", yref="paper",
        )

    top_margin = 70 if show_title else 45

    fig.update_layout(
        title=dict(text=config["label"], font=dict(size=16, weight=700)) if show_title else dict(text=""),
        height=400,
        margin=dict(t=top_margin, b=110, l=50, r=30),
        paper_bgcolor="#ffffff",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(tickfont=dict(size=12), tickmode="array",
                   tickvals=labels, ticktext=labels,
                   fixedrange=True),
        yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.06)",
                   tickfont=dict(size=11),
                   tickprefix="Dhs " if is_currency else "",
                   rangemode="tozero",
                   fixedrange=True),
        bargap=0.35,
        dragmode=False,
    )

    st.plotly_chart(fig, key=f"chart_v2_{active_key}",
                    use_container_width=True,
                    config={"displayModeBar": False,
                            "scrollZoom": False,
                            "staticPlot": False})


# =====================================================================
# GLOBAL STYLES
# =====================================================================

def inject_global_styles():
    """Inject the shared CSS styles into the page."""
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Jost:wght@400;600;700&display=swap');

        html, body, [class*="st-"], .stMarkdown, .stSelectbox,
        h1, h2, h3, h4, h5, h6, p, span, div, label {
            font-family: 'Futura', 'Jost', 'Trebuchet MS', sans-serif !important;
        }

        .stApp {
            background-color: #F7F5F0;
            background-image:
                radial-gradient(circle, #ffffff 4.8px, transparent 4.8px),
                radial-gradient(circle, #ffffff 4.8px, transparent 4.8px);
            background-size: 48px 48px;
            background-position: 0 0, 24px 24px;
        }

        .stMarkdown { margin-bottom: 0 !important; }
        div[data-testid="stVerticalBlock"] > div { gap: 0.3rem !important; }
        div[data-testid="stSelectbox"] { max-width: 200px; }

        div[data-testid="stPlotlyChart"] {
            background: #fff;
            border-radius: 0.75rem;
            box-shadow: 0 2px 8px rgba(0,0,0,0.10);
            padding: 1.2rem 1rem;
            overflow: hidden;
            box-sizing: border-box;
        }
        div[data-testid="stPlotlyChart"] iframe,
        div[data-testid="stPlotlyChart"] > div {
            overflow: hidden !important;
        }

        /* page_link — subtle centered nav link */
        a[data-testid="stPageLink-NavLink"] {
            justify-content: center !important;
            font-size: 0.82rem !important;
            opacity: 0.7;
            transition: opacity 0.15s ease;
        }
        a[data-testid="stPageLink-NavLink"]:hover {
            opacity: 1;
        }

        /* back link on detail pages — left-aligned, muted */
        .detail-back a[data-testid="stPageLink-NavLink"] {
            justify-content: flex-start !important;
            font-size: 0.85rem !important;
            opacity: 0.6;
        }
        .detail-back a[data-testid="stPageLink-NavLink"]:hover {
            opacity: 1;
        }
    </style>
    """, unsafe_allow_html=True)


# =====================================================================
# MONTH SELECTOR (shared between all pages)
# =====================================================================

def month_selector(con, show_title=True):
    """Render title + month dropdown.  Returns (selected_month, available_months).

    Persists the selected month in st.session_state so navigating
    between pages keeps the same month selected.
    """
    months_df = con.execute("""
        SELECT DISTINCT p.YearMonth
        FROM sales s
        JOIN dim_period p ON s.OrderCohortMonth = p.Date
        WHERE s.Earned_Date IS NOT NULL
        ORDER BY p.YearMonth
    """).df()

    if len(months_df) == 0:
        st.error("No data found.")
        st.stop()

    available_months = months_df["YearMonth"].tolist()
    month_labels = [format_month_label(m) for m in available_months]
    label_to_ym = dict(zip(month_labels, available_months))
    ym_to_label = dict(zip(available_months, month_labels))

    reversed_labels = list(reversed(month_labels))

    # Determine default index from persisted session state
    default_idx = 0  # most recent month
    stored = st.session_state.get("selected_month")
    if stored and stored in ym_to_label:
        stored_label = ym_to_label[stored]
        if stored_label in reversed_labels:
            default_idx = reversed_labels.index(stored_label)

    if show_title:
        st.title("LOOMI Monthly Report")

    selected_label = st.selectbox(
        "Month", options=reversed_labels,
        index=default_idx, label_visibility="collapsed",
    )
    selected_month = label_to_ym[selected_label]

    # Persist selection for cross-page navigation
    st.session_state["selected_month"] = selected_month

    return selected_month, available_months


# =====================================================================
# DETAIL PAGE CONFIG & SHARED RENDERER
# =====================================================================

PAGE_CONFIG = {
    "customers": {
        "title": "Active Customers (Monthly Look-back)",
        "color": "#004D40",
        "headline_metric": "customers",
        "headline_format": fmt_count,
        "sub_metrics": [
            {"label": "Clients",     "key": "clients",     "format": fmt_count},
            {"label": "Subscribers", "key": "subscribers",  "format": fmt_count},
        ],
    },
    "items": {
        "title": "Items Processed (Monthly Look-back)",
        "color": "#4E342E",
        "headline_metric": "items",
        "headline_format": fmt_count,
        "sub_metrics": [
            {"label": "Client Items",     "key": "items_client", "format": fmt_count},
            {"label": "Subscriber Items", "key": "items_sub",    "format": fmt_count},
        ],
    },
    "revenues": {
        "title": "Revenue Performance (Monthly Look-back)",
        "color": "#4A148C",
        "headline_metric": "revenues",
        "headline_format": fmt_dhs,
        "sub_metrics": [
            {"label": "Client Revenue",     "key": "rev_client", "format": fmt_dhs_sub},
            {"label": "Subscriber Revenue", "key": "rev_sub",    "format": fmt_dhs_sub},
        ],
    },
    "stops": {
        "title": "Delivery &amp; Pickup Activity (Monthly Look-back)",
        "color": "#1A5276",
        "headline_metric": "stops",
        "headline_format": fmt_count,
        "sub_metrics": [
            {"label": "Deliveries", "key": "deliveries", "format": fmt_count},
            {"label": "Pickups",    "key": "pickups",    "format": fmt_count},
        ],
    },
}


def render_detail_page(page_key):
    """Render a complete detail page for the given metric category."""
    cfg = PAGE_CONFIG[page_key]
    category = page_key

    inject_global_styles()
    con = get_connection()

    # Header row: back link (left) + month selector (right)
    left, right = st.columns([3, 1])
    with left:
        st.markdown('<div class="detail-back">', unsafe_allow_html=True)
        st.page_link("pages/overview.py", label="\u2190 Back to Overview", icon="\U0001f3e0")
        st.markdown('</div>', unsafe_allow_html=True)
    with right:
        selected_month, available_months = month_selector(con, show_title=False)

    st.markdown("---")

    # Page title
    st.markdown(
        f'<h2 style="text-align:center; color:{cfg["color"]}; font-weight:700; '
        f'font-size:1.5rem; margin:0.8rem 0 0.6rem 0; letter-spacing:0.02em;">'
        f'{cfg["title"]}</h2>',
        unsafe_allow_html=True,
    )

    # Fetch data (6-month window + one extra for MoM)
    window = get_6_month_window(selected_month, available_months)
    first_idx = available_months.index(window[0])
    fetch_months = (
        [available_months[first_idx - 1]] if first_idx > 0 else []
    ) + window
    trend_data = fetch_measures_batch(con, tuple(fetch_months))

    cur = trend_data.get(selected_month, {})
    idx = available_months.index(selected_month)
    prev = trend_data.get(available_months[idx - 1], {}) if idx > 0 else {}

    hdr = COLORS[category]["header"]
    sub_bg = COLORS[category]["sub"]
    headline_key = cfg["headline_metric"]

    # Headline card
    st.markdown(
        headline_card(
            METRIC_CONFIG[headline_key]["label"],
            cfg["headline_format"](cur.get(headline_key, 0)),
            change_html(cur.get(headline_key, 0), prev.get(headline_key)),
            hdr,
        ),
        unsafe_allow_html=True,
    )

    st.markdown('<div style="height:1.2rem;"></div>', unsafe_allow_html=True)

    # Headline chart (centered 90% width)
    spacer_l, chart_col, spacer_r = st.columns([1, 18, 1])
    with chart_col:
        render_trend_chart_v2(
            headline_key, trend_data, window, available_months,
            METRIC_CONFIG[headline_key], hdr, show_title=False,
        )

    st.markdown('<div style="height:0.5rem;"></div>', unsafe_allow_html=True)

    # Sub-metrics side by side
    sub1, sub2 = cfg["sub_metrics"]
    c1, c2 = st.columns(2)

    for col, sub in [(c1, sub1), (c2, sub2)]:
        with col:
            st.markdown(
                sub_card(
                    sub["label"], sub["format"](cur.get(sub["key"], 0)),
                    change_html(cur.get(sub["key"], 0), prev.get(sub["key"])),
                    sub_bg,
                ),
                unsafe_allow_html=True,
            )
            st.markdown('<div style="height:0.8rem;"></div>', unsafe_allow_html=True)
            render_trend_chart_v2(
                sub["key"], trend_data, window, available_months,
                METRIC_CONFIG[sub["key"]], hdr, show_title=False,
            )

    # Footer
    st.markdown("---")
    st.caption(f"Data: {SALES_CSV}")
    st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
