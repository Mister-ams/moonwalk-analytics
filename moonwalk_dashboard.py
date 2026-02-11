"""
LOOMI Monthly Report — Streamlit + DuckDB
4 metric columns with MoM change indicators.

Run with: python -m streamlit run moonwalk_dashboard.py
"""

import streamlit as st
import duckdb
from datetime import datetime

# =====================================================================
# DIRHAM SYMBOL (CBUAE official SVG, base64-encoded for inline use)
# Per CBUAE guidelines: symbol left of numeral, height = text height,
# black for functional use, never alongside "AED", min 12px.
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

def _change_html(current, previous):
    """Return HTML for a MoM change pill with colored background."""
    if previous is None or previous == 0:
        return '<span style="font-size:0.8rem;color:#aaa;">—</span>'
    pct = (current - previous) / abs(previous) * 100
    if pct > 0.5:
        arrow, bg, fg = "▲", "#a5d6a7", "#1b5e20"
    elif pct < -0.5:
        arrow, bg, fg = "▼", "#ef9a9a", "#b71c1c"
    else:
        arrow, bg, fg = "■", "#fff176", "#f57f17"
    return (
        f'<span style="display:inline-block;background:{bg};color:{fg};'
        f'font-size:0.8rem;font-weight:700;padding:0.2rem 0.6rem;'
        f'border-radius:0.75rem;letter-spacing:0.02em;">'
        f'{arrow} {pct:+.0f}%</span>'
    )


def _headline_row(columns):
    """Unified headline row spanning full width.

    *columns* is a list of (label, value_html, change_html, header_color) tuples.
    Renders as a single white card with colored header banners, thin vertical
    separators, and a stronger shadow so it appears elevated above sub-cards.
    """
    n = len(columns)
    cells = []
    for i, (label, value_html, change, hdr) in enumerate(columns):
        border = 'border-right:1px solid #e0e0e0;' if i < n - 1 else ''
        cells.append(
            f'<div style="flex:1;{border}">'
            f'<div style="background:{hdr};padding:0.5rem 0;text-align:center;">'
            f'<span style="color:#fff;font-weight:700;font-size:0.95rem;'
            f'letter-spacing:0.04em;">{label}</span></div>'
            f'<div style="padding:0.5rem 0.5rem 0.4rem;text-align:center;">'
            f'<div style="font-size:2rem;font-weight:700;color:#0e1117;'
            f'line-height:1.3;">{value_html}</div>'
            f'<div style="margin-top:0.2rem;">{change}</div>'
            f'</div></div>'
        )
    return (
        '<div style="display:flex;border-radius:0.75rem;overflow:hidden;'
        'background:#fff;box-shadow:0 4px 16px rgba(0,0,0,0.18);'
        'margin-bottom:0.6rem;">'
        + ''.join(cells)
        + '</div>'
    )


def _sub_card(label, value_html, change, bg_color):
    """Sub-card with tinted background, stacked below the main card."""
    return (
        f'<div style="background:{bg_color};border-radius:0.4rem;'
        f'padding:0.45rem 0.4rem;text-align:center;height:110px;'
        f'box-shadow:0 2px 6px rgba(0,0,0,0.13);'
        f'margin:0 0.6rem 0.4rem 0.6rem;'
        f'display:flex;flex-direction:column;justify-content:center;">'
        f'<div style="font-size:0.8rem;color:#555;font-weight:600;">{label}</div>'
        f'<div style="font-size:1.4rem;font-weight:700;color:#0e1117;'
        f'line-height:1.3;">{value_html}</div>'
        f'<div style="margin-top:0.15rem;">{change}</div>'
        '</div>'
    )


# 60-30-10 color scheme
# 10% accent: header colors (british racing green, brown, purple, teal blue)
# 30% secondary: white main cards + column-tinted sub-cards
# 60% dominant: light polka-dot background
_COLORS = {
    "customers": {"header": "#004D40", "sub": "#E0F2F1"},
    "items":     {"header": "#4E342E", "sub": "#F5EBE6"},
    "revenues":  {"header": "#4A148C", "sub": "#EDE7F6"},
    "stops":     {"header": "#1A5276", "sub": "#E3EEF6"},
}

# =====================================================================
# CONFIGURATION
# =====================================================================

SALES_CSV = r"C:\Users\MRAL-\Downloads\Lime Reporting\All_Sales_Python.csv"
ITEMS_CSV = r"C:\Users\MRAL-\Downloads\Lime Reporting\All_Items_Python.csv"
DIMPERIOD_CSV = r"C:\Users\MRAL-\Downloads\Lime Reporting\DimPeriod_Python.csv"

# =====================================================================
# PAGE SETUP
# =====================================================================

st.set_page_config(
    page_title="LOOMI Monthly Report",
    page_icon="🧼",
    layout="wide",
)

# Global styles
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Jost:wght@400;600;700&display=swap');

    html, body, [class*="st-"], .stMarkdown, .stSelectbox,
    h1, h2, h3, h4, h5, h6, p, span, div, label {
        font-family: 'Futura', 'Jost', 'Trebuchet MS', sans-serif !important;
    }

    /* 60% — offset polka-dot background (white dots, staggered rows) */
    .stApp {
        background-color: #F7F5F0;
        background-image:
            radial-gradient(circle, #ffffff 4.8px, transparent 4.8px),
            radial-gradient(circle, #ffffff 4.8px, transparent 4.8px);
        background-size: 48px 48px;
        background-position: 0 0, 24px 24px;
    }

    /* tighten vertical gap between stacked cards */
    .stMarkdown { margin-bottom: 0 !important; }
    div[data-testid="stVerticalBlock"] > div { gap: 0.3rem !important; }

    /* keep month selector compact under the title */
    div[data-testid="stSelectbox"] { max-width: 200px; }
</style>
""", unsafe_allow_html=True)

# =====================================================================
# DATABASE CONNECTION & DATA LOADING
# =====================================================================

@st.cache_resource
def get_connection():
    """Create a DuckDB connection and load all 3 CSVs as tables."""
    con = duckdb.connect()
    con.execute(f"CREATE TABLE sales AS SELECT * FROM read_csv_auto('{SALES_CSV}')")
    con.execute(f"CREATE TABLE items AS SELECT * FROM read_csv_auto('{ITEMS_CSV}')")
    con.execute(f"CREATE TABLE dim_period AS SELECT * FROM read_csv_auto('{DIMPERIOD_CSV}')")
    return con


con = get_connection()

# =====================================================================
# MEASURES HELPER
# =====================================================================

def _fetch_measures(con, month):
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


# =====================================================================
# MONTH SELECTOR
# =====================================================================

months_df = con.execute("""
    SELECT DISTINCT p.YearMonth
    FROM sales s
    JOIN dim_period p ON s.OrderCohortMonth = p.Date
    WHERE s.Earned_Date IS NOT NULL
    ORDER BY p.YearMonth
""").df()

if len(months_df) == 0:
    st.error("No data found. Please check that the CSV files exist and contain data.")
    st.stop()

available_months = months_df["YearMonth"].tolist()


def _format_month_label(ym):
    y, m = ym.split("-")
    return datetime(int(y), int(m), 1).strftime("%b %Y")


month_labels = [_format_month_label(m) for m in available_months]
label_to_ym = dict(zip(month_labels, available_months))

# =====================================================================
# HEADER — title with month selector as subtitle
# =====================================================================

st.title("LOOMI Monthly Report")
selected_label = st.selectbox(
    "Month",
    options=list(reversed(month_labels)),
    index=0,
    label_visibility="collapsed",
)
selected_month = label_to_ym[selected_label]
st.markdown("---")

# =====================================================================
# COMPUTE CURRENT & PREVIOUS MONTH MEASURES
# =====================================================================

cur = _fetch_measures(con, selected_month)

idx = available_months.index(selected_month)
prev = _fetch_measures(con, available_months[idx - 1]) if idx > 0 else None


def _chg(key):
    return _change_html(cur[key], prev[key] if prev else None)


# =====================================================================
# FORMAT HELPERS
# =====================================================================
# CBUAE: symbol height = text height, symbol left of numeral, no unit label.

def _fmt_count(v):
    return f"{v:,}"


def _fmt_dhs(v):
    """Main card: Dirham SVG sized to match 2rem text (~28px)."""
    return (
        f'<img src="data:image/svg+xml;base64,{_DIRHAM_B64}" '
        f'style="height:1.6rem;vertical-align:baseline;margin-right:0.25rem;" '
        f'alt="Dhs" />{v:,.0f}'
    )


def _fmt_dhs_sub(v):
    """Sub-card: Dirham SVG sized to match 1.4rem text (~20px)."""
    return (
        f'<img src="data:image/svg+xml;base64,{_DIRHAM_B64}" '
        f'style="height:1.15rem;vertical-align:baseline;margin-right:0.2rem;" '
        f'alt="Dhs" />{v:,.0f}'
    )


# =====================================================================
# HEADLINE ROW (unified white card spanning all 4 columns)
# =====================================================================

st.markdown(
    _headline_row([
        ("Customers", _fmt_count(cur["customers"]), _chg("customers"),
         _COLORS["customers"]["header"]),
        ("Items", _fmt_count(cur["items"]), _chg("items"),
         _COLORS["items"]["header"]),
        ("Revenues", _fmt_dhs(cur["revenues"]), _chg("revenues"),
         _COLORS["revenues"]["header"]),
        ("Stops", _fmt_count(cur["stops"]), _chg("stops"),
         _COLORS["stops"]["header"]),
    ]),
    unsafe_allow_html=True,
)

# =====================================================================
# SUB-CARDS (two per column, below the headline row)
# =====================================================================

col1, col2, col3, col4 = st.columns(4)

with col1:
    c = _COLORS["customers"]["sub"]
    st.markdown(_sub_card("Clients", _fmt_count(cur["clients"]),
                          _chg("clients"), c), unsafe_allow_html=True)
    st.markdown(_sub_card("Subscribers", _fmt_count(cur["subscribers"]),
                          _chg("subscribers"), c), unsafe_allow_html=True)

with col2:
    c = _COLORS["items"]["sub"]
    st.markdown(_sub_card("Clients", _fmt_count(cur["items_client"]),
                          _chg("items_client"), c), unsafe_allow_html=True)
    st.markdown(_sub_card("Subscribers", _fmt_count(cur["items_sub"]),
                          _chg("items_sub"), c), unsafe_allow_html=True)

with col3:
    c = _COLORS["revenues"]["sub"]
    st.markdown(_sub_card("Clients", _fmt_dhs_sub(cur["rev_client"]),
                          _chg("rev_client"), c), unsafe_allow_html=True)
    st.markdown(_sub_card("Subscribers", _fmt_dhs_sub(cur["rev_sub"]),
                          _chg("rev_sub"), c), unsafe_allow_html=True)

with col4:
    c = _COLORS["stops"]["sub"]
    st.markdown(_sub_card("Deliveries", _fmt_count(cur["deliveries"]),
                          _chg("deliveries"), c), unsafe_allow_html=True)
    st.markdown(_sub_card("Pickups", _fmt_count(cur["pickups"]),
                          _chg("pickups"), c), unsafe_allow_html=True)

# =====================================================================
# FOOTER
# =====================================================================

st.markdown("---")
st.caption(f"Data: {SALES_CSV}")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.caption("Powered by Streamlit + DuckDB")
