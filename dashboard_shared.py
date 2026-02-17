"""
Shared utilities for the LOOMI Monthly Report multi-page dashboard.

Provides: DB connection, Dirham formatting, card rendering, SQL measures,
trend chart rendering, global CSS injection, and month selector.
"""

import streamlit as st
import duckdb
import plotly.graph_objects as go
import time
import json
import logging
from datetime import datetime, date
from pathlib import Path

# =====================================================================
# DATA PATHS (centralized in config.py)
# =====================================================================

from config import SALES_CSV, ITEMS_CSV, DIMPERIOD_CSV, DB_PATH, LOGS_PATH

_dash_logger = logging.getLogger("dashboard.profiling")


def _log_query_time(func_name: str, elapsed: float, periods: int = 0) -> None:
    """Log query timing for dashboard profiling."""
    _dash_logger.debug(f"[QUERY] {func_name}: {elapsed:.3f}s ({periods} periods)")


def write_dashboard_profile(timings: dict) -> None:
    """Write dashboard profiling results to JSON (called once per session)."""
    LOGS_PATH.mkdir(parents=True, exist_ok=True)
    profile_path = LOGS_PATH / f"dashboard_profile_{datetime.now():%Y-%m-%d_%H%M%S}.json"
    profile = {"timestamp": datetime.now().isoformat(), "queries": timings}
    profile_path.write_text(json.dumps(profile, indent=2))

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

def change_html(current, previous, size="normal"):
    """Return HTML for a MoM change pill with colored background.

    Parameters:
        size -- "normal" (default) or "compact" for sub-metric rows
    """
    if size == "compact":
        dash_style = 'font-size:0.7rem;color:#aaa;'
        pill_style = 'font-size:0.65rem;font-weight:700;padding:0.1rem 0.4rem;border-radius:0.5rem;'
    else:
        dash_style = 'font-size:0.8rem;color:#aaa;'
        pill_style = 'font-size:0.8rem;font-weight:700;padding:0.2rem 0.6rem;border-radius:0.75rem;letter-spacing:0.02em;'

    if previous is None or previous == 0:
        return f'<span style="{dash_style}">&mdash;</span>'
    pct = (current - previous) / abs(previous) * 100
    if pct > 0.5:
        arrow, bg, fg = "\u25b2", "#a5d6a7", "#1b5e20"
    elif pct < -0.5:
        arrow, bg, fg = "\u25bc", "#ef9a9a", "#b71c1c"
    else:
        arrow, bg, fg = "\u25a0", "#fff176", "#f57f17"
    return (
        f'<span style="display:inline-block;background:{bg};color:{fg};'
        f'{pill_style}">'
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


def headline_card_with_subs(label, value_html, change, header_color, subs):
    """Headline card with integrated sub-metric rows.

    Parameters:
        label        -- main card heading text
        value_html   -- formatted main value (HTML safe)
        change       -- MoM change HTML from change_html()
        header_color -- colored header banner background
        subs         -- list of (sub_label, sub_value_html, sub_change_html) tuples
    """
    html = (
        f'<div class="headline-card-wrap" style="border-radius:0.75rem;overflow:hidden;'
        f'background:#fff;box-shadow:0 4px 16px rgba(0,0,0,0.18);'
        f'transition:transform 0.15s ease, box-shadow 0.15s ease;">'
        f'<div style="background:{header_color};padding:0.5rem 0;text-align:center;">'
        f'<span style="color:#fff;font-weight:700;font-size:0.95rem;'
        f'letter-spacing:0.04em;">{label}</span></div>'
        f'<div style="padding:0.5rem 0.5rem 0.3rem;text-align:center;">'
        f'<div style="font-size:2rem;font-weight:700;color:#0e1117;'
        f'line-height:1.3;">{value_html}</div>'
        f'<div style="margin-top:0.2rem;">{change}</div>'
        f'</div>'
    )
    if subs:
        html += (
            '<div style="border-top:1px solid #eee;margin:0 0.5rem;"></div>'
            '<div style="padding:0.3rem 0.6rem 0.5rem;">'
        )
        for sub_label, sub_val, sub_chg in subs:
            html += (
                f'<div style="display:flex;justify-content:space-between;'
                f'align-items:center;padding:0.2rem 0;">'
                f'<span style="font-size:0.82rem;color:#666;">{sub_label}</span>'
                f'<span style="font-size:0.95rem;font-weight:700;color:#0e1117;">'
                f'{sub_val} {sub_chg}</span>'
                f'</div>'
            )
        html += '</div>'
    # "View Details" footer cue
    html += (
        '<div style="border-top:1px solid #eee;margin:0 0.5rem;"></div>'
        '<div style="text-align:center;padding:0.3rem 0;color:#999;'
        'font-size:0.78rem;">View Details &rarr;</div>'
    )
    html += '</div>'
    return html


def sub_card(label, value_html, change, bg_color):
    """Display-only sub-card with tinted background."""
    return (
        f'<div style="background:{bg_color};border-radius:0.4rem;'
        f'padding:0.45rem 0.4rem;text-align:center;min-height:110px;'
        f'box-shadow:0 2px 6px rgba(0,0,0,0.13);'
        f'display:flex;flex-direction:column;justify-content:center;">'
        f'<div style="font-size:0.8rem;color:#555;font-weight:600;">{label}</div>'
        f'<div style="font-size:1.4rem;font-weight:700;color:#0e1117;'
        f'line-height:1.3;">{value_html}</div>'
        f'<div style="margin-top:0.15rem;">{change}</div>'
        '</div>'
    )


def detail_card(title, rows, title_color, bg_color):
    """Render a titled detail box with key-value rows inside.

    Parameters:
        title       -- card heading text
        rows        -- list of (label, value_html, change_html) tuples
        title_color -- color for the heading text
        bg_color    -- card background color
    """
    html = (
        f'<div style="background:{bg_color};border-radius:0.75rem;padding:1rem;'
        f'box-shadow:0 2px 6px rgba(0,0,0,0.1);">'
        f'<h4 style="text-align:center;color:{title_color};font-weight:700;'
        f'font-size:0.95rem;margin:0 0 0.6rem 0;">{title}</h4>'
    )
    for label, value_html, chg_html in rows:
        html += (
            f'<div style="display:flex;justify-content:space-between;'
            f'align-items:center;padding:0.3rem 0.5rem;">'
            f'<span style="font-size:0.85rem;color:#555;font-weight:600;">{label}</span>'
            f'<span style="font-size:1.1rem;font-weight:700;">{value_html} {chg_html}</span>'
            f'</div>'
        )
    html += '</div>'
    return html


# =====================================================================
# COLORS & METRIC CONFIG
# =====================================================================

COLORS = {
    # Primary — boldest treatment for the "money" metric
    "revenues":          {"header": "#4A148C", "sub": "#F3F0F8"},
    # Secondary — important but not financial
    "customers":         {"header": "#00695C", "sub": "#F0F7F6"},
    "items":             {"header": "#5D4037", "sub": "#F5F1EF"},
    # Tertiary — operational / analytical, neutral tones
    "stops":             {"header": "#546E7A", "sub": "#F2F5F7"},
    "customer_report":   {"header": "#795548", "sub": "#F5F1EF"},
    "customer_insights": {"header": "#795548", "sub": "#F5F1EF"},
    "cohort":            {"header": "#2E7D32", "sub": "#F1F7F1"},
    "logistics":         {"header": "#37474F", "sub": "#F2F4F5"},
    "payments":          {"header": "#455A64", "sub": "#F2F4F5"},
    # Accent — needs contrast for interactive selectors
    "operations":        {"header": "#BF360C", "sub": "#FCF3F0"},
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
    "deliveries":       {"key": "deliveries",       "label": "Deliveries",          "category": "stops",           "is_currency": False, "is_percentage": False},
    "pickups":          {"key": "pickups",          "label": "Pickups",             "category": "stops",           "is_currency": False, "is_percentage": False},
    "active_customers": {"key": "active_customers", "label": "Active Customers",    "category": "customer_report", "is_currency": False, "is_percentage": False},
    "new_customers":    {"key": "new_customers",    "label": "New Customers",       "category": "customer_report", "is_currency": False, "is_percentage": False},
    "new_customer_pct":         {"key": "new_customer_pct",         "label": "New Customer %",             "category": "customer_report", "is_currency": False, "is_percentage": True},
    "items_per_customer":        {"key": "items_per_customer",        "label": "Items per Customer",         "category": "customer_report", "is_currency": False, "is_percentage": False, "is_ratio": True},
    "sub_items_per_customer":    {"key": "sub_items_per_customer",    "label": "Items per Subscriber",       "category": "customer_report", "is_currency": False, "is_percentage": False, "is_ratio": True},
    "client_items_per_customer": {"key": "client_items_per_customer", "label": "Items per Client",           "category": "customer_report", "is_currency": False, "is_percentage": False, "is_ratio": True},
    "sub_items_pct":             {"key": "sub_items_pct",             "label": "Subscriber Items %",         "category": "customer_report", "is_currency": False, "is_percentage": True},
    "cr_items_sub":              {"key": "cr_items_sub",              "label": "Subscriber Items",           "category": "customer_report", "is_currency": False, "is_percentage": False},
    "cr_items_client":           {"key": "cr_items_client",           "label": "Client Items",               "category": "customer_report", "is_currency": False, "is_percentage": False},
    "rev_per_customer":          {"key": "rev_per_customer",          "label": "Revenue per Customer",       "category": "customer_report", "is_currency": True},
    "rev_per_client":            {"key": "rev_per_client",            "label": "Revenue per Client",         "category": "customer_report", "is_currency": True},
    "rev_per_subscriber":        {"key": "rev_per_subscriber",        "label": "Revenue per Subscriber",     "category": "customer_report", "is_currency": True},
    "existing_customers":        {"key": "existing_customers",        "label": "Existing Customers",         "category": "customer_report", "is_currency": False},
    "new_items":                 {"key": "new_items",                 "label": "New Customer Items",         "category": "customer_report", "is_currency": False},
    "existing_items":            {"key": "existing_items",            "label": "Existing Customer Items",    "category": "customer_report", "is_currency": False},
    "new_revenue":               {"key": "new_revenue",               "label": "New Customer Revenue",       "category": "customer_report", "is_currency": True},
    "existing_revenue":          {"key": "existing_revenue",          "label": "Existing Customer Revenue",  "category": "customer_report", "is_currency": True},
    # 02 — Customer Insights
    "ci_active_customers":   {"key": "ci_active_customers",   "label": "Active Customers",          "category": "customer_insights", "is_currency": False},
    "ci_multi_service":      {"key": "ci_multi_service",      "label": "Multi Service Customers",   "category": "customer_insights", "is_currency": False},
    "ci_spend_threshold":    {"key": "ci_spend_threshold",    "label": "Spend Threshold",           "category": "customer_insights", "is_currency": True},
    "ci_top20_spend_rev":    {"key": "ci_top20_spend_rev",    "label": "Top 20% Revenue (Spend)",   "category": "customer_insights", "is_currency": True},
    "ci_spend_share":        {"key": "ci_spend_share",        "label": "Revenue Share (Spend)",     "category": "customer_insights", "is_currency": False, "is_percentage": True},
    "ci_volume_threshold":   {"key": "ci_volume_threshold",   "label": "Volume Threshold",          "category": "customer_insights", "is_currency": False},
    "ci_top20_vol_rev":      {"key": "ci_top20_vol_rev",      "label": "Top 20% Revenue (Volume)",  "category": "customer_insights", "is_currency": True},
    "ci_volume_share":       {"key": "ci_volume_share",       "label": "Revenue Share (Volume)",    "category": "customer_insights", "is_currency": False, "is_percentage": True},
    # 03 — Cohort Analysis
    "m0_customers":          {"key": "m0_customers",          "label": "M0 Customers",              "category": "cohort", "is_currency": False},
    "m0_items":              {"key": "m0_items",              "label": "M0 Items",                  "category": "cohort", "is_currency": False},
    "m0_revenue":            {"key": "m0_revenue",            "label": "M0 Revenue",                "category": "cohort", "is_currency": True},
    "m0_rev_per_customer":   {"key": "m0_rev_per_customer",   "label": "M0 Rev/Customer",           "category": "cohort", "is_currency": True},
    "m0_items_per_customer": {"key": "m0_items_per_customer", "label": "M0 Items/Customer",         "category": "cohort", "is_currency": False, "is_ratio": True},
    "m1_customers":          {"key": "m1_customers",          "label": "M1 Customers",              "category": "cohort", "is_currency": False},
    "m1_items":              {"key": "m1_items",              "label": "M1 Items",                  "category": "cohort", "is_currency": False},
    "m1_revenue":            {"key": "m1_revenue",            "label": "M1 Revenue",                "category": "cohort", "is_currency": True},
    "m1_rev_per_customer":   {"key": "m1_rev_per_customer",   "label": "M1 Rev/Customer",           "category": "cohort", "is_currency": True},
    "m1_items_per_customer": {"key": "m1_items_per_customer", "label": "M1 Items/Customer",         "category": "cohort", "is_currency": False, "is_ratio": True},
    # 04 — Logistics
    "lg_total_stops":        {"key": "lg_total_stops",        "label": "Total Stops",               "category": "logistics", "is_currency": False},
    "lg_items_delivered":    {"key": "lg_items_delivered",    "label": "Items Delivered",            "category": "logistics", "is_currency": False},
    "lg_delivery_rev_pct":   {"key": "lg_delivery_rev_pct",  "label": "Delivery Rev %",             "category": "logistics", "is_currency": False, "is_percentage": True},
    "lg_delivery_rate":      {"key": "lg_delivery_rate",     "label": "Delivery Rate %",            "category": "logistics", "is_currency": False, "is_percentage": True},
    "lg_deliveries":         {"key": "lg_deliveries",        "label": "Deliveries",                 "category": "logistics", "is_currency": False},
    "lg_pickups":            {"key": "lg_pickups",           "label": "Pickups",                    "category": "logistics", "is_currency": False},
    # 06 — Payments
    "pm_revenue":            {"key": "pm_revenue",            "label": "Revenues",                  "category": "payments", "is_currency": True},
    "pm_total_collections":  {"key": "pm_total_collections",  "label": "Total Collections",         "category": "payments", "is_currency": True},
    "pm_stripe":             {"key": "pm_stripe",             "label": "Stripe",                    "category": "payments", "is_currency": True},
    "pm_terminal":           {"key": "pm_terminal",           "label": "Terminal",                  "category": "payments", "is_currency": True},
    "pm_cash":                {"key": "pm_cash",                "label": "Cash",                      "category": "payments", "is_currency": True},
    "pm_avg_days_to_payment": {"key": "pm_avg_days_to_payment","label": "Avg Days To Payment",      "category": "payments", "is_currency": False, "is_ratio": True},
    # 05 — Operations: processing efficiency
    "ops_avg_processing_time": {"key": "ops_avg_processing_time", "label": "Avg Processing Time", "category": "operations", "is_currency": False, "is_ratio": True},
    "ops_avg_time_in_store":   {"key": "ops_avg_time_in_store",   "label": "Avg Time In Store",   "category": "operations", "is_currency": False, "is_ratio": True},
    # 05 — Operations: category breakdowns (5 categories x 2 metrics)
    "cat_professional_wear_items": {"key": "cat_professional_wear_items", "label": "Professional Wear Items",   "category": "operations", "is_currency": False},
    "cat_professional_wear_rev":   {"key": "cat_professional_wear_rev",   "label": "Professional Wear Revenue", "category": "operations", "is_currency": True},
    "cat_traditional_wear_items":  {"key": "cat_traditional_wear_items",  "label": "Traditional Wear Items",    "category": "operations", "is_currency": False},
    "cat_traditional_wear_rev":    {"key": "cat_traditional_wear_rev",    "label": "Traditional Wear Revenue",  "category": "operations", "is_currency": True},
    "cat_home_linens_items":       {"key": "cat_home_linens_items",       "label": "Home Linens Items",         "category": "operations", "is_currency": False},
    "cat_home_linens_rev":         {"key": "cat_home_linens_rev",         "label": "Home Linens Revenue",       "category": "operations", "is_currency": True},
    "cat_extras_items":            {"key": "cat_extras_items",            "label": "Extras Items",              "category": "operations", "is_currency": False},
    "cat_extras_rev":              {"key": "cat_extras_rev",              "label": "Extras Revenue",            "category": "operations", "is_currency": True},
    "cat_others_items":            {"key": "cat_others_items",            "label": "Others Items",              "category": "operations", "is_currency": False},
    "cat_others_rev":              {"key": "cat_others_rev",              "label": "Others Revenue",            "category": "operations", "is_currency": True},
    # 05 — Operations: service type breakdowns (4 services x 2 metrics)
    "svc_wash_and_press_items":    {"key": "svc_wash_and_press_items",    "label": "Wash & Press Items",        "category": "operations", "is_currency": False},
    "svc_wash_and_press_rev":      {"key": "svc_wash_and_press_rev",      "label": "Wash & Press Revenue",      "category": "operations", "is_currency": True},
    "svc_dry_cleaning_items":      {"key": "svc_dry_cleaning_items",      "label": "Dry Cleaning Items",        "category": "operations", "is_currency": False},
    "svc_dry_cleaning_rev":        {"key": "svc_dry_cleaning_rev",        "label": "Dry Cleaning Revenue",      "category": "operations", "is_currency": True},
    "svc_press_only_items":        {"key": "svc_press_only_items",        "label": "Press Only Items",          "category": "operations", "is_currency": False},
    "svc_press_only_rev":          {"key": "svc_press_only_rev",          "label": "Press Only Revenue",        "category": "operations", "is_currency": True},
    "svc_other_service_items":     {"key": "svc_other_service_items",     "label": "Other Service Items",       "category": "operations", "is_currency": False},
    "svc_other_service_rev":       {"key": "svc_other_service_rev",       "label": "Other Service Revenue",     "category": "operations", "is_currency": True},
}


# =====================================================================
# FORMAT HELPERS
# =====================================================================

def fmt_count(v):
    """Format numeric value with thousand separators."""
    return f"{v:,}"


def fmt_pct(v):
    """Format a ratio (0.0–1.0) as a percentage string, e.g. '25.0%'."""
    return f"{v * 100:.1f}%"


def fmt_ratio(v):
    """Format a decimal ratio with 1 decimal place, e.g. '4.2'."""
    return f"{v:,.1f}"


def fmt_dhs(v):
    """Format Dirham value with inline SVG symbol for main card display (~28px)."""
    return (
        f'<img src="data:image/svg+xml;base64,{_DIRHAM_B64}" '
        f'style="height:1.6rem;vertical-align:baseline;margin-right:0.25rem;" '
        f'alt="Dhs" />{v:,.0f}'
    )


def fmt_dhs_sub(v):
    """Format Dirham value with inline SVG symbol for sub-metric rows (~20px)."""
    return (
        f'<img src="data:image/svg+xml;base64,{_DIRHAM_B64}" '
        f'style="height:1.12rem;vertical-align:baseline;margin-right:0.2rem;" '
        f'alt="Dhs" />{v:,.0f}'
    )


def fmt_days(v):
    """Format a float as days with 1 decimal place, e.g. '3.2 days'."""
    return f"{v:,.1f} days"


def is_weekly(period_str):
    """Check if a period string represents weekly granularity."""
    return "W" in str(period_str)


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
            # Create order_lookup if missing (backward compat with older DB files)
            if "order_lookup" not in tables:
                con.execute("""
                    CREATE TABLE order_lookup AS
                    SELECT DISTINCT OrderID_Std, IsSubscriptionService FROM sales
                """)
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
    con.execute("""
        CREATE TABLE order_lookup AS
        SELECT DISTINCT OrderID_Std, IsSubscriptionService FROM sales
    """)
    return con


# =====================================================================
# MEASURES
# =====================================================================

def get_grain_context(period_or_periods):
    """Return dict with period_col and sales_join for current grain."""
    sample = period_or_periods[0] if isinstance(period_or_periods, (list, tuple)) else period_or_periods
    weekly = is_weekly(sample)
    return {
        "period_col": "p.ISOWeekLabel" if weekly else "p.YearMonth",
        "sales_join": "s.Earned_Date = p.Date" if weekly else "s.OrderCohortMonth = p.Date",
    }


def fetch_measures(con, period):
    """Return all snapshot measures for a given period string (monthly or weekly)."""
    ctx = get_grain_context(period)
    period_col, sales_join = ctx["period_col"], ctx["sales_join"]

    cust_row = con.execute(f"""
        SELECT
            COUNT(DISTINCT s.CustomerID_Std),
            COUNT(DISTINCT CASE
                WHEN s.Transaction_Type = 'Subscription' THEN s.CustomerID_Std
            END)
        FROM sales s
        JOIN dim_period p ON {sales_join}
        WHERE s.Transaction_Type <> 'Invoice Payment'
          AND s.Earned_Date IS NOT NULL
          AND {period_col} = $1
    """, [period]).fetchone()
    customers = int(cust_row[0])
    subscribers = int(cust_row[1])
    clients = customers - subscribers

    items_row = con.execute(f"""
        SELECT
            COALESCE(SUM(sub.qty), 0),
            COALESCE(SUM(CASE WHEN sub.iss = 0 THEN sub.qty END), 0),
            COALESCE(SUM(CASE WHEN sub.iss = 1 THEN sub.qty END), 0)
        FROM (
            SELECT i.Quantity AS qty,
                   COALESCE(ol.IsSubscriptionService, 0) AS iss
            FROM items i
            JOIN dim_period p ON i.ItemDate = p.Date
            LEFT JOIN order_lookup ol ON i.OrderID_Std = ol.OrderID_Std
            WHERE {period_col} = $1
        ) sub
    """, [period]).fetchone()
    items_total = int(items_row[0])
    items_client = int(items_row[1])
    items_sub = int(items_row[2])

    rev_row = con.execute(f"""
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
        JOIN dim_period p ON {sales_join}
        WHERE s.Earned_Date IS NOT NULL
          AND {period_col} = $1
    """, [period]).fetchone()
    rev_total = float(rev_row[0])
    rev_client = float(rev_row[1])
    rev_sub = float(rev_row[2])

    stops_row = con.execute(f"""
        SELECT
            COALESCE(SUM(s.HasDelivery), 0),
            COALESCE(SUM(s.HasPickup), 0)
        FROM sales s
        JOIN dim_period p ON {sales_join}
        WHERE s.Earned_Date IS NOT NULL
          AND {period_col} = $1
    """, [period]).fetchone()
    deliveries = int(stops_row[0])
    pickups = int(stops_row[1])

    return {
        "customers": customers, "clients": clients, "subscribers": subscribers,
        "items": items_total, "items_client": items_client, "items_sub": items_sub,
        "revenues": rev_total, "rev_client": rev_client, "rev_sub": rev_sub,
        "deliveries": deliveries, "pickups": pickups, "stops": deliveries + pickups,
    }


@st.cache_data(ttl=300)
def fetch_measures_batch(_con, periods_tuple):
    """Fetch all measures for multiple periods in 4 batched SQL queries."""
    _t0 = time.perf_counter()
    periods = list(periods_tuple)
    ctx = get_grain_context(periods)
    period_col, sales_join = ctx["period_col"], ctx["sales_join"]
    placeholders = ", ".join(f"'{p}'" for p in periods)

    cust_df = _con.execute(f"""
        SELECT {period_col} AS period,
               COUNT(DISTINCT s.CustomerID_Std) AS customers,
               COUNT(DISTINCT CASE
                   WHEN s.Transaction_Type = 'Subscription' THEN s.CustomerID_Std
               END) AS subscribers
        FROM sales s
        JOIN dim_period p ON {sales_join}
        WHERE s.Transaction_Type <> 'Invoice Payment'
          AND s.Earned_Date IS NOT NULL
          AND {period_col} IN ({placeholders})
        GROUP BY {period_col}
    """).df()

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

    stops_df = _con.execute(f"""
        SELECT {period_col} AS period,
               COALESCE(SUM(s.HasDelivery), 0) AS deliveries,
               COALESCE(SUM(s.HasPickup), 0) AS pickups
        FROM sales s
        JOIN dim_period p ON {sales_join}
        WHERE s.Earned_Date IS NOT NULL
          AND {period_col} IN ({placeholders})
        GROUP BY {period_col}
    """).df()

    result = {}
    for p in periods:
        c_row = cust_df[cust_df["period"] == p]
        customers = int(c_row["customers"].iloc[0]) if len(c_row) else 0
        subscribers = int(c_row["subscribers"].iloc[0]) if len(c_row) else 0

        i_row = items_df[items_df["period"] == p]
        items_total = int(i_row["items_total"].iloc[0]) if len(i_row) else 0
        items_client = int(i_row["items_client"].iloc[0]) if len(i_row) else 0
        items_sub = int(i_row["items_sub"].iloc[0]) if len(i_row) else 0

        r_row = rev_df[rev_df["period"] == p]
        rev_total = float(r_row["rev_total"].iloc[0]) if len(r_row) else 0.0
        rev_client = float(r_row["rev_client"].iloc[0]) if len(r_row) else 0.0
        rev_sub = float(r_row["rev_sub"].iloc[0]) if len(r_row) else 0.0

        s_row = stops_df[stops_df["period"] == p]
        deliveries = int(s_row["deliveries"].iloc[0]) if len(s_row) else 0
        pickups = int(s_row["pickups"].iloc[0]) if len(s_row) else 0

        result[p] = {
            "customers": customers, "clients": customers - subscribers,
            "subscribers": subscribers,
            "items": items_total, "items_client": items_client,
            "items_sub": items_sub,
            "revenues": rev_total, "rev_client": rev_client, "rev_sub": rev_sub,
            "deliveries": deliveries, "pickups": pickups,
            "stops": deliveries + pickups,
        }
    _log_query_time("fetch_measures_batch", time.perf_counter() - _t0, len(periods))
    return result


# =====================================================================
# TREND CHART
# =====================================================================

def format_period_label(period):
    """Format a period string for display. Monthly: 'Feb 2025', Weekly: '3 Feb '26'."""
    if is_weekly(period):
        parts = period.split("-W")
        year, week = int(parts[0]), int(parts[1])
        monday = date.fromisocalendar(year, week, 1)
        return f"{monday.day} {monday.strftime('%b %y')}"
    y, m = period.split("-")
    return datetime(int(y), int(m), 1).strftime("%b %Y")


# Backward compatibility
format_month_label = format_period_label


def get_display_window(selected_period, available_periods):
    """Return display window: 6 periods (monthly) or 13 periods (weekly)."""
    count = 13 if is_weekly(selected_period) else 6
    idx = available_periods.index(selected_period)
    start = max(0, idx - (count - 1))
    return available_periods[start:idx + 1]


# Backward compatibility
get_6_month_window = get_display_window


def compute_fetch_periods(selected_period, available_periods):
    """Compute display window + fetch periods (prepend 1 prior for MoM)."""
    window = get_display_window(selected_period, available_periods)
    first_idx = available_periods.index(window[0])
    fetch_periods = ([available_periods[first_idx - 1]] if first_idx > 0 else []) + window
    return window, fetch_periods


def render_trend_chart_v2(active_key, trend_data, display_periods,
                          available_periods, config, bar_color,
                          show_title=True, height=400):
    """V2 chart: period-over-period annotations below date labels.

    Compact charts (height < 400) hide below-bar annotations and show
    MoM change in hover tooltip instead, saving ~70px of vertical space.
    """
    metric_key = config["key"]
    is_currency = config["is_currency"]
    is_percentage = config.get("is_percentage", False)
    is_ratio = config.get("is_ratio", False)

    labels = [format_period_label(m) for m in display_periods]
    values = [trend_data.get(m, {}).get(metric_key, 0) for m in display_periods]

    if is_percentage:
        text_labels = [fmt_pct(v) for v in values]
    elif is_ratio:
        text_labels = [fmt_ratio(v) for v in values]
    elif is_currency:
        text_labels = [fmt_dirham(v) for v in values]
    else:
        text_labels = [f"{v:,}" for v in values]

    # Reduce text size for weekly charts (13 bars)
    weekly = len(display_periods) > 6
    bar_text_size = 10 if weekly else 13
    ann_text_size = 10 if weekly else 13

    # Compact charts: skip below-bar annotations, use hover instead
    show_annotations = height >= 400

    # Build per-bar MoM change data for hover on compact charts
    mom_texts = []
    for i, m in enumerate(display_periods):
        val = values[i]
        m_idx = available_periods.index(m) if m in available_periods else -1
        prev_m = available_periods[m_idx - 1] if m_idx > 0 else None
        prev_val = trend_data.get(prev_m, {}).get(metric_key) if prev_m else None
        if prev_val is not None and prev_val != 0:
            pct = (val - prev_val) / abs(prev_val) * 100
            mom_texts.append(f"{pct:+.0f}%")
        else:
            mom_texts.append("--")

    hover_template = (
        "<b>%{x}</b><br>%{text}<br>MoM: %{customdata}<extra></extra>"
        if not show_annotations else None
    )

    fig = go.Figure(go.Bar(
        x=labels, y=values,
        text=text_labels, textposition="outside",
        textfont=dict(size=bar_text_size, weight=700),
        marker_color=bar_color, marker_line=dict(width=0),
        cliponaxis=False,
        customdata=mom_texts if not show_annotations else None,
        hovertemplate=hover_template,
    ))

    if show_annotations:
        for i, m in enumerate(display_periods):
            val = values[i]
            m_idx = available_periods.index(m) if m in available_periods else -1
            prev_m = available_periods[m_idx - 1] if m_idx > 0 else None
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
                font=dict(size=ann_text_size, color=fg),
                xref="x", yref="paper",
            )

    top_margin = 70 if show_title else 45
    bot_margin = 60 if not show_annotations else 110

    fig.update_layout(
        title=dict(text=config["label"], font=dict(size=16, weight=700)) if show_title else dict(text=""),
        height=height,
        margin=dict(t=top_margin, b=bot_margin, l=50, r=30),
        paper_bgcolor="#ffffff",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(tickfont=dict(size=10 if weekly else 12), tickmode="array",
                   tickvals=labels, ticktext=labels,
                   tickangle=-45 if weekly else 0,
                   fixedrange=True),
        yaxis=dict(showgrid=True, gridcolor="rgba(0,0,0,0.06)",
                   tickfont=dict(size=11),
                   tickformat=".0%" if is_percentage else (".1f" if is_ratio else ""),
                   tickprefix="" if (is_percentage or is_ratio) else ("Dhs " if is_currency else ""),
                   rangemode="tozero",
                   fixedrange=True),
        bargap=0.25 if weekly else 0.35,
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
        h1, h2, h3, h4, h5, h6, p, label {
            font-family: 'Jost', 'Futura', 'Trebuchet MS', sans-serif !important;
        }
        /* Preserve Material Symbols ligature rendering */
        [data-testid="stIconMaterial"],
        [class*="material-symbols"] {
            font-family: 'Material Symbols Rounded' !important;
        }

        .stApp {
            background-color: #F7F5F0;
        }

        .stMarkdown { margin-bottom: 0 !important; }
        div[data-testid="stVerticalBlock"] > div { gap: 0.6rem !important; }
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

        /* sidebar — muted background to match app theme */
        section[data-testid="stSidebar"] {
            background-color: #EFEDE8;
        }
        section[data-testid="stSidebar"] [data-testid="stSidebarNav"] {
            padding-top: 1rem;
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

        /* headline card hover — lift effect for interactivity cue */
        .headline-card-wrap:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(0,0,0,0.22) !important;
            cursor: pointer;
        }

        /* pills — compact granularity toggle */
        div[data-testid="stPills"] button {
            font-size: 0.85rem !important;
            padding: 0.25rem 0.75rem !important;
        }
    </style>
    """, unsafe_allow_html=True)


# =====================================================================
# PERIOD SELECTOR (shared between all pages)
# =====================================================================

def period_selector(con, show_title=True):
    """Render title + period dropdown with segmented monthly/weekly control.

    Returns (selected_period, available_periods).
    Persists selection in st.session_state for cross-page navigation.
    """
    if show_title:
        st.title("LOOMI Monthly Report")

    # Persist toggle across page navigation (widget keys get cleaned up)
    if "_weekly_persist" not in st.session_state:
        st.session_state["_weekly_persist"] = False

    # Segmented control: pills widget
    current_default = "Weekly" if st.session_state["_weekly_persist"] else "Monthly"
    selected_grain = st.pills(
        "Granularity", options=["Monthly", "Weekly"],
        default=current_default, label_visibility="collapsed",
    )
    weekly_mode = selected_grain == "Weekly"
    st.session_state["_weekly_persist"] = weekly_mode

    # Cache period lists in session state (clears on browser refresh)
    cache_key = "_cached_periods_weekly" if weekly_mode else "_cached_periods_monthly"
    if cache_key not in st.session_state:
        _ps_t0 = time.perf_counter()
        if weekly_mode:
            periods_df = con.execute("""
                SELECT DISTINCT p.ISOWeekLabel
                FROM sales s
                JOIN dim_period p ON s.Earned_Date = p.Date
                WHERE s.Earned_Date IS NOT NULL
                ORDER BY p.ISOWeekLabel
            """).df()
            if len(periods_df) == 0:
                st.error("No data found.")
                st.stop()
            st.session_state[cache_key] = periods_df["ISOWeekLabel"].tolist()
        else:
            periods_df = con.execute("""
                SELECT DISTINCT p.YearMonth
                FROM sales s
                JOIN dim_period p ON s.OrderCohortMonth = p.Date
                WHERE s.Earned_Date IS NOT NULL
                ORDER BY p.YearMonth
            """).df()
            if len(periods_df) == 0:
                st.error("No data found.")
                st.stop()
            st.session_state[cache_key] = periods_df["YearMonth"].tolist()
        _log_query_time("period_selector", time.perf_counter() - _ps_t0)
    available_periods = st.session_state[cache_key]

    period_labels = [format_period_label(p) for p in available_periods]
    label_to_period = dict(zip(period_labels, available_periods))
    period_to_label = dict(zip(available_periods, period_labels))

    reversed_labels = list(reversed(period_labels))

    # Determine default index from persisted session state
    default_idx = 0  # most recent period
    stored = st.session_state.get("selected_period")
    if stored and stored in period_to_label:
        stored_label = period_to_label[stored]
        if stored_label in reversed_labels:
            default_idx = reversed_labels.index(stored_label)

    selected_label = st.selectbox(
        "Period", options=reversed_labels,
        index=default_idx, label_visibility="collapsed",
    )
    selected_period = label_to_period[selected_label]

    # Persist selection for cross-page navigation
    st.session_state["selected_period"] = selected_period
    st.session_state["selected_month"] = selected_period  # backward compat

    return selected_period, available_periods


# Backward compatibility
month_selector = period_selector


# =====================================================================
# SHARED PAGE HELPERS
# =====================================================================

def render_page_header(con):
    """Render the standard detail-page header: back link (left) + period selector (right).

    Returns (selected_period, available_periods).
    """
    left, right = st.columns([2, 1])
    with left:
        st.markdown('<div class="detail-back">', unsafe_allow_html=True)
        st.page_link("pages/overview.py", label="\u2190 Back to Overview", icon="\U0001f3e0")
        st.markdown('</div>', unsafe_allow_html=True)
    with right:
        selected_period, available_periods = period_selector(con, show_title=False)
    st.markdown("---")
    return selected_period, available_periods


def render_section_heading(text, color):
    """Render a centered section sub-heading with consistent styling."""
    st.markdown(
        f'<h3 style="text-align:center; color:{color}; font-weight:600; '
        f'font-size:1.1rem; margin:1.2rem 0 0.5rem 0;">{text}</h3>',
        unsafe_allow_html=True,
    )


def render_page_title(text, color):
    """Render a centered page title with consistent styling."""
    st.markdown(
        f'<h2 style="text-align:center; color:{color}; font-weight:700; '
        f'font-size:1.5rem; margin:0.8rem 0 0.6rem 0; letter-spacing:0.02em;">'
        f'{text}</h2>',
        unsafe_allow_html=True,
    )


def render_footer():
    """Render the standard page footer with data freshness timestamp."""
    st.markdown("---")
    db_path = Path(DB_PATH)
    if db_path.exists():
        mtime = datetime.fromtimestamp(db_path.stat().st_mtime)
        label = f"{mtime.day} {mtime.strftime('%b %Y')}"
    else:
        now = datetime.now()
        label = f"{now.day} {now.strftime('%b %Y')}"
    st.caption(f":clock1: Data as of {label}")


# =====================================================================
# DETAIL PAGE CONFIG & SHARED RENDERER
# =====================================================================

PAGE_CONFIG = {
    "customers": {
        "title": "Active Customers (Monthly Look-back)",
        "color_key": "customers",
        "fetch": "monthly",
        "state_key": "detail_customers",
        "metrics": [
            ("Total Customers", "customers"),
            ("Clients", "clients"),
            ("Subscribers", "subscribers"),
        ],
    },
    "items": {
        "title": "Items Processed (Monthly Look-back)",
        "color_key": "items",
        "fetch": "monthly",
        "state_key": "detail_items",
        "metrics": [
            ("Total Items", "items"),
            ("Client Items", "items_client"),
            ("Subscriber Items", "items_sub"),
        ],
    },
    "revenues": {
        "title": "Revenue Performance (Monthly Look-back)",
        "color_key": "revenues",
        "fetch": "monthly",
        "state_key": "detail_revenues",
        "metrics": [
            ("Total Revenue", "revenues"),
            ("Client Revenue", "rev_client"),
            ("Subscriber Revenue", "rev_sub"),
        ],
    },
    "customer_report": {
        "title": "Items per Customer",
        "color_key": "customer_report",
        "fetch": "customer",
        "state_key": "detail_cr_items",
        "metrics": [
            ("Items per Client", "client_items_per_customer"),
            ("Items per Subscriber", "sub_items_per_customer"),
        ],
    },
    "customer_report_revenue": {
        "title": "Revenue per Customer",
        "color_key": "customer_report",
        "fetch": "customer",
        "state_key": "detail_cr_revenue",
        "metrics": [
            ("Revenue per Client", "rev_per_client"),
            ("Revenue per Subscriber", "rev_per_subscriber"),
        ],
    },
    "new_customers": {
        "title": "New Customer Analysis",
        "color_key": "customer_report",
        "fetch": "new_customer",
        "state_key": "detail_new_customers",
        "monthly_only": True,
        "metrics": [
            ("New Customer Items", "new_items"),
            ("New Customer Revenue", "new_revenue"),
        ],
    },
}


def _set_metric(state_key, key):
    """Callback for metric selector buttons."""
    st.session_state[state_key] = key


def _navigate_to(page):
    """Callback for navigating to a detail page."""
    st.switch_page(page)


def _value_block(data_key, val, current, previous, height):
    """Centered value + MoM delta as a fixed-height HTML block.

    Auto-formats based on METRIC_CONFIG for the given data_key.
    """
    cfg = METRIC_CONFIG[data_key]
    if cfg.get("is_currency"):
        formatted = fmt_dirham(val, 0)
    elif cfg.get("is_ratio"):
        formatted = fmt_ratio(val)
    elif cfg.get("is_percentage"):
        formatted = fmt_pct(val)
    else:
        formatted = fmt_count(val)

    if previous is None or previous == 0:
        delta = '<span style="color:#999;font-size:0.9rem;">&mdash;</span>'
    else:
        pct = (current - previous) / abs(previous) * 100
        if pct > 0.5:
            arrow, color = "\u2191", "#09ab3b"
        elif pct < -0.5:
            arrow, color = "\u2193", "#ff2b2b"
        else:
            arrow, color = "\u2192", "#999"
        delta = (
            f'<span style="color:{color};font-size:0.9rem;font-weight:600;">'
            f'{arrow} {pct:+.0f}%</span>'
        )

    return (
        f'<div style="height:{height}px;display:flex;flex-direction:column;'
        f'justify-content:center;align-items:center;">'
        f'<div style="font-size:2.2rem;font-weight:700;color:#0e1117;'
        f'line-height:1.2;">{formatted}</div>'
        f'<div style="margin-top:0.25rem;">{delta}</div>'
        f'</div>'
    )


def render_metric_selector(metrics, trend_data, window, available_periods,
                           selected_period, state_key, header_color,
                           chart_height=None, detail_link=None):
    """Render interactive button-selector layout: buttons+values (left), chart (right).

    Parameters:
        metrics      -- list of (label, data_key) tuples (1-3 items)
        trend_data   -- dict {period_str: {key: value, ...}}
        window       -- list of display periods
        available_periods -- full list of available periods
        selected_period   -- currently selected period
        state_key    -- session state key for tracking active metric
        header_color -- bar color for the chart
        chart_height -- optional override; defaults based on metric count
        detail_link  -- optional {"page": "...", "label": "..."} for overview use
    """
    n = len(metrics)
    if n >= 3:
        card_h, gap, default_chart_h = 150, 5, 500
    elif n == 2:
        card_h, gap, default_chart_h = 200, 5, 420
    else:
        card_h, gap, default_chart_h = 200, 0, 300

    chart_h = chart_height or default_chart_h

    first_key = metrics[0][1]
    if state_key not in st.session_state:
        st.session_state[state_key] = first_key

    # Zero-gap CSS for card column
    st.markdown("""
    <style>
        [data-testid="stHorizontalBlock"] > div:first-child
            [data-testid="stVerticalBlock"] > div {
            gap: 0 !important;
        }
    </style>
    """, unsafe_allow_html=True)

    cur = trend_data.get(selected_period, {})
    p_idx = available_periods.index(selected_period)
    prev = trend_data.get(available_periods[p_idx - 1], {}) if p_idx > 0 else {}

    card_col, chart_col = st.columns([1, 2])

    with card_col:
        for i, (label, key) in enumerate(metrics):
            is_active = st.session_state[state_key] == key
            val = cur.get(key, 0)

            if i > 0:
                st.markdown(
                    f'<div style="height:{gap}px;"></div>',
                    unsafe_allow_html=True,
                )

            if detail_link and n == 1:
                st.button(
                    label,
                    key=f"btn_{state_key}_{key}",
                    on_click=_navigate_to,
                    args=(detail_link["page"],),
                    use_container_width=True,
                    type="primary",
                )
            else:
                st.button(
                    label,
                    key=f"btn_{state_key}_{key}",
                    on_click=_set_metric,
                    args=(state_key, key),
                    use_container_width=True,
                    type="primary" if is_active else "secondary",
                )
            st.markdown(
                _value_block(key, val, val, prev.get(key), card_h - 42),
                unsafe_allow_html=True,
            )

    with chart_col:
        active_key = st.session_state[state_key]
        render_trend_chart_v2(
            f"{state_key}_{active_key}", trend_data, window, available_periods,
            METRIC_CONFIG[active_key], header_color,
            show_title=False, height=chart_h,
        )


def render_detail_page(page_key):
    """Render a complete detail page for the given metric category."""
    cfg = PAGE_CONFIG[page_key]
    color_key = cfg["color_key"]

    inject_global_styles()
    con = get_connection()

    selected_period, available_periods = render_page_header(con)

    hdr = COLORS[color_key]["header"]
    render_page_title(cfg["title"], hdr)

    # Monthly-only pages: show notice in weekly mode
    if cfg.get("monthly_only") and is_weekly(selected_period):
        st.info("This page is available in monthly view only. Toggle back to monthly to view data.")
        render_footer()
        return

    # Fetch data (display window + one extra for period-over-period)
    window, fetch_periods = compute_fetch_periods(selected_period, available_periods)

    fetch_type = cfg["fetch"]
    if fetch_type == "monthly":
        trend_data = fetch_measures_batch(con, tuple(fetch_periods))
    elif fetch_type == "customer":
        from customer_report_shared import fetch_customer_measures_batch
        trend_data = fetch_customer_measures_batch(con, tuple(fetch_periods))
    elif fetch_type == "new_customer":
        from customer_report_shared import fetch_new_customer_detail_batch
        trend_data = fetch_new_customer_detail_batch(con, tuple(fetch_periods))

    render_metric_selector(
        metrics=cfg["metrics"],
        trend_data=trend_data,
        window=window,
        available_periods=available_periods,
        selected_period=selected_period,
        state_key=cfg["state_key"],
        header_color=hdr,
    )

    render_footer()
