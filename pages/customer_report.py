"""
Customer Report page — customer acquisition and per-customer item measures.

Layout: button selectors with value display (left 1/3),
trend chart for active metric (right 2/3).
Heights are pixel-matched: 3 × 150px cards + 2 × 10px gaps = 470px.
"""

import streamlit as st
from datetime import datetime
from dashboard_shared import (
    inject_global_styles, month_selector, get_connection,
    get_6_month_window, render_trend_chart_v2,
    fmt_ratio,
    COLORS, METRIC_CONFIG, SALES_CSV,
)
from customer_report_shared import fetch_customer_measures_batch

# ─── Height constants ─────────────────────────────────────────────────
_CARD_H = 150       # total height per metric group (button + value)
_GAP = 5            # gap between groups
_CHART_H = 500

inject_global_styles()

# Page CSS: zero-gap in card column for pixel-precise layout
st.markdown(f"""
<style>
    /* Remove default gap in the card column so we control all spacing */
    [data-testid="stHorizontalBlock"] > div:first-child
        [data-testid="stVerticalBlock"] > div {{
        gap: 0 !important;
    }}
</style>
""", unsafe_allow_html=True)

con = get_connection()
selected_month, available_months = month_selector(con, show_title=False)

# Page title
hdr_color = COLORS["customer_report"]["header"]
st.markdown(
    f'<h2 style="text-align:center; color:{hdr_color}; font-weight:700; '
    f'font-size:1.5rem; margin:0.8rem 0 0.6rem 0; letter-spacing:0.02em;">'
    f'Customer Report</h2>',
    unsafe_allow_html=True,
)

# Fetch data
window = get_6_month_window(selected_month, available_months)
first_idx = available_months.index(window[0])
fetch_months = (
    [available_months[first_idx - 1]] if first_idx > 0 else []
) + window
trend_data = fetch_customer_measures_batch(con, tuple(fetch_months))

cur = trend_data.get(selected_month, {})
idx = available_months.index(selected_month)
prev = trend_data.get(available_months[idx - 1], {}) if idx > 0 else {}

# ─── Metric definitions ──────────────────────────────────────────────
METRICS = [
    ("Items per Customer",   "items_per_customer"),
    ("Items per Client",     "client_items_per_customer"),
    ("Items per Subscriber", "sub_items_per_customer"),
]

if "cr_active" not in st.session_state:
    st.session_state["cr_active"] = "items_per_customer"


def _set_active(key):
    st.session_state["cr_active"] = key


def _value_block(val, current, previous, height):
    """Centered value + MoM delta as a fixed-height HTML block."""
    formatted = fmt_ratio(val)
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


# ─── Layout ───────────────────────────────────────────────────────────
card_col, chart_col = st.columns([1, 2])

with card_col:
    for i, (label, key) in enumerate(METRICS):
        is_active = st.session_state["cr_active"] == key
        val = cur.get(key, 0)

        if i > 0:
            st.markdown(
                f'<div style="height:{_GAP}px;"></div>',
                unsafe_allow_html=True,
            )

        st.button(
            label,
            key=f"btn_{key}",
            on_click=_set_active,
            args=(key,),
            use_container_width=True,
            type="primary" if is_active else "secondary",
        )
        # Value area fills the remaining height (card minus button)
        st.markdown(
            _value_block(val, val, prev.get(key), _CARD_H - 42),
            unsafe_allow_html=True,
        )

with chart_col:
    active_key = st.session_state["cr_active"]
    render_trend_chart_v2(
        active_key, trend_data, window, available_months,
        METRIC_CONFIG[active_key], hdr_color,
        show_title=False, height=_CHART_H,
    )

# ─── Footer ──────────────────────────────────────────────────────────
st.markdown("---")
st.caption(f"Data: {SALES_CSV}")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
