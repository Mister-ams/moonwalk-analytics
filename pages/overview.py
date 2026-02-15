"""
Overview page — 4 compound headline cards with integrated sub-metrics.
"""

import streamlit as st
from dashboard_shared import (
    inject_global_styles, period_selector, get_connection,
    fetch_measures_batch, change_html, headline_card_with_subs,
    render_footer,
    fmt_count, fmt_dhs, fmt_dhs_sub,
    COLORS,
)

inject_global_styles()
con = get_connection()

selected_period, available_periods = period_selector(con, show_title=True)
st.markdown("---")

# ─── Fetch current + previous period via cached batch call ───────────
idx = available_periods.index(selected_period)
fetch_periods = [selected_period]
if idx > 0:
    fetch_periods.insert(0, available_periods[idx - 1])
batch = fetch_measures_batch(con, tuple(fetch_periods))
cur = batch.get(selected_period, {})
prev = batch.get(available_periods[idx - 1]) if idx > 0 else None


def _chg(key):
    return change_html(cur[key], prev[key] if prev else None)


# ─── Compact MoM pill for sub-rows (smaller font) ────────────────────
def _sub_chg(key):
    """Smaller inline change indicator for sub-metric rows."""
    return change_html(cur[key], prev[key] if prev else None, size="compact")


# ─── Compound headline cards (4 columns) ─────────────────────────────
_PAGES = ["pages/customers.py", "pages/items.py", "pages/revenues.py", "pages/logistics.py"]

hcols = st.columns(4)

with hcols[0]:
    st.markdown(
        headline_card_with_subs(
            "Customers", fmt_count(cur["customers"]), _chg("customers"),
            COLORS["customers"]["header"],
            subs=[
                ("Clients", fmt_count(cur["clients"]), _sub_chg("clients")),
                ("Subscribers", fmt_count(cur["subscribers"]), _sub_chg("subscribers")),
            ],
        ),
        unsafe_allow_html=True,
    )
    st.page_link(_PAGES[0], label="View Details \u2192", icon="\U0001f4ca")

with hcols[1]:
    st.markdown(
        headline_card_with_subs(
            "Items", fmt_count(cur["items"]), _chg("items"),
            COLORS["items"]["header"],
            subs=[
                ("Client Items", fmt_count(cur["items_client"]), _sub_chg("items_client")),
                ("Subscriber Items", fmt_count(cur["items_sub"]), _sub_chg("items_sub")),
            ],
        ),
        unsafe_allow_html=True,
    )
    st.page_link(_PAGES[1], label="View Details \u2192", icon="\U0001f4ca")

with hcols[2]:
    st.markdown(
        headline_card_with_subs(
            "Revenues", fmt_dhs(cur["revenues"]), _chg("revenues"),
            COLORS["revenues"]["header"],
            subs=[
                ("Client Revenue", fmt_dhs_sub(cur["rev_client"]), _sub_chg("rev_client")),
                ("Subscriber Revenue", fmt_dhs_sub(cur["rev_sub"]), _sub_chg("rev_sub")),
            ],
        ),
        unsafe_allow_html=True,
    )
    st.page_link(_PAGES[2], label="View Details \u2192", icon="\U0001f4ca")

with hcols[3]:
    st.markdown(
        headline_card_with_subs(
            "Stops", fmt_count(cur["stops"]), _chg("stops"),
            COLORS["stops"]["header"],
            subs=[
                ("Deliveries", fmt_count(cur["deliveries"]), _sub_chg("deliveries")),
                ("Pickups", fmt_count(cur["pickups"]), _sub_chg("pickups")),
            ],
        ),
        unsafe_allow_html=True,
    )
    st.page_link(_PAGES[3], label="View Details \u2192", icon="\U0001f4ca")

# ─── Footer ──────────────────────────────────────────────────────────
render_footer()
