"""Monthly PDF report generator for Moonwalk Analytics.

Generates a 5-page business review PDF for a given period using data
already loaded into DuckDB.  All data fetching reuses existing batch
functions from dashboard_shared and section_data — no new SQL.

Usage:
    from generate_report import generate_monthly_report
    pdf_bytes = generate_monthly_report(con, "2025-01", available_periods)
"""

from __future__ import annotations

from datetime import datetime

from fpdf import FPDF

from dashboard_shared import (
    compute_fetch_periods,
    fetch_measures_batch,
    fmt_dirham,
    format_period_label,
)


# ─── PDF STYLE CONSTANTS ────────────────────────────────────────────────────

_PRIMARY = (74, 20, 140)      # #4A148C — revenue purple
_CUSTOMER = (0, 105, 92)      # #00695C — teal
_ITEM = (93, 64, 55)          # #5D4037 — brown
_STOP = (84, 110, 122)        # #546E7A — blue-grey
_PAYMENT = (69, 90, 100)      # #455A64 — dark blue-grey
_OPS = (191, 54, 12)          # #BF360C — deep orange
_DARK = (14, 17, 23)
_GREY = (100, 100, 100)
_LIGHT = (230, 230, 230)


class _ReportPDF(FPDF):
    """Subclass with reusable section helpers."""

    def section_header(self, text: str, r: int, g: int, b: int) -> None:
        """Colored full-width banner with white title text."""
        self.set_fill_color(r, g, b)
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", style="B", size=13)
        self.cell(0, 10, text, new_x="LMARGIN", new_y="NEXT", fill=True)
        self.set_text_color(*_DARK)
        self.ln(3)

    def metric_row(self, label: str, value: str, change: str = "") -> None:
        """Single metric row: label (left), value (right), optional MoM."""
        self.set_font("Helvetica", size=10)
        self.set_text_color(*_GREY)
        self.cell(70, 7, label)
        self.set_font("Helvetica", style="B", size=10)
        self.set_text_color(*_DARK)
        self.cell(50, 7, value)
        if change:
            self.set_font("Helvetica", size=9)
            self.set_text_color(*_GREY)
            self.cell(0, 7, change, new_x="LMARGIN", new_y="NEXT")
        else:
            self.ln(7)

    def divider(self) -> None:
        self.set_draw_color(*_LIGHT)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(3)

    def kv_table(self, rows: list[tuple[str, str]], col_widths: list[int] | None = None) -> None:
        """Simple 2-column key-value table."""
        cw = col_widths or [80, 100]
        for label, value in rows:
            self.set_font("Helvetica", size=10)
            self.set_text_color(*_GREY)
            self.cell(cw[0], 7, label)
            self.set_font("Helvetica", style="B", size=10)
            self.set_text_color(*_DARK)
            self.cell(cw[1], 7, value, new_x="LMARGIN", new_y="NEXT")


# ─── SECTION RENDERERS ──────────────────────────────────────────────────────


def _cover_page(pdf: _ReportPDF, period_label: str) -> None:
    pdf.add_page()
    pdf.set_y(60)
    pdf.set_font("Helvetica", style="B", size=26)
    pdf.set_text_color(*_PRIMARY)
    pdf.cell(0, 12, "Moonwalk Business Review", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(6)
    pdf.set_font("Helvetica", size=16)
    pdf.set_text_color(*_DARK)
    pdf.cell(0, 9, f"Period: {period_label}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)
    pdf.set_font("Helvetica", size=11)
    pdf.set_text_color(*_GREY)
    pdf.cell(0, 7, f"Generated: {datetime.now().strftime('%d %b %Y')}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(40)
    pdf.divider()
    pdf.set_font("Helvetica", style="I", size=9)
    pdf.set_text_color(*_GREY)
    pdf.cell(0, 6, "Confidential - Internal Use Only", align="C")


def _mom_str(current: float, previous: float | None) -> str:
    """Return plain-text MoM change string, e.g. '+12%' or '--'."""
    if previous is None or previous == 0:
        return "--"
    pct = (current - previous) / abs(previous) * 100
    arrow = "+" if pct >= 0 else ""
    return f"MoM: {arrow}{pct:.0f}%"


def _executive_section(
    pdf: _ReportPDF,
    measures: dict,
    yoy: dict,
    selected_period: str,
    available_periods: list,
) -> None:
    pdf.add_page()
    pdf.section_header("Executive Pulse", *_PRIMARY)

    p_idx = available_periods.index(selected_period) if selected_period in available_periods else -1
    prev_period = available_periods[p_idx - 1] if p_idx > 0 else None

    cur = measures.get(selected_period, {})
    prev = measures.get(prev_period, {}) if prev_period else {}
    yoy_cur = yoy.get(selected_period, {}) if yoy else {}

    rows: list[tuple[str, str, str]] = [
        (
            "Customers",
            f"{cur.get('customers', 0):,}",
            _mom_str(cur.get("customers", 0), prev.get("customers")),
        ),
        (
            "Items",
            f"{cur.get('items', 0):,}",
            _mom_str(cur.get("items", 0), prev.get("items")),
        ),
        (
            "Revenue",
            fmt_dirham(cur.get("revenues", 0)),
            _mom_str(cur.get("revenues", 0), prev.get("revenues")),
        ),
        (
            "Stops",
            f"{cur.get('stops', 0):,}",
            _mom_str(cur.get("stops", 0), prev.get("stops")),
        ),
    ]
    for label, value, change in rows:
        pdf.metric_row(label, value, change)
    pdf.ln(3)
    pdf.divider()

    # YoY sub-section
    if yoy_cur:
        pdf.set_font("Helvetica", style="B", size=10)
        pdf.set_text_color(*_GREY)
        pdf.cell(0, 7, "vs Prior Year", new_x="LMARGIN", new_y="NEXT")
        yoy_rows: list[tuple[str, str]] = [
            ("Customers", f"{yoy_cur.get('customers', 0):,}"),
            ("Items", f"{yoy_cur.get('items', 0):,}"),
            ("Revenue", fmt_dirham(yoy_cur.get("revenues", 0))),
        ]
        pdf.kv_table(yoy_rows)

    # Clients / Subscribers split
    pdf.ln(3)
    pdf.divider()
    pdf.set_font("Helvetica", style="B", size=10)
    pdf.set_text_color(*_GREY)
    pdf.cell(0, 7, "Customer Split", new_x="LMARGIN", new_y="NEXT")
    pdf.kv_table([
        ("Clients", f"{cur.get('clients', 0):,}"),
        ("Subscribers", f"{cur.get('subscribers', 0):,}"),
        ("Client Revenue", fmt_dirham(cur.get("rev_client", 0))),
        ("Subscriber Revenue", fmt_dirham(cur.get("rev_sub", 0))),
    ])


def _customer_section(pdf: _ReportPDF, con, selected_period: str) -> None:
    from customer_report_shared import fetch_new_customer_detail_batch
    from section_data import fetch_rfm_snapshot

    pdf.add_page()
    pdf.section_header("Customer Analytics", *_CUSTOMER)

    # Acquisition
    acq = fetch_new_customer_detail_batch(con, (selected_period,))
    cur = acq.get(selected_period, {})

    pdf.set_font("Helvetica", style="B", size=10)
    pdf.set_text_color(*_GREY)
    pdf.cell(0, 7, "Acquisition Split", new_x="LMARGIN", new_y="NEXT")
    pdf.kv_table([
        ("New Customers", f"{cur.get('new_customers', 0):,}"),
        ("Existing Customers", f"{cur.get('existing_customers', 0):,}"),
        ("New Revenue", fmt_dirham(cur.get("new_revenue", 0))),
        ("Existing Revenue", fmt_dirham(cur.get("existing_revenue", 0))),
    ])
    pdf.ln(3)
    pdf.divider()

    # RFM segments
    rfm_df = fetch_rfm_snapshot(con, selected_period)
    if len(rfm_df) >= 5:
        import pandas as pd

        rfm_df = rfm_df.copy()
        rfm_df["r_score"] = pd.qcut(rfm_df["recency"], 5, labels=[5, 4, 3, 2, 1]).astype(int)
        rfm_df["f_score"] = pd.qcut(rfm_df["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]).astype(int)

        def _seg(row):
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
            return "Other"

        rfm_df["segment"] = rfm_df.apply(_seg, axis=1)
        seg_counts = (
            rfm_df.groupby("segment")
            .agg(customers=("CustomerID_Std", "count"), revenue=("monetary", "sum"))
            .reset_index()
            .sort_values("customers", ascending=False)
        )

        pdf.set_font("Helvetica", style="B", size=10)
        pdf.set_text_color(*_GREY)
        pdf.cell(0, 7, "RFM Segments", new_x="LMARGIN", new_y="NEXT")

        # Table header
        pdf.set_fill_color(*_CUSTOMER)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", style="B", size=9)
        pdf.cell(60, 7, "Segment", fill=True)
        pdf.cell(40, 7, "Customers", fill=True)
        pdf.cell(0, 7, "Revenue", fill=True, new_x="LMARGIN", new_y="NEXT")

        pdf.set_text_color(*_DARK)
        pdf.set_font("Helvetica", size=9)
        for _, row in seg_counts.iterrows():
            pdf.cell(60, 6, str(row["segment"]))
            pdf.cell(40, 6, f"{int(row['customers']):,}")
            pdf.cell(0, 6, fmt_dirham(row["revenue"]), new_x="LMARGIN", new_y="NEXT")


def _operations_section(pdf: _ReportPDF, con, fetch_periods: tuple) -> None:
    from section_data import fetch_logistics_batch, fetch_operations_batch

    pdf.add_page()
    pdf.section_header("Operations Center", *_OPS)

    lg_data = fetch_logistics_batch(con, fetch_periods)
    selected_period = fetch_periods[-1]
    lg_cur = lg_data.get(selected_period, {})

    # Logistics summary
    pdf.set_font("Helvetica", style="B", size=10)
    pdf.set_text_color(*_GREY)
    pdf.cell(0, 7, "Logistics", new_x="LMARGIN", new_y="NEXT")
    pdf.kv_table([
        ("Total Stops", f"{lg_cur.get('lg_total_stops', 0):,}"),
        ("Deliveries", f"{lg_cur.get('lg_deliveries', 0):,}"),
        ("Pickups", f"{lg_cur.get('lg_pickups', 0):,}"),
        ("Items Delivered", f"{lg_cur.get('lg_items_delivered', 0):,}"),
        ("Rev per Delivery", fmt_dirham(lg_cur.get("lg_rev_per_delivery", 0))),
    ])
    pdf.ln(3)
    pdf.divider()

    # Geography split
    geo = lg_cur.get("geo", {})
    inside = geo.get("Inside Abu Dhabi", {})
    outer = geo.get("Outer Abu Dhabi", {})
    pdf.set_font("Helvetica", style="B", size=10)
    pdf.set_text_color(*_GREY)
    pdf.cell(0, 7, "Geography", new_x="LMARGIN", new_y="NEXT")
    pdf.kv_table([
        ("Inside Abu Dhabi - Stops", f"{inside.get('stops', 0):,}"),
        ("Inside Abu Dhabi - Revenue", fmt_dirham(inside.get("revenue", 0))),
        ("Outer Abu Dhabi - Stops", f"{outer.get('stops', 0):,}"),
        ("Outer Abu Dhabi - Revenue", fmt_dirham(outer.get("revenue", 0))),
    ])
    pdf.ln(3)
    pdf.divider()

    # Top 5 categories
    ops_data = fetch_operations_batch(con, fetch_periods)
    ops_cur = ops_data.get(selected_period, {})
    categories = ops_cur.get("categories", {})
    if categories:
        sorted_cats = sorted(categories.items(), key=lambda x: x[1].get("items", 0), reverse=True)
        pdf.set_font("Helvetica", style="B", size=10)
        pdf.set_text_color(*_GREY)
        pdf.cell(0, 7, "Top Categories by Items", new_x="LMARGIN", new_y="NEXT")
        pdf.kv_table(
            [(cat, f"{data.get('items', 0):,} items") for cat, data in sorted_cats[:5]]
        )


def _financial_section(pdf: _ReportPDF, con, selected_period: str, available_periods: list) -> None:
    from section_data import fetch_outstanding, fetch_payments_batch

    pdf.add_page()
    pdf.section_header("Financial Performance", *_PAYMENT)

    p_idx = available_periods.index(selected_period) if selected_period in available_periods else -1
    prev_period = available_periods[p_idx - 1] if p_idx > 0 else None

    pm_data = fetch_payments_batch(con, (selected_period,) if not prev_period else (prev_period, selected_period))
    pm_cur = pm_data.get(selected_period, {})
    pm_prev = pm_data.get(prev_period, {}) if prev_period else {}

    # Collections
    rev = pm_cur.get("pm_revenue", 0) or 0
    coll = pm_cur.get("pm_total_collections", 0) or 0
    coll_rate = coll / rev if rev > 0 else 0
    prev_rev = (pm_prev.get("pm_revenue", 0) or 0) if pm_prev else 0

    pdf.set_font("Helvetica", style="B", size=10)
    pdf.set_text_color(*_GREY)
    pdf.cell(0, 7, "Collections", new_x="LMARGIN", new_y="NEXT")
    pdf.kv_table([
        ("Revenue", fmt_dirham(rev)),
        ("Total Collections", fmt_dirham(coll)),
        ("Collection Rate", f"{coll_rate * 100:.1f}%"),
        ("Stripe", fmt_dirham(pm_cur.get("pm_stripe", 0))),
        ("Terminal", fmt_dirham(pm_cur.get("pm_terminal", 0))),
        ("Cash", fmt_dirham(pm_cur.get("pm_cash", 0))),
    ])
    pdf.ln(3)
    pdf.divider()

    # Payment cycle
    pdf.set_font("Helvetica", style="B", size=10)
    pdf.set_text_color(*_GREY)
    pdf.cell(0, 7, "Payment Cycle (CC_2025 orders)", new_x="LMARGIN", new_y="NEXT")
    dtp = pm_cur.get("pm_avg_days_to_payment", 0) or 0
    pdf.kv_table([("Avg Days to Payment", f"{dtp:.1f} days")])
    pdf.ln(3)
    pdf.divider()

    # Outstanding
    outstanding = fetch_outstanding(con)
    total_out = outstanding.get("total_outstanding", 0)
    order_count = outstanding.get("order_count", 0)
    pdf.set_font("Helvetica", style="B", size=10)
    pdf.set_text_color(*_GREY)
    pdf.cell(0, 7, "Outstanding (CC_2025)", new_x="LMARGIN", new_y="NEXT")
    pdf.kv_table([
        ("Total Outstanding", fmt_dirham(total_out)),
        ("Open Orders", f"{order_count:,}"),
    ])

    # Top 10 outstanding
    top20_df = outstanding.get("top20")
    if top20_df is not None and len(top20_df) > 0:
        pdf.ln(3)
        pdf.set_font("Helvetica", style="B", size=9)
        pdf.set_text_color(*_GREY)
        pdf.cell(0, 6, "Top Outstanding Orders", new_x="LMARGIN", new_y="NEXT")
        pdf.set_fill_color(*_PAYMENT)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", style="B", size=8)
        pdf.cell(70, 6, "Customer", fill=True)
        pdf.cell(30, 6, "Amount", fill=True)
        pdf.cell(0, 6, "Days", fill=True, new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(*_DARK)
        pdf.set_font("Helvetica", size=8)
        for _, row in top20_df.head(10).iterrows():
            name = str(row.get("CustomerName", ""))[:28]
            amount = fmt_dirham(row.get("Total_Num", 0))
            days = str(int(row.get("days_outstanding", 0)))
            pdf.cell(70, 5, name)
            pdf.cell(30, 5, amount)
            pdf.cell(0, 5, days, new_x="LMARGIN", new_y="NEXT")


# ─── PUBLIC API ──────────────────────────────────────────────────────────────


def generate_monthly_report(con, selected_period: str, available_periods: list) -> bytes:
    """Return PDF bytes for the Moonwalk monthly business review.

    Parameters:
        con              -- open DuckDB connection (analytics database)
        selected_period  -- period string, e.g. "2025-01"
        available_periods -- ordered list of all available periods

    Returns:
        bytes — the generated PDF content
    """
    from section_data import fetch_yoy_batch

    window, fetch_periods = compute_fetch_periods(selected_period, available_periods)
    period_label = format_period_label(selected_period)

    measures = fetch_measures_batch(con, tuple(fetch_periods))
    try:
        yoy = fetch_yoy_batch(con, tuple(fetch_periods))
    except Exception:
        yoy = {}

    pdf = _ReportPDF()
    pdf.set_margins(left=15, top=15, right=15)
    pdf.set_auto_page_break(auto=True, margin=15)

    _cover_page(pdf, period_label)
    _executive_section(pdf, measures, yoy, selected_period, available_periods)
    _customer_section(pdf, con, selected_period)
    _operations_section(pdf, con, tuple(fetch_periods))
    _financial_section(pdf, con, selected_period, available_periods)

    return bytes(pdf.output())
