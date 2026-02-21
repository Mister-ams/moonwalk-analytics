"""Post-refresh step: generate structured LLM insights and push to Notion.

Manages a toggleable '📊 Latest Insights' heading block on the Notion portal page.
Each persona callout includes 3 LLM-generated insights plus a direct dashboard link.
On first run the block is created at the end of the page. Subsequent runs clear and refill it.

Called automatically by refresh_cli.py after DuckDB rebuild when NOTION_API_KEY
and OPENAI_API_KEY are set.  Safe to skip — logs and exits cleanly if either key
is missing.

Usage (standalone):
    python notion_push.py
"""

import datetime
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import DB_PATH, DUCKDB_KEY, NOTION_API_KEY, NOTION_PAGE_ID, NOTION_TOKEN

_BASE_URL = "https://loomi-performance-analytics.streamlit.app"
_INSIGHTS_HEADING = "📊 Latest Insights"
_EP_HEADING = "📈 Executive Pulse"

_PERSONA_CONFIG = [
    ("executive_pulse", "🎯", "Executive Pulse", "yellow_background", "", "snapshot"),
    ("customer_analytics", "👥", "Customer Analytics", "green_background", "customer_analytics", "segmentation"),
    ("operations_center", "🚚", "Operations Center", "blue_background", "operations_center", "logistics"),
    (
        "financial_performance",
        "💰",
        "Financial Performance",
        "purple_background",
        "financial_performance",
        "collections",
    ),
]


def _open_db():
    """Open encrypted DuckDB connection using the ATTACH pattern."""
    import duckdb

    con = duckdb.connect(":memory:")
    if DUCKDB_KEY:
        con.execute(f"ATTACH '{DB_PATH}' AS db (ENCRYPTION_KEY '{DUCKDB_KEY}', READ_ONLY)")
    else:
        con.execute(f"ATTACH '{DB_PATH}' AS db (READ_ONLY)")
    con.execute("USE db")
    return con


def _fetch_context(con) -> dict:
    """Fetch current period KPIs + monthly insights rules for the LLM prompt."""
    period_row = con.execute("SELECT MAX(period) FROM insights WHERE granularity = 'monthly'").fetchone()
    if not period_row or period_row[0] is None:
        raise ValueError("No rows in insights table — run cleancloud_to_duckdb.py first")
    period = period_row[0]

    rules = con.execute(
        """
        SELECT category, headline, detail, sentiment
        FROM insights
        WHERE period = ? AND granularity = 'monthly'
        ORDER BY category, rule_id
        """,
        [period],
    ).fetchall()

    kpis = con.execute(
        """
        SELECT period, customers, items, revenues, stops
        FROM (
            SELECT
                OrderCohortMonth                         AS period,
                COUNT(DISTINCT CustomerID_Std)           AS customers,
                COUNT(*)                                  AS items,
                SUM(Total_Num)                            AS revenues,
                SUM(HasDelivery::INT + HasPickup::INT)    AS stops
            FROM sales
            WHERE Is_Earned = 1
              AND OrderCohortMonth IN (
                  SELECT CAST(MAX(period) || '-01' AS DATE) FROM insights WHERE granularity = 'monthly'
                  UNION ALL
                  SELECT DATE_TRUNC('month', CAST(MAX(period) || '-01' AS DATE) - INTERVAL '1 month')::DATE
                  FROM insights WHERE granularity = 'monthly'
              )
            GROUP BY OrderCohortMonth
        )
        ORDER BY period
        """
    ).fetchall()

    return {"period": period, "rules": rules, "kpis": kpis}


def _fetch_weekly_context(con) -> dict | None:
    """Fetch the latest completed ISO week's insights. Returns None if unavailable."""
    row = con.execute("SELECT MAX(period) FROM insights WHERE granularity = 'weekly'").fetchone()
    if not row or not row[0]:
        return None
    week = row[0]
    rules = con.execute(
        "SELECT rule_id, category, headline, sentiment FROM insights "
        "WHERE granularity = 'weekly' AND period = ? ORDER BY category, rule_id",
        [week],
    ).fetchall()
    return {"week": week, "rules": rules}


def _fetch_ep_snapshot(con) -> dict | None:
    """Fetch last completed month EP metrics with MoM and YoY comparison."""
    rows = con.execute("""
        WITH cur_period AS (
            SELECT MAX(OrderCohortMonth) AS m
            FROM sales
            WHERE Is_Earned = TRUE
              AND OrderCohortMonth < date_trunc('month', current_date)
        ),
        prev_period AS (
            SELECT MAX(OrderCohortMonth) AS m
            FROM sales
            WHERE Is_Earned = TRUE
              AND OrderCohortMonth < (SELECT m FROM cur_period)
        ),
        yoy_period AS (
            SELECT ((SELECT m FROM cur_period) - INTERVAL '12 months')::DATE AS m
        ),
        target AS (
            SELECT m FROM cur_period
            UNION ALL SELECT m FROM prev_period
            UNION ALL SELECT m FROM yoy_period
        ),
        sales_agg AS (
            SELECT
                s.OrderCohortMonth                              AS period,
                COUNT(DISTINCT s.CustomerID_Std)               AS customers,
                ROUND(SUM(s.Total_Num), 0)                     AS revenue,
                SUM(s.HasDelivery::INT + s.HasPickup::INT)     AS stops
            FROM sales s
            WHERE s.Is_Earned = TRUE
              AND s.OrderCohortMonth IN (SELECT m FROM target)
            GROUP BY s.OrderCohortMonth
        ),
        items_agg AS (
            SELECT ItemCohortMonth AS period, SUM(Quantity) AS items
            FROM items
            WHERE ItemCohortMonth IN (SELECT m FROM target)
            GROUP BY ItemCohortMonth
        )
        SELECT
            a.period,
            a.customers,
            COALESCE(i.items, 0)        AS items,
            a.revenue,
            a.stops,
            (SELECT m FROM cur_period)  AS cur_month,
            (SELECT m FROM prev_period) AS prev_month,
            (SELECT m FROM yoy_period)  AS yoy_month
        FROM sales_agg a
        LEFT JOIN items_agg i ON a.period = i.period
        ORDER BY a.period DESC
    """).fetchall()

    if not rows:
        return None

    cur_month = rows[0][5]
    prev_month = rows[0][6]
    yoy_month = rows[0][7]

    cols = ["customers", "items", "revenue", "stops"]
    by_period = {row[0]: dict(zip(cols, row[1:5])) for row in rows}

    cur = by_period.get(cur_month, {})
    prev = by_period.get(prev_month, {})
    yoy = by_period.get(yoy_month, {})

    def pct_chg(cur_val, ref_val):
        if not ref_val:
            return None
        return round((cur_val / ref_val - 1) * 100, 1)

    try:
        period_label = cur_month.strftime("%b %Y")
    except Exception:
        period_label = str(cur_month)

    return {
        "period": period_label,
        "current": cur,
        "mom": {k: pct_chg(cur.get(k, 0), prev.get(k)) for k in cols},
        "yoy": {k: pct_chg(cur.get(k, 0), yoy.get(k)) for k in cols},
    }


def _build_ep_blocks(ep: dict) -> list:
    """Build a Notion table block for the EP snapshot (4 metrics, MoM, YoY)."""
    cur = ep["current"]
    mom = ep["mom"]
    yoy = ep["yoy"]

    def fmt_val(key, val):
        if val is None:
            return "—"
        if key == "revenue":
            return f"Dhs {float(val):,.0f}"
        return f"{int(val):,}"

    def fmt_pct(val):
        if val is None:
            return "—"
        sign = "+" if val > 0 else ""
        return f"{sign}{val:.1f}%"

    def cell(text, bold=False):
        obj = {"type": "text", "text": {"content": text}}
        if bold:
            obj["annotations"] = {"bold": True}
        return [obj]

    METRICS = [
        ("Revenue", "revenue"),
        ("Customers", "customers"),
        ("Items", "items"),
        ("Stops", "stops"),
    ]

    header_row = {
        "object": "block",
        "type": "table_row",
        "table_row": {
            "cells": [
                cell("Metric", bold=True),
                cell(ep["period"], bold=True),
                cell("vs Prior Month", bold=True),
                cell("vs Prior Year", bold=True),
            ]
        },
    }

    data_rows = []
    for label, key in METRICS:
        data_rows.append(
            {
                "object": "block",
                "type": "table_row",
                "table_row": {
                    "cells": [
                        cell(label, bold=True),
                        cell(fmt_val(key, cur.get(key))),
                        cell(fmt_pct(mom.get(key))),
                        cell(fmt_pct(yoy.get(key))),
                    ]
                },
            }
        )

    return [
        {
            "object": "block",
            "type": "table",
            "table": {
                "table_width": 4,
                "has_column_header": True,
                "has_row_header": True,
                "children": [header_row] + data_rows,
            },
        }
    ]


def _build_weekly_callout(wctx: dict) -> dict:
    """Build a single Notion callout block for the weekly signals (rule-based, no LLM)."""
    SENTIMENT_ICON = {"positive": "\u2705", "negative": "\u26a0\ufe0f", "neutral": "\u2139\ufe0f"}
    lines = []
    for rule_id, category, headline, sentiment in wctx["rules"]:
        icon = SENTIMENT_ICON.get(sentiment, "\u2022")
        lines.append(f"  {icon}  {headline}")
    bullet_text = "\n".join(lines)
    return {
        "object": "block",
        "type": "callout",
        "callout": {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": f"Weekly Signals — {wctx['week']}\n"},
                    "annotations": {"bold": True},
                },
                {"type": "text", "text": {"content": bullet_text}},
            ],
            "icon": {"type": "emoji", "emoji": "\U0001f4c6"},
            "color": "default",
        },
    }


def _build_prompt(ctx: dict) -> str:
    period = ctx["period"]
    rules_text = "\n".join(f"[{r[0].upper()} / {r[3]}] {r[1]} — {r[2]}" for r in ctx["rules"])
    kpis_text = "\n".join(
        f"  {k[0]}: {k[1]} customers, {k[2]} items, Dhs {k[3]:,.0f} revenue, {k[4]} stops" for k in ctx["kpis"]
    )
    return f"""Summarise {period} performance for Moonwalk, a dry-cleaning service in Abu Dhabi.

Output ONLY valid JSON — no preamble, no explanation, no markdown fences.

Required format:
{{
  "executive_pulse": ["<insight 1>", "<insight 2>", "<insight 3>"],
  "customer_analytics": ["<insight 1>", "<insight 2>", "<insight 3>"],
  "operations_center": ["<insight 1>", "<insight 2>", "<insight 3>"],
  "financial_performance": ["<insight 1>", "<insight 2>", "<insight 3>"]
}}

Rules per insight: 1 sentence, max 20 words, reference specific numbers, active voice.
No asterisks, no bold markers, no introductory phrases like "The business...".

Period: {period}

KPI Snapshot:
{kpis_text}

Signals ({len(ctx["rules"])} rules):
{rules_text}
"""


def _parse_sections(raw: str) -> dict:
    """Parse JSON sections from LLM output. Falls back gracefully on parse failure."""
    cleaned = re.sub(r"```[a-z]*\n?", "", raw).strip()
    keys = ["executive_pulse", "customer_analytics", "operations_center", "financial_performance"]
    try:
        data = json.loads(cleaned)
        return {k: data.get(k, ["No data available."]) for k in keys}
    except Exception:
        paragraphs = [p.strip() for p in cleaned.split("\n\n") if p.strip()][:4]
        return {k: [p[:200]] for k, p in zip(keys, paragraphs + [""] * 4)}


def _find_or_create_toggle(notion_client, page_id: str, heading: str, log) -> str:
    """Find or create a toggleable heading_2 block. Creates at page end if not found."""
    response = notion_client.blocks.children.list(page_id)
    for block in response.get("results", []):
        block_type = block.get("type", "")
        block_data = block.get(block_type, {})
        rt = block_data.get("rich_text", [])
        text = "".join(t.get("text", {}).get("content", "") for t in rt)
        if heading in text:
            if block_data.get("is_toggleable"):
                return block["id"]
            log(f"Notion: replacing non-toggleable '{heading}' with toggleable version")
            notion_client.blocks.delete(block["id"])

    result = notion_client.blocks.children.append(
        page_id,
        children=[
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": heading}}],
                    "is_toggleable": True,
                },
            }
        ],
    )
    block_id = result["results"][0]["id"]
    log(f"Notion: created '{heading}' toggle ({block_id})")
    return block_id


def _find_or_create_insights_toggle(notion_client, page_id: str, log) -> str:
    """Return the block ID of the '📊 Latest Insights' toggleable heading."""
    return _find_or_create_toggle(notion_client, page_id, _INSIGHTS_HEADING, log)


def _clear_block_children(notion_client, block_id: str):
    """Delete all direct children of a block (handles pagination)."""
    while True:
        results = notion_client.blocks.children.list(block_id).get("results", [])
        if not results:
            break
        for child in results:
            notion_client.blocks.delete(child["id"])


def _build_insight_blocks(ts: str, period: str, sections: dict, notion_token: str = "") -> list:
    """Build structured Notion blocks: timestamp callout + 4 colored persona callouts.

    Each persona callout has its section name as bold heading, 3 bullet insights,
    and a direct link to the corresponding dashboard page.
    """
    blocks = [
        {
            "object": "block",
            "type": "callout",
            "callout": {
                "rich_text": [{"type": "text", "text": {"content": f"Updated {ts}  |  {period}"}}],
                "icon": {"type": "emoji", "emoji": "🔄"},
                "color": "gray_background",
            },
        }
    ]

    for key, emoji, label, color, page_path, tab_param in _PERSONA_CONFIG:
        bullets = sections.get(key, ["No data available."])
        bullet_lines = "\n".join(f"  \u2022  {b}" for b in bullets)
        url = f"{_BASE_URL}/{page_path}".rstrip("/")
        params = []
        if notion_token:
            params.append(f"token={notion_token}")
        if tab_param:
            params.append(f"tab={tab_param}")
        if params:
            url += "?" + "&".join(params)
        blocks.append(
            {
                "object": "block",
                "type": "callout",
                "callout": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": label + "\n"},
                            "annotations": {"bold": True},
                        },
                        {
                            "type": "text",
                            "text": {"content": bullet_lines + "\n"},
                        },
                        {
                            "type": "text",
                            "text": {"content": f"Open {label} \u2192", "link": {"url": url}},
                        },
                    ],
                    "icon": {"type": "emoji", "emoji": emoji},
                    "color": color,
                },
            }
        )

    return blocks


def run(log=print):
    """Entry point called from refresh_cli.py after DuckDB rebuild."""
    if not NOTION_API_KEY:
        log("Notion push skipped - NOTION_API_KEY not configured")
        return
    if not NOTION_PAGE_ID:
        log("Notion push skipped - NOTION_PAGE_ID not configured")
        return

    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if not openai_key:
        try:
            import streamlit as st

            openai_key = st.secrets.get("OPENAI_API_KEY", "")
        except Exception:
            pass
    if not openai_key:
        log("Notion push skipped - OPENAI_API_KEY not configured")
        return

    log("Notion push: fetching context from DuckDB...")
    con = _open_db()
    try:
        ctx = _fetch_context(con)
        wctx = _fetch_weekly_context(con)
        ep = _fetch_ep_snapshot(con)
    finally:
        con.close()

    log(f"Notion push: generating structured insights for {ctx['period']}...")
    from openai import OpenAI

    openai_client = OpenAI(api_key=openai_key)
    response = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        max_tokens=512,
        messages=[{"role": "user", "content": _build_prompt(ctx)}],
    )
    raw = response.choices[0].message.content
    sections = _parse_sections(raw)

    from notion_client import Client

    notion_client = Client(auth=NOTION_API_KEY)

    # Push EP snapshot (direct DuckDB query, no database storage)
    if ep:
        log(f"Notion push: updating EP snapshot ({ep['period']})...")
        ep_toggle_id = _find_or_create_toggle(notion_client, NOTION_PAGE_ID, _EP_HEADING, log)
        _clear_block_children(notion_client, ep_toggle_id)
        notion_client.blocks.children.append(ep_toggle_id, children=_build_ep_blocks(ep))
        log("Notion push: EP snapshot published")

    # Push LLM insights + weekly signals
    log("Notion push: updating insights section...")
    container_id = _find_or_create_insights_toggle(notion_client, NOTION_PAGE_ID, log)
    _clear_block_children(notion_client, container_id)

    ts = datetime.datetime.now().strftime("%d %b %Y %H:%M")
    blocks = _build_insight_blocks(ts, ctx["period"], sections, notion_token=NOTION_TOKEN)
    if wctx:
        blocks.append(_build_weekly_callout(wctx))
    notion_client.blocks.children.append(container_id, children=blocks)
    weekly_note = f" + {wctx['week']}" if wctx else ""
    log(f"Notion push: done - {ctx['period']}{weekly_note} ({len(blocks)} blocks published)")


if __name__ == "__main__":
    run()
