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

_PERSONA_CONFIG = [
    ("executive_pulse", "🎯", "Executive Pulse", "yellow_background", "", "snapshot"),
    ("customer_analytics", "👥", "Customer Analytics", "green_background", "customer_analytics", "segmentation"),
    ("operations_center", "🚚", "Operations Center", "blue_background", "operations_center", "logistics"),
    ("financial_performance", "💰", "Financial Performance", "purple_background", "financial_performance", "collections"),
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
    """Fetch current period KPIs + insights rules for the LLM prompt."""
    period_row = con.execute("SELECT MAX(period) FROM insights").fetchone()
    if not period_row or period_row[0] is None:
        raise ValueError("No rows in insights table — run cleancloud_to_duckdb.py first")
    period = period_row[0]

    rules = con.execute(
        """
        SELECT category, headline, detail, sentiment
        FROM insights
        WHERE period = ?
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
                  SELECT CAST(MAX(period) || '-01' AS DATE) FROM insights
                  UNION ALL
                  SELECT DATE_TRUNC('month', CAST(MAX(period) || '-01' AS DATE) - INTERVAL '1 month')::DATE
                  FROM insights
              )
            GROUP BY OrderCohortMonth
        )
        ORDER BY period
        """
    ).fetchall()

    return {"period": period, "rules": rules, "kpis": kpis}


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


def _find_or_create_insights_toggle(notion_client, page_id: str, log) -> str:
    """Return the block ID of the '📊 Latest Insights' toggleable heading.

    Creates the block at the end of the page if not found.
    """
    response = notion_client.blocks.children.list(page_id)
    for block in response.get("results", []):
        block_type = block.get("type", "")
        block_data = block.get(block_type, {})
        rt = block_data.get("rich_text", [])
        text = "".join(t.get("text", {}).get("content", "") for t in rt)
        if _INSIGHTS_HEADING in text:
            if block_data.get("is_toggleable"):
                return block["id"]
            # Found a non-toggleable heading (e.g. created by page replace) — delete and recreate
            log("Notion: replacing non-toggleable 'Latest Insights' heading with toggleable version")
            notion_client.blocks.delete(block["id"])

    result = notion_client.blocks.children.append(
        page_id,
        children=[
            {
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [{"type": "text", "text": {"content": _INSIGHTS_HEADING}}],
                    "is_toggleable": True,
                },
            }
        ],
    )
    block_id = result["results"][0]["id"]
    log(f"Notion: created 'Latest Insights' toggle ({block_id})")
    return block_id


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

    log("Notion push: updating insights section...")
    from notion_client import Client

    notion_client = Client(auth=NOTION_API_KEY)
    container_id = _find_or_create_insights_toggle(notion_client, NOTION_PAGE_ID, log)
    _clear_block_children(notion_client, container_id)

    ts = datetime.datetime.now().strftime("%d %b %Y %H:%M")
    blocks = _build_insight_blocks(ts, ctx["period"], sections, notion_token=NOTION_TOKEN)
    notion_client.blocks.children.append(container_id, children=blocks)
    log(f"Notion push: done - {ctx['period']} ({len(blocks)} blocks published)")


if __name__ == "__main__":
    run()
