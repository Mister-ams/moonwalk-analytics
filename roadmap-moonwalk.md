---
project: moonwalk
type: roadmap
status: active
created: 2026-02-16
updated: 2026-02-19 (v5.8 — post Tick 7B)
---

# Moonwalk Analytics — Master Project Roadmap

**Pattern:** Tick-tock within phases; phases aligned to OS v1.0 execution plan

---

## Strategic Context

This roadmap serves the **analytics layer** of the SME Internal Operating System (OS v1.0). The system has four delivery phases:

| Phase | Scope | Analytics Role |
|-------|-------|---------------|
| **POC** | Notion + Appsmith + Streamlit + Prefect (on DuckDB) | **Current phase** — full app experience validated on current stack |
| **Migrate** | Postgres + Alembic + materialized views | Dedicated cycle: swap DuckDB for single Postgres |
| **MVP** | RBAC, FastAPI hardening, data contracts | Production-grade backend on Postgres |
| **Beta/Production** | Cloud, HA, security hardening, observability | Enterprise-grade analytics |

**Key architectural decisions:**

1. **Postgres is deferred to a dedicated migration cycle after POC.** The POC validates the full app experience (Notion portal, Appsmith operational UI, Streamlit analytics, Prefect orchestration) on the current DuckDB stack. Postgres migration is a big investment that is only justified after the POC proves the UX and workflows.

2. **Single Postgres replaces DuckDB (no dual-database).** At Moonwalk's scale (~7K orders, ~1.2K customers), Postgres handles both OLTP and OLAP workloads. Materialized views serve dashboard aggregations. DuckDB can be re-added later via `postgres_scanner` if analytical workloads outgrow Postgres.

3. **Prefect replaces n8n for orchestration.** Python-native orchestration avoids context-switching between Python code and a visual node editor. The entire stack stays in one language.

4. **pgvector for future unstructured data.** If vector embeddings are needed (customer communications, document search), pgvector runs inside Postgres — avoiding a third database.

---

## Completed Work (Reference)

| Cycle | Type | Key Deliverables |
|-------|------|-----------------|
| Tick 1-2 | Feature | DuckDB file-based, config centralization, `render_detail_page()`, launcher workflows |
| Tock 3-3.5 | Quality | Structured logging, type hints, in-process orchestrator, plugins, MCP servers, skills |
| Tick 4-4.6 | Feature | 13-page dashboard, weekly/monthly toggle, 3-tier color hierarchy, compound cards |
| Tock Audit 1-4 | Quality | ETL fixes, BOOLEAN/SMALLINT/ENUM casts, Parquet dual-output, profiling, portable paths |
| Tock Polars | Quality | Full ETL pandas-to-Polars: 8.75x speedup (7s to 0.8s), 16x less memory |
| Tock 5 | Quality | 82 tests, PS hardening, `refresh_cli.py`, `pyproject.toml` |
| Tock 6 | Quality | Data integrity: subscription overlap merge, CohortMonth validation, TRY_CAST logging, ENUM pre-validation, Polars join idioms. 93 tests. |
| Tick 6A | Feature | Dashboard polish: sidebar regrouped to 4 sections, period selector SQL cached in session state, CohortMonth null filter fixed, stops.py references removed, docstrings added. 12 pages. |
| Tock 7 | Quality | Test coverage expansion: 93 → 145 tests. TRY_CAST edge cases (19), order_lookup consistency (3), empty DataFrame (10), Playwright smoke tests (23). |
| Tick 6B | Feature | Streamlit Cloud deployment. `IS_CLOUD` config detection, cloud-resilient logging/profiling, `analytics.duckdb` in repo (11MB), `requirements.txt`, `.streamlit/config.toml`. GitHub: `Mister-ams/moonwalk-analytics` (public). |
| **Tock 7B** | **Quality** | **Security: Password gate (`hmac` + `st.secrets`), DuckDB AES-256 encryption (ATTACH pattern), `DUCKDB_KEY` in config, Playwright auth tests. Git history purged (`git-filter-repo`), encrypted DB pushed to cloud. Streamlit Cloud secrets configured, Notion embed confirmed.** |
| **Tick 7** | **Feature** | **Persona-based dashboard redesign: 12 pages → 4 persona pages (Executive Pulse, Customer Analytics, Operations Center, Financial Performance) with 15 tabs. YoY overlays + 3P moving avg (`render_trend_chart_v3`), M0-M3+ extended cohort, all-time retention heatmap, RFM segmentation (6 segments + CLV), 20-rule insights engine, outstanding balances (aging buckets), pareto/concentration chart. Legacy→paid fix. 8 new `section_data.py` functions. 122 tests pass.** |
| **Tick 8** | **Feature** | **Closed period filter (`IsCurrentMonth = 0` / `IsCurrentISOWeek = 0` in period selector SQL), UI polish (1rem border-radius, layered shadows, neutral MoM pill `#CFD8DC`/`#37474F`, refined card typography + chart config), Notion portal page (`30ca2f71-fdb0-81fa-a12b-c5e844be2bf3`) with 4 persona callout blocks + Streamlit links, LLM narrative pipeline: new `notion_push.py` generates 4-paragraph GPT-4o-mini narrative from `insights` table and appends to Notion after each `refresh_cli.py` run. READ_ONLY DuckDB ATTACH for concurrent access. `NOTION_API_KEY` + `OPENAI_API_KEY` wired into `config.py` + `.streamlit/secrets.toml`.** |
| **Tock 8** | **Quality** | **Playwright tests rewritten for 4-page structure (20 tests, was 23). 12 dead page files deleted. Cloud deploy verified on all 4 pages and 15 tabs. 147 total tests.** |
| **Tick 9** | **Feature** | **FastAPI operational API: `api/` package (auth, SQLite, employees CRUD), Railway deployment artifacts (`Procfile`, `runtime.txt`, `.env.example`), `Start-API.ps1`, 13 FastAPI tests. 160 total tests.** |

### Items Resolved (Previously Listed as Open)

| Item | Resolution |
|------|------------|
| Move Processing_Days to Operations | **Already done** — Operations displays these; Payments shows only DaysToPayment |
| Pre-populate Operations METRIC_CONFIG | **Not a real issue** — METRIC_CONFIG is read-only; no runtime mutation |
| Split `dashboard_shared.py` (1,290 LOC) | **Over-engineering** — file is cohesive; splitting risks circular imports |
| Consolidate MoM computation (3 places) | **Not worth it** — 3 uses are legitimately different (HTML, Plotly customdata, Plotly annotation) |
| VARCHAR dates to DATE in DuckDB | **Done** in Audit Tier 3 |
| Drop redundant `Delivery` column | **Done** in Audit Tier 3 |

### Known Data Gaps (Documented, No Code Fix)

- **468 orphan orders** in items with no sales match (6.9%) — CleanCloud CSV export mismatch.
- **Customer CC-0008** missing — single un-earned order (Dhs 10), zero impact.
- **Hielo customers** in sales but not customers table — Moon Walk-only CleanCloud export.

---

## Code Review Findings (Feb 2026)

Deep review of all 39 files (9,200 LOC) identified issues across every layer:

| Severity | # | Key Issues | Status |
|----------|---|------------|--------|
| CRITICAL | 3 | Subscription flag cartesian join, null CohortMonth unvalidated, TRY_CAST silent NULLs | **All fixed (Tock 6)** |
| HIGH | 3 | ENUM extensibility, `.to_list()` anti-patterns (3 instances), uncached period selector | **All fixed (Tock 6 + Tick 6A)** |
| MEDIUM | 3 | Order ID non-deterministic, B2B filter single-source, magic numbers scattered | Deferred to Tock 7 / MVP |
| LOW | 6 | Sidebar grouping, state key naming, geo card, docstrings, `.clone()` calls, backward-compat aliases | **3 fixed (Tick 6A)**, 3 in Tock 7 |

---

## POC Delivery Plan

### Tock 6 — Data Integrity & ETL Hardening — COMPLETED 2026-02-17

**Focus:** Fix silent data quality bugs affecting revenue attribution accuracy.
**Scope:** ETL helpers, DuckDB loader. No new infrastructure.

| # | Item | Severity | File | Status |
|---|------|----------|------|--------|
| 6.1 | **Subscription flag: merge overlapping periods** | CRITICAL | `helpers.py` | Done — `_merge_overlapping_periods()` + 11 new tests |
| 6.2 | **CohortMonth null validation + logging** | CRITICAL | `transform_all_sales.py` | Done — warns with affected customer IDs on earned rows |
| 6.3 | **TRY_CAST null logging in DuckDB loader** | CRITICAL | `cleancloud_to_duckdb.py` | Done — pre/post meaningful-value comparison; zero false positives |
| 6.4 | **ENUM extensibility** (pre-load validation) | HIGH | `cleancloud_to_duckdb.py` | Done — queries distinct values, logs unknowns before ENUM creation |
| 6.5 | **Replace `.to_list()` anti-patterns with Polars joins** | HIGH | `transform_all_sales.py` | Done — 3 dict lookups → Polars joins; 2 intermediate dicts eliminated |

**Results:** 93 tests (11 new), golden baselines verified, ETL 0.8s, DuckDB 0.5s.

**Implementation notes:**

- **6.1**: `_merge_overlapping_periods()` sorts by ValidFrom, merges touching/overlapping ranges. Applied before the customer-order join in `polars_subscription_flag()` to prevent cartesian products. Tests cover: full overlap, partial overlap, boundary touch, three-way chain, no overlap, empty, single.
- **6.2**: After the Phase 10 customer-data join, counts null CohortMonth on earned rows. Logs warning with count, percentage, and up to 20 affected CustomerID_Std values.
- **6.3**: `_count_meaningful_values()` counts non-null, non-empty-string values before cast. `_count_non_null()` after cast. Difference = actual data loss from failed parsing (not pre-existing nulls). Covers all 15 DATE, 9 BOOLEAN, 5 INTEGER, and 9 ENUM casts.
- **6.4**: Before each ENUM type creation, queries `SELECT DISTINCT` from source column. Logs any values not in the defined spec. Current data has zero unknowns.
- **6.5**: `customer_name_lookup` dict replaced with `name_lookup_df` DataFrame; `customer_cohort`/`customer_route` dicts replaced with `customer_lookup_df` join from `df_all_customers`. All three consumption sites now use `df.join(..., how="left")`.

---

### Tick 6A — Dashboard Polish — COMPLETED 2026-02-17

**Focus:** Cache, sidebar reorg, quick wins before Notion portal work.
**Scope:** 3 code files + 2 doc files.

| # | Item | Source | Status |
|---|------|--------|--------|
| 6.6 | **Remove stale `stops.py` references** | DO-14 | Done — references removed from CLAUDE.md |
| 6.7 | **Regroup sidebar into 4 logical sections** | DO-9 | Done — Monthly Report (4), Customer Intelligence (5), Operations (2), Financials (1) |
| 6.8 | **Cache period selector queries in session state** | Code review | Done — `_cached_periods_monthly`/`_cached_periods_weekly`, SQL runs once per session |
| QW-1 | **Fix `CohortMonth == ""` → `.is_null()`** | Quick win | Done — post-Polars migration, date columns are typed |
| QW-4 | **Add docstrings to `fmt_count/fmt_dhs/fmt_dhs_sub`** | Quick win | Done |

**Results:** 88 tests passed, dashboard verified with 4 sidebar sections and 12 pages.

---

### Tick 6B — Cloud Deployment + Notion Portal — COMPLETED 2026-02-17

**Focus:** Deploy dashboard to Streamlit Community Cloud, connect via Notion portal.
**Scope:** Config changes, cloud resilience, GitHub repo, Streamlit Cloud.

| # | Item | Source | Status |
|---|------|--------|--------|
| 6.9 | **Cloud-ready config** (`IS_CLOUD`, resilient logging/profiling) | Sprint plan | Done |
| 6.10 | **GitHub repo + Streamlit Cloud deploy** | Sprint plan | Done — `Mister-ams/moonwalk-analytics` (public) |
| 6.11 | **Notion portal setup** | OS v1.0 POC | Done — Streamlit Cloud embed live in Notion |

**Data refresh workflow:**
1. Download CSVs from CleanCloud
2. Run ETL: `python cleancloud_to_excel_MASTER.py`
3. Rebuild DuckDB: `python cleancloud_to_duckdb.py`
4. Push: `cp ../analytics.duckdb . && git add analytics.duckdb && git commit -m "Refresh data" && git push`
5. Streamlit Cloud auto-redeploys (~1 min)

---

### Tock 7 — Testing & Quality — COMPLETED 2026-02-17

**Focus:** Close test coverage gaps before expanding features.
**Scope:** 52 new tests (93 → 145 total).

| # | Item | Status |
|---|------|--------|
| 7.1 | **TRY_CAST edge case tests** (19 tests: DATE/BOOLEAN/SMALLINT/ENUM) | Done |
| 7.2 | **order_lookup value consistency tests** (3 tests: value match, unique IDs, no nulls) | Done |
| 7.3 | **Empty DataFrame edge cases** (10 tests: all ETL helpers) | Done |
| 7.4 | **Dashboard Playwright smoke tests** (23 tests: all 12 pages, toggle, cards, charts) | Done |
| 7.5 | **Centralize magic numbers** | Skipped — already centralized in `config.py` |
| 7.6 | **Remove `.clone()` calls** | Skipped — all 8 calls justified (shared_data isolation) |

---

### Tock 7B — Security — COMPLETED 2026-02-18

**Focus:** Protect PII in public-repo Streamlit deployment (2,293 customer names, Dhs 735K revenue data).
**Scope:** Password gate + DuckDB encryption + Playwright test updates.

| # | Item | Details | Status |
|---|------|---------|--------|
| 7B.1 | **DuckDB AES-256 encryption** | `ATTACH ... (ENCRYPTION_KEY)` pattern in builder + reader. `DUCKDB_KEY` from `st.secrets` or `MOONWALK_DUCKDB_KEY` env var. `duckdb>=1.4` required. | Done |
| 7B.2 | **Password gate** | `hmac.compare_digest` + `st.secrets["DASHBOARD_PASSWORD"]` in `moonwalk_dashboard.py`. No gate when no password configured (local dev backward compat). | Done |
| 7B.3 | **Secrets management** | `.streamlit/secrets.toml` (gitignored). Streamlit Cloud secrets via Settings UI. | Done |
| 7B.4 | **Playwright test auth** | `_goto_and_auth()` helper reads password from secrets.toml, authenticates at root, navigates via sidebar links to preserve Streamlit session. | Done |
| 7B.5 | **Git history purge** | Removed unencrypted `analytics.duckdb` from git history via `git-filter-repo`. Encrypted DB (8.3 MB, AES-256) committed and force-pushed. | Done |

**Key decisions:**
- DuckDB ATTACH pattern (not direct `duckdb.connect(key=...)`) — DuckDB 1.4 encryption only works via ATTACH
- Password gate uses `st.secrets.get()` with empty-string default — no secrets file = no gate (local dev works unchanged)
- `@st.cache_resource` on `get_connection()` unchanged — encrypted connection created once per process

---

### Tick 7 — Persona-Based Dashboard Redesign — COMPLETED 2026-02-19

**Focus:** Restructure the entire dashboard around 4 user personas. Replace 12 flat sidebar pages with 4 dense, tabbed pages. Add YoY, extended cohort, rules-based insights, outstanding balances, and pareto analysis.
**Scope:** Full dashboard rewrite (all 12 pages → 4 pages), new SQL queries, rules-based insights engine, DuckDB data fixes.

**Results:** 4 persona pages, 15 tabs, 122 tests pass. `analytics.duckdb` rebuilt with `insights` table (20 rules). Committed and pushed to `Mister-ams/moonwalk-analytics` (master `7c85096`). Streamlit Cloud auto-redeploying.

#### Design Decisions (Feb 2026)

| Decision | Rationale |
|----------|-----------|
| **4 personas, 4 pages** | Management, Sales, Operations, Performance — each gets one page with 2-4 tabs. No sidebar bouncing. |
| **Drop order-level metrics** | Orders conflate subscriptions, invoices, multi-item orders. Primary KPIs: Customers, Items, Revenue, Stops. |
| **Revenue per delivery, not per stop** | A delivery order can have 2 stops (pickup + delivery). Revenue / deliveries = Dhs 45.23. Stops = logistics productivity only. |
| **Legacy orders assumed paid** | `Source = 'Legacy'` has 10,774 unpaid orders (Dhs 464K) from RePOS archive. Real outstanding = CC_2025 only: 735 orders, Dhs 20K. |
| **Rules-based insights, not live LLM** | 15-20 templates computed during DuckDB rebuild. Deterministic, fast, no API cost. Stored as `insights` table. |
| **Notion = portal (separate container)** | Notion for narrative + links + KPI database (pushed via API). Streamlit for all interactive data. Not embedded. |
| **Streamlit stays analytical (read-only)** | Actionable views (mark paid, edit customer) deferred to Appsmith (Tick 8). |
| **`st.tabs()` for intra-page nav** | Reduces page count, preserves context, avoids sidebar clutter. |

#### Data Verified (Feb 2026)

| Metric | Value |
|--------|-------|
| History depth | 26 months (Jan 2024 – Feb 2026), enough for 13-month YoY |
| Cohort depth | M0 through M16+, classic retention decay (M0→M1: 43%) |
| Real outstanding | Dhs 20,102 across 735 CC_2025 orders (not Dhs 484K including legacy) |
| Multi-service customers | 446 (17.5% of active base) |
| Payment timing coverage | 22-34% of earned rows (CC_2025 source; caveat required on dashboard) |
| Deliveries | 3,590 deliveries, Dhs 162K revenue, Dhs 45.23 per delivery |

#### Page Structure

```
Executive Pulse (Management)         Customer Analytics (Sales)
├── Snapshot: 4 KPIs + MoM + YoY    ├── Acquisition: New/Existing/Reactivated
├── Trends: 6-mo charts + YoY +     ├── Segmentation: Client/Sub, Multi-Svc,
│   3-mo moving avg forecast             Top 20%, RFM scoring, Simple CLV
└── Insights: Rules-based bullets    ├── Cohort: M0-M3+, Retention Heatmap
                                     └── Per-Customer: Items/Rev per customer

Operations Center (Operations)       Financial Performance (Performance)
├── Logistics: Stops, Deliveries     ├── Collections: Rev vs Collections, Methods
├── Geography: Inside/Outer split    ├── Payment Cycle: DTP, Processing, Time-in-Store
└── Service Mix: Category, Type,     ├── Concentration: Pareto, Multi-Svc, Top 20 Rev
    Express orders                   └── Outstanding: CC_2025 only, Top 20, Aging
                                         (→ Appsmith for actions in Tick 8)

Cross-cutting: CSV export on every page (st.download_button)
```

#### Implementation Items

| # | Item | Page | Complexity | Status |
|---|------|------|-----------|--------|
| 7.1 | **Executive Pulse — Snapshot tab** | Exec | Medium | Done |
| | 4 KPI cards (Customers, Items, Revenue, Stops) with MoM + YoY | | | |
| | Sub-metrics: Client/Subscriber split per card | | | |
| | Period selector (monthly/weekly toggle) | | | |
| 7.2 | **Executive Pulse — Trends tab** | Exec | Medium | Done |
| | 4 trend charts (6-month/13-week bars) | | | |
| | YoY overlay (prior year as dashed line) | | | |
| | Average Order Value trend (Revenue / Items) | | | |
| | 3-month moving average forecast overlay (dashed line) | | | |
| 7.3 | **Executive Pulse — Insights tab** | Exec | Medium | Done |
| | Rules-based insights engine (15-20 templates) | | | |
| | `insights` table in DuckDB, generated during rebuild | | | |
| | Seasonal context annotations | | | |
| 7.4 | **Customer Analytics — Acquisition tab** | Sales | Medium | Done |
| | New vs Existing vs Reactivated customers, items, revenue | | | |
| | New/Existing/Reactivated split trend chart | | | |
| | Reactivation rate: dormant 3+ months returning (self-join on customer×month) | | | |
| 7.5 | **Customer Analytics — Segmentation tab** | Sales | High | Done |
| | Client vs Subscriber breakdown | | | |
| | Multi-service customers (count, %, revenue share) | | | |
| | Top 20% by spend and volume | | | |
| | RFM scoring: Recency (months since last order) × Frequency (order count) × Monetary (total revenue), quintile-based (1-5 per dimension) | | | |
| | Simple CLV estimate: avg monthly revenue × avg active lifespan from cohort decay | | | |
| 7.6 | **Customer Analytics — Cohort tab** | Sales | High | Done |
| | M0-M3+ metrics (extend from current M0/M1) | | | |
| | Retention heatmap (cohort month × M0-M6 grid) | | | |
| | Per-customer ratios by cohort month | | | |
| 7.7 | **Customer Analytics — Per-Customer tab** | Sales | Low | Done |
| | Items per Customer (Total, Client, Subscriber) | | | |
| | Revenue per Customer (Total, Client, Subscriber) | | | |
| 7.8 | **Operations Center — Logistics tab** | Ops | Low | Done |
| | Stops, Deliveries, Pickups headline cards | | | |
| | Revenue per Delivery (not per stop), Delivery Rate | | | |
| | Items Delivered, Items per Delivery | | | |
| 7.9 | **Operations Center — Geography tab** | Ops | Low | Done |
| | Inside vs Outer Abu Dhabi stacked bar | | | |
| | Detail cards: Customers, Items, Stops, Revenue by geo | | | |
| | Geo trend chart (shift over time) | | | |
| 7.10 | **Operations Center — Service Mix tab** | Ops | Medium | Done |
| | Item Category breakdown (5 types × volume + revenue) | | | |
| | Service Type breakdown (4 types × volume + revenue) | | | |
| | Express order share (volume and revenue) | | | |
| 7.11 | **Financial Performance — Collections tab** | Perf | Medium | Done |
| | Revenue vs Collections headline + gap analysis | | | |
| | Collection methods (Stripe/Terminal/Cash) | | | |
| | Collection Rate % | | | |
| | Payment method trend (digital adoption over time) | | | |
| 7.12 | **Financial Performance — Payment Cycle tab** | Perf | Low | Done |
| | Avg Days to Payment, Processing Time, Time in Store | | | |
| | Data coverage caveat ("Based on 34% of CC_2025 orders") | | | |
| 7.13 | **Financial Performance — Concentration tab** | Perf | Medium | Done |
| | Pareto chart (cumulative revenue curve) | | | |
| | Multi-service customers (count, revenue share) | | | |
| | Top 20 customers by revenue (`st.dataframe()`) | | | |
| 7.14 | **Financial Performance — Outstanding tab** | Perf | Medium | Done |
| | Total outstanding (CC_2025 only: Dhs 20K) | | | |
| | Top 20 customers by outstanding balance | | | |
| | Top 20 oldest unpaid orders | | | |
| | Outstanding by aging bucket (0-30, 31-60, 61-90, 90+ days) | | | |
| 7.15 | **DuckDB data fixes** | ETL | Low | Done |
| | Set `Paid = true` for all `Source = 'Legacy'` rows | | | |
| | Generate `insights` table during DuckDB rebuild | | | |
| 7.16 | **Playwright test rewrite** | Test | Medium | Done |
| | Update all smoke tests for 4-page/tab structure | | | |
| | Test tab navigation, persona-specific content | | | |
| 7.17 | **Export/download functionality** | All | Low | Done |
| | `st.download_button()` on each of 4 persona pages | | | |
| | Export current view as CSV | | | |

#### Metrics NOT Included (Deferred)

| Metric | Reason | When |
|--------|--------|------|
| Hielo store parity | CleanCloud export is Moon Walk-only; needs separate Hielo export | When data available |
| Full probabilistic CLV (BG/NBD model) | Simple CLV pulled into 7.5; advanced model needs more validation | Post-POC |

**Pulled into Tick 7 (formerly deferred):** RFM scoring (→ 7.5), Simple CLV (→ 7.5), Reactivation rate (→ 7.4), Moving average forecast (→ 7.2), Export/download (→ 7.17).

---

### Tick 8 — UI Polish + Closed Periods + Notion Intelligence Layer — COMPLETED 2026-02-19

**Focus:** Dashboard accuracy (closed periods only), visual refinements, token-based Notion access, and automated LLM narrative generation pushed to Notion on each data refresh.
**Scope:** `dashboard_shared.py`, `moonwalk_dashboard.py`, `cleancloud_to_duckdb.py`, `config.py`, new `notion_push.py`, `refresh_cli.py`.

| # | Item | Details | Status |
|---|------|---------|--------|
| 8.1 | **Closed period filter** | `period_selector()` monthly SQL: `AND p.IsCurrentMonth = 0`. Weekly SQL: `AND p.IsCurrentISOWeek = 0`. Period dropdown defaults to last closed period. `create_insights_table()` in `cleancloud_to_duckdb.py` changed from `<= current month` to `< current month` — insights always built on last completed month. | Done |
| 8.2 | **UI polish — CSS** | `inject_global_styles()`: `1rem` border-radius on cards + chart containers, layered shadows (`0 1px 3px … 0 4px 14px …`), neutral MoM pill (`#CFD8DC` bg / `#37474F` fg, replacing alarming yellow), `0.75rem` vertical gap. | Done |
| 8.3 | **UI polish — card typography** | `headline_card()`/`headline_card_with_subs()`: header padding `0.6rem 0.5rem`, font-size `0.875rem`, letter-spacing `0.06em`, main value `1.85rem`, sub-row padding `0.25rem 0`, dividers `rgba(0,0,0,0.07)`, footer `color:#666; font-size:0.8rem`. | Done |
| 8.4 | **UI polish — charts** | `render_trend_chart_v2/v3`: `gridcolor="rgba(0,0,0,0.08)"`, `bargap=0.30` monthly (was 0.35), `font=dict(size=15, weight=600)`. AOV chart in Trends tab replaced with Stops. | Done |
| 8.5 | **Weekly period labels** | `format_period_label()` weekly format changed from date-based ("9 Feb 26") to ISO week format ("26W07"). | Done |
| 8.6 | **Token-based password bypass** | `moonwalk_dashboard.py` `_check_password()` checks `?token=<NOTION_TOKEN>` query param via `st.query_params`. Visitors from Notion links bypass password gate. `NOTION_TOKEN` added to `config.py` + `.streamlit/secrets.toml` + Streamlit Cloud secrets. | Done |
| 8.7 | **Notion portal page** | Page `30ca2f71-fdb0-81fa-a12b-c5e844be2bf3`. Single `📊 Latest Insights` toggle (owned entirely by `notion_push.py` via API). All 5 Streamlit links include `?token=loomi-portal-2026`. No separate PERSONA PAGES section — links embedded inside each persona callout in the toggle. | Done |
| 8.8 | **LLM narrative pipeline (`notion_push.py`)** | Fetches `insights` table (last completed month) + KPI snapshot → GPT-4o-mini JSON prompt → 3 bullets per persona → `📊 Latest Insights` toggle populated: 1 timestamp callout + 4 colored persona callouts (bold heading + bullets + `Open [Page] →` link). `_find_or_create_insights_toggle()` verifies `is_toggleable` and recreates if a non-toggle heading is found. Uses READ_ONLY DuckDB ATTACH. | Done |
| 8.9 | **Wire into `refresh_cli.py`** | After successful DuckDB rebuild: `from notion_push import run as notion_run; notion_run(log=logger.info)` — non-fatal, logs and skips if either API key absent. | Done |

**Key decisions:**
- `create_insights_table()` uses `<` (not `<=`) on `YearMonth < current month` — insights always reflect fully closed periods, consistent with dashboard period filter
- Notion portal `📊 Latest Insights` toggle is **owned by `notion_push.py` via API only** — never include it in MCP `replace_content` calls (MCP recreates heading as non-toggleable, breaking `children.append`)
- READ_ONLY DuckDB ATTACH — `notion_push.py` can run while dashboard holds the write lock
- OpenAI (`gpt-4o-mini`) JSON output format — 3 crisp bullets per persona, specific numbers, active voice; fallback parser on JSON parse failure
- Neutral MoM pill — gray `#CFD8DC`/`#37474F` replaces alarming yellow

**Results:** 122 tests unchanged. Cloud deployed (master `5942b93`). End-to-end refresh: DuckDB rebuild (0.9s) + GPT-4o-mini (~10s) + Notion push = ~12s total.

---

### Tock 8 — Test & Quality After Restructure

**Focus:** Stabilize the restructured dashboard. Update tests, clean up dead code.
**Scope:** Playwright rewrite, golden baseline updates, dead code removal.

| # | Item | Details |
|---|------|---------|
| 8.1 | **Playwright test rewrite** | 4 pages × 2-4 tabs = ~20 smoke tests (replaces 23 existing tests for old 12-page structure) | Done |
| 8.2 | **Remove old page files** | Delete 12 old page files (`overview`, `customers`, `items`, `revenues`, `customer_insights`, `customer_report`, `customer_report_revenue`, `new_customers`, `cohort`, `logistics`, `operations`, `payments`) | Done |
| 8.3 | **Update CLAUDE.md** | Test counts (122→147), playwright line added, Tock 8 completed | Done |
| 8.4 | **Cloud deploy verification** | All 4 persona pages verified on Streamlit Cloud — no errors, all tabs render, real data | Done |

**Results:** 20 Playwright tests (was 23), 147 total tests. 12 dead page files removed. Cloud verified on all 4 pages and 15 tabs. Committed `15b611c`, pushed to `Mister-ams/moonwalk-analytics`.

---

### Tick 9 — FastAPI + Appsmith Infrastructure — COMPLETED 2026-02-19

**Focus:** Build the operational API layer. First CRUD use case: employee information management.
**Scope:** FastAPI + SQLite skeleton, `api/` package, Railway deployment artifacts, 13 tests. No outstanding balances (deferred). No Postgres.

| # | Item | Details | Status |
|---|------|---------|--------|
| 9.1 | **FastAPI skeleton** | `api/` package: `main.py`, `auth.py`, `database.py`, `models.py`, `routers/health.py`, `routers/employees.py`. Full CRUD with X-API-Key auth. | Done |
| 9.2 | **SQLite operational DB** | `operational.db` — separate from `analytics.duckdb`. Schema: `employees` table. WAL mode for concurrent uvicorn workers. Test-isolated via `_DB_PATH` redirect. | Done |
| 9.3 | **API key auth** | `X-API-Key` header via `MOONWALK_API_KEY` env var. Read at request time (not import time) for test isolation. Fail-closed: no key = all requests rejected. | Done |
| 9.4 | **Employee CRUD** | `GET /employees` (list + status filter), `POST /employees` (create), `GET /employees/{id}`, `PATCH /employees/{id}` (true PATCH), `DELETE /employees/{id}` (soft + hard). | Done |
| 9.5 | **Railway deployment artifacts** | `Procfile` (`uvicorn api.main:app --host 0.0.0.0 --port $PORT`), `runtime.txt` (python-3.13), `.env.example`. | Done |
| 9.6 | **Local dev launcher** | `Start-API.ps1` — mirrors `Start-Dashboard.ps1` structure; checks .env, uvicorn, binds to 127.0.0.1:8000 with --reload. Added to Moonwalk Launcher. | Done |
| 9.7 | **13 FastAPI tests** | `tests/test_api.py` — health, auth (3), CRUD (7), validation (1). All in-process, no network, no running server. | Done |
| 9.3d | **Outstanding balance management** | Deferred to future app build cycle. | Deferred |
| 9.4d | **Customer lookup/edit** | Deferred to future app build cycle. | Deferred |
| 9.5d | **Notion portal update** | Manual Notion edit (2 min). Code update deferred to Tick 10. | Deferred |
| 9.6d | **Phase out Excel PowerPivot** | Deferred until Appsmith + Streamlit provide equivalent coverage. | Deferred |

**Key architectural decisions:**

| Decision | Choice | Reason |
|----------|--------|--------|
| Operational DB | SQLite (`operational.db`) | No install, simple, separate from analytics DuckDB |
| FastAPI deployment | Railway (free tier) | Appsmith Cloud must call a public URL |
| API auth | `X-API-Key` header via env var | Safe for public GitHub; never committed |
| Appsmith | Appsmith Cloud | Multi-user; matches Streamlit cloud model |

**Railway ephemeral note:** SQLite resets on redeploy. Acceptable for POC. Fix: add persistent disk ($0.25/GB) or migrate to Postgres (Tock M).

**Results:** 13 tests pass. 160 total tests. FastAPI imports clean. `api/` package ready for Railway deploy.

**Next steps (manual):**
1. `railway login && railway init` in PythonScript/ → set `MOONWALK_API_KEY=<uuid>` → `railway up`
2. Appsmith Cloud: new workspace "Moonwalk", add API datasource pointing to Railway URL
3. Build Employee Directory page in Appsmith (Table + Form + PATCH + soft-delete)

---

### Tick 10 — Prefect Orchestration + Notion KPI Database

**Focus:** Replace PowerShell automation with Python-native orchestration. Add structured Notion KPI database (LLM narrative push already done in Tick 8).
**Scope:** Prefect deployment, ETL scheduling, Notion KPI database via API.

| # | Item | Details |
|---|------|---------|
| 10.1 | **Prefect deployment** | Local Prefect server or Prefect Cloud (free tier). |
| 10.2 | **ETL flow** | Wrap `cleancloud_to_excel_MASTER.py` as a Prefect flow with task-level retries. |
| 10.3 | **DuckDB rebuild task** | `cleancloud_to_duckdb.py` as a downstream Prefect task, triggered after ETL. |
| 10.4 | **Insights generation task** | Compute rules-based insights + store in DuckDB `insights` table. |
| 10.5 | **Notion KPI database** | Post-ETL task: write period KPIs to Notion database via API (`notion-client`). Notion renders as native cards/gallery. (LLM narrative push already done in Tick 8 via `notion_push.py`.) |
| 10.6 | **Scheduling + notifications** | Cron-based or file-watcher trigger. Email/Slack alerts on failure via Prefect automations. |
| 10.7 | **Phase out PowerShell** | Prefect handles all orchestration. Retire `refresh_moonwalk_data.ps1`. |

---

### Tick 7B — Dashboard Enhancements ✅ Completed 2026-02-19

**Focus:** Second-wave features that build on the Tick 7 persona structure.

| # | Item | Status |
|---|------|--------|
| 7B.1 | URL tab selection (`?tab=snapshot`) — `activate_tab_from_url()` in all 4 pages | **Done** |
| 7B.2 | PDF report download — `generate_report.py` (5 pages, fpdf2), button in Snapshot tab | **Done** |
| 7B.3 | RFM segment definitions panel — 6 colored cards in Segmentation tab expander | **Done** |

**Also shipped:** Notion `?tab=` params in `notion_push.py` — persona callout links now open specific tabs. 4 new integration tests (`test_report.py`) — 164 total tests.

**Deferred:** Creative Notion embeds (iframe), advanced RFM alert automation.

---

## Notion + Streamlit Integration Strategy

**Principle:** Notion = narrative layer. Streamlit = analytical engine. Separate containers.

### Phase 1 (Tick 7 + Tick 8 — Completed): Portal with Links + LLM Narrative

```
Notion                                Streamlit
─────                                 ─────────
Monthly Review template               Executive Pulse (interactive)
├── Context narrative                  Customer Analytics (interactive)
├── [→ Open Executive Pulse]           Operations Center (interactive)
├── [→ Open Customer Analytics]        Financial Performance (interactive)
├── Action items & discussion
└── Per-persona SOPs
```

### Phase 2 (Tick 10 — Prefect): Portal with KPI Database

```
Notion                                Streamlit
─────                                 ─────────
KPI Database (auto-populated)         Full interactive dashboards
├── Feb 2026: Customers 242 ▲+8%      (unchanged)
├── Feb 2026: Revenue Dhs 25.7K
├── [→ Drill down in Streamlit]
└── Updated automatically via
    Prefect → Notion API
```

### Phase 3 (Tick 7B — Future): Portal with Embedded Frames

```
Notion Page: "Monthly Business Review"
├── Narrative context
├── [Embedded Streamlit: KPI snapshot]  ← iframe, specific tab
├── Discussion & action items
├── [Embedded Streamlit: Cohort]        ← iframe, specific tab
└── Requires: URL-based tab selection (?tab=snapshot)
```

---

## Postgres Migration (Dedicated Cycle, After POC)

Deferred until the full POC is validated (Notion + Appsmith + Streamlit + Prefect all working on DuckDB). This is a significant infrastructure investment — a dedicated tock cycle focused entirely on the database migration.

**Architecture: Single Postgres (no DuckDB alongside)**

```
BEFORE (POC)                         AFTER (Migration)
────────────                         ─────────────────
Polars ETL → CSV/Parquet             Polars ETL → Postgres (single source of truth)
              ↓                                    ↓
           DuckDB                    Materialized views (in Postgres)
              ↓                                    ↓
           Streamlit                 Streamlit (queries Postgres directly)
           Appsmith → FastAPI → DuckDB    Appsmith → FastAPI → Postgres
```

At Moonwalk's scale (~7K orders, ~1.2K customers), Postgres handles both OLTP and OLAP. Materialized views pre-compute dashboard aggregations. DuckDB can be re-added later via `postgres_scanner` if analytical workloads outgrow Postgres.

### Phase M1 — Schema & Migration

| # | Item | Details |
|---|------|---------|
| M.1 | **Design Postgres schema** | Tables for sales, customers, items, customer_quality, dim_period + HR/finance. Explicit DDL with types, constraints, FOREIGN KEYs. |
| M.2 | **Stand up local Postgres** | Docker or native install. Alembic migrations from day 1. |
| M.3 | **Materialized views** | Pre-computed monthly/weekly summaries for dashboard queries. Refresh triggered by Prefect after ETL. |
| M.4 | **ETL writes to Postgres** | ETL outputs to Postgres (primary) AND CSV/Parquet (Excel compatibility). |
| M.5 | **Retire DuckDB** | Remove `cleancloud_to_duckdb.py`. Dashboard and FastAPI read from Postgres. |

### Phase M2 — Hardening

| # | Item | Details |
|---|------|---------|
| M.6 | **FastAPI → Postgres** | Swap DuckDB data source for SQLAlchemy + Postgres. API contracts unchanged. |
| M.7 | **pgvector** (if needed) | Install extension for future vector embeddings (customer comms, document search). |
| M.8 | **Deterministic Order IDs** | Replace row-index with content hash (in Postgres schema). |
| M.9 | **DQ validation framework** | Structured JSON artifact, integrated with Prefect. |
| M.10 | **Run manifest** | Per-run JSON with source hashes, row counts, DQ summary. |

### When Postgres Arrives

| Concern | POC (DuckDB) | Post-Migration (Postgres) |
|---------|-------------|--------------------------|
| System of record | CSV files in local staging | Postgres |
| Type enforcement | TRY_CAST + ALTER TABLE (30+ ops, with loss logging) | CREATE TABLE with explicit DDL |
| Referential integrity | None (468 orphans accepted) | FOREIGN KEYs enforced |
| Concurrent access | Single reader + single writer | Multiple via connection pool |
| ENUM extensibility | Hard-coded in Python (with pre-validation) | Postgres ENUM or CHECK constraints |
| Transactions | None | Native ACID |
| Analytics | DuckDB (primary) | Materialized views in Postgres (re-add DuckDB via `postgres_scanner` if needed) |
| Vector search | Not available | pgvector extension |

---

## MVP Delivery Plan (After Postgres Migration)

These items build on the Postgres foundation. They add enterprise features to the validated POC.

### Phase 3A — Security & Access

| # | Item | Details |
|---|------|---------|
| V.1 | **RBAC** | FastAPI JWT auth + Postgres RLS. Role-based Appsmith views. |
| V.2 | **Approval workflows** | Prefect + Appsmith + FastAPI for HR/operational approvals. |
| V.3 | **SSO/IdP integration** | Azure AD or similar for single sign-on. |

### Phase 3B — Data Model Evolution

| # | Item | Details |
|---|------|---------|
| V.4 | **Data contracts** | Schema validation between ETL and Postgres. Breaking change detection. |
| V.5 | **dim_order / dim_item** (if needed) | Build in Postgres with proper surrogate keys. |
| V.6 | **Audit logging** | Track all data changes with timestamps and user attribution. |

---

## What NOT to Change

| # | Item | Why |
|---|------|-----|
| 1 | Don't introduce Postgres during POC | Validate the full app experience first (Notion + Appsmith + Streamlit + Prefect). Postgres is a dedicated migration cycle after POC. |
| 2 | Don't run DuckDB alongside Postgres | At Moonwalk's scale, single Postgres with materialized views handles both OLTP and OLAP. Re-add DuckDB via `postgres_scanner` only if analytical workloads outgrow Postgres. |
| 3 | Don't use order counts as KPIs | Orders conflate subscriptions, invoices, multi-item orders. Use Customers, Items, Revenue, Stops as primary measures. |
| 4 | Don't divide revenue by stops | Stops = logistics productivity (physical trips). Revenue per delivery = revenue on HasDelivery orders / delivery count. |
| 5 | Don't show legacy outstanding | `Source = 'Legacy'` orders assumed paid (RePOS archive). Only `Source = 'CC_2025'` for receivables analysis. |
| 6 | Don't use live LLM calls in dashboard | Rules-based templates computed during DuckDB rebuild. Deterministic, cached, no API cost or latency. |
| 7 | Don't invest further in PowerShell | Phased out by Prefect in Tick 9; current Tock 5 hardening is sufficient until then. |
| 8 | Don't add RBAC for POC | Auth comes post-migration (SSO/IdP). |
| 9 | Don't use lazy evaluation in ETL | Eager loading by design (shared data dict across transforms). |
| 10 | Don't make Streamlit do CRUD | Streamlit = analytical (read-only). Appsmith = operational (CRUD). Separation of concerns. Appsmith is Tick 9. |

---

## Tech Stack Summary

### POC Stack (Full — Target)

```
CleanCloud CSV (manual download)
    → Polars ETL (0.8s, in-process)
    → CSV + Parquet (local staging)
    → DuckDB (analytics engine, file-based, 0.5s rebuild)
        + insights table (rules-based, generated during rebuild)
    → Streamlit (4 persona pages, 15 tabs, analytics dashboards)
    → notion_push.py (LLM narrative: GPT-4o-mini → Notion, runs after each refresh)
    → FastAPI (lightweight API layer, reads DuckDB)
    → Appsmith (operational UI: outstanding balances, customer lookup)
    → Notion (portal: narrative + links + KPI database via API)

Orchestration: Prefect (replaces PowerShell)
Development: Claude Code
Tests: 147 (pytest + Playwright)
Deployment: Streamlit Community Cloud (auto-deploy on push)
```

### Post-Migration Stack (Single Postgres)

```
CleanCloud CSV → Polars ETL → Postgres (single source of truth)
                                  ↓
                              Materialized views (dashboard aggregations)
                                  ↓
                              FastAPI (API layer)
                              ↓            ↓
                          Appsmith    Streamlit
                        (operational)  (analytics)

Orchestration: Prefect (ETL → mat view refresh → Notion push → notifications)
Portal: Notion (narrative + KPI database + embedded Streamlit frames)
Future: pgvector (vector search inside Postgres)
Development: Claude Code
```

---

## Quick Wins (Can Do Anytime)

| # | Item | File | Status |
|---|------|------|--------|
| QW-1 | Fix `CohortMonth == ""` (should be `.is_null()`) | `transform_all_customers.py:212` | **Done (Tick 6A)** |
| QW-2 | Add comment explaining Polars `!= ""` checks | `helpers.py:60` | Open |
| QW-3 | Remove redundant DimPeriod columns at source | `generate_dimperiod.py` | Skipped — affects DuckDB schema + possibly Excel |
| QW-4 | Add docstrings to `fmt_*` helpers | `dashboard_shared.py:366-401` | **Done (Tick 6A)** |

---

## Priority Summary

```
DONE
├── Tock 6: 3 CRITICAL + 2 HIGH data integrity fixes (2026-02-17)
├── Tick 6A: Dashboard polish — sidebar, caching, quick wins (2026-02-17)
├── Tock 7: Test coverage 93 → 145 (TRY_CAST, Playwright, edge cases) (2026-02-17)
├── Tick 6B: Streamlit Cloud deploy + cloud-ready config (2026-02-17)
├── Tock 7B: Security — password gate + DuckDB AES-256 encryption (2026-02-18)
├── Tick 7: Persona-based dashboard redesign — 4 pages, 15 tabs (2026-02-19)
├── Tick 8: Closed periods, UI polish, Notion portal + LLM narrative pipeline (2026-02-19)
└── Tock 8: Playwright rewrite (4-page), 12 dead pages deleted, 147 tests (2026-02-19)
│
NOW (POC — operational layer)
├── Tick 9: Appsmith operational UI (outstanding balances, customer lookup)
├── Tick 10: Prefect orchestration + Notion KPI database (LLM narrative push done in Tick 8)
└── Tick 7B: Dashboard enhancements (URL tabs, PDF report, RFM defs) ✅ DONE

NEXT (Postgres Migration — dedicated cycle)
├── Postgres schema + Alembic migrations
├── Materialized views for dashboard aggregations
├── ETL writes to Postgres (+ CSV for Excel compat)
├── Retire DuckDB, FastAPI/Streamlit → Postgres
└── pgvector (if unstructured data needed)

LATER (MVP — enterprise features on Postgres)
├── RBAC (JWT + Postgres RLS)
├── Approval workflows (Prefect + Appsmith)
├── Data contracts, audit logging
├── SSO/IdP, cloud deployment
└── HA, observability, security hardening
```

---

## Source Documents

| Document | Location | Status |
|----------|----------|--------|
| `roadmap-os-v1.md` | `Downloads/` | Strategic reference — defines POC/MVP/Beta/Production phases |
| `spec-stripe-recon.md` | `Downloads/Lime Reporting/` | Planned — parallel Stripe reconciliation module |
| `guide-agentic-dev-workflow.md` | `Downloads/` | Reference — agentic finance dev framework |
| `template-project-intake.md` | `Downloads/Lime Reporting/` | Template — new project planning scaffold |
| `CLAUDE.md` | Project root | Living document, updated per cycle — v5.6 (post Tick 8) |
| **Code review (Feb 2026)** | This document | 3 CRITICAL + 3 HIGH fixed; 3 MEDIUM + 3 LOW remaining |

**Deleted (absorbed into this roadmap):** `Design_overhaul.md`, `Python_ETL_Cleanup.md`, `Tier5_Harden.md`, `productivity_wins.md`
