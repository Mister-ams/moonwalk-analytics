---
project: moonwalk
type: history
status: archived
created: 2026-02-19
note: Removed from CLAUDE.md on 2026-02-19 to keep it lean. Canonical history lives in roadmap-moonwalk.md.
---

# Moonwalk Analytics — Completed Cycle History

## Early Cycles (pre-Audit, Feb 2026)

| Cycle | Tier | Focus | Key Deliverables |
|-------|------|-------|-----------------|
| Tick | 1 — Quick Wins | Functionality fixes | DuckDB file-based connection, vectorized transforms, NaT validation, temp-file swap, pipeline integration |
| Tick | 2 — Refactoring | Code consolidation | Config centralization (`config.py`/`config.ps1`), `render_detail_page()` shared renderer, `fetch_measures_batch()`, launcher workflows |
| Tock | 3 — Engineering | Reliability & performance | Structured logging (8 scripts), type hints (34 functions), in-process orchestrator, memory optimization, lock file protection |
| Tock | 3.5 — Tooling | Claude Code infrastructure | 5 plugins, 2 MCP servers, 5 custom skills, Ruff auto-format hook |
| Tick | 4 — Dashboard | 6-section reporting | 5 new pages (Customer Insights, Cohort, Logistics, Operations, Payments), `section_data.py`, sidebar nav, 12 total pages |
| Tick | 4.5 — Weekly Toggle | Weekly/monthly granularity | Weekly/monthly toggle, grain-aware SQL across all pages, 13-week chart window, sidebar icon fix, cross-page toggle persistence |
| Tick | 4.6 — Visual Upgrade | UX/design polish | 3-tier color hierarchy, compound headline cards, `st.pills` segmented control, compact chart annotations, geo split chart, clean footer |
| Tock | Audit 1-3 — Critical/Quality/Perf | Full-stack audit fixes | ETL import bugs, `order_lookup` table, `get_grain_context()`, BOOLEAN/SMALLINT casts, redundant col drops, inter-stage validation, portable paths |
| Tock | Audit 4 — Tech Stack | Profiling + modernization | ETL/dashboard profiling (tracemalloc + JSON), Parquet dual-output, DuckDB ENUM types (7 types, 9 cols), Polars PoC, dashboard pandas eval (stayed) |
| Tock | Polars Migration | Full ETL migration | All 7 ETL files migrated from pandas to Polars. 9x speedup (7s to 0.8s), 16x less memory (90 MB to 5.6 MB). Golden baseline verified. |

## Full-Stack Audit Re-Tiered Plan (Feb 2026)

Audit covered ~6,900 LOC across 34 files. Found 3 critical bugs, 468 orphaned item orders, significant optimization opportunities.
Full plan: `.claude/plans/tier5-full-stack-audit.md`

| Tier | Focus | Status |
|------|-------|--------|
| 1 — Critical Fixes | ETL crash bugs (missing imports), missing index, pre-flight CSV gap | Completed 2026-02-16 |
| 2 — Code Quality | Extract shared helpers, materialize `order_lookup`, unify MoM logic, standardize paths | Completed 2026-02-16 |
| 3 — Perf & Integrity | DuckDB native `read_csv_auto()`, BOOLEAN/SMALLINT casts, redundant col drops, dim_period indexes, groupby consolidation, inter-stage validation, NaT warnings, portable paths | Completed 2026-02-16 |
| 4 — Tech Stack | Profile baselines, Parquet migration, Polars PoC, ENUM types | Completed 2026-02-16 |
| Polars Migration | Full ETL pandas to Polars (9x speedup, 16x memory reduction) | Completed 2026-02-16 |
| 5 — Cloud Readiness | Test suite (82 tests), PS reliability (timeouts/logging), env-based config, cross-platform CLI | Completed 2026-02-16 |
| Tock 6 — Data Integrity | Subscription overlap merge, CohortMonth null validation, TRY_CAST loss logging, ENUM pre-validation, Polars join idioms. 93 tests. | Completed 2026-02-17 |

## Completed Cycles (post-Audit, Feb 2026)

| Cycle | Type | Focus | Completed |
|-------|------|-------|-----------|
| Tick 6A | Feature | Dashboard polish — sidebar regroup, period cache, CohortMonth fix | 2026-02-17 |
| Tock 7 | Quality | Test coverage: 93 to 145 tests (TRY_CAST, Playwright, empty DataFrame, order_lookup) | 2026-02-17 |
| Tick 6B | Feature | Streamlit Cloud deploy, `IS_CLOUD` config, cloud-resilient logging | 2026-02-17 |
| Tock 7B | Quality | Security — password gate (`hmac`), DuckDB AES-256, git history purge, Streamlit Cloud secrets | 2026-02-18 |
| Tick 7 | Feature | Persona-based redesign — 4 pages, 15 tabs, YoY, M0-M3+ cohort, RFM, CLV, insights engine, pareto, outstanding | 2026-02-19 |
| Tick 8 | Feature | Closed period filter, ISO week labels, token bypass, Notion portal, GPT-4o-mini insights | 2026-02-19 |
| Tock 8 | Quality | Playwright rewrite (20 tests), delete 12 old pages, cloud deploy verification. 147 tests. | 2026-02-19 |
| Tick 9 | Feature | FastAPI `api/` package, SQLite employee CRUD, Railway artifacts, `Start-API.ps1`. 160 tests. | 2026-02-19 |
| Tick 7B | Feature | URL tab selection (`activate_tab_from_url`), PDF report (`generate_report.py`, fpdf2), RFM segment definitions panel, Notion `?tab=` links. 164 tests. | 2026-02-19 |
