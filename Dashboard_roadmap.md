# Moonwalk Analytics — Master Project Roadmap

**Created:** 16 February 2026
**Last updated:** 18 February 2026 (v5.3 — Tock 7B Security complete)
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
| **Tock 7B** | **Quality** | **Security: Password gate (`hmac` + `st.secrets`), DuckDB AES-256 encryption (ATTACH pattern), `DUCKDB_KEY` in config, Playwright auth tests. Git history purged (`git-filter-repo`), encrypted DB pushed to cloud.** |

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
| 6.11 | **Notion portal setup** | OS v1.0 POC | Pending (manual) |

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

### Tick 7 — New Reporting Features

**Focus:** Expand analytics capabilities. These persist into MVP and beyond.
**Scope:** New pages, new SQL queries.

| # | Item | Complexity | Business Value |
|---|------|-----------|---------------|
| 7.7 | Year-over-Year comparison mode | Medium | High — trend context beyond MoM |
| 7.8 | Customer segmentation (RFM analysis) | Medium | High — actionable customer tiers |
| 7.9 | Subscription churn/retention dashboard | Medium | High — subscription health visibility |
| 7.10 | Revenue forecasting trend lines | Low | Medium — visual projection |
| 7.11 | Hielo store parity in customers table | Low | Medium — complete customer view |
| 7.12 | Export/download functionality | Low | Medium — share reports outside dashboard |

---

### Tick 8 — Appsmith Operational UI

**Focus:** Build the operational application layer on the current DuckDB/CSV stack.
**Scope:** Appsmith deployment, FastAPI skeleton, Notion portal integration. No Postgres.

| # | Item | Details | Effort |
|---|------|---------|--------|
| 8.1 | **FastAPI skeleton** | CRUD endpoints reading from DuckDB/CSV, health endpoint. Lightweight — no auth yet. | 3-4 hrs |
| 8.2 | **Appsmith deployment** | Local or cloud Appsmith instance. Connect to FastAPI. | 2-3 hrs |
| 8.3 | **HR registry UI** | Employee/document management screens in Appsmith. | 3-4 hrs |
| 8.4 | **Notion portal update** | Links to Appsmith (operational) + Streamlit (analytics). SOPs. | 1-2 hrs |
| 8.5 | **Phase out Excel PowerPivot** | Once Streamlit + Appsmith provide equivalent coverage. | 1 hr |

**Note:** FastAPI reads from DuckDB during POC. When Postgres arrives, only the data source changes — API contracts and Appsmith UI remain unchanged.

---

### Tick 9 — Prefect Orchestration

**Focus:** Replace PowerShell automation with Python-native orchestration.
**Scope:** Prefect deployment, ETL scheduling, failure notifications.

| # | Item | Details | Effort |
|---|------|---------|--------|
| 9.1 | **Prefect deployment** | Local Prefect server or Prefect Cloud (free tier). | 1-2 hrs |
| 9.2 | **ETL flow** | Wrap `cleancloud_to_excel_MASTER.py` as a Prefect flow with task-level retries. | 2-3 hrs |
| 9.3 | **DuckDB rebuild task** | `cleancloud_to_duckdb.py` as a downstream Prefect task, triggered after ETL. | 1 hr |
| 9.4 | **Scheduling** | Cron-based or file-watcher trigger for ETL runs. | 1 hr |
| 9.5 | **Failure notifications** | Email/Slack alerts on flow failure via Prefect automations. | 1 hr |
| 9.6 | **Phase out PowerShell automation** | Prefect handles all orchestration. Retire `refresh_moonwalk_data.ps1`. | 1 hr |

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
| 3 | Don't split `dashboard_shared.py` | Cohesive at 1,290 LOC; splitting fragments related logic |
| 4 | Don't consolidate `_chg()` closures | Legitimate scope-based duplication |
| 5 | Don't consolidate MoM computation | 3 uses are different (HTML, customdata, annotation) |
| 6 | Don't invest further in PowerShell | Phased out by Prefect in Tick 9; current Tock 5 hardening is sufficient until then |
| 7 | Don't add RBAC for POC | Auth comes post-migration (SSO/IdP) |
| 8 | Don't use lazy evaluation in ETL | Eager loading by design (shared data dict across transforms) |
| 9 | Don't use n8n/LangChain for orchestration | Prefect is Python-native — no context-switching. LangChain solves a different problem (LLM agents, not workflow automation). |

---

## Tech Stack Summary

### POC Stack (Full — Target)

```
CleanCloud CSV (manual download)
    → Polars ETL (0.8s, in-process)
    → CSV + Parquet (local staging)
    → DuckDB (analytics engine, file-based, 0.5s rebuild)
    → FastAPI (lightweight API layer, reads DuckDB)
    → Streamlit (12 pages, analytics dashboards)
    → Appsmith (operational UI, connects to FastAPI)
    → Notion (portal, links to Appsmith + Streamlit)

Orchestration: Prefect (replaces PowerShell)
Development: Claude Code
Tests: 145 (pytest — 60 unit + 29 integration + 23 Playwright + 17 transform + 5 golden baseline)
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

Orchestration: Prefect (ETL → mat view refresh → notifications)
Portal: Notion (links to Appsmith + Streamlit)
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
│
NOW (POC — validate full app experience on DuckDB)
├── Tick 6.11: Notion portal setup (manual)
├── Tick 7: New reporting features (YoY, RFM, churn)
├── Tick 8: Appsmith operational UI + lightweight FastAPI (on DuckDB)
└── Tick 9: Prefect orchestration (replaces PowerShell)

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
| `Operating_System_v1.0.md` | `Downloads/` | Strategic reference — defines POC/MVP/Beta/Production phases |
| `Design_overhaul.md` | `PythonScript/` | Phase 1 complete; remaining items absorbed into Tick 6 |
| `Python_ETL_Cleanup.md` | `Downloads/Lime Reporting/` | Aspirational items deferred to MVP Phase 2C |
| `Tier5_Harden.md` | `.claude/plans/` | Completed; no further investment in PS |
| `productivity_wins.md` | `Downloads/Lime Reporting/` | Phases 1-4 complete; CI/CD deferred to MVP |
| `CLAUDE.md` | Project root | Living document, updated per cycle |
| **Code review (Feb 2026)** | This document | 3 CRITICAL + 3 HIGH fixed; 3 MEDIUM + 3 LOW remaining |
