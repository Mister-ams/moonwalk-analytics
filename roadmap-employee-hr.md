---
project: employee-hr
type: roadmap
status: active
created: 2026-02-21
updated: 2026-02-21
---

# Employee HR Database — Project Roadmap

**Objective**
Auto-populating employee database driven by structured documents uploaded to OneDrive. Extracts employee profile, salary, and compliance data from PDFs; surfaces expiry alerts and compliance status; managed via Appsmith portal with RBAC.

---

## Architecture

```
OneDrive (/HR/Contracts/, /HR/Compensation/, /HR/Benefits/)
        |
        v  Graph webhook + delta sync
onedrive-sync          <- webhook validation, delta token persistence, 429/retry-after backoff
        |
        v  file reference + trigger
prefect-flows          <- orchestrates ingest -> parse -> validate -> upsert -> alert
        |
        +-> doc-normalizer    <- PDF parser, confidence scoring, field precedence + provenance
        |         |
        |         v  valid records / low-confidence -> exception queue
        +-> employee-api      <- FastAPI, RBAC via Entra ID OIDC, upsert, field-level masking
        |         |
        |         v
        +-> hr-postgres       <- employees, salary_records, compliance_docs, audit_events, snapshots
        |
        +-> compliance-rules  <- expiry monitoring -> audit-alerts -> 60/30/7 day notifications
        |
        v
Appsmith HR Portal     <- employee list, compliance dashboard, exception queue, CSV export
```

### Component Roles

| Component | Role |
|-----------|------|
| `onedrive-sync` | Graph webhook handling, delta token persistence, 429 backoff, full resync on token invalidation |
| `doc-normalizer` | Document-type parsers (contract PDF first), confidence scoring per field, field precedence + provenance metadata |
| `employee-api` | FastAPI — upsert, filter, field-level masking by role, RBAC via Entra ID OIDC |
| `hr-postgres` | Canonical store — `employees`, `salary_records`, `compliance_docs`, `audit_events`, `snapshots` |
| `compliance-rules` | Evaluates expiry/coverage rules, creates alert records |
| `audit-alerts` | Notification delivery (email/Slack), retention/deletion event logging, legal-hold flags |
| `prefect-flows` | Pipeline orchestration — shared deployment with Moonwalk |
| `appsmith-portal` | HR portal pages — employee records, exception queue, compliance dashboard, CSV export |

### RBAC Roles (Entra ID)

| Role | Salary fields | Compliance fields | Write |
|------|:---:|:---:|:---:|
| HR Admin | yes | yes | yes |
| Payroll Analyst | yes | read | salary records |
| Compliance Officer | no | yes | alert dispositions |
| Read-Only Auditor | **masked** | yes | no |

### Stack Alignment to Moonwalk

| Layer | This project | Moonwalk |
|-------|-------------|----------|
| Backend | FastAPI (`hr/` prefix on existing Railway app) | FastAPI (`api/`) |
| Storage | hr-postgres (separate DB, `hr` schema) | DuckDB -> Postgres (Tock M) |
| Portal | Appsmith (existing instance, new HR pages) | Appsmith |
| Orchestration | Prefect (shared or separate workspace) | Prefect |
| Auth | Microsoft Entra ID OIDC | (none yet) |

---

## PoC Document Contract — Employment Contract PDF (v1)

**File convention**: `contract_{employee_id}_{YYYYMMDD}.pdf`
**OneDrive path**: `/HR/Contracts/`
**Assumption**: machine-readable PDF text layer (not scanned). Validate with 3 real samples before Sprint 1 starts.

**Required fields** — all must be extracted with confidence >= 0.95 or record routes to exception queue:

| Field | Type | Notes |
|-------|------|-------|
| `employee_id` | string | Staff/employee number |
| `full_name` | string | As on Emirates ID / passport |
| `job_title` | string | |
| `base_salary` | decimal | Monthly, AED |
| `contract_start_date` | date | ISO 8601 |
| `contract_expiry_date` | date | ISO 8601 — primary compliance field |
| `insurance_status` | enum | `active` / `expired` / `missing` |

**Confidence scoring**: Field confidence = extraction method weight (exact regex = 1.0, fuzzy pattern = 0.85, heuristic = 0.70). Record-level confidence = min(field confidences). Records < 0.95 route to Appsmith exception queue with per-field scores visible to reviewer.

---

## Roadmap

### POC — Single contract PDF, end-to-end

**Sprint 1 — POC Tick** (8 pts)
Goal: One contract PDF lands in OneDrive, gets parsed, upserts to hr-postgres, visible in Appsmith.

- [P1][M] `onedrive-sync` receives a test Graph webhook for `/HR/Contracts/` and stores one file reference with `status=queued`
- [P1][M] `doc-normalizer` extracts all 7 required fields from a sample contract PDF with confidence >= 0.95
- [P1][M] `employee-api` upsert writes one `employees` row + one `salary_records` row to hr-postgres, returns HTTP 200
- [P2][M] Appsmith employee detail page displays salary and contract expiry for the inserted employee
- [RT-SEC-001] `onedrive-sync` validates Graph `clientState` + `validationToken` handshake; rejects unsigned payloads before enqueueing

**Sprint 2 — POC Tock** (6 pts)
Goal: Schema validation, idempotency, baseline RBAC, audit trail.

- [P1][S] `doc-normalizer` rejects a malformed PDF with deterministic error code `PARSE_VALIDATION_FAILED`
- [P1][S] `doc-normalizer` routes low-confidence records (< 0.95) to Appsmith exception queue with per-field confidence scores
- [P1][M] Reprocessing the same `file_id` does not create duplicate rows in hr-postgres (idempotent upsert on `employee_id` + `file_id`)
- [P1][M] `employee-api` returns HTTP 403 for salary fields when called with a non-Payroll Analyst token
- [P2][S] `audit-alerts` writes one audit event linking `source_document_id` -> `target_employee_id` per successful upsert
- [RT-SEC-001] `onedrive-sync` returns HTTP 401 when `clientState` is missing or mismatched

---

### MVP — Multi-document, compliance automation, full exception handling

**Sprint 3 — MVP Tick** (12 pts)
Goal: Three document types, compliance rules automated, exception workflow in Appsmith.

- [P1][L] `prefect-flows` processes three document types (contract PDF, compensation sheet, benefits form) and updates related hr-postgres tables
- [P1][M] `compliance-rules` creates an alert record when `contract_expiry_date` is within 30 days
- [P1][M] `compliance-rules` marks `insurance_status = non-compliant` when status is `missing` or `expired`
- [P2][M] Appsmith exception queue allows a reviewer to correct one failed parse and resubmit to `employee-api`
- [P2][M] `employee-api` filter endpoint returns employees with expiring contracts in < 1 second for 10k records
- [RT-INT-001] `onedrive-sync` integration test simulates Graph 429 and verifies retry-after backoff with eventual successful sync
- [RT-INT-001] `onedrive-sync` executes full delta resync when stored delta token is invalid, records recovery event in `audit-alerts`

**Sprint 4 — MVP Tock** (10 pts)
Goal: Test coverage >= 80%, observability, secrets hygiene, schema docs.

- [P1][L] Backend test suite covers `onedrive-sync` + `doc-normalizer` + `employee-api` integration path at >= 80% line coverage
- [P1][M] `prefect-flows` retries transient Graph API failures with exponential backoff within configured retry budget
- [P1][S] All OneDrive and database secrets loaded from environment/secret manager; none hardcoded in repo
- [P2][S] `docs/data-contracts` publishes versioned schema docs for all three supported document types with sample payloads
- [P2][M] `audit-alerts` error rate and ingestion latency visible on one Appsmith dashboard with alert thresholds
- [RT-DEL-001] CI pipeline blocks merge unless unit/integration tests, migration dry-run, and secret-scan pass
- [RT-INT-001] `onedrive-sync` integration test covers expired delta token and confirms full resync restores ingestion within SLA

---

### Production — Notifications, masking, throughput, governance

**Sprint 5 — Production Tick A** (10 pts)
Goal: Full lifecycle compliance notifications, field masking, historical snapshots.

- [P1][M] `audit-alerts` sends contract expiry notifications at 60/30/7 day intervals and records delivery status
- [P1][M] `employee-api` enforces field-level masking so Read-Only Auditor role cannot view `base_salary` values
- [P2][L] hr-postgres stores historical snapshots for salary and compliance changes with effective timestamps
- [RT-ARC-001] `doc-normalizer` stores provenance metadata per field (source doc type, timestamp, applied precedence policy)
- [RT-SEC-002] Column-level encryption for salary and insurance fields with managed key references

**Sprint 6 — Production Tick B** (12 pts)
Goal: High-volume ingestion throughput, retention/deletion governance, key rotation validation.

- [P1][XL] `onedrive-sync` + `prefect-flows` ingest 1000 documents/hour at >= 99% successful completion
- [P1][M] Appsmith exports compliance report (CSV) with `employee_id`, `contract_expiry_date`, `insurance_status`, `alert_state`
- [RT-DAT-002] Retention job deletes expired raw documents and derived records per policy; preserves immutable audit entries
- [RT-SEC-002] Production run validates key rotation for encrypted salary/compliance fields without query downtime

**Sprint 7 — Production Tock** (10 pts)
Goal: DR validation, security controls, runbook, operational handoff.

- [P1][L] Restore test recovers hr-postgres from backup into clean environment with RPO <= 15 minutes
- [P1][M] Load test confirms `employee-api` P95 read latency <= 500ms at 200 concurrent users
- [P1][M] Security test verifies OIDC token expiry and refresh handling across Appsmith portal and `employee-api`
- [P2][S] Production runbook documents incident ownership, escalation paths, recovery for `onedrive-sync` and `prefect-flows`; defines on-call owner for sync failures
- [P3][S] Release checklist requires signed approval from HR owner and compliance owner before deployment
- [RT-DAT-002] Retention/deletion workflow executes against staged dataset and produces signed audit report with legal-hold exceptions

---

## Red-Team Controls

| ID | Severity | Finding | Control | Sprint |
|----|----------|---------|---------|--------|
| RT-SEC-001 | High | Webhook authenticity unvalidated — spoofed callbacks can poison records | `clientState` + `validationToken` handshake; HTTP 401 on mismatch | 1-2 |
| RT-SEC-002 | High | No formalized encryption for salary/compliance data at rest | Column-level encryption in hr-postgres; key rotation validated in Production | 5, 6 |
| RT-DAT-001 | High | No confidence threshold or human-review policy for low-confidence parses | Per-field confidence scoring; records < 0.95 routed to Appsmith exception queue | 2 |
| RT-DAT-002 | Medium | No data retention/deletion policy for raw docs and derived records | Retention schedules, legal-hold rules, deletion workflows with audit logging | 6, 7 |
| RT-INT-001 | Medium | No Graph API rate-limit handling or delta token expiry recovery | 429 backoff with retry-after; full resync on token invalidation; integration tests | 3, 4 |
| RT-DEL-001 | Medium | No CI/CD pipeline or deployment gates | CI workflow with test/secret/migration checks; manual approval gate for production | 4 |
| RT-ARC-001 | Medium | No canonical source-of-truth policy when multiple docs update same field | Field-level precedence rules + provenance metadata (source, timestamp, policy) | 5 |
| RT-INF-001 | Low | No environment isolation strategy (dev/staging/prod) | Environment isolation standards + seeded anonymized test datasets | 4 |

---

## Open Items — Resolve Before Sprint 1

- [ ] **PDF quality** — Validate 3 real contract PDFs are machine-readable (not scanned). If scanned, OCR (Azure Document Intelligence) adds significant Sprint 1 scope.
- [ ] **Insurance status source** — Is insurance status in the contract PDF or a separate benefits document? If separate, `insurance_status` will be `null` in PoC until Sprint 3.
- [ ] **OneDrive permissions** — Entra app registration needs `Files.Read.All` + webhook subscription rights provisioned before Sprint 1.
- [ ] **Alert delivery channel** — Email (Graph Mail API) or Slack? Must be decided before Sprint 5.
- [ ] **Appsmith auth** — Confirm whether Entra ID OIDC app registration is shared with `employee-api` or separate instance. Needed before Sprint 2 RBAC work.
