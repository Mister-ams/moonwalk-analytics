"""Prefect orchestration flow for Moonwalk Analytics refresh pipeline.

Wraps ETL -> DuckDB -> Notion pipeline as a Prefect flow with retries
and task-level visibility.  Notion tasks are non-fatal: exceptions are
caught and logged as warnings so the flow always completes.

Usage:
    python moonwalk_flow.py             # run directly
    prefect flow run moonwalk_flow.py  # via Prefect server (if deployed)
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from prefect import flow, task, get_run_logger

from config import ANALYTICS_DATABASE_URL, DB_PATH, LOCAL_STAGING_PATH


def _resolve_db_path():
    """Return the freshest readable DuckDB path.

    After a rebuild the live file may still be locked by the dashboard;
    cleancloud_to_duckdb.py writes the new DB to DB_PATH + '.tmp' in that case.
    This function returns the .tmp path if it is newer than the live file,
    otherwise returns the live DB_PATH.
    """
    tmp = DB_PATH.with_suffix(".duckdb.tmp")
    if tmp.exists():
        live_mtime = DB_PATH.stat().st_mtime if DB_PATH.exists() else 0
        if tmp.stat().st_mtime > live_mtime:
            return tmp
    return DB_PATH


_REQUIRED_CSVS = [
    "All_Sales_Python.csv",
    "All_Customers_Python.csv",
    "All_Items_Python.csv",
    "Customer_Quality_Monthly_Python.csv",
    "DimPeriod_Python.csv",
]


# =====================================================================
# TASKS
# =====================================================================


@task(name="validate-source-csvs", retries=0)
def validate_source_csvs():
    """Fail fast if required source CSVs are missing — no point retrying."""
    log = get_run_logger()
    missing = [f for f in _REQUIRED_CSVS if not (LOCAL_STAGING_PATH / f).exists()]
    if missing:
        raise FileNotFoundError(f"Missing required CSVs in {LOCAL_STAGING_PATH}: {', '.join(missing)}")
    log.info(f"Validated {len(_REQUIRED_CSVS)} source CSVs in {LOCAL_STAGING_PATH}")


@task(name="run-etl", retries=1, retry_delay_seconds=5)
def run_etl():
    """Run Polars ETL pipeline (~0.8s)."""
    log = get_run_logger()
    log.info("Starting ETL pipeline...")
    import cleancloud_to_excel_MASTER

    cleancloud_to_excel_MASTER.main()
    log.info("ETL pipeline complete")


@task(name="run-duckdb", retries=1, retry_delay_seconds=5)
def run_duckdb():
    """Rebuild DuckDB analytics database (~0.5s)."""
    log = get_run_logger()
    log.info("Starting DuckDB rebuild...")
    import cleancloud_to_duckdb

    cleancloud_to_duckdb.main()

    # Try to promote .tmp -> live; if locked by dashboard, log and continue.
    # Subsequent tasks use _resolve_db_path() to prefer .tmp when it is fresher.
    tmp_path = DB_PATH.with_suffix(".duckdb.tmp")
    if tmp_path.exists():
        try:
            import shutil

            shutil.move(str(tmp_path), str(DB_PATH))
            log.info(f"DuckDB rebuild complete - promoted .tmp to {DB_PATH}")
        except Exception:
            log.warning(f"DuckDB rebuild complete - live file locked, .tmp retained at {tmp_path}")
    elif DB_PATH.exists():
        size_mb = DB_PATH.stat().st_size / (1024 * 1024)
        log.info(f"DuckDB rebuild complete - {size_mb:.1f} MB at {DB_PATH}")
    else:
        log.info("DuckDB rebuild complete")


@task(name="run-postgres", retries=1, retry_delay_seconds=5)
def run_postgres():
    """Bulk-load Parquet files into Railway Postgres analytics schema (non-fatal).

    Skips silently when ANALYTICS_DATABASE_URL is not configured so the flow
    continues to work in local/DuckDB-only environments.
    """
    log = get_run_logger()
    if not ANALYTICS_DATABASE_URL:
        log.info("ANALYTICS_DATABASE_URL not set — skipping Postgres sync")
        return
    try:
        import cleancloud_to_postgres

        summary = cleancloud_to_postgres.main()
        rows = {k: v for k, v in summary.items() if k != "elapsed_s"}
        log.info(
            f"Postgres sync complete in {summary.get('elapsed_s', 0):.1f}s — "
            + ", ".join(f"{k}={v}" for k, v in rows.items())
        )
    except Exception as exc:
        log.warning(f"Postgres sync failed (non-fatal): {exc}")


@task(name="push-notion-narrative", retries=2, retry_delay_seconds=10)
def push_notion_narrative():
    """Push GPT-4o-mini narrative insights to Notion portal toggle (non-fatal)."""
    log = get_run_logger()
    try:
        import notion_push

        notion_push.DB_PATH = _resolve_db_path()
        notion_push.run(log=log.info)
    except Exception as exc:
        log.warning(f"Notion narrative push failed (non-fatal): {exc}")


# =====================================================================
# FLOW
# =====================================================================


@flow(name="moonwalk-refresh", log_prints=True)
def moonwalk_refresh():
    """Full Moonwalk Analytics refresh: ETL -> DuckDB -> Postgres -> Notion."""
    log = get_run_logger()
    flow_start = time.time()
    timings: dict[str, float] = {}

    def _run(label: str, fn):
        t0 = time.time()
        fn()
        timings[label] = time.time() - t0

    _run("Validate CSVs", validate_source_csvs)
    _run("ETL pipeline", run_etl)
    _run("DuckDB rebuild", run_duckdb)
    _run("Postgres sync", run_postgres)
    _run("Notion narrative", push_notion_narrative)

    total = time.time() - flow_start
    log.info("")
    log.info("=" * 50)
    log.info("PERFORMANCE SUMMARY")
    log.info("=" * 50)
    for label, elapsed in timings.items():
        log.info(f"  {label:<22} {elapsed:>6.1f}s")
    log.info("-" * 50)
    log.info(f"  {'TOTAL':<22} {total:>6.1f}s")
    log.info("=" * 50)


if __name__ == "__main__":
    moonwalk_refresh()
