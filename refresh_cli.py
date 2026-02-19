"""Cross-platform refresh CLI for Moonwalk Analytics.

Runs the ETL pipeline and/or DuckDB rebuild without requiring PowerShell
or Excel COM automation. Suitable for non-Windows environments.

Usage:
    python refresh_cli.py              # ETL + DuckDB (default)
    python refresh_cli.py --etl-only   # ETL pipeline only
    python refresh_cli.py --duckdb-only  # DuckDB rebuild only
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import LOCAL_STAGING_PATH, PYTHON_SCRIPT_FOLDER, DB_PATH
from logger_config import setup_logger

logger = setup_logger("refresh_cli")

REQUIRED_CSVS = [
    "All_Sales_Python.csv",
    "All_Customers_Python.csv",
    "All_Items_Python.csv",
    "Customer_Quality_Monthly_Python.csv",
    "DimPeriod_Python.csv",
]


def run_etl() -> bool:
    """Run the ETL pipeline (cleancloud_to_excel_MASTER.py)."""
    logger.info("=" * 60)
    logger.info("ETL Pipeline")
    logger.info("=" * 60)

    start = time.perf_counter()
    try:
        import cleancloud_to_excel_MASTER

        cleancloud_to_excel_MASTER.main()
    except SystemExit as e:
        if e.code and e.code != 0:
            logger.error(f"ETL pipeline exited with code {e.code}")
            return False
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        return False

    elapsed = time.perf_counter() - start
    logger.info(f"ETL completed in {elapsed:.1f}s")

    # Verify outputs
    missing = [f for f in REQUIRED_CSVS if not (LOCAL_STAGING_PATH / f).exists()]
    if missing:
        logger.error(f"Missing output CSVs: {', '.join(missing)}")
        return False

    logger.info(f"All {len(REQUIRED_CSVS)} output CSVs verified")
    return True


def run_duckdb() -> bool:
    """Rebuild the DuckDB analytics database."""
    logger.info("=" * 60)
    logger.info("DuckDB Rebuild")
    logger.info("=" * 60)

    start = time.perf_counter()
    try:
        import cleancloud_to_duckdb

        cleancloud_to_duckdb.main()
    except SystemExit as e:
        if e.code and e.code != 0:
            logger.error(f"DuckDB rebuild exited with code {e.code}")
            return False
    except Exception as e:
        logger.error(f"DuckDB rebuild failed: {e}")
        return False

    elapsed = time.perf_counter() - start
    logger.info(f"DuckDB rebuild completed in {elapsed:.1f}s")

    if DB_PATH.exists():
        size_mb = DB_PATH.stat().st_size / (1024 * 1024)
        logger.info(f"Database: {DB_PATH} ({size_mb:.1f} MB)")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Moonwalk Analytics — Cross-platform refresh CLI",
    )
    parser.add_argument(
        "--etl-only",
        action="store_true",
        help="Run ETL pipeline only (skip DuckDB rebuild)",
    )
    parser.add_argument(
        "--duckdb-only",
        action="store_true",
        help="Rebuild DuckDB only (skip ETL pipeline)",
    )
    args = parser.parse_args()

    # Default: both
    run_etl_flag = not args.duckdb_only
    run_duckdb_flag = not args.etl_only

    logger.info("Moonwalk Analytics Refresh CLI")
    logger.info(f"  ETL: {'yes' if run_etl_flag else 'skip'}")
    logger.info(f"  DuckDB: {'yes' if run_duckdb_flag else 'skip'}")

    total_start = time.perf_counter()
    success = True

    if run_etl_flag:
        if not run_etl():
            success = False
            if run_duckdb_flag:
                logger.warning("ETL failed — skipping DuckDB rebuild")
                run_duckdb_flag = False

    if run_duckdb_flag:
        if not run_duckdb():
            success = False
        else:
            # Optional Notion push (only if env vars set; logs and exits cleanly if not)
            try:
                from notion_push import run as notion_run

                notion_run(log=logger.info)
            except Exception as e:
                logger.warning(f"Notion push failed (non-fatal): {e}")

    total_elapsed = time.perf_counter() - total_start

    logger.info("")
    logger.info("=" * 60)
    if success:
        logger.info(f"Refresh completed successfully in {total_elapsed:.1f}s")
    else:
        logger.error(f"Refresh completed with errors in {total_elapsed:.1f}s")
    logger.info("=" * 60)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
