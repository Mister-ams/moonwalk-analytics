"""Centralized path and constant configuration with env-var overrides.

All hardcoded paths and magic numbers live here.  Override any path
via the corresponding MOONWALK_* environment variable for portability.
"""

import os
from pathlib import Path

# =====================================================================
# DIRECTORY ROOTS (derived from this file's location)
# =====================================================================

_SCRIPT_DIR = Path(__file__).resolve().parent  # PythonScript/
_DATA_DIR = _SCRIPT_DIR.parent  # Moonwalk Data/

# =====================================================================
# CLOUD DETECTION
# =====================================================================

IS_CLOUD = bool(os.environ.get("STREAMLIT_CLOUD")) or not (Path.home() / "Downloads").exists()

# =====================================================================
# PATHS (override via env vars for portability)
# =====================================================================

_HOME = Path.home()

if IS_CLOUD:
    DOWNLOADS_PATH = _SCRIPT_DIR
    LOCAL_STAGING_PATH = _SCRIPT_DIR
    ONEDRIVE_SALES_DATA_PATH = _SCRIPT_DIR
    PYTHON_SCRIPT_FOLDER = _SCRIPT_DIR
    DB_PATH = _SCRIPT_DIR / "analytics.duckdb"
else:
    DOWNLOADS_PATH = Path(os.environ.get("MOONWALK_DOWNLOADS", str(_HOME / "Downloads")))
    LOCAL_STAGING_PATH = Path(os.environ.get("MOONWALK_STAGING", str(_HOME / "Downloads" / "Lime Reporting")))
    ONEDRIVE_SALES_DATA_PATH = Path(os.environ.get("MOONWALK_ONEDRIVE_DATA", str(_DATA_DIR / "Sales Data")))
    PYTHON_SCRIPT_FOLDER = Path(os.environ.get("MOONWALK_SCRIPTS", str(_SCRIPT_DIR)))
    DB_PATH = _DATA_DIR / "analytics.duckdb"

# Derived file paths
SALES_CSV = str(LOCAL_STAGING_PATH / "All_Sales_Python.csv")
ITEMS_CSV = str(LOCAL_STAGING_PATH / "All_Items_Python.csv")
DIMPERIOD_CSV = str(LOCAL_STAGING_PATH / "DimPeriod_Python.csv")

# =====================================================================
# ENVIRONMENT
# =====================================================================

MOONWALK_ENV = os.environ.get("MOONWALK_ENV", "production")  # production | development


def _get_duckdb_key():
    """Load DuckDB encryption key from env var or Streamlit secrets.

    Returns empty string if no key is configured (backward compat — no encryption).
    """
    key = os.environ.get("MOONWALK_DUCKDB_KEY")
    if key:
        return key
    try:
        import streamlit as st

        return st.secrets.get("DUCKDB_KEY", "")
    except Exception:
        return ""


DUCKDB_KEY = _get_duckdb_key()

# =====================================================================
# LOGGING CONFIGURATION
# =====================================================================

LOGS_PATH = (_SCRIPT_DIR / "logs") if IS_CLOUD else (_DATA_DIR / "logs")
LOG_LEVEL = os.environ.get(
    "MOONWALK_LOG_LEVEL",
    "DEBUG" if MOONWALK_ENV == "development" else "INFO",
)

# =====================================================================
# BUSINESS CONSTANTS
# =====================================================================

EXCEL_SERIAL_DATE_BASE = "1899-12-30"
MOONWALK_STORE_ID = "36319"
HIELO_STORE_ID = "38516"
SUBSCRIPTION_VALIDITY_DAYS = 30

# =====================================================================
# NOTION INTEGRATION (optional — push LLM narrative after refresh)
# =====================================================================


def _get_notion_api_key():
    key = os.environ.get("NOTION_API_KEY")
    if key:
        return key
    try:
        import streamlit as st

        return st.secrets.get("NOTION_API_KEY", "")
    except Exception:
        return ""


NOTION_API_KEY = _get_notion_api_key()
NOTION_PAGE_ID = os.environ.get("NOTION_PAGE_ID", "30ca2f71-fdb0-81fa-a12b-c5e844be2bf3")


def _get_notion_token():
    """Read-only bypass token for Notion portal visitors (embedded in dashboard URLs)."""
    key = os.environ.get("NOTION_TOKEN")
    if key:
        return key
    try:
        import streamlit as st

        return st.secrets.get("NOTION_TOKEN", "")
    except Exception:
        return ""


NOTION_TOKEN = _get_notion_token()


def _get_notion_kpi_db_id():
    key = os.environ.get("NOTION_KPI_DB_ID")
    if key:
        return key
    try:
        import streamlit as st

        return st.secrets.get("NOTION_KPI_DB_ID", "")
    except Exception:
        return ""


NOTION_KPI_DB_ID = _get_notion_kpi_db_id()

# =====================================================================
# FASTAPI (Operational API — runs independently of Streamlit)
# =====================================================================

# Pure env var — no st.secrets fallback (FastAPI runs without Streamlit)
FASTAPI_API_KEY = os.environ.get("MOONWALK_API_KEY", "")
FASTAPI_PORT = int(os.environ.get("MOONWALK_API_PORT", "8000"))
API_VERSION = "0.1.0"

# Operational SQLite (separate from analytics.duckdb)
OPERATIONAL_DB_PATH = Path(
    os.environ.get("MOONWALK_OPERATIONAL_DB", str(_SCRIPT_DIR / "operational.db"))
)
