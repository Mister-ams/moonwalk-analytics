"""Centralized path and constant configuration with env-var overrides.

All hardcoded paths and magic numbers live here.  Override any path
via the corresponding MOONWALK_* environment variable for portability.
"""

import os
from pathlib import Path

# =====================================================================
# DIRECTORY ROOTS (derived from this file's location)
# =====================================================================

_SCRIPT_DIR = Path(__file__).resolve().parent        # PythonScript/
_DATA_DIR   = _SCRIPT_DIR.parent                     # Moonwalk Data/

# =====================================================================
# PATHS (override via env vars for portability)
# =====================================================================

DOWNLOADS_PATH = Path(
    os.environ.get("MOONWALK_DOWNLOADS", r"C:\Users\MRAL-\Downloads")
)

LOCAL_STAGING_PATH = Path(
    os.environ.get("MOONWALK_STAGING", r"C:\Users\MRAL-\Downloads\Lime Reporting")
)

ONEDRIVE_SALES_DATA_PATH = Path(
    os.environ.get("MOONWALK_ONEDRIVE_DATA", str(_DATA_DIR / "Sales Data"))
)

PYTHON_SCRIPT_FOLDER = Path(
    os.environ.get("MOONWALK_SCRIPTS", str(_SCRIPT_DIR))
)

# Derived file paths
SALES_CSV     = str(LOCAL_STAGING_PATH / "All_Sales_Python.csv")
ITEMS_CSV     = str(LOCAL_STAGING_PATH / "All_Items_Python.csv")
DIMPERIOD_CSV = str(LOCAL_STAGING_PATH / "DimPeriod_Python.csv")
DB_PATH       = _DATA_DIR / "analytics.duckdb"

# =====================================================================
# LOGGING CONFIGURATION
# =====================================================================

LOGS_PATH = _DATA_DIR / "logs"
LOG_LEVEL = os.environ.get("MOONWALK_LOG_LEVEL", "INFO")

# =====================================================================
# BUSINESS CONSTANTS
# =====================================================================

EXCEL_SERIAL_DATE_BASE     = "1899-12-30"
MOONWALK_STORE_ID          = "36319"
HIELO_STORE_ID             = "38516"
SUBSCRIPTION_VALIDITY_DAYS = 30
