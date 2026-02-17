"""Centralized logging configuration for Moonwalk Analytics ETL pipeline."""

import logging
import sys
from pathlib import Path
from datetime import datetime
from config import LOGS_PATH, LOG_LEVEL


def setup_logger(name: str) -> logging.Logger:
    """
    Configure logger with file + console handlers.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured logger instance
    """
    # Logger instance
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    # Prevent duplicate handlers
    if logger.handlers:
        return logger

    # ASCII-safe formatter (cp1252 compat for PowerShell)
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-7s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File handler (daily rotation) — skipped on read-only filesystems (cloud)
    try:
        LOGS_PATH.mkdir(parents=True, exist_ok=True)
        log_file = LOGS_PATH / f"moonwalk_etl_{datetime.now():%Y-%m-%d}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except (OSError, PermissionError):
        pass  # Console-only logging on cloud

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
