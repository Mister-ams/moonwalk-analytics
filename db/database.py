"""SQLAlchemy engine and session factory for the Moonwalk analytics schema.

Usage:
    from db.database import get_engine, check_db

    engine = get_engine()          # returns None if ANALYTICS_DATABASE_URL not set
    status = check_db()            # "connected" | "not_configured" | "error: ..."
"""

from sqlalchemy import create_engine, text

from config import ANALYTICS_DATABASE_URL


def get_engine():
    """Return a SQLAlchemy engine pointed at the analytics schema.

    Returns None when ANALYTICS_DATABASE_URL is not configured so that the
    dashboard can fall back to DuckDB during the parallel-run period (Phase M2-M5).
    """
    if not ANALYTICS_DATABASE_URL:
        return None
    return create_engine(
        ANALYTICS_DATABASE_URL,
        pool_pre_ping=True,
        # Set search_path so all unqualified table references resolve to analytics.
        connect_args={"options": "-csearch_path=analytics,public"},
    )


def check_db() -> str:
    """Return connection status string for health checks and logging."""
    engine = get_engine()
    if engine is None:
        return "not_configured"
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return "connected"
    except Exception as exc:
        return f"error: {exc}"
