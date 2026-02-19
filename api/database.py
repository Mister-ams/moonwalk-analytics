"""SQLite connection for operational CRUD data."""
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from config import OPERATIONAL_DB_PATH

_DB_PATH = OPERATIONAL_DB_PATH

_SCHEMA = """
CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employee_name TEXT NOT NULL,
    role TEXT,
    department TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    join_date TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    notes TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def init_db(db_path: Path | None = None) -> None:
    path = db_path or _DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    try:
        con.executescript(_SCHEMA)
        con.commit()
    finally:
        con.close()


@contextmanager
def get_db(db_path: Path | None = None) -> Generator[sqlite3.Connection, None, None]:
    path = db_path or _DB_PATH
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")  # safe for multiple uvicorn workers
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
