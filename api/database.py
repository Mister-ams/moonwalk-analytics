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
    -- Core
    employee_name TEXT NOT NULL,
    role TEXT,
    department TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    join_date TEXT,
    contract_type TEXT,
    contract_end_date TEXT,
    notes TEXT,
    -- Personal
    nationality TEXT,
    date_of_birth TEXT,
    address TEXT,
    emergency_contact_name TEXT,
    emergency_contact_phone TEXT,
    -- Contact
    contact_email TEXT,
    contact_phone TEXT,
    -- Financial
    salary REAL,
    salary_currency TEXT DEFAULT 'AED',
    pay_frequency TEXT,
    bank_name TEXT,
    bank_account TEXT,
    iban TEXT,
    -- Documents
    passport_number TEXT,
    passport_expiry TEXT,
    visa_type TEXT,
    visa_expiry TEXT,
    emirates_id TEXT,
    emirates_id_expiry TEXT,
    -- Timestamps
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

# Columns added after v1 schema — applied via ALTER TABLE on existing DBs.
_MIGRATIONS = [
    ("nationality", "TEXT"),
    ("date_of_birth", "TEXT"),
    ("address", "TEXT"),
    ("emergency_contact_name", "TEXT"),
    ("emergency_contact_phone", "TEXT"),
    ("salary", "REAL"),
    ("salary_currency", "TEXT DEFAULT 'AED'"),
    ("pay_frequency", "TEXT"),
    ("bank_name", "TEXT"),
    ("bank_account", "TEXT"),
    ("iban", "TEXT"),
    ("passport_number", "TEXT"),
    ("passport_expiry", "TEXT"),
    ("visa_type", "TEXT"),
    ("visa_expiry", "TEXT"),
    ("emirates_id", "TEXT"),
    ("emirates_id_expiry", "TEXT"),
    ("contract_type", "TEXT"),
    ("contract_end_date", "TEXT"),
]


def _migrate(con: sqlite3.Connection) -> None:
    """Add any missing columns to an existing employees table."""
    existing = {row[1] for row in con.execute("PRAGMA table_info(employees)").fetchall()}
    for col_name, col_def in _MIGRATIONS:
        if col_name not in existing:
            con.execute(f"ALTER TABLE employees ADD COLUMN {col_name} {col_def}")


def init_db(db_path: Path | None = None) -> None:
    path = db_path or _DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    try:
        con.executescript(_SCHEMA)
        _migrate(con)
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
