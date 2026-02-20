"""FastAPI endpoint tests — no network, no running server required.

Uses FastAPI's TestClient (HTTPX-backed, in-process) with a temporary
SQLite database for full test isolation.
"""
import os
import tempfile
from pathlib import Path

import pytest

# Set the API key before importing any api module so config.py picks it up.
os.environ.setdefault("MOONWALK_API_KEY", "test-key-12345")

# Redirect the DB to a temp file before the app module is imported.
import api.database as _db_module

_tmp_db = Path(tempfile.mktemp(suffix=".db"))
_db_module._DB_PATH = _tmp_db
_db_module.init_db(_tmp_db)  # create schema (lifespan only fires inside context manager)

from fastapi.testclient import TestClient  # noqa: E402 (must follow env setup)

from api.main import app  # noqa: E402

client = TestClient(app)
AUTH = {"X-API-Key": "test-key-12345"}
WRONG_AUTH = {"X-API-Key": "wrong-key"}


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_health_returns_200():
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["db"] == "connected"
    assert "version" in body


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_auth_rejected_without_key():
    r = client.get("/employees")
    assert r.status_code == 403


@pytest.mark.api
def test_auth_rejected_with_wrong_key():
    r = client.get("/employees", headers=WRONG_AUTH)
    assert r.status_code == 403


@pytest.mark.api
def test_auth_accepted():
    r = client.get("/employees", headers=AUTH)
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# CRUD — create
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_create_employee_returns_201():
    r = client.post(
        "/employees",
        json={"employee_name": "Ali Hassan", "role": "Driver", "department": "Logistics"},
        headers=AUTH,
    )
    assert r.status_code == 201
    body = r.json()
    assert body["id"] > 0
    assert body["employee_name"] == "Ali Hassan"
    assert body["status"] == "active"


# ---------------------------------------------------------------------------
# CRUD — list
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_list_employees():
    # Ensure at least one employee exists
    client.post("/employees", json={"employee_name": "Sara Khalil"}, headers=AUTH)
    r = client.get("/employees", headers=AUTH)
    assert r.status_code == 200
    names = [e["employee_name"] for e in r.json()]
    assert "Sara Khalil" in names


# ---------------------------------------------------------------------------
# CRUD — get by id
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_get_employee_by_id():
    created = client.post(
        "/employees",
        json={"employee_name": "Fatima Nasser", "role": "Cashier"},
        headers=AUTH,
    ).json()
    emp_id = created["id"]

    r = client.get(f"/employees/{emp_id}", headers=AUTH)
    assert r.status_code == 200
    assert r.json()["employee_name"] == "Fatima Nasser"


@pytest.mark.api
def test_get_nonexistent_returns_404():
    r = client.get("/employees/99999", headers=AUTH)
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# CRUD — patch
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_patch_updates_field():
    emp_id = client.post(
        "/employees",
        json={"employee_name": "Omar Saeed", "role": "Presser", "department": "Production"},
        headers=AUTH,
    ).json()["id"]

    r = client.patch(f"/employees/{emp_id}", json={"role": "Senior Presser"}, headers=AUTH)
    assert r.status_code == 200
    assert r.json()["role"] == "Senior Presser"


@pytest.mark.api
def test_patch_preserves_other_fields():
    emp_id = client.post(
        "/employees",
        json={"employee_name": "Layla Mansour", "department": "Customer Service"},
        headers=AUTH,
    ).json()["id"]

    client.patch(f"/employees/{emp_id}", json={"role": "Supervisor"}, headers=AUTH)

    r = client.get(f"/employees/{emp_id}", headers=AUTH)
    assert r.json()["department"] == "Customer Service"
    assert r.json()["role"] == "Supervisor"


# ---------------------------------------------------------------------------
# CRUD — delete
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_soft_delete():
    emp_id = client.post(
        "/employees",
        json={"employee_name": "Yusuf Al-Ahmad"},
        headers=AUTH,
    ).json()["id"]

    r = client.delete(f"/employees/{emp_id}", headers=AUTH)
    assert r.status_code == 204

    # Employee still exists — just inactive
    r2 = client.get(f"/employees/{emp_id}", headers=AUTH)
    assert r2.status_code == 200
    assert r2.json()["status"] == "inactive"


@pytest.mark.api
def test_hard_delete():
    emp_id = client.post(
        "/employees",
        json={"employee_name": "Nour Ibrahim"},
        headers=AUTH,
    ).json()["id"]

    r = client.delete(f"/employees/{emp_id}?hard=true", headers=AUTH)
    assert r.status_code == 204

    # Employee is gone
    r2 = client.get(f"/employees/{emp_id}", headers=AUTH)
    assert r2.status_code == 404


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_create_missing_name_422():
    r = client.post(
        "/employees",
        json={"role": "Driver"},  # missing employee_name
        headers=AUTH,
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Seed script
# ---------------------------------------------------------------------------


@pytest.mark.api
def test_seed_count_is_sufficient():
    """Seed inserts at least 6 demo employees into a fresh database."""
    tmp = Path(tempfile.mktemp(suffix=".db"))
    from seed_employees import seed

    n = seed(tmp)
    assert n >= 6
    tmp.unlink(missing_ok=True)


@pytest.mark.api
def test_seed_all_employees_active():
    """All seeded employees should have status='active'."""
    tmp = Path(tempfile.mktemp(suffix=".db"))
    import api.database as _db

    from seed_employees import seed

    seed(tmp)
    with _db.get_db(tmp) as con:
        rows = con.execute("SELECT status FROM employees").fetchall()
    assert all(r[0] == "active" for r in rows)
    tmp.unlink(missing_ok=True)


@pytest.mark.api
def test_seed_is_idempotent():
    """Running seed twice inserts rows only on the first call."""
    tmp = Path(tempfile.mktemp(suffix=".db"))
    from seed_employees import seed

    first = seed(tmp)
    second = seed(tmp)
    assert first > 0
    assert second == 0  # skipped — table already populated
    tmp.unlink(missing_ok=True)
