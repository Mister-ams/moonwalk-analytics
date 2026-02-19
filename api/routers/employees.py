"""Employee CRUD endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query, status

from api.auth import require_api_key
from api.database import get_db
from api.models import Employee, EmployeeCreate, EmployeeUpdate

router = APIRouter()


def _row_to_employee(row) -> Employee:
    return Employee(
        id=row["id"],
        employee_name=row["employee_name"],
        role=row["role"],
        department=row["department"],
        contact_email=row["contact_email"],
        contact_phone=row["contact_phone"],
        join_date=row["join_date"],
        status=row["status"],
        notes=row["notes"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _get_or_404(con, employee_id: int) -> Employee:
    row = con.execute("SELECT * FROM employees WHERE id = ?", (employee_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee not found")
    return _row_to_employee(row)


@router.get("", response_model=list[Employee])
def list_employees(
    status_filter: str | None = Query(None, alias="status"),
    _: str = Depends(require_api_key),
):
    with get_db() as con:
        if status_filter:
            rows = con.execute("SELECT * FROM employees WHERE status = ? ORDER BY id", (status_filter,)).fetchall()
        else:
            rows = con.execute("SELECT * FROM employees ORDER BY id").fetchall()
    return [_row_to_employee(r) for r in rows]


@router.post("", response_model=Employee, status_code=status.HTTP_201_CREATED)
def create_employee(body: EmployeeCreate, _: str = Depends(require_api_key)):
    with get_db() as con:
        cur = con.execute(
            """INSERT INTO employees
               (employee_name, role, department, contact_email, contact_phone, join_date, status, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                body.employee_name,
                body.role,
                body.department,
                body.contact_email,
                body.contact_phone,
                body.join_date,
                body.status,
                body.notes,
            ),
        )
        return _get_or_404(con, cur.lastrowid)


@router.get("/{employee_id}", response_model=Employee)
def get_employee(employee_id: int, _: str = Depends(require_api_key)):
    with get_db() as con:
        return _get_or_404(con, employee_id)


@router.patch("/{employee_id}", response_model=Employee)
def update_employee(employee_id: int, body: EmployeeUpdate, _: str = Depends(require_api_key)):
    updates = body.model_dump(exclude_none=True)
    if not updates:
        with get_db() as con:
            return _get_or_404(con, employee_id)

    set_clause = ", ".join(f"{k} = ?" for k in updates)
    set_clause += ", updated_at = datetime('now')"
    values = list(updates.values()) + [employee_id]

    with get_db() as con:
        _get_or_404(con, employee_id)  # 404 check before update
        con.execute(f"UPDATE employees SET {set_clause} WHERE id = ?", values)
        return _get_or_404(con, employee_id)


@router.delete("/{employee_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_employee(
    employee_id: int,
    hard: bool = Query(False),
    _: str = Depends(require_api_key),
):
    with get_db() as con:
        _get_or_404(con, employee_id)  # 404 check
        if hard:
            con.execute("DELETE FROM employees WHERE id = ?", (employee_id,))
        else:
            con.execute(
                "UPDATE employees SET status = 'inactive', updated_at = datetime('now') WHERE id = ?",
                (employee_id,),
            )
