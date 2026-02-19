"""Pydantic schemas for Employee CRUD."""
from typing import Literal

from pydantic import BaseModel, Field


class EmployeeBase(BaseModel):
    employee_name: str = Field(..., min_length=1, max_length=200)
    role: str | None = None
    department: str | None = None
    contact_email: str | None = None
    contact_phone: str | None = None
    join_date: str | None = None  # ISO 8601: "2025-03-01"
    notes: str | None = None


class EmployeeCreate(EmployeeBase):
    status: Literal["active", "inactive"] = "active"


class EmployeeUpdate(BaseModel):
    """All fields optional — true PATCH semantics."""

    employee_name: str | None = Field(None, min_length=1, max_length=200)
    role: str | None = None
    department: str | None = None
    contact_email: str | None = None
    contact_phone: str | None = None
    join_date: str | None = None
    status: Literal["active", "inactive"] | None = None
    notes: str | None = None


class Employee(EmployeeBase):
    id: int
    status: Literal["active", "inactive"]
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}
