"""Pydantic schemas for Employee CRUD."""
from typing import Literal

from pydantic import BaseModel, Field


class EmployeeBase(BaseModel):
    # Core
    employee_name: str = Field(..., min_length=1, max_length=200)
    role: str | None = None
    department: str | None = None
    join_date: str | None = None          # ISO 8601: "2025-03-01"
    contract_type: str | None = None      # permanent | fixed-term | probation
    contract_end_date: str | None = None  # ISO 8601; null for permanent
    notes: str | None = None
    # Personal
    nationality: str | None = None
    date_of_birth: str | None = None      # ISO 8601
    address: str | None = None
    emergency_contact_name: str | None = None
    emergency_contact_phone: str | None = None
    # Contact
    contact_email: str | None = None
    contact_phone: str | None = None
    # Financial
    salary: float | None = None
    salary_currency: str | None = "AED"
    pay_frequency: str | None = None      # monthly | bi-weekly | weekly
    bank_name: str | None = None
    bank_account: str | None = None
    iban: str | None = None
    # Documents
    passport_number: str | None = None
    passport_expiry: str | None = None    # ISO 8601
    visa_type: str | None = None          # employment | family | investor | citizen
    visa_expiry: str | None = None        # ISO 8601
    emirates_id: str | None = None        # 784-YYYY-XXXXXXX-X
    emirates_id_expiry: str | None = None # ISO 8601


class EmployeeCreate(EmployeeBase):
    status: Literal["active", "inactive"] = "active"


class EmployeeUpdate(BaseModel):
    """All fields optional — true PATCH semantics."""
    employee_name: str | None = Field(None, min_length=1, max_length=200)
    role: str | None = None
    department: str | None = None
    status: Literal["active", "inactive"] | None = None
    join_date: str | None = None
    contract_type: str | None = None
    contract_end_date: str | None = None
    notes: str | None = None
    nationality: str | None = None
    date_of_birth: str | None = None
    address: str | None = None
    emergency_contact_name: str | None = None
    emergency_contact_phone: str | None = None
    contact_email: str | None = None
    contact_phone: str | None = None
    salary: float | None = None
    salary_currency: str | None = None
    pay_frequency: str | None = None
    bank_name: str | None = None
    bank_account: str | None = None
    iban: str | None = None
    passport_number: str | None = None
    passport_expiry: str | None = None
    visa_type: str | None = None
    visa_expiry: str | None = None
    emirates_id: str | None = None
    emirates_id_expiry: str | None = None


class Employee(EmployeeBase):
    id: int
    status: Literal["active", "inactive"]
    created_at: str
    updated_at: str

    model_config = {"from_attributes": True}
