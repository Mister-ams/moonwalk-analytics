---
project: moonwalk
type: guide
status: active
created: 2026-02-20
updated: 2026-02-20
---

# Appsmith Employee Directory — Setup Guide

**API base URL:** `https://moonwalk-api-production.up.railway.app`
**Auth header:** `X-API-Key: <MOONWALK_API_KEY from Railway variables>`
**Interactive docs:** `https://moonwalk-api-production.up.railway.app/docs`

---

## 1. Appsmith Workspace

1. Log in at https://app.appsmith.com
2. Create workspace: **Moonwalk**
3. Create app: **Employee Directory**

---

## 2. Add API Datasource

Datasources → + New Datasource → REST API:

| Field | Value |
|-------|-------|
| Name | `MoonwalkAPI` |
| URL | `https://moonwalk-api-production.up.railway.app` |
| Header | `X-API-Key` = `f03c734f-6123-4d01-83ae-686b127a94b8` |

Save & Test → should return `200 OK`.

---

## 3. Create API Queries

### `GetEmployees`
- GET `/employees`

### `GetEmployee`
- GET `/employees/{{EmployeeTable.selectedRow.id}}`

### `CreateEmployee`
- POST `/employees`
- Body (JSON):
```json
{
  "employee_name": "{{CreateForm.data.employee_name}}",
  "role": "{{CreateForm.data.role}}",
  "department": "{{CreateForm.data.department}}",
  "nationality": "{{CreateForm.data.nationality}}",
  "date_of_birth": "{{CreateForm.data.date_of_birth}}",
  "address": "{{CreateForm.data.address}}",
  "emergency_contact_name": "{{CreateForm.data.emergency_contact_name}}",
  "emergency_contact_phone": "{{CreateForm.data.emergency_contact_phone}}",
  "contact_email": "{{CreateForm.data.contact_email}}",
  "contact_phone": "{{CreateForm.data.contact_phone}}",
  "join_date": "{{CreateForm.data.join_date}}",
  "contract_type": "{{CreateForm.data.contract_type}}",
  "contract_end_date": "{{CreateForm.data.contract_end_date}}",
  "salary": {{CreateForm.data.salary}},
  "salary_currency": "{{CreateForm.data.salary_currency}}",
  "pay_frequency": "{{CreateForm.data.pay_frequency}}",
  "bank_name": "{{CreateForm.data.bank_name}}",
  "bank_account": "{{CreateForm.data.bank_account}}",
  "iban": "{{CreateForm.data.iban}}",
  "passport_number": "{{CreateForm.data.passport_number}}",
  "passport_expiry": "{{CreateForm.data.passport_expiry}}",
  "visa_type": "{{CreateForm.data.visa_type}}",
  "visa_expiry": "{{CreateForm.data.visa_expiry}}",
  "emirates_id": "{{CreateForm.data.emirates_id}}",
  "emirates_id_expiry": "{{CreateForm.data.emirates_id_expiry}}",
  "notes": "{{CreateForm.data.notes}}"
}
```

### `UpdateEmployee`
- PATCH `/employees/{{EmployeeTable.selectedRow.id}}`
- Body: same structure as CreateEmployee but bound to `EditForm.data.*`

### `DeactivateEmployee`
- PATCH `/employees/{{EmployeeTable.selectedRow.id}}`
- Body: `{"status": "inactive"}`

### `HardDeleteEmployee`
- DELETE `/employees/{{EmployeeTable.selectedRow.id}}?hard=true`

---

## 4. Page Layout

### Main view — Employee Table

Full-width Table widget (`EmployeeTable`):
- Data: `{{GetEmployees.data}}`
- Visible columns: `employee_name`, `role`, `department`, `nationality`, `salary`, `status`, `visa_expiry`, `emirates_id_expiry`
- Status column: Tag cell — active=green, inactive=gray
- Row click → open Edit drawer

Control bar above table:
- **+ Add Employee** button (primary) → opens Create drawer
- **Deactivate** button (danger, disabled if no row selected) → runs `DeactivateEmployee` + `GetEmployees.run()`
- Select widget for status filter (All / Active / Inactive)

---

## 5. Edit Drawer — Three Tabs

When a row is selected, open a side drawer with three tabs:

### Tab 1 — Personal

| Field | Widget | Bound to |
|-------|--------|----------|
| Full Name | Text Input | `selectedRow.employee_name` |
| Role | Text Input | `selectedRow.role` |
| Department | Select | `selectedRow.department` |
| Status | Select (active/inactive) | `selectedRow.status` |
| Join Date | Date Picker | `selectedRow.join_date` |
| Contract Type | Select (permanent/fixed-term/probation) | `selectedRow.contract_type` |
| Contract End Date | Date Picker | `selectedRow.contract_end_date` |
| Nationality | Text Input | `selectedRow.nationality` |
| Date of Birth | Date Picker | `selectedRow.date_of_birth` |
| Address | Multi-line Text | `selectedRow.address` |
| Emergency Contact | Text Input | `selectedRow.emergency_contact_name` |
| Emergency Phone | Phone Input | `selectedRow.emergency_contact_phone` |
| Work Email | Email Input | `selectedRow.contact_email` |
| Work Phone | Phone Input | `selectedRow.contact_phone` |
| Notes | Multi-line Text | `selectedRow.notes` |

### Tab 2 — Financial

| Field | Widget | Bound to |
|-------|--------|----------|
| Salary | Number Input | `selectedRow.salary` |
| Currency | Select (AED/USD/EUR) | `selectedRow.salary_currency` |
| Pay Frequency | Select (monthly/bi-weekly/weekly) | `selectedRow.pay_frequency` |
| Bank Name | Text Input | `selectedRow.bank_name` |
| Account Number | Text Input | `selectedRow.bank_account` |
| IBAN | Text Input | `selectedRow.iban` |

### Tab 3 — Documents

| Field | Widget | Bound to |
|-------|--------|----------|
| Passport Number | Text Input | `selectedRow.passport_number` |
| Passport Expiry | Date Picker | `selectedRow.passport_expiry` |
| Visa Type | Select (employment/family/investor/citizen) | `selectedRow.visa_type` |
| Visa Expiry | Date Picker | `selectedRow.visa_expiry` |
| Emirates ID | Text Input | `selectedRow.emirates_id` |
| Emirates ID Expiry | Date Picker | `selectedRow.emirates_id_expiry` |

Drawer footer buttons:
- **Save Changes** → runs `UpdateEmployee`, on success: `GetEmployees.run()` + close drawer
- **Delete** (danger) → confirm dialog → `HardDeleteEmployee` → `GetEmployees.run()` + close
- **Cancel** → close drawer

---

## 6. Create Drawer

Same three-tab structure, all fields empty. On **Save**:
1. Runs `CreateEmployee`
2. On success: `GetEmployees.run()` + close drawer

---

## 7. Expiry Alerts (Optional Enhancement)

In the table, add conditional row highlighting for expired or expiring documents:
```js
// Row background — flag rows with documents expiring within 60 days
{{
  (() => {
    const expiries = [
      currentRow.visa_expiry,
      currentRow.emirates_id_expiry,
      currentRow.passport_expiry
    ].filter(Boolean);
    const soon = expiries.some(d => {
      const days = (new Date(d) - new Date()) / 86400000;
      return days >= 0 && days <= 60;
    });
    const expired = expiries.some(d => new Date(d) < new Date());
    return expired ? "#FFEBEE" : soon ? "#FFF8E1" : "transparent";
  })()
}}
```

---

## API Reference

| Endpoint | Method | Auth | Notes |
|----------|--------|------|-------|
| `/health` | GET | None | Railway health probe |
| `/employees` | GET | Key | `?status=active` filter |
| `/employees` | POST | Key | Create |
| `/employees/{id}` | GET | Key | Single record |
| `/employees/{id}` | PATCH | Key | Partial update |
| `/employees/{id}` | DELETE | Key | Soft (default) or `?hard=true` |

Full schema at: `https://moonwalk-api-production.up.railway.app/docs`
