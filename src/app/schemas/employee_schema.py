"""Pydantic models for employee create, update, and read. Used by the employees API and services."""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class EmployeeBase(BaseModel):
    client_employee_id: str
    first_name: str
    middle_name: Optional[str] = None
    last_name: str
    preferred_name: Optional[str] = None
    job_code: Optional[str] = None
    job_title: Optional[str] = None
    organization_id: Optional[str] = None
    department_id: Optional[str] = None
    manager_employee_id: Optional[str] = None
    hire_date: Optional[date] = None
    term_date: Optional[date] = None
    dob: Optional[date] = None
    years_of_experience: Optional[float] = None
    scheduled_weekly_hour: Optional[float] = None
    is_active: Optional[bool] = True
    is_per_diem: Optional[bool] = False


class EmployeeCreate(EmployeeBase):
    pass


class EmployeeUpdate(BaseModel):
    first_name: Optional[str] = None
    middle_name: Optional[str] = None
    last_name: Optional[str] = None
    preferred_name: Optional[str] = None
    job_code: Optional[str] = None
    job_title: Optional[str] = None
    organization_id: Optional[str] = None
    department_id: Optional[str] = None
    manager_employee_id: Optional[str] = None
    hire_date: Optional[date] = None
    term_date: Optional[date] = None
    dob: Optional[date] = None
    years_of_experience: Optional[float] = None
    scheduled_weekly_hour: Optional[float] = None
    is_active: Optional[bool] = None
    is_per_diem: Optional[bool] = None


class EmployeeRead(EmployeeBase):
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
