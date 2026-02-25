"""Pydantic models for analytics API responses (gold-layer metrics: headcount, departments, attendance, organization, daily summary)."""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class HeadcountTrendRead(BaseModel):
    id: int
    year: int
    month: int
    active_headcount: int
    new_hires: int
    terminations: int
    early_attrition_count: int
    early_attrition_rate: float
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class DepartmentMetricsRead(BaseModel):
    id: int
    department_id: Optional[str] = None
    department_name: Optional[str] = None
    year: int
    month: int
    active_headcount: int
    total_hires: int
    total_terminations: int
    turnover_rate: float
    avg_tenure_days: Optional[float] = None
    avg_weekly_hours: float
    late_arrival_rate: float
    early_departure_rate: float
    overtime_rate: float
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class EmployeeAttendanceRead(BaseModel):
    id: int
    client_employee_id: str
    year: int
    month: int
    total_shifts: int
    days_worked: int
    total_hours_worked: float
    avg_hours_per_day: float
    avg_hours_per_week: float
    late_arrival_count: int
    late_arrival_rate: float
    early_departure_count: int
    early_departure_rate: float
    overtime_count: int
    overtime_rate: float
    avg_variance_minutes: Optional[float] = None
    rolling_avg_hours_4w: Optional[float] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class OrganizationMetricsRead(BaseModel):
    id: int
    organization_id: Optional[str] = None
    organization_name: Optional[str] = None
    year: int
    month: int
    total_employees: int
    active_employees: int
    total_departments: int
    avg_tenure_days: Optional[float] = None
    turnover_rate: float
    avg_late_arrival_rate: float
    avg_early_departure_rate: float
    avg_overtime_rate: float
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class TimesheetDailySummaryRead(BaseModel):
    id: int
    client_employee_id: str
    work_date: date
    department_id: Optional[str] = None
    total_shifts: int
    total_worked_minutes: Optional[float] = None
    total_scheduled_minutes: Optional[float] = None
    total_hours_worked: Optional[float] = None
    late_arrival_count: int
    early_departure_count: int
    overtime_count: int
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True
