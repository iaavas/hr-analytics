"""Analytics API. Pre-aggregated metrics from gold layer: headcount, departments, attendance, organization, daily timesheet summary."""

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from src.app.database import get_db
from src.app.core.security import get_current_user
from src.app.schemas.analytics_schema import (
    HeadcountTrendRead,
    DepartmentMetricsRead,
    EmployeeAttendanceRead,
    OrganizationMetricsRead,
    TimesheetDailySummaryRead,
)
from src.app.schemas.response_schema import ApiResponse
from src.app.services import analytics_service

router = APIRouter(prefix="/analytics", tags=["analytics"])


def _serialize_list(schema, items):
    """Map ORM objects to list of dicts via the given Pydantic schema."""
    return [schema.model_validate(x).model_dump() for x in items]


@router.get("/headcount", response_model=ApiResponse)
def get_headcount_trend(
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(12, le=24),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Headcount trend over time. Optional year/month filter; limit caps number of rows (max 24)."""
    data = analytics_service.get_headcount_trend(db, year, month, limit)
    return ApiResponse.ok(
        data=_serialize_list(HeadcountTrendRead, data),
        message="Headcount trend data retrieved successfully.",
    )


@router.get("/departments", response_model=ApiResponse)
def get_department_metrics(
    department_id: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(100, le=200),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Department-level metrics (headcount, turnover, tenure, attendance rates). Optional filters; limit max 200."""
    data = analytics_service.get_department_metrics(
        db, department_id, year, month, limit
    )
    return ApiResponse.ok(
        data=_serialize_list(DepartmentMetricsRead, data),
        message="Department metrics retrieved successfully.",
    )


@router.get("/employees/{employee_id}/attendance", response_model=ApiResponse)
def get_employee_attendance(
    employee_id: str,
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(12, le=24),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Attendance metrics for a single employee. Returns 404 if employee not found. Limit max 24."""
    data = analytics_service.get_employee_attendance(
        db, employee_id, year, month, limit
    )
    return ApiResponse.ok(
        data=_serialize_list(EmployeeAttendanceRead, data),
        message="Attendance metrics for employee retrieved successfully.",
    )


@router.get("/employees/attendance", response_model=ApiResponse)
def get_all_employee_attendance(
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(100, le=200),
    skip: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Attendance metrics for all employees. Optional year/month; pagination via skip and limit (max 200)."""
    data = analytics_service.get_all_employee_attendance(
        db, year, month, limit, skip
    )
    return ApiResponse.ok(
        data=_serialize_list(EmployeeAttendanceRead, data),
        message="Attendance metrics for all employees retrieved successfully.",
    )


@router.get("/organization", response_model=ApiResponse)
def get_organization_metrics(
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(12, le=24),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Organization-wide KPIs over time. Optional year/month; limit max 24."""
    data = analytics_service.get_organization_metrics(db, year, month, limit)
    return ApiResponse.ok(
        data=_serialize_list(OrganizationMetricsRead, data),
        message="Organization-wide metrics retrieved successfully.",
    )


@router.get("/timesheets/daily", response_model=ApiResponse)
def get_timesheet_daily_summary(
    employee_id: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    limit: int = Query(100, le=500),
    skip: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Daily timesheet summary. Optional employee_id and date range (YYYY-MM-DD). Pagination via skip and limit (max 500)."""
    data = analytics_service.get_timesheet_daily_summary(
        db, employee_id, start_date, end_date, limit, skip
    )
    return ApiResponse.ok(
        data=_serialize_list(TimesheetDailySummaryRead, data),
        message="Daily timesheet summary retrieved successfully.",
    )
