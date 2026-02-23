from typing import List, Optional

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
from src.app.services import analytics_service

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/headcount", response_model=List[HeadcountTrendRead])
def get_headcount_trend(
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(12, le=24),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return analytics_service.get_headcount_trend(db, year, month, limit)


@router.get("/departments", response_model=List[DepartmentMetricsRead])
def get_department_metrics(
    department_id: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(100, le=200),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return analytics_service.get_department_metrics(db, department_id, year, month, limit)


@router.get("/employees/{employee_id}/attendance", response_model=List[EmployeeAttendanceRead])
def get_employee_attendance(
    employee_id: str,
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(12, le=24),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return analytics_service.get_employee_attendance(db, employee_id, year, month, limit)


@router.get("/employees/attendance", response_model=List[EmployeeAttendanceRead])
def get_all_employee_attendance(
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(100, le=200),
    skip: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return analytics_service.get_all_employee_attendance(db, year, month, limit, skip)


@router.get("/organization", response_model=List[OrganizationMetricsRead])
def get_organization_metrics(
    year: Optional[int] = Query(None),
    month: Optional[int] = Query(None),
    limit: int = Query(12, le=24),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return analytics_service.get_organization_metrics(db, year, month, limit)


@router.get("/timesheets/daily", response_model=List[TimesheetDailySummaryRead])
def get_timesheet_daily_summary(
    employee_id: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="Format: YYYY-MM-DD"),
    limit: int = Query(100, le=500),
    skip: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return analytics_service.get_timesheet_daily_summary(
        db, employee_id, start_date, end_date, limit, skip
    )
