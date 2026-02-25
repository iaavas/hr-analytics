from typing import List, Optional

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.app.core.exceptions import ServiceError
from src.app.services import employee_service
from src.db.models.gold import (
    HeadcountTrend,
    DepartmentMonthlyMetrics,
    EmployeeAttendanceMetrics,
    OrganizationMetrics,
    TimesheetDailySummary,
)


def get_headcount_trend(
    db: Session,
    year: Optional[int] = None,
    month: Optional[int] = None,
    limit: int = 12,
) -> List[HeadcountTrend]:
    try:
        query = db.query(HeadcountTrend)
        if year:
            query = query.filter(HeadcountTrend.year == year)
        if month:
            query = query.filter(HeadcountTrend.month == month)
        return (
            query.order_by(HeadcountTrend.year.desc(), HeadcountTrend.month.desc())
            .limit(limit)
            .all()
        )
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to fetch headcount trend: {e!s}") from e


def get_department_metrics(
    db: Session,
    department_id: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
    limit: int = 100,
) -> List[DepartmentMonthlyMetrics]:
    try:
        query = db.query(DepartmentMonthlyMetrics)
        if department_id:
            query = query.filter(DepartmentMonthlyMetrics.department_id == department_id)
        if year:
            query = query.filter(DepartmentMonthlyMetrics.year == year)
        if month:
            query = query.filter(DepartmentMonthlyMetrics.month == month)
        return (
            query.order_by(
                DepartmentMonthlyMetrics.year.desc(), DepartmentMonthlyMetrics.month.desc()
            )
            .limit(limit)
            .all()
        )
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to fetch department metrics: {e!s}") from e


def get_employee_attendance(
    db: Session,
    employee_id: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
    limit: int = 12,
) -> List[EmployeeAttendanceMetrics]:
    employee_service.get_employee_or_raise(db, employee_id)
    try:
        query = db.query(EmployeeAttendanceMetrics).filter(
            EmployeeAttendanceMetrics.client_employee_id == employee_id
        )
        if year:
            query = query.filter(EmployeeAttendanceMetrics.year == year)
        if month:
            query = query.filter(EmployeeAttendanceMetrics.month == month)
        return (
            query.order_by(
                EmployeeAttendanceMetrics.year.desc(),
                EmployeeAttendanceMetrics.month.desc(),
            )
            .limit(limit)
            .all()
        )
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to fetch employee attendance: {e!s}") from e


def get_all_employee_attendance(
    db: Session,
    year: Optional[int] = None,
    month: Optional[int] = None,
    limit: int = 100,
    skip: int = 0,
) -> List[EmployeeAttendanceMetrics]:
    try:
        query = db.query(EmployeeAttendanceMetrics)
        if year:
            query = query.filter(EmployeeAttendanceMetrics.year == year)
        if month:
            query = query.filter(EmployeeAttendanceMetrics.month == month)
        return (
            query.order_by(
                EmployeeAttendanceMetrics.client_employee_id,
                EmployeeAttendanceMetrics.year.desc(),
                EmployeeAttendanceMetrics.month.desc(),
            )
            .offset(skip)
            .limit(limit)
            .all()
        )
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to fetch employee attendance: {e!s}") from e


def get_organization_metrics(
    db: Session,
    year: Optional[int] = None,
    month: Optional[int] = None,
    limit: int = 12,
) -> List[OrganizationMetrics]:
    try:
        query = db.query(OrganizationMetrics)
        if year:
            query = query.filter(OrganizationMetrics.year == year)
        if month:
            query = query.filter(OrganizationMetrics.month == month)
        return (
            query.order_by(
                OrganizationMetrics.year.desc(), OrganizationMetrics.month.desc()
            )
            .limit(limit)
            .all()
        )
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to fetch organization metrics: {e!s}") from e


def get_timesheet_daily_summary(
    db: Session,
    employee_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 100,
    skip: int = 0,
) -> List[TimesheetDailySummary]:
    if employee_id:
        employee_service.get_employee_or_raise(db, employee_id)
    try:
        query = db.query(TimesheetDailySummary)
        if employee_id:
            query = query.filter(
                TimesheetDailySummary.client_employee_id == employee_id
            )
        if start_date:
            query = query.filter(TimesheetDailySummary.work_date >= start_date)
        if end_date:
            query = query.filter(TimesheetDailySummary.work_date <= end_date)
        return (
            query.order_by(TimesheetDailySummary.work_date.desc())
            .offset(skip)
            .limit(limit)
            .all()
        )
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to fetch timesheet daily summary: {e!s}") from e
