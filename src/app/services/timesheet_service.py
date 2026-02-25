from datetime import date
from typing import List, Optional

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.app.core.exceptions import ServiceError
from src.app.services import employee_service
from src.db.models.silver import Timesheet


def get_all_timesheets(
    db: Session, skip: int = 0, limit: int = 100
) -> List[Timesheet]:
    try:
        return db.query(Timesheet).offset(skip).limit(limit).all()
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to list timesheets: {e!s}") from e


def get_timesheets_by_employee(
    db: Session, employee_id: str, skip: int = 0, limit: int = 100
) -> List[Timesheet]:
    employee_service.get_employee_or_raise(db, employee_id)
    try:
        return (
            db.query(Timesheet)
            .filter(Timesheet.client_employee_id == employee_id)
            .offset(skip)
            .limit(limit)
            .all()
        )
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to fetch timesheets for employee: {e!s}") from e


def get_timesheets_by_date_range(
    db: Session,
    start_date: date,
    end_date: date,
    employee_id: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[Timesheet]:
    if employee_id:
        employee_service.get_employee_or_raise(db, employee_id)
    try:
        query = db.query(Timesheet).filter(
            Timesheet.work_date >= start_date,
            Timesheet.work_date <= end_date,
        )
        if employee_id:
            query = query.filter(Timesheet.client_employee_id == employee_id)
        return query.offset(skip).limit(limit).all()
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to fetch timesheets by date range: {e!s}") from e
