from datetime import date
from typing import List, Optional

from sqlalchemy.orm import Session

from src.db.models.silver import Timesheet


def get_all_timesheets(db: Session, skip: int = 0, limit: int = 100) -> List[Timesheet]:
    return db.query(Timesheet).offset(skip).limit(limit).all()


def get_timesheets_by_employee(
    db: Session, employee_id: str, skip: int = 0, limit: int = 100
) -> List[Timesheet]:
    return (
        db.query(Timesheet)
        .filter(Timesheet.client_employee_id == employee_id)
        .offset(skip)
        .limit(limit)
        .all()
    )


def get_timesheets_by_date_range(
    db: Session,
    start_date: date,
    end_date: date,
    employee_id: Optional[str] = None,
    skip: int = 0,
    limit: int = 100,
) -> List[Timesheet]:
    query = db.query(Timesheet).filter(
        Timesheet.work_date >= start_date,
        Timesheet.work_date <= end_date,
    )
    if employee_id:
        query = query.filter(Timesheet.client_employee_id == employee_id)
    return query.offset(skip).limit(limit).all()
