from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from src.app.database import get_db
from src.app.core.security import get_current_user
from src.app.schemas.timesheet_schema import TimesheetRead
from src.app.services import timesheet_service

router = APIRouter(prefix="/timesheets", tags=["timesheets"])


@router.get("", response_model=List[TimesheetRead])
def get_timesheets(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return timesheet_service.get_all_timesheets(db, skip=skip, limit=limit)


@router.get("/employee/{employee_id}", response_model=List[TimesheetRead])
def get_timesheets_by_employee(
    employee_id: str,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return timesheet_service.get_timesheets_by_employee(db, employee_id, skip=skip, limit=limit)


@router.get("/filter", response_model=List[TimesheetRead])
def filter_timesheets(
    start_date: date = Query(...),
    end_date: date = Query(...),
    employee_id: Optional[str] = Query(None),
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return timesheet_service.get_timesheets_by_date_range(
        db, start_date, end_date, employee_id, skip=skip, limit=limit
    )
