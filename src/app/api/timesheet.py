from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from src.app.database import get_db
from src.app.core.security import get_current_user
from src.app.schemas.timesheet_schema import TimesheetRead
from src.app.schemas.response_schema import ApiResponse
from src.app.services import timesheet_service

router = APIRouter(prefix="/timesheets", tags=["timesheets"])


@router.get("", response_model=ApiResponse)
def get_timesheets(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    timesheets = timesheet_service.get_all_timesheets(db, skip=skip, limit=limit)
    data = [TimesheetRead.model_validate(t).model_dump() for t in timesheets]
    return ApiResponse.ok(
        data=data, message="Timesheet list retrieved successfully."
    )


@router.get("/employee/{employee_id}", response_model=ApiResponse)
def get_timesheets_by_employee(
    employee_id: str,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    timesheets = timesheet_service.get_timesheets_by_employee(
        db, employee_id, skip=skip, limit=limit
    )
    data = [TimesheetRead.model_validate(t).model_dump() for t in timesheets]
    return ApiResponse.ok(
        data=data,
        message="Timesheets for employee retrieved successfully.",
    )


@router.get("/filter", response_model=ApiResponse)
def filter_timesheets(
    start_date: date = Query(...),
    end_date: date = Query(...),
    employee_id: Optional[str] = Query(None),
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    timesheets = timesheet_service.get_timesheets_by_date_range(
        db, start_date, end_date, employee_id, skip=skip, limit=limit
    )
    data = [TimesheetRead.model_validate(t).model_dump() for t in timesheets]
    return ApiResponse.ok(
        data=data,
        message="Timesheets for the given date range retrieved successfully.",
    )
