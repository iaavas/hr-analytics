from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


class TimesheetBase(BaseModel):
    client_employee_id: str
    department_id: Optional[str] = None
    punch_apply_date: Optional[date] = None
    punch_in_datetime: Optional[datetime] = None
    punch_out_datetime: Optional[datetime] = None
    scheduled_start_datetime: Optional[datetime] = None
    scheduled_end_datetime: Optional[datetime] = None
    worked_minutes: Optional[float] = None
    scheduled_minutes: Optional[float] = None
    hours_worked: Optional[float] = None
    work_date: Optional[date] = None
    pay_code: Optional[str] = None
    punch_in_comment: Optional[str] = None
    punch_out_comment: Optional[str] = None


class TimesheetRead(TimesheetBase):
    id: int
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
