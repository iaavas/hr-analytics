from sqlalchemy import Column, String, Integer, Float, DateTime
from sqlalchemy.sql import func
from src.app.database import Base


class EmployeeBronze(Base):
    __tablename__ = "employee_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_employee_id = Column(String(50))
    first_name = Column(String(100))
    middle_name = Column(String(100))
    last_name = Column(String(100))
    preferred_name = Column(String(100))
    job_code = Column(String(50))
    job_title = Column(String(255))
    job_start_date = Column(String(20))
    organization_id = Column(String(50))
    organization_name = Column(String(255))
    department_id = Column(String(50))
    department_name = Column(String(255))
    dob = Column(String(20))
    hire_date = Column(String(20))
    recent_hire_date = Column(String(20))
    anniversary_date = Column(String(20))
    term_date = Column(String(20))
    years_of_experience = Column(String(50))
    work_email = Column(String(255))
    address = Column(String(500))
    city = Column(String(100))
    state = Column(String(100))
    zip = Column(String(20))
    country = Column(String(100))
    manager_employee_id = Column(String(50))
    manager_employee_name = Column(String(255))
    fte_status = Column(String(50))
    is_per_deim = Column(String(50))
    cell_phone = Column(String(50))
    work_phone = Column(String(50))
    scheduled_weekly_hour = Column(String(20))
    active_status = Column(String(10))
    termination_reason = Column(String(255))
    clinical_level = Column(String(50))
    created_at = Column(DateTime, default=func.now())
    source_file = Column(String(255))


class TimesheetBronze(Base):
    __tablename__ = "timesheet_raw"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_employee_id = Column(String(255))
    department_id = Column(String(255))
    department_name = Column(String(255))
    home_department_id = Column(String(255))
    home_department_name = Column(String(255))
    pay_code = Column(String(255))
    punch_in_comment = Column(String(500))
    punch_out_comment = Column(String(500))
    hours_worked = Column(Float)
    punch_apply_date = Column(String(20))
    punch_in_datetime = Column(String(255))
    punch_out_datetime = Column(String(255))
    scheduled_start_datetime = Column(String(255))
    scheduled_end_datetime = Column(String(255))
    created_at = Column(DateTime, default=func.now())
    source_file = Column(String(255))
