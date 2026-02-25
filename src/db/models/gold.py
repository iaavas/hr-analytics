import sqlalchemy as sa
from sqlalchemy.sql import func

from src.app.database import Base


class EmployeeMonthlySnapshot(Base):
    __tablename__ = "employee_monthly_snapshot"
    __table_args__ = (
        sa.Index("ix_snapshot_emp_month", "client_employee_id", "year", "month"),
        {"schema": "gold"},
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    client_employee_id = sa.Column(sa.String(50), nullable=False)
    year = sa.Column(sa.Integer, nullable=False)
    month = sa.Column(sa.Integer, nullable=False)
    is_active = sa.Column(sa.Boolean)
    hire_date = sa.Column(sa.Date)
    term_date = sa.Column(sa.Date)
    department_id = sa.Column(sa.String(50))
    department_name = sa.Column(sa.String(255))
    job_title = sa.Column(sa.String(255))
    tenure_days = sa.Column(sa.Float)
    created_at = sa.Column(sa.DateTime, server_default=func.now())


class TimesheetDailySummary(Base):
    __tablename__ = "timesheet_daily_summary"
    __table_args__ = (
        sa.Index("ix_daily_emp_date", "client_employee_id", "work_date"),
        {"schema": "gold"},
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    client_employee_id = sa.Column(sa.String(50), nullable=False)
    work_date = sa.Column(sa.Date, nullable=False)
    department_id = sa.Column(sa.String(50))
    total_shifts = sa.Column(sa.Integer)
    total_worked_minutes = sa.Column(sa.Float)
    total_scheduled_minutes = sa.Column(sa.Float)
    total_hours_worked = sa.Column(sa.Float)
    late_minutes_total = sa.Column(sa.Float)
    early_minutes_total = sa.Column(sa.Float)
    overtime_minutes_total = sa.Column(sa.Float)
    avg_variance_minutes = sa.Column(sa.Float)
    late_arrival_count = sa.Column(sa.Integer)
    early_departure_count = sa.Column(sa.Integer)
    overtime_count = sa.Column(sa.Integer)
    created_at = sa.Column(sa.DateTime, server_default=func.now())


class DepartmentMonthlyMetrics(Base):
    __tablename__ = "department_monthly_metrics"
    __table_args__ = (
        sa.Index("ix_dept_metrics", "department_id", "year", "month"),
        {"schema": "gold"},
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    department_id = sa.Column(sa.String(50))
    department_name = sa.Column(sa.String(255))
    year = sa.Column(sa.Integer, nullable=False)
    month = sa.Column(sa.Integer, nullable=False)
    active_headcount = sa.Column(sa.Integer)
    total_hires = sa.Column(sa.Integer)
    total_terminations = sa.Column(sa.Integer)
    turnover_rate = sa.Column(sa.Float)
    avg_tenure_days = sa.Column(sa.Float)
    avg_weekly_hours = sa.Column(sa.Float)
    late_arrival_rate = sa.Column(sa.Float)
    early_departure_rate = sa.Column(sa.Float)
    overtime_rate = sa.Column(sa.Float)
    created_at = sa.Column(sa.DateTime, server_default=func.now())


class EmployeeAttendanceMetrics(Base):
    __tablename__ = "employee_attendance_metrics"
    __table_args__ = (
        sa.Index("ix_emp_attendance", "client_employee_id", "year", "month"),
        {"schema": "gold"},
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    client_employee_id = sa.Column(sa.String(50), nullable=False)
    year = sa.Column(sa.Integer, nullable=False)
    month = sa.Column(sa.Integer, nullable=False)
    total_shifts = sa.Column(sa.Integer)
    days_worked = sa.Column(sa.Integer)
    total_hours_worked = sa.Column(sa.Float)
    avg_hours_per_day = sa.Column(sa.Float)
    avg_hours_per_week = sa.Column(sa.Float)
    late_arrival_count = sa.Column(sa.Integer)
    late_arrival_rate = sa.Column(sa.Float)
    early_departure_count = sa.Column(sa.Integer)
    early_departure_rate = sa.Column(sa.Float)
    overtime_count = sa.Column(sa.Integer)
    overtime_rate = sa.Column(sa.Float)
    avg_variance_minutes = sa.Column(sa.Float)
    rolling_avg_hours_4w = sa.Column(sa.Float)
    created_at = sa.Column(sa.DateTime, server_default=func.now())


class HeadcountTrend(Base):
    __tablename__ = "headcount_trend"
    __table_args__ = (
        sa.Index("ix_headcount_trend", "year", "month"),
        {"schema": "gold"},
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    year = sa.Column(sa.Integer, nullable=False)
    month = sa.Column(sa.Integer, nullable=False)
    active_headcount = sa.Column(sa.Integer)
    new_hires = sa.Column(sa.Integer)
    terminations = sa.Column(sa.Integer)
    early_attrition_count = sa.Column(sa.Integer)
    early_attrition_rate = sa.Column(sa.Float)
    created_at = sa.Column(sa.DateTime, server_default=func.now())


class OrganizationMetrics(Base):
    __tablename__ = "organization_metrics"
    __table_args__ = {"schema": "gold"}

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    organization_id = sa.Column(sa.String(50))
    organization_name = sa.Column(sa.String(255))
    year = sa.Column(sa.Integer, nullable=False)
    month = sa.Column(sa.Integer, nullable=False)
    total_employees = sa.Column(sa.Integer)
    active_employees = sa.Column(sa.Integer)
    total_departments = sa.Column(sa.Integer)
    avg_tenure_days = sa.Column(sa.Float)
    turnover_rate = sa.Column(sa.Float)
    avg_late_arrival_rate = sa.Column(sa.Float)
    avg_early_departure_rate = sa.Column(sa.Float)
    avg_overtime_rate = sa.Column(sa.Float)
    created_at = sa.Column(sa.DateTime, server_default=func.now())
