from __future__ import annotations

from datetime import date, timedelta
from typing import Iterable, Optional

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session

from src.db.models.silver import Employee, Timesheet


GRACE_MINUTES = 5
ROLLING_WINDOW_WEEKS = 4


def get_employees_from_db(db: Session) -> pd.DataFrame:
    employees = db.query(Employee).all()
    data = []
    for emp in employees:
        data.append(
            {
                "client_employee_id": emp.client_employee_id,
                "first_name": emp.first_name,
                "last_name": emp.last_name,
                "department_id": emp.department_id,
                "job_title": emp.job_title,
                "hire_date": emp.hire_date,
                "term_date": emp.term_date,
                "is_active": emp.is_active,
                "tenure_days": (
                    (emp.term_date or date.today() - emp.hire_date).days if emp.hire_date else None
                ),
                "is_early_attrition": (
                    not emp.is_active
                    and emp.term_date
                    and emp.hire_date
                    and (emp.term_date - emp.hire_date).days <= 90
                )
                if emp.is_active is not None
                else None,
            }
        )
    return pd.DataFrame(data)


def get_timesheets_from_db(db: Session) -> pd.DataFrame:
    timesheets = db.query(Timesheet).all()
    data = []
    for ts in timesheets:
        data.append(
            {
                "id": ts.id,
                "client_employee_id": ts.client_employee_id,
                "department_id": ts.department_id,
                "work_date": ts.work_date,
                "punch_in_datetime": ts.punch_in_datetime,
                "punch_out_datetime": ts.punch_out_datetime,
                "scheduled_start_datetime": ts.scheduled_start_datetime,
                "scheduled_end_datetime": ts.scheduled_end_datetime,
                "worked_minutes": ts.worked_minutes,
                "scheduled_minutes": ts.scheduled_minutes,
                "hours_worked": ts.hours_worked,
                "is_late": (
                    ts.punch_in_datetime
                    and ts.scheduled_start_datetime
                    and (ts.punch_in_datetime - ts.scheduled_start_datetime).total_seconds() / 60
                    > GRACE_MINUTES
                ),
                "is_early_departure": (
                    ts.punch_out_datetime
                    and ts.scheduled_end_datetime
                    and (ts.scheduled_end_datetime - ts.punch_out_datetime).total_seconds() / 60
                    > GRACE_MINUTES
                ),
                "is_overtime": (
                    ts.worked_minutes
                    and ts.scheduled_minutes
                    and ts.worked_minutes - ts.scheduled_minutes > GRACE_MINUTES
                ),
            }
        )
    return pd.DataFrame(data)


def calculate_headcount_trend(
    employees: pd.DataFrame,
    year: int,
    month: int,
) -> dict:
    ref_date = date(year, month, 1)

    month_employees = employees[
        (employees["hire_date"].notna())
        & ((employees["term_date"].isna()) | (employees["term_date"] >= ref_date))
    ]
    active_count = len(month_employees)

    month_end = (ref_date + pd.offsets.MonthEnd(0)).date()
    new_hires = len(
        employees[
            (employees["hire_date"].notna())
            & (employees["hire_date"] >= ref_date)
            & (employees["hire_date"] <= month_end)
        ]
    )
    terminations = len(
        employees[
            (employees["term_date"].notna())
            & (employees["term_date"] >= ref_date)
            & (employees["term_date"] <= month_end)
        ]
    )
    early_attrition_count = len(
        employees[
            (employees["is_early_attrition"] == True)
            & (
                employees["term_date"].notna()
                & (employees["term_date"] >= ref_date)
                & (employees["term_date"] <= month_end)
            )
        ]
    )

    early_attrition_rate = (early_attrition_count / new_hires * 100) if new_hires > 0 else 0.0

    return {
        "year": year,
        "month": month,
        "active_headcount": active_count,
        "new_hires": new_hires,
        "terminations": terminations,
        "early_attrition_count": early_attrition_count,
        "early_attrition_rate": round(early_attrition_rate, 2),
    }


def calculate_department_metrics(
    employees: pd.DataFrame,
    timesheets: pd.DataFrame,
    year: int,
    month: int,
) -> list[dict]:
    ref_date = date(year, month, 1)
    month_end = (ref_date + pd.offsets.MonthEnd(0)).date()

    dept_metrics = []

    for dept_id in employees["department_id"].dropna().unique():
        dept_employees = employees[employees["department_id"] == dept_id]

        active_emp = dept_employees[
            (dept_employees["hire_date"].notna())
            & ((dept_employees["term_date"].isna()) | (dept_employees["term_date"] >= ref_date))
        ]
        active_headcount = len(active_emp)

        new_hires = len(
            dept_employees[
                (dept_employees["hire_date"].notna())
                & (dept_employees["hire_date"] >= ref_date)
                & (dept_employees["hire_date"] <= month_end)
            ]
        )

        terminations = len(
            dept_employees[
                (dept_employees["term_date"].notna())
                & (dept_employees["term_date"] >= ref_date)
                & (dept_employees["term_date"] <= month_end)
            ]
        )

        turnover_rate = (terminations / active_headcount * 100) if active_headcount > 0 else 0.0

        avg_tenure = active_emp["tenure_days"].mean() if len(active_emp) > 0 else None

        dept_name = dept_employees.iloc[0].get("department_name")

        emp_ids = dept_employees["client_employee_id"].tolist()
        dept_timesheets = timesheets[
            (timesheets["client_employee_id"].isin(emp_ids))
            & (timesheets["work_date"] >= ref_date)
            & (timesheets["work_date"] <= month_end)
        ]

        total_shifts = len(dept_timesheets)
        if total_shifts > 0:
            late_count = dept_timesheets["is_late"].sum()
            early_count = dept_timesheets["is_early_departure"].sum()
            overtime_count = dept_timesheets["is_overtime"].sum()

            late_rate = late_count / total_shifts * 100
            early_rate = early_count / total_shifts * 100
            overtime_rate = overtime_count / total_shifts * 100

            avg_weekly_hours = (
                dept_timesheets["hours_worked"].sum()
                / len(dept_timesheets["client_employee_id"].unique())
                / 4
                if len(dept_timesheets["client_employee_id"].unique()) > 0
                else 0
            )
        else:
            late_rate = 0.0
            early_rate = 0.0
            overtime_rate = 0.0
            avg_weekly_hours = 0.0

        dept_metrics.append(
            {
                "department_id": dept_id,
                "department_name": dept_name,
                "year": year,
                "month": month,
                "active_headcount": active_headcount,
                "total_hires": new_hires,
                "total_terminations": terminations,
                "turnover_rate": round(turnover_rate, 2),
                "avg_tenure_days": round(avg_tenure, 2) if avg_tenure else None,
                "avg_weekly_hours": round(avg_weekly_hours, 2),
                "late_arrival_rate": round(late_rate, 2),
                "early_departure_rate": round(early_rate, 2),
                "overtime_rate": round(overtime_rate, 2),
            }
        )

    return dept_metrics


def calculate_employee_daily_summary(
    timesheets: pd.DataFrame,
) -> pd.DataFrame:
    if timesheets.empty:
        return pd.DataFrame()

    daily = (
        timesheets.groupby(["client_employee_id", "work_date", "department_id"])
        .agg(
            {
                "id": "count",
                "worked_minutes": "sum",
                "scheduled_minutes": "sum",
                "hours_worked": "sum",
                "is_late": "sum",
                "is_early_departure": "sum",
                "is_overtime": "sum",
            }
        )
        .reset_index()
    )

    daily.columns = [
        "client_employee_id",
        "work_date",
        "department_id",
        "total_shifts",
        "total_worked_minutes",
        "total_scheduled_minutes",
        "total_hours_worked",
        "late_arrival_count",
        "early_departure_count",
        "overtime_count",
    ]

    return daily


def calculate_employee_monthly_metrics(
    timesheets: pd.DataFrame,
    year: int,
    month: int,
) -> list[dict]:
    ref_date = date(year, month, 1)
    month_end = (ref_date + pd.offsets.MonthEnd(0)).date()

    monthly_data = timesheets[
        (timesheets["work_date"] >= ref_date) & (timesheets["work_date"] <= month_end)
    ]

    if monthly_data.empty:
        return []

    metrics = []
    for emp_id in monthly_data["client_employee_id"].unique():
        emp_data = monthly_data[monthly_data["client_employee_id"] == emp_id]

        total_shifts = len(emp_data)
        days_worked = emp_data["work_date"].nunique()
        total_hours = emp_data["hours_worked"].sum()

        avg_hours_per_day = total_hours / days_worked if days_worked > 0 else 0
        avg_hours_per_week = total_hours / 4

        late_count = emp_data["is_late"].sum()
        late_rate = (late_count / total_shifts * 100) if total_shifts > 0 else 0

        early_count = emp_data["is_early_departure"].sum()
        early_rate = (early_count / total_shifts * 100) if total_shifts > 0 else 0

        overtime_count = emp_data["is_overtime"].sum()
        overtime_rate = (overtime_count / total_shifts * 100) if total_shifts > 0 else 0

        avg_variance = emp_data.get("variance_minutes", pd.Series()).mean()

        metrics.append(
            {
                "client_employee_id": emp_id,
                "year": year,
                "month": month,
                "total_shifts": total_shifts,
                "days_worked": days_worked,
                "total_hours_worked": round(total_hours, 2),
                "avg_hours_per_day": round(avg_hours_per_day, 2),
                "avg_hours_per_week": round(avg_hours_per_week, 2),
                "late_arrival_count": int(late_count),
                "late_arrival_rate": round(late_rate, 2),
                "early_departure_count": int(early_count),
                "early_departure_rate": round(early_rate, 2),
                "overtime_count": int(overtime_count),
                "overtime_rate": round(overtime_rate, 2),
                "avg_variance_minutes": round(avg_variance, 2) if pd.notna(avg_variance) else None,
            }
        )

    return metrics


def calculate_rolling_avg_hours(
    timesheets: pd.DataFrame,
    employee_id: str,
    current_date: date,
    weeks: int = 4,
) -> Optional[float]:
    start_date = current_date - timedelta(weeks=weeks * 7)

    emp_data = timesheets[
        (timesheets["client_employee_id"] == employee_id)
        & (timesheets["work_date"] >= start_date)
        & (timesheets["work_date"] <= current_date)
    ]

    if emp_data.empty:
        return None

    daily_hours = emp_data.groupby("work_date")["hours_worked"].sum()
    return round(daily_hours.mean(), 2)
