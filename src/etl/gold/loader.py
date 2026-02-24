from datetime import date
from typing import Any, Optional

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session


def _to_native(val: Any) -> Any:
    """Convert numpy scalars to native Python types for DB/JSON compatibility."""
    if isinstance(val, (np.floating, np.integer)):
        return val.item()
    return val


def _native_dict(d: dict) -> dict:
    return {k: _to_native(v) for k, v in d.items()}

from src.app.database import SessionLocal
from src.db.models.gold import (
    EmployeeMonthlySnapshot,
    TimesheetDailySummary,
    DepartmentMonthlyMetrics,
    EmployeeAttendanceMetrics,
    HeadcountTrend,
    OrganizationMetrics,
)
from src.etl.gold.transform import (
    get_employees_from_db,
    get_timesheets_from_db,
    calculate_headcount_trend,
    calculate_department_metrics,
    calculate_employee_monthly_metrics,
    calculate_employee_daily_summary,
    calculate_rolling_avg_hours,
)


def clear_gold_tables(db: Session):
    db.query(EmployeeMonthlySnapshot).delete()
    db.query(TimesheetDailySummary).delete()
    db.query(DepartmentMonthlyMetrics).delete()
    db.query(EmployeeAttendanceMetrics).delete()
    db.query(HeadcountTrend).delete()
    db.query(OrganizationMetrics).delete()
    db.commit()


def _get_month_periods(employees: pd.DataFrame, timesheets: pd.DataFrame) -> pd.PeriodIndex:
    """Return continuous month periods covering all known employee/timesheet dates."""
    candidate_dates = []

    if not employees.empty:
        hire_dates = employees["hire_date"].dropna()
        if len(hire_dates) > 0:
            candidate_dates.append(hire_dates.min())
        term_dates = employees["term_date"].dropna()
        if len(term_dates) > 0:
            candidate_dates.append(term_dates.max())

    if not timesheets.empty:
        work_dates = timesheets["work_date"].dropna()
        if len(work_dates) > 0:
            candidate_dates.append(work_dates.min())
            candidate_dates.append(work_dates.max())

    if not candidate_dates:
        today = date.today().replace(day=1)
        return pd.period_range(start=today, end=today, freq="M")

    start = pd.to_datetime(min(candidate_dates)).to_period("M")
    end = pd.to_datetime(max(candidate_dates)).to_period("M")
    return pd.period_range(start=start, end=end, freq="M")


def _process_month(
    db: Session,
    employees: pd.DataFrame,
    timesheets: pd.DataFrame,
    year: int,
    month: int,
):
    print(f"  Processing metrics for {year}-{month:02d}...")

    load_headcount_trend(db, employees, year, month)
    db.commit()

    load_department_metrics(db, employees, timesheets, year, month)
    db.commit()

    load_employee_monthly_snapshots(db, employees, year, month)
    db.commit()

    if not timesheets.empty:
        load_employee_attendance_metrics(db, timesheets, year, month)
        db.commit()

    load_organization_metrics(db, employees, timesheets, year, month)
    db.commit()


def load_headcount_trend(db: Session, employees: pd.DataFrame, year: int, month: int):
    trend_data = calculate_headcount_trend(employees, year, month)
    trend = HeadcountTrend(**_native_dict(trend_data))
    db.add(trend)


def load_department_metrics(
    db: Session,
    employees: pd.DataFrame,
    timesheets: pd.DataFrame,
    year: int,
    month: int,
):
    dept_metrics = calculate_department_metrics(
        employees, timesheets, year, month)
    for metric in dept_metrics:
        dept = DepartmentMonthlyMetrics(**_native_dict(metric))
        db.add(dept)


def load_employee_monthly_snapshots(db: Session, employees: pd.DataFrame, year: int, month: int):
    ref_date = date(year, month, 1)

    for _, emp in employees.iterrows():
        is_active = pd.notna(emp.get("hire_date")) and (
            pd.isna(emp.get("term_date")) or emp.get("term_date") >= ref_date
        )

        snapshot = EmployeeMonthlySnapshot(
            client_employee_id=emp["client_employee_id"],
            year=year,
            month=month,
            is_active=is_active,
            hire_date=emp.get("hire_date"),
            term_date=emp.get("term_date"),
            department_id=emp.get("department_id"),
            tenure_days=_to_native(emp.get("tenure_days")),
        )
        db.add(snapshot)


def load_timesheet_daily_summary(db: Session, timesheets: pd.DataFrame):
    daily_data = calculate_employee_daily_summary(timesheets)

    for _, row in daily_data.iterrows():
        summary = TimesheetDailySummary(
            client_employee_id=row["client_employee_id"],
            work_date=row["work_date"],
            department_id=row.get("department_id"),
            total_shifts=_to_native(int(row["total_shifts"])),
            total_worked_minutes=_to_native(round(row["total_worked_minutes"], 2))
            if pd.notna(row["total_worked_minutes"])
            else None,
            total_scheduled_minutes=_to_native(round(row["total_scheduled_minutes"], 2))
            if pd.notna(row["total_scheduled_minutes"])
            else None,
            total_hours_worked=_to_native(round(row["total_hours_worked"], 2))
            if pd.notna(row["total_hours_worked"])
            else None,
            late_arrival_count=_to_native(int(row["late_arrival_count"])),
            early_departure_count=_to_native(int(row["early_departure_count"])),
            overtime_count=_to_native(int(row["overtime_count"])),
        )
        db.add(summary)


def load_employee_attendance_metrics(
    db: Session,
    timesheets: pd.DataFrame,
    year: int,
    month: int,
):
    metrics = calculate_employee_monthly_metrics(timesheets, year, month)

    for metric in metrics:
        rolling_avg = calculate_rolling_avg_hours(
            timesheets,
            metric["client_employee_id"],
            date(year, month, 1),
        )
        metric["rolling_avg_hours_4w"] = rolling_avg

        emp_metric = EmployeeAttendanceMetrics(**_native_dict(metric))
        db.add(emp_metric)


def load_organization_metrics(
    db: Session,
    employees: pd.DataFrame,
    timesheets: pd.DataFrame,
    year: int,
    month: int,
):
    ref_date = date(year, month, 1)
    month_end = (ref_date + pd.offsets.MonthEnd(0)).date()

    total_employees = len(employees)
    active_employees = len(
        employees[
            (employees["hire_date"].notna())
            & ((employees["term_date"].isna()) | (employees["term_date"] >= ref_date))
        ]
    )
    total_departments = (
        employees["department_id"].nunique(
        ) if "department_id" in employees.columns else 0
    )

    active_emp = employees[
        (employees["hire_date"].notna())
        & ((employees["term_date"].isna()) | (employees["term_date"] >= ref_date))
    ]
    avg_tenure = (
        active_emp["tenure_days"].mean()
        if len(active_emp) > 0 and "tenure_days" in active_emp.columns
        else 0
    )

    turnover_rate = 0.0
    avg_late_rate = 0
    avg_early_rate = 0
    avg_overtime_rate = 0

    if not timesheets.empty and "work_date" in timesheets.columns:
        month_ts = timesheets[
            (timesheets["work_date"] >= ref_date) & (
                timesheets["work_date"] <= month_end)
        ]

        if len(month_ts) > 0:
            total_shifts = len(month_ts)
            avg_late_rate = month_ts["is_late"].sum() / total_shifts * 100
            avg_early_rate = month_ts["is_early_departure"].sum(
            ) / total_shifts * 100
            avg_overtime_rate = month_ts["is_overtime"].sum(
            ) / total_shifts * 100

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
    if active_employees > 0:
        turnover_rate = terminations / active_employees * 100

    org_metrics = OrganizationMetrics(
        organization_id="ORG001",
        organization_name="Company",
        year=year,
        month=month,
        total_employees=total_employees,
        active_employees=active_employees,
        total_departments=total_departments,
        avg_tenure_days=_to_native(round(avg_tenure, 2)) if avg_tenure else None,
        turnover_rate=_to_native(round(turnover_rate, 2)),
        avg_late_arrival_rate=_to_native(round(avg_late_rate, 2)),
        avg_early_departure_rate=_to_native(round(avg_early_rate, 2)),
        avg_overtime_rate=_to_native(round(avg_overtime_rate, 2)),
    )
    db.add(org_metrics)


def run_gold_etl(
    year: Optional[int] = None,
    month: Optional[int] = None,
    all_months: bool = False,
):
    """Run gold ETL.

    If ``all_months`` is True *or* either ``year`` or ``month`` is omitted, the
    loader will process every month that appears in the silver data range.
    Otherwise it processes only the specified year/month.
    """

    process_all = all_months or year is None or month is None

    db: Session = SessionLocal()

    try:
        print("Running Gold ETL...")

        print("  Loading silver data...")
        employees = get_employees_from_db(db)
        timesheets = get_timesheets_from_db(db)

        if employees.empty:
            print("  No employee data in silver layer. Skipping gold ETL.")
            return

        print(
            f"  Found {len(employees)} employees, {len(timesheets)} timesheets")

        print("  Clearing existing gold data...")
        clear_gold_tables(db)

        if not timesheets.empty:
            print("  Loading timesheet daily summary across all dates...")
            load_timesheet_daily_summary(db, timesheets)
            db.commit()
        else:
            print("  No timesheet data to process for daily summary")

        if process_all:
            periods = _get_month_periods(employees, timesheets)
            print(
                f"  Processing full history: {len(periods)} month(s) from "
                f"{periods[0].year}-{periods[0].month:02d} to {periods[-1].year}-{periods[-1].month:02d}"
            )
            for p in periods:
                _process_month(db, employees, timesheets, p.year, p.month)
        else:
            _process_month(db, employees, timesheets, int(year), int(month))

        print("Gold ETL completed")

    except Exception as e:
        print(f"Error in Gold ETL: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    run_gold_etl(all_months=True)
