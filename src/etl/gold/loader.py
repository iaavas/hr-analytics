from datetime import date
from typing import Optional

from sqlalchemy.orm import Session
import pandas as pd

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


def load_headcount_trend(db: Session, employees: pd.DataFrame, year: int, month: int):
    trend_data = calculate_headcount_trend(employees, year, month)
    trend = HeadcountTrend(**trend_data)
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
        dept = DepartmentMonthlyMetrics(**metric)
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
            tenure_days=emp.get("tenure_days"),
        )
        db.add(snapshot)


def load_timesheet_daily_summary(db: Session, timesheets: pd.DataFrame):
    daily_data = calculate_employee_daily_summary(timesheets)

    for _, row in daily_data.iterrows():
        summary = TimesheetDailySummary(
            client_employee_id=row["client_employee_id"],
            work_date=row["work_date"],
            department_id=row.get("department_id"),
            total_shifts=int(row["total_shifts"]),
            total_worked_minutes=round(row["total_worked_minutes"], 2)
            if pd.notna(row["total_worked_minutes"])
            else None,
            total_scheduled_minutes=round(row["total_scheduled_minutes"], 2)
            if pd.notna(row["total_scheduled_minutes"])
            else None,
            total_hours_worked=round(row["total_hours_worked"], 2)
            if pd.notna(row["total_hours_worked"])
            else None,
            late_arrival_count=int(row["late_arrival_count"]),
            early_departure_count=int(row["early_departure_count"]),
            overtime_count=int(row["overtime_count"]),
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

        emp_metric = EmployeeAttendanceMetrics(**metric)
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
        avg_tenure_days=round(avg_tenure, 2) if avg_tenure else None,
        turnover_rate=round(turnover_rate, 2),
        avg_late_arrival_rate=round(avg_late_rate, 2),
        avg_early_departure_rate=round(avg_early_rate, 2),
        avg_overtime_rate=round(avg_overtime_rate, 2),
    )
    db.add(org_metrics)


def run_gold_etl(year: Optional[int] = None, month: Optional[int] = None):
    if year is None or month is None:
        today = date.today()
        year = year or today.year
        month = month or today.month

    db: Session = SessionLocal()

    try:
        print(f"Running Gold ETL for {year}-{month:02d}...")

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

        print("  Loading headcount trend...")
        load_headcount_trend(db, employees, year, month)
        db.commit()

        print("  Loading department metrics...")
        load_department_metrics(db, employees, timesheets, year, month)
        db.commit()

        print("  Loading employee monthly snapshots...")
        load_employee_monthly_snapshots(db, employees, year, month)
        db.commit()

        if not timesheets.empty:
            print("  Loading timesheet daily summary...")
            load_timesheet_daily_summary(db, timesheets)
            db.commit()

            print("  Loading employee attendance metrics...")
            load_employee_attendance_metrics(db, timesheets, year, month)
            db.commit()
        else:
            print("  No timesheet data to process")

        print("  Loading organization metrics...")
        load_organization_metrics(db, employees, timesheets, year, month)
        db.commit()

        print(f"Gold ETL completed for {year}-{month:02d}")

    except Exception as e:
        print(f"Error in Gold ETL: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    run_gold_etl()
