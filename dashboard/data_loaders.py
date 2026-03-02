from __future__ import annotations

import pandas as pd


def first_of_month(year: int, month: int) -> pd.Timestamp:
    return pd.to_datetime(f"{year}-{month:02d}-01")


def load_headcount(engine) -> pd.DataFrame:
    q = """
    SELECT year, month, active_headcount, new_hires, terminations,
           early_attrition_count, early_attrition_rate
    FROM gold.headcount_trend
    ORDER BY year, month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [first_of_month(y, m) for y, m in zip(df.year, df.month)]
    rate = df["terminations"] / df["active_headcount"].replace(0, pd.NA) * 100
    df["turnover_rate"] = pd.to_numeric(rate, errors="coerce").round(2)
    df["hire_rate"] = pd.to_numeric(
        df["new_hires"] / df["active_headcount"].replace(0, pd.NA) * 100,
        errors="coerce",
    ).round(2)
    df["net_change"] = df["new_hires"] - df["terminations"]
    return df


def load_department_metrics(engine) -> pd.DataFrame:
    q = """
    SELECT department_id, department_name, year, month,
           active_headcount, total_hires, total_terminations, turnover_rate,
           avg_tenure_days, avg_weekly_hours,
           late_arrival_rate, early_departure_rate, overtime_rate
    FROM gold.department_monthly_metrics
    ORDER BY year, month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [first_of_month(y, m) for y, m in zip(df.year, df.month)]
    df["department_label"] = (
        df["department_name"].fillna(df["department_id"]).fillna("Unknown")
    )
    df["avg_tenure_months"] = pd.to_numeric(
        df["avg_tenure_days"] / 30.44, errors="coerce"
    ).round(1)
    return df


def load_employee_snapshots(engine) -> pd.DataFrame:
    q = """
    SELECT
        s.client_employee_id,
        s.department_id,
        s.department_name,
        s.job_title,
        s.hire_date,
        s.term_date,
        s.tenure_days,
        e.organization_id,
        COALESCE(o.organization_name, e.organization_id) AS organization_name,
        s.year,
        s.month,
        s.is_active
    FROM gold.employee_monthly_snapshot s
    LEFT JOIN silver.employee e
        ON s.client_employee_id = e.client_employee_id
    LEFT JOIN silver.organization o
        ON e.organization_id = o.organization_id
    ORDER BY s.year, s.month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [first_of_month(y, m) for y, m in zip(df.year, df.month)]
    df["hire_date"] = pd.to_datetime(df["hire_date"], errors="coerce")
    df["term_date"] = pd.to_datetime(df["term_date"], errors="coerce")
    return df


def load_attendance(engine) -> pd.DataFrame:
    q = """
    SELECT client_employee_id, year, month,
           total_shifts, days_worked,
           total_hours_worked, avg_hours_per_day, avg_hours_per_week,
           late_arrival_count, late_arrival_rate,
           early_departure_count, early_departure_rate,
           overtime_count, overtime_rate,
           avg_variance_minutes, rolling_avg_hours_4w
    FROM gold.employee_attendance_metrics
    ORDER BY year, month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [first_of_month(y, m) for y, m in zip(df.year, df.month)]
    return df


def load_timesheet_daily(engine) -> pd.DataFrame:
    q = """
    SELECT
        t.client_employee_id,
        NULLIF(
            TRIM(
                COALESCE(NULLIF(e.preferred_name, ''), NULLIF(e.first_name, ''))
                || ' ' ||
                COALESCE(NULLIF(e.last_name, ''), '')
            ),
            ''
        ) AS employee_name,
        t.department_id,
        d.department_name,
        t.work_date,
        t.total_hours_worked,
        t.total_worked_minutes,
        t.total_scheduled_minutes,
        t.late_minutes_total,
        t.early_minutes_total,
        t.overtime_minutes_total,
        t.late_arrival_count,
        t.early_departure_count,
        t.overtime_count
    FROM gold.timesheet_daily_summary t
    LEFT JOIN silver.employee e
        ON t.client_employee_id = e.client_employee_id
    LEFT JOIN silver.department d
        ON t.department_id = d.department_id
    ORDER BY t.work_date;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["work_date"] = pd.to_datetime(df["work_date"])
    df["department_label"] = (
        df["department_name"].fillna(df["department_id"]).fillna("Unknown")
    )
    return df


def load_org_metrics(engine) -> pd.DataFrame:
    q = """
    SELECT organization_id, organization_name, year, month,
           total_employees, active_employees, total_departments,
           turnover_rate, avg_tenure_days,
           avg_late_arrival_rate, avg_early_departure_rate, avg_overtime_rate
    FROM gold.organization_metrics
    ORDER BY year, month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [first_of_month(y, m) for y, m in zip(df.year, df.month)]
    df["avg_tenure_months"] = pd.to_numeric(
        df["avg_tenure_days"] / 30.44, errors="coerce"
    ).round(1)
    return df

