from __future__ import annotations

from datetime import date
from typing import Iterable

import numpy as np
import pandas as pd

GRACE_MINUTES = 5


def _strip_strings(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for col in cols:
        df[col] = df[col].apply(
            lambda x: x.strip() if isinstance(x, str) else x
        )
    return df


def _bool_from_string(val):
    if pd.isna(val):
        return None

    if isinstance(val, (bool, np.bool_)):
        return bool(val)

    normalized = str(val).strip().lower()

    if normalized in {"1", "true", "t", "yes", "y", "active"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "inactive"}:
        return False

    return None


def _safe_float(val):
    try:
        if pd.isna(val) or val == "":
            return None
        return float(val)
    except Exception:
        return None


def clean_employee(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    string_cols = df.select_dtypes(include=["object"]).columns
    df = _strip_strings(df, string_cols)

    df.replace({"": np.nan, " ": np.nan}, inplace=True)

    date_cols = [
        "dob",
        "hire_date",
        "recent_hire_date",
        "job_start_date",
        "term_date",
    ]

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    df["years_of_experience"] = df["years_of_experience"].apply(_safe_float)
    df["scheduled_weekly_hour"] = df["scheduled_weekly_hour"].apply(
        _safe_float)

    df["is_per_diem"] = df.get("is_per_deim").apply(_bool_from_string)
    df["active_status_bool"] = df.get("active_status").apply(_bool_from_string)

    date_cols_used = [c for c in ("hire_date", "recent_hire_date", "job_start_date", "term_date") if c in df.columns]
    as_of = date.today()
    if date_cols_used:
        max_per_col = df[date_cols_used].max()
        valid = max_per_col.dropna()
        if len(valid) > 0:
            latest = valid.max()
            as_of = latest.date() if hasattr(latest, "date") else latest

    def _hire_start(row):
        for col in ("hire_date", "recent_hire_date", "job_start_date"):
            if pd.notna(row.get(col)):
                return row[col]
        return None

    def _tenure_days(row):
        start = _hire_start(row)
        if start is None:
            return None

        end = row["term_date"] if pd.notna(row["term_date"]) else as_of
        return (end - start).days

    df["tenure_days"] = df.apply(_tenure_days, axis=1)
    df["tenure_months"] = df["tenure_days"].apply(
        lambda x: round(x / 30, 2) if x else None
    )

    # Normalize manager IDs to string, keeping missing as None
    df["manager_employee_id"] = df["manager_employee_id"].apply(
        lambda m: str(m).strip() if pd.notna(m) and str(m).strip() != "" else None
    )

    # Drop manager references that don't exist in the current dataset to avoid FK failures
    emp_ids = set(df["client_employee_id"].astype(str))
    df["manager_employee_id"] = df["manager_employee_id"].apply(
        lambda m: m if m in emp_ids else None
    )

    df["is_active"] = df.apply(
        lambda r: bool(r["active_status_bool"])
        if pd.notna(r["active_status_bool"])
        else pd.isna(r["term_date"]),
        axis=1,
    )

    df["is_early_attrition"] = df.apply(
        lambda r: (
            not r["is_active"]
            and r["tenure_days"] is not None
            and r["tenure_days"] <= 90
        ),
        axis=1,
    )

    df.sort_values(
        by=["client_employee_id", "hire_date"],
        inplace=True,
    )

    df = df.drop_duplicates(
        subset=["client_employee_id"],
        keep="last",
    )

    return df


def clean_timesheet(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    string_cols = df.select_dtypes(include=["object"]).columns
    df = _strip_strings(df, string_cols)

    df.replace({"": np.nan}, inplace=True)

    datetime_cols = [
        "punch_in_datetime",
        "punch_out_datetime",
        "scheduled_start_datetime",
        "scheduled_end_datetime",
    ]

    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    df["punch_apply_date"] = pd.to_datetime(
        df["punch_apply_date"],
        errors="coerce",
    ).dt.date

    df["worked_minutes"] = (
        (df["punch_out_datetime"] - df["punch_in_datetime"])
        .dt.total_seconds() / 60
    )

    df["scheduled_minutes"] = (
        (df["scheduled_end_datetime"] - df["scheduled_start_datetime"])
        .dt.total_seconds() / 60
    )

    df["variance_minutes"] = (
        df["worked_minutes"] - df["scheduled_minutes"]
    )

    df["lateness_minutes"] = (
        (df["punch_in_datetime"] - df["scheduled_start_datetime"])
        .dt.total_seconds() / 60
    )

    df.loc[df["lateness_minutes"] <= GRACE_MINUTES, "lateness_minutes"] = 0

    df["early_departure_minutes"] = (
        (df["scheduled_end_datetime"] - df["punch_out_datetime"])
        .dt.total_seconds() / 60
    )

    df.loc[
        df["early_departure_minutes"] <= GRACE_MINUTES,
        "early_departure_minutes",
    ] = 0

    df["overtime_minutes"] = df["variance_minutes"]
    df.loc[df["overtime_minutes"] <= GRACE_MINUTES, "overtime_minutes"] = 0

    df["is_late"] = df["lateness_minutes"] > 0
    df["is_early_departure"] = df["early_departure_minutes"] > 0
    df["is_overtime"] = df["overtime_minutes"] > 0

    df["work_date"] = df["punch_apply_date"]
    df["work_date"] = df["work_date"].fillna(
        df["punch_in_datetime"].dt.date
    )

    df["day_of_week"] = df["work_date"].apply(
        lambda d: d.weekday() if pd.notna(d) else None
    )

    df.sort_values(
        by=["client_employee_id", "punch_in_datetime"],
        inplace=True,
    )

    return df
