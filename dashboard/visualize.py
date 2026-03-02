from __future__ import annotations

import argparse
import os
from pathlib import Path

from sqlalchemy import create_engine

from data_loaders import (
    load_attendance,
    load_department_metrics,
    load_employee_snapshots,
    load_headcount,
    load_org_metrics,
    load_timesheet_daily,
)
from dashboards import (
    attendance_discipline_dashboard,
    executive_summary_dashboard,
    work_hours_dashboard,
    workforce_dashboard,
)


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"


def safe_save(fig, name: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    outfile = OUTPUT_DIR / name
    fig.write_html(outfile, include_plotlyjs="cdn")
    print(f"Wrote {outfile}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate HR Insights Dashboards")
    parser.add_argument(
        "--db",
        dest="db_url",
        default=os.environ.get(
            "DATABASE_URL",
            "postgresql://hr_insights:hr_insights@localhost:5432/hr_insights",
        ),
        help="Database URL; defaults to DATABASE_URL env or local dev value.",
    )
    args = parser.parse_args()

    engine = create_engine(args.db_url)

    headcount = load_headcount(engine)
    dept_metrics = load_department_metrics(engine)
    snapshots = load_employee_snapshots(engine)
    attendance = load_attendance(engine)
    daily = load_timesheet_daily(engine)
    org_metrics = load_org_metrics(engine)

    exec_fig = executive_summary_dashboard(
        headcount, dept_metrics, org_metrics, attendance, daily
    )
    safe_save(exec_fig, "executive_summary.html")

    wf_fig = workforce_dashboard(
        headcount, dept_metrics, snapshots, org_metrics)
    safe_save(wf_fig, "workforce_trend.html")

    hours_fig = work_hours_dashboard(attendance, daily, snapshots)
    safe_save(hours_fig, "work_hours_overtime.html")

    discipline_fig = attendance_discipline_dashboard(daily)
    safe_save(discipline_fig, "attendance_discipline.html")


if __name__ == "__main__":
    main()
