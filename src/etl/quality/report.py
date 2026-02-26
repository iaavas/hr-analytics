import os
from datetime import datetime
from typing import Any, Dict

from sqlalchemy import text

from src.app.database import engine


def _run(conn, sql: str, params=None) -> Any:
    return conn.execute(text(sql), params or {}).fetchone()[0]


def collect_qc_stats() -> Dict[str, Any]:
    stats = {}
    with engine.connect() as conn:
        stats["employee_raw"] = _run(conn, "SELECT COUNT(*) FROM employee_raw")
        stats["timesheet_raw"] = _run(conn, "SELECT COUNT(*) FROM timesheet_raw")
        stats["silver_employee"] = _run(conn, "SELECT COUNT(*) FROM silver.employee")
        stats["silver_timesheet"] = _run(conn, "SELECT COUNT(*) FROM silver.timesheet")
        stats["gold_headcount_trend"] = _run(
            conn, "SELECT COUNT(*) FROM gold.headcount_trend"
        )
        stats["gold_timesheet_daily_summary"] = _run(
            conn, "SELECT COUNT(*) FROM gold.timesheet_daily_summary"
        )
        stats["gold_employee_attendance_metrics"] = _run(
            conn, "SELECT COUNT(*) FROM gold.employee_attendance_metrics"
        )
        stats["gold_department_monthly_metrics"] = _run(
            conn, "SELECT COUNT(*) FROM gold.department_monthly_metrics"
        )
        stats["gold_organization_metrics"] = _run(
            conn, "SELECT COUNT(*) FROM gold.organization_metrics"
        )
    return stats


def write_report(out_dir: str = "logs/reports") -> str:
    os.makedirs(out_dir, exist_ok=True)
    stats = collect_qc_stats()
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    path = os.path.join(out_dir, f"qc_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.txt")
    with open(path, "w") as f:
        f.write(f"QC Report {ts}\n")
        f.write("-" * 40 + "\n")
        f.write("Bronze: employee_raw=%s timesheet_raw=%s\n" % (
            stats["employee_raw"], stats["timesheet_raw"]))
        f.write("Silver: employee=%s timesheet=%s\n" % (
            stats["silver_employee"], stats["silver_timesheet"]))
        f.write("Gold: headcount_trend=%s timesheet_daily_summary=%s\n" % (
            stats["gold_headcount_trend"], stats["gold_timesheet_daily_summary"]))
        f.write("       employee_attendance_metrics=%s department_monthly_metrics=%s organization_metrics=%s\n" % (
            stats["gold_employee_attendance_metrics"],
            stats["gold_department_monthly_metrics"],
            stats["gold_organization_metrics"],
        ))
    return path
