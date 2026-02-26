from dataclasses import dataclass
from typing import List

from sqlalchemy import text

from src.app.database import engine


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def _run(conn, sql: str, params=None):
    return conn.execute(text(sql), params or {})


def validate_bronze() -> List[CheckResult]:
    results = []
    with engine.connect() as conn:
        row = _run(
            conn,
            "SELECT COUNT(*) AS c FROM bronze.employee_raw WHERE client_employee_id IS NULL OR TRIM(COALESCE(client_employee_id, '')) = ''",
        ).fetchone()
        null_emp = row[0] if row else 0
        results.append(
            CheckResult(
                "bronze_employee_client_id_not_null",
                null_emp == 0,
                f"{null_emp} rows with null/empty client_employee_id",
            )
        )

        row = _run(
            conn,
            """
            SELECT COUNT(*) FROM (
                SELECT client_employee_id FROM bronze.employee_raw
                WHERE client_employee_id IS NOT NULL AND TRIM(client_employee_id) != ''
                GROUP BY client_employee_id HAVING COUNT(*) > 1
            ) x
            """,
        ).fetchone()
        dup_emp = row[0] if row else 0
        results.append(
            CheckResult(
                "bronze_employee_unique_client_id",
                dup_emp == 0,
                f"{dup_emp} duplicate client_employee_id (must be unique)",
            )
        )

        row = _run(
            conn,
            """
            SELECT COUNT(*) FROM bronze.timesheet_raw
            WHERE hours_worked IS NOT NULL AND (hours_worked < 0 OR hours_worked > 24)
            """,
        ).fetchone()
        bad_hours = row[0] if row else 0
        results.append(
            CheckResult(
                "bronze_timesheet_hours_range",
                bad_hours == 0,
                f"{bad_hours} rows with hours_worked outside 0–24",
            )
        )

        row = _run(
            conn,
            """
            SELECT COUNT(*) FROM (
                SELECT client_employee_id, punch_in_datetime, punch_out_datetime
                FROM bronze.timesheet_raw
                GROUP BY client_employee_id, punch_in_datetime, punch_out_datetime
                HAVING COUNT(*) > 1
            ) x
            """,
        ).fetchone()
        dup_ts = row[0] if row else 0
        results.append(
            CheckResult(
                "bronze_timesheet_duplicate_shifts",
                dup_ts == 0,
                f"{dup_ts} duplicate (employee, punch_in, punch_out)",
            )
        )

        row = _run(
            conn,
            "SELECT COUNT(*) FROM bronze.timesheet_raw WHERE client_employee_id IS NULL OR TRIM(COALESCE(client_employee_id, '')) = ''",
        ).fetchone()
        null_ts_emp = row[0] if row else 0
        results.append(
            CheckResult(
                "bronze_timesheet_client_id_not_null",
                null_ts_emp == 0,
                f"{null_ts_emp} timesheet rows with null/empty client_employee_id",
            )
        )

    return results


def validate_silver() -> List[CheckResult]:
    results = []
    with engine.connect() as conn:
        row = _run(
            conn,
            "SELECT COUNT(*) FROM silver.employee WHERE client_employee_id IS NULL OR TRIM(COALESCE(client_employee_id, '')) = ''",
        ).fetchone()
        n = row[0] if row else 0
        results.append(
            CheckResult(
                "silver_employee_client_id_not_null",
                n == 0,
                f"{n} rows with null/empty client_employee_id",
            )
        )

        row = _run(
            conn,
            """
            SELECT COUNT(*) FROM (
                SELECT client_employee_id FROM silver.employee
                GROUP BY client_employee_id HAVING COUNT(*) > 1
            ) x
            """,
        ).fetchone()
        dup_emp = row[0] if row else 0
        results.append(
            CheckResult(
                "silver_employee_unique_client_id",
                dup_emp == 0,
                f"{dup_emp} duplicate client_employee_id in silver.employee",
            )
        )

        row = _run(
            conn,
            """
            SELECT COUNT(*) FROM (
                SELECT client_employee_id, punch_in_datetime, punch_out_datetime
                FROM silver.timesheet
                GROUP BY client_employee_id, punch_in_datetime, punch_out_datetime
                HAVING COUNT(*) > 1
            ) x
            """,
        ).fetchone()
        dup = row[0] if row else 0
        results.append(
            CheckResult(
                "silver_timesheet_unique_shift",
                dup == 0,
                f"{dup} duplicate (employee, punch_in, punch_out)",
            )
        )

        row = _run(
            conn,
            "SELECT COUNT(*) FROM silver.timesheet t LEFT JOIN silver.employee e ON t.client_employee_id = e.client_employee_id WHERE e.client_employee_id IS NULL",
        ).fetchone()
        orphan = row[0] if row else 0
        results.append(
            CheckResult(
                "silver_timesheet_employee_fk",
                orphan == 0,
                f"{orphan} timesheet rows reference missing employee",
            )
        )

        # Timesheet: worked_minutes in 0–1440 (minutes per day)
        row = _run(
            conn,
            """
            SELECT COUNT(*) FROM silver.timesheet
            WHERE worked_minutes IS NOT NULL AND (worked_minutes < 0 OR worked_minutes > 1440)
            """,
        ).fetchone()
        bad_mins = row[0] if row else 0
        results.append(
            CheckResult(
                "silver_timesheet_worked_minutes_range",
                bad_mins == 0,
                f"{bad_mins} rows with worked_minutes outside 0–1440",
            )
        )

        # Timesheet: hours_worked in 0–24
        row = _run(
            conn,
            """
            SELECT COUNT(*) FROM silver.timesheet
            WHERE hours_worked IS NOT NULL AND (hours_worked < 0 OR hours_worked > 24)
            """,
        ).fetchone()
        bad_hrs = row[0] if row else 0
        results.append(
            CheckResult(
                "silver_timesheet_hours_worked_range",
                bad_hrs == 0,
                f"{bad_hrs} rows with hours_worked outside 0–24",
            )
        )

        row = _run(
            conn,
            """
            SELECT COUNT(*) FROM silver.timesheet
            WHERE work_date IS NOT NULL AND (work_date < '1990-01-01' OR work_date > (CURRENT_DATE + INTERVAL '1 year')::date)
            """,
        ).fetchone()
        bad_dates = row[0] if row else 0
        results.append(
            CheckResult(
                "silver_timesheet_work_date_range",
                bad_dates == 0,
                f"{bad_dates} rows with work_date outside 1990–(today+1yr)",
            )
        )

    return results
