"""Initial schema: bronze, silver, and gold tables

Revision ID: initial_one
Revises:
Create Date: (single initial migration)

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "initial_one"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"


def upgrade() -> None:
    """Create schemas and all tables (bronze, silver, gold)."""
    conn = op.get_bind()
    conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS bronze"))
    conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS silver"))
    conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS gold"))

    # Bronze (public schema)
    op.create_table(
        "employee_raw",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("client_employee_id", sa.String(50), nullable=True),
        sa.Column("first_name", sa.String(100), nullable=True),
        sa.Column("middle_name", sa.String(100), nullable=True),
        sa.Column("last_name", sa.String(100), nullable=True),
        sa.Column("preferred_name", sa.String(100), nullable=True),
        sa.Column("job_code", sa.String(50), nullable=True),
        sa.Column("job_title", sa.String(255), nullable=True),
        sa.Column("job_start_date", sa.String(20), nullable=True),
        sa.Column("organization_id", sa.String(50), nullable=True),
        sa.Column("organization_name", sa.String(255), nullable=True),
        sa.Column("department_id", sa.String(50), nullable=True),
        sa.Column("department_name", sa.String(255), nullable=True),
        sa.Column("dob", sa.String(20), nullable=True),
        sa.Column("hire_date", sa.String(20), nullable=True),
        sa.Column("recent_hire_date", sa.String(20), nullable=True),
        sa.Column("anniversary_date", sa.String(20), nullable=True),
        sa.Column("term_date", sa.String(20), nullable=True),
        sa.Column("years_of_experience", sa.String(50), nullable=True),
        sa.Column("work_email", sa.String(255), nullable=True),
        sa.Column("address", sa.String(500), nullable=True),
        sa.Column("city", sa.String(100), nullable=True),
        sa.Column("state", sa.String(100), nullable=True),
        sa.Column("zip", sa.String(20), nullable=True),
        sa.Column("country", sa.String(100), nullable=True),
        sa.Column("manager_employee_id", sa.String(50), nullable=True),
        sa.Column("manager_employee_name", sa.String(255), nullable=True),
        sa.Column("fte_status", sa.String(50), nullable=True),
        sa.Column("is_per_deim", sa.String(50), nullable=True),
        sa.Column("cell_phone", sa.String(50), nullable=True),
        sa.Column("work_phone", sa.String(50), nullable=True),
        sa.Column("scheduled_weekly_hour", sa.String(20), nullable=True),
        sa.Column("active_status", sa.String(10), nullable=True),
        sa.Column("termination_reason", sa.String(255), nullable=True),
        sa.Column("clinical_level", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.Column("source_file", sa.String(255), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    op.create_table(
        "timesheet_raw",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("client_employee_id", sa.String(255), nullable=True),
        sa.Column("department_id", sa.String(255), nullable=True),
        sa.Column("department_name", sa.String(255), nullable=True),
        sa.Column("home_department_id", sa.String(255), nullable=True),
        sa.Column("home_department_name", sa.String(255), nullable=True),
        sa.Column("pay_code", sa.String(255), nullable=True),
        sa.Column("punch_in_comment", sa.String(500), nullable=True),
        sa.Column("punch_out_comment", sa.String(500), nullable=True),
        sa.Column("hours_worked", sa.Float(), nullable=True),
        sa.Column("punch_apply_date", sa.String(20), nullable=True),
        sa.Column("punch_in_datetime", sa.String(255), nullable=True),
        sa.Column("punch_out_datetime", sa.String(255), nullable=True),
        sa.Column("scheduled_start_datetime", sa.String(255), nullable=True),
        sa.Column("scheduled_end_datetime", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.Column("source_file", sa.String(255), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )

    # Silver
    op.create_table(
        "organization",
        sa.Column("organization_id", sa.String(50), nullable=False),
        sa.Column("organization_name", sa.String(255), nullable=False),
        sa.PrimaryKeyConstraint("organization_id"),
        schema=SCHEMA_SILVER,
    )
    op.create_table(
        "department",
        sa.Column("department_id", sa.String(50), nullable=False),
        sa.Column("department_name", sa.String(255), nullable=False),
        sa.Column(
            "organization_id",
            sa.String(50),
            sa.ForeignKey("silver.organization.organization_id"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("department_id"),
        schema=SCHEMA_SILVER,
    )
    op.create_index(
        "ix_department_org",
        "department",
        ["organization_id"],
        unique=False,
        schema=SCHEMA_SILVER,
    )
    op.create_table(
        "employee",
        sa.Column("client_employee_id", sa.String(50), nullable=False),
        sa.Column("first_name", sa.String(100), nullable=False),
        sa.Column("middle_name", sa.String(100), nullable=True),
        sa.Column("last_name", sa.String(100), nullable=False),
        sa.Column("preferred_name", sa.String(100), nullable=True),
        sa.Column("job_code", sa.String(50), nullable=True),
        sa.Column("job_title", sa.String(255), nullable=True),
        sa.Column(
            "organization_id",
            sa.String(50),
            sa.ForeignKey("silver.organization.organization_id"),
            nullable=True,
        ),
        sa.Column(
            "department_id",
            sa.String(50),
            sa.ForeignKey("silver.department.department_id"),
            nullable=True,
        ),
        sa.Column(
            "manager_employee_id",
            sa.String(50),
            sa.ForeignKey(
                "silver.employee.client_employee_id",
                deferrable=True,
                initially="DEFERRED",
            ),
            nullable=True,
        ),
        sa.Column("hire_date", sa.Date(), nullable=True),
        sa.Column("term_date", sa.Date(), nullable=True),
        sa.Column("dob", sa.Date(), nullable=True),
        sa.Column("years_of_experience", sa.Float(), nullable=True),
        sa.Column("scheduled_weekly_hour", sa.Float(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=True, server_default="true"),
        sa.Column("is_per_diem", sa.Boolean(), nullable=True),
        sa.Column("source_file", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("client_employee_id"),
        schema=SCHEMA_SILVER,
    )
    op.create_index(
        "ix_employee_department",
        "employee",
        ["department_id"],
        unique=False,
        schema=SCHEMA_SILVER,
    )
    op.create_index(
        "ix_employee_org",
        "employee",
        ["organization_id"],
        unique=False,
        schema=SCHEMA_SILVER,
    )
    op.create_table(
        "timesheet",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "client_employee_id",
            sa.String(50),
            sa.ForeignKey("silver.employee.client_employee_id"),
            nullable=False,
        ),
        sa.Column(
            "department_id",
            sa.String(50),
            sa.ForeignKey("silver.department.department_id"),
            nullable=True,
        ),
        sa.Column("punch_apply_date", sa.Date(), nullable=True),
        sa.Column("punch_in_datetime", sa.DateTime(), nullable=True),
        sa.Column("punch_out_datetime", sa.DateTime(), nullable=True),
        sa.Column("scheduled_start_datetime", sa.DateTime(), nullable=True),
        sa.Column("scheduled_end_datetime", sa.DateTime(), nullable=True),
        sa.Column("worked_minutes", sa.Float(), nullable=True),
        sa.Column("scheduled_minutes", sa.Float(), nullable=True),
        sa.Column("hours_worked", sa.Float(), nullable=True),
        sa.Column("work_date", sa.Date(), nullable=True),
        sa.Column("pay_code", sa.String(100), nullable=True),
        sa.Column("punch_in_comment", sa.String(500), nullable=True),
        sa.Column("punch_out_comment", sa.String(500), nullable=True),
        sa.Column("source_file", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "client_employee_id",
            "punch_in_datetime",
            "punch_out_datetime",
            name="uq_timesheet_shift",
        ),
        schema=SCHEMA_SILVER,
    )
    op.create_index(
        "ix_timesheet_emp_date",
        "timesheet",
        ["client_employee_id", "work_date"],
        unique=False,
        schema=SCHEMA_SILVER,
    )

    # Gold (add_daily_minutes_columns migration adds late/early/overtime/variance columns to timesheet_daily_summary)
    op.create_table(
        "employee_monthly_snapshot",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("client_employee_id", sa.String(50), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=True),
        sa.Column("hire_date", sa.Date(), nullable=True),
        sa.Column("term_date", sa.Date(), nullable=True),
        sa.Column("department_id", sa.String(50), nullable=True),
        sa.Column("department_name", sa.String(255), nullable=True),
        sa.Column("job_title", sa.String(255), nullable=True),
        sa.Column("tenure_days", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA_GOLD,
    )
    op.create_index(
        "ix_snapshot_emp_month",
        "employee_monthly_snapshot",
        ["client_employee_id", "year", "month"],
        unique=False,
        schema=SCHEMA_GOLD,
    )
    op.create_table(
        "timesheet_daily_summary",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("client_employee_id", sa.String(50), nullable=False),
        sa.Column("work_date", sa.Date(), nullable=False),
        sa.Column("department_id", sa.String(50), nullable=True),
        sa.Column("total_shifts", sa.Integer(), nullable=True),
        sa.Column("total_worked_minutes", sa.Float(), nullable=True),
        sa.Column("total_scheduled_minutes", sa.Float(), nullable=True),
        sa.Column("total_hours_worked", sa.Float(), nullable=True),
        sa.Column("late_arrival_count", sa.Integer(), nullable=True),
        sa.Column("early_departure_count", sa.Integer(), nullable=True),
        sa.Column("overtime_count", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA_GOLD,
    )
    op.create_index(
        "ix_daily_emp_date",
        "timesheet_daily_summary",
        ["client_employee_id", "work_date"],
        unique=False,
        schema=SCHEMA_GOLD,
    )
    op.create_table(
        "department_monthly_metrics",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("department_id", sa.String(50), nullable=True),
        sa.Column("department_name", sa.String(255), nullable=True),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("active_headcount", sa.Integer(), nullable=True),
        sa.Column("total_hires", sa.Integer(), nullable=True),
        sa.Column("total_terminations", sa.Integer(), nullable=True),
        sa.Column("turnover_rate", sa.Float(), nullable=True),
        sa.Column("avg_tenure_days", sa.Float(), nullable=True),
        sa.Column("avg_weekly_hours", sa.Float(), nullable=True),
        sa.Column("late_arrival_rate", sa.Float(), nullable=True),
        sa.Column("early_departure_rate", sa.Float(), nullable=True),
        sa.Column("overtime_rate", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA_GOLD,
    )
    op.create_index(
        "ix_dept_metrics",
        "department_monthly_metrics",
        ["department_id", "year", "month"],
        unique=False,
        schema=SCHEMA_GOLD,
    )
    op.create_table(
        "employee_attendance_metrics",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("client_employee_id", sa.String(50), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("total_shifts", sa.Integer(), nullable=True),
        sa.Column("days_worked", sa.Integer(), nullable=True),
        sa.Column("total_hours_worked", sa.Float(), nullable=True),
        sa.Column("avg_hours_per_day", sa.Float(), nullable=True),
        sa.Column("avg_hours_per_week", sa.Float(), nullable=True),
        sa.Column("late_arrival_count", sa.Integer(), nullable=True),
        sa.Column("late_arrival_rate", sa.Float(), nullable=True),
        sa.Column("early_departure_count", sa.Integer(), nullable=True),
        sa.Column("early_departure_rate", sa.Float(), nullable=True),
        sa.Column("overtime_count", sa.Integer(), nullable=True),
        sa.Column("overtime_rate", sa.Float(), nullable=True),
        sa.Column("avg_variance_minutes", sa.Float(), nullable=True),
        sa.Column("rolling_avg_hours_4w", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA_GOLD,
    )
    op.create_index(
        "ix_emp_attendance",
        "employee_attendance_metrics",
        ["client_employee_id", "year", "month"],
        unique=False,
        schema=SCHEMA_GOLD,
    )
    op.create_table(
        "headcount_trend",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("active_headcount", sa.Integer(), nullable=True),
        sa.Column("new_hires", sa.Integer(), nullable=True),
        sa.Column("terminations", sa.Integer(), nullable=True),
        sa.Column("early_attrition_count", sa.Integer(), nullable=True),
        sa.Column("early_attrition_rate", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA_GOLD,
    )
    op.create_index(
        "ix_headcount_trend",
        "headcount_trend",
        ["year", "month"],
        unique=False,
        schema=SCHEMA_GOLD,
    )
    op.create_table(
        "organization_metrics",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("organization_id", sa.String(50), nullable=True),
        sa.Column("organization_name", sa.String(255), nullable=True),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("total_employees", sa.Integer(), nullable=True),
        sa.Column("active_employees", sa.Integer(), nullable=True),
        sa.Column("total_departments", sa.Integer(), nullable=True),
        sa.Column("avg_tenure_days", sa.Float(), nullable=True),
        sa.Column("turnover_rate", sa.Float(), nullable=True),
        sa.Column("avg_late_arrival_rate", sa.Float(), nullable=True),
        sa.Column("avg_early_departure_rate", sa.Float(), nullable=True),
        sa.Column("avg_overtime_rate", sa.Float(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        schema=SCHEMA_GOLD,
    )


def downgrade() -> None:
    """Drop all tables and schemas."""
    # Silver
    op.drop_index(
        "ix_timesheet_emp_date",
        table_name="timesheet",
        schema=SCHEMA_SILVER,
    )
    op.drop_table("timesheet", schema=SCHEMA_SILVER)
    op.drop_index("ix_employee_org", table_name="employee", schema=SCHEMA_SILVER)
    op.drop_index("ix_employee_department", table_name="employee", schema=SCHEMA_SILVER)
    op.drop_table("employee", schema=SCHEMA_SILVER)
    op.drop_index("ix_department_org", table_name="department", schema=SCHEMA_SILVER)
    op.drop_table("department", schema=SCHEMA_SILVER)
    op.drop_table("organization", schema=SCHEMA_SILVER)
    # Bronze
    op.drop_table("timesheet_raw")
    op.drop_table("employee_raw")
    # Gold
    op.drop_index("ix_headcount_trend", table_name="headcount_trend", schema=SCHEMA_GOLD)
    op.drop_table("headcount_trend", schema=SCHEMA_GOLD)
    op.drop_index("ix_emp_attendance", table_name="employee_attendance_metrics", schema=SCHEMA_GOLD)
    op.drop_table("employee_attendance_metrics", schema=SCHEMA_GOLD)
    op.drop_index("ix_dept_metrics", table_name="department_monthly_metrics", schema=SCHEMA_GOLD)
    op.drop_table("department_monthly_metrics", schema=SCHEMA_GOLD)
    op.drop_index("ix_daily_emp_date", table_name="timesheet_daily_summary", schema=SCHEMA_GOLD)
    op.drop_table("timesheet_daily_summary", schema=SCHEMA_GOLD)
    op.drop_index("ix_snapshot_emp_month", table_name="employee_monthly_snapshot", schema=SCHEMA_GOLD)
    op.drop_table("employee_monthly_snapshot", schema=SCHEMA_GOLD)
    op.drop_table("organization_metrics", schema=SCHEMA_GOLD)
    # Schemas
    conn = op.get_bind()
    conn.execute(sa.text("DROP SCHEMA IF EXISTS gold"))
    conn.execute(sa.text("DROP SCHEMA IF EXISTS silver"))
    conn.execute(sa.text("DROP SCHEMA IF EXISTS bronze"))
