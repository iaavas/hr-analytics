"""Initial schema and bronze tables

Revision ID: 001
Revises:
Create Date: (matches existing DB state)

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create schemas and bronze tables (idempotent for existing DBs)."""
    conn = op.get_bind()
    conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS bronze"))
    conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS silver"))
    conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS gold"))

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


def downgrade() -> None:
    """Drop bronze tables and schemas."""
    op.drop_table("timesheet_raw")
    op.drop_table("employee_raw")
    conn = op.get_bind()
    conn.execute(sa.text("DROP SCHEMA IF EXISTS gold"))
    conn.execute(sa.text("DROP SCHEMA IF EXISTS silver"))
    conn.execute(sa.text("DROP SCHEMA IF EXISTS bronze"))
