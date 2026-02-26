"""Move bronze tables into bronze schema

Revision ID: move_bronze_to_schema
Revises: add_daily_minutes_columns
Create Date: 2026-02-26
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "move_bronze_to_schema"
down_revision: Union[str, Sequence[str], None] = "add_daily_minutes_columns"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Ensure schema exists (idempotent)
    op.execute(sa.text("CREATE SCHEMA IF NOT EXISTS bronze"))

    # Move existing tables that were previously created in public
    op.execute(sa.text("ALTER TABLE IF EXISTS public.employee_raw SET SCHEMA bronze"))
    op.execute(sa.text("ALTER TABLE IF EXISTS public.timesheet_raw SET SCHEMA bronze"))


def downgrade() -> None:
    # Move tables back to public schema
    op.execute(sa.text("ALTER TABLE IF EXISTS bronze.employee_raw SET SCHEMA public"))
    op.execute(sa.text("ALTER TABLE IF EXISTS bronze.timesheet_raw SET SCHEMA public"))
