"""Add minute-level attendance aggregates to gold.timesheet_daily_summary

Revision ID: add_daily_minutes_columns
Revises: initial_one
Create Date: 2026-02-24
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "add_daily_minutes_columns"
down_revision: Union[str, Sequence[str], None] = "initial_one"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Ensure gold schema exists
    op.execute(sa.text("CREATE SCHEMA IF NOT EXISTS gold"))

    # The gold tables are typically created via SQLAlchemy metadata; this migration
    # only adds the new columns when the table already exists.
    with op.batch_alter_table("timesheet_daily_summary", schema="gold") as batch:
        batch.add_column(sa.Column("late_minutes_total", sa.Float(), nullable=True))
        batch.add_column(sa.Column("early_minutes_total", sa.Float(), nullable=True))
        batch.add_column(sa.Column("overtime_minutes_total", sa.Float(), nullable=True))
        batch.add_column(sa.Column("avg_variance_minutes", sa.Float(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("timesheet_daily_summary", schema="gold") as batch:
        batch.drop_column("avg_variance_minutes")
        batch.drop_column("overtime_minutes_total")
        batch.drop_column("early_minutes_total")
        batch.drop_column("late_minutes_total")
