import logging
from datetime import datetime
from typing import Any, Optional

import pandas as pd
from sqlalchemy.orm import Session

from src.app.database import SessionLocal
from src.db.models.silver import (
    Department as DepartmentSilver,
    Employee as EmployeeSilver,
    Organization as OrganizationSilver,
    Timesheet as TimesheetSilver,
)

logger = logging.getLogger(__name__)


def _scalar_or_none(val: Any):
    if val is None:
        return None
    if pd.isna(val):
        return None
    return val


class SilverLoader:

    def __init__(self, db_session: Optional[Session] = None):
        self.db = db_session or SessionLocal()
        self._owner = db_session is None

    def load_organizations(self, df: pd.DataFrame):
        df = df[
            ["organization_id", "organization_name"]
        ].dropna(subset=["organization_id"])

        df = df.drop_duplicates(subset=["organization_id"])

        records = [
            OrganizationSilver(
                organization_id=str(r.organization_id),
                organization_name=r.organization_name,
            )
            for r in df.itertuples()
        ]

        self.db.bulk_save_objects(records)
        self.db.commit()

        return len(records)

    def load_departments(self, df: pd.DataFrame):
        df = df[
            ["department_id", "department_name", "organization_id"]
        ].dropna(subset=["department_id"])

        df = df.drop_duplicates(subset=["department_id"])

        records = [
            DepartmentSilver(
                department_id=str(r.department_id),
                department_name=r.department_name,
                organization_id=r.organization_id,
            )
            for r in df.itertuples()
        ]

        self.db.bulk_save_objects(records)
        self.db.commit()

        return len(records)

    def load_employee_data(self, df: pd.DataFrame):

        records = []

        for r in df.itertuples():
            records.append(
                EmployeeSilver(
                    client_employee_id=str(r.client_employee_id),
                    first_name=r.first_name,
                    last_name=r.last_name,
                    organization_id=r.organization_id,
                    department_id=r.department_id,
                    manager_employee_id=r.manager_employee_id,
                    hire_date=r.hire_date,
                    term_date=r.term_date,
                    is_active=r.is_active,
                    created_at=datetime.utcnow(),
                )
            )

        self.db.bulk_save_objects(records)
        self.db.commit()

        return len(records)

    def load_timesheet_data(self, df: pd.DataFrame):
        self.db.query(TimesheetSilver).delete()
        self.db.commit()

        records = []
        for r in df.itertuples():
            records.append(
                TimesheetSilver(
                    client_employee_id=str(r.client_employee_id),
                    department_id=_scalar_or_none(
                        getattr(r, "department_id", None)),
                    punch_apply_date=_scalar_or_none(
                        getattr(r, "punch_apply_date", None)),
                    punch_in_datetime=_scalar_or_none(
                        getattr(r, "punch_in_datetime", None)),
                    punch_out_datetime=_scalar_or_none(
                        getattr(r, "punch_out_datetime", None)),
                    scheduled_start_datetime=_scalar_or_none(
                        getattr(r, "scheduled_start_datetime", None)),
                    scheduled_end_datetime=_scalar_or_none(
                        getattr(r, "scheduled_end_datetime", None)),
                    worked_minutes=_scalar_or_none(
                        getattr(r, "worked_minutes", None)),
                    scheduled_minutes=_scalar_or_none(
                        getattr(r, "scheduled_minutes", None)),
                    hours_worked=_scalar_or_none(
                        getattr(r, "hours_worked", None)),
                    work_date=_scalar_or_none(getattr(r, "work_date", None)),
                    pay_code=_scalar_or_none(getattr(r, "pay_code", None)),
                    punch_in_comment=_scalar_or_none(
                        getattr(r, "punch_in_comment", None)),
                    punch_out_comment=_scalar_or_none(
                        getattr(r, "punch_out_comment", None)),
                    source_file=_scalar_or_none(
                        getattr(r, "source_file", None)),
                )
            )
        self.db.bulk_save_objects(records)
        self.db.commit()
        return len(records)

    def close(self):
        if self._owner:
            self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
