import hashlib
import logging
import os

import luigi
import pandas as pd
from sqlalchemy import text

from src.app.database import engine
from src.etl.quality.tasks import ValidateBronze
from src.etl.silver.loader import SilverLoader
from src.etl.silver.transform import clean_employee, clean_timesheet

logger = logging.getLogger(__name__)


def _path_hash(path: str) -> str:
    return hashlib.sha256(path.encode()).hexdigest()[:12]


class TransformEmployeeSilver(luigi.Task):

    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        return ValidateBronze(
            source=self.source,
            prefix=self.prefix,
        )

    def output(self):
        h = _path_hash(self.input().path)
        return luigi.LocalTarget(f"logs/markers/silver_employee_{h}.done")

    def run(self):

        with engine.begin() as conn:
            conn.execute(
                text(
                    "TRUNCATE TABLE silver.timesheet, silver.employee, silver.department, silver.organization RESTART IDENTITY CASCADE"
                )
            )

        with engine.begin() as conn:
            df = pd.read_sql(
                text("SELECT * FROM employee_raw"),
                conn,
            )

        clean_df = clean_employee(df)

        with SilverLoader() as loader:
            try:
                loader.load_organizations(clean_df)
                loader.load_departments(clean_df)
                count = loader.load_employee_data(clean_df)
                loader.commit()
            except Exception as e:
                loader.rollback()
                logger.error("Silver employee load failed: %s",
                             e, exc_info=True)
                raise

        os.makedirs("logs/markers", exist_ok=True)

        with self.output().open("w") as f:
            f.write(str(count))

        logger.info("Employee silver complete")


class TransformTimesheetSilver(luigi.Task):

    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        return TransformEmployeeSilver(
            source=self.source,
            prefix=self.prefix,
        )

    def output(self):
        h = _path_hash(self.input().path)
        return luigi.LocalTarget(f"logs/markers/silver_timesheet_{h}.done")

    def run(self):

        with engine.begin() as conn:
            df = pd.read_sql(
                text("SELECT * FROM timesheet_raw"),
                conn,
            )

        clean_df = clean_timesheet(df)

        with SilverLoader() as loader:
            try:
                count = loader.load_timesheet_data(clean_df)
                loader.commit()
            except Exception as e:
                loader.rollback()
                logger.error("Silver timesheet load failed: %s",
                             e, exc_info=True)
                raise

        os.makedirs("logs/markers", exist_ok=True)

        with self.output().open("w") as f:
            f.write(str(count))

        logger.info("Timesheet silver complete")


class LoadAllSilver(luigi.Task):

    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        return TransformTimesheetSilver(
            source=self.source,
            prefix=self.prefix,
        )

    def output(self):
        h = _path_hash(self.input().path)
        return luigi.LocalTarget(f"logs/markers/silver_all_{h}.done")

    def run(self):

        os.makedirs("logs/markers", exist_ok=True)

        with self.output().open("w") as f:
            f.write("done")

        logger.info("Silver layer complete")
