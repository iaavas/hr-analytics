import hashlib
import logging
import os
from datetime import date

import luigi

from src.app.config import settings
from src.etl.gold.loader import run_gold_etl

logger = logging.getLogger(__name__)


def _path_hash(path: str) -> str:
    return hashlib.sha256(path.encode()).hexdigest()[:12]


class TransformGoldLayer(luigi.Task):
    year = luigi.IntParameter(default=0)
    month = luigi.IntParameter(default=0)
    all_months = luigi.BoolParameter(
        default=False, description="Process full history across all months")
    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        from src.etl.quality.tasks import ValidateSilver
        return ValidateSilver(source=self.source, prefix=self.prefix)

    def output(self):
        h = _path_hash(self.input().path)
        if self.all_months:
            return luigi.LocalTarget(f"logs/markers/gold_all_months_{h}.done")
        return luigi.LocalTarget(
            f"logs/markers/gold_{self.year}_{self.month}_{h}.done"
        )

    def run(self):
        year = self.year
        month = self.month

        if self.all_months:
            run_gold_etl(all_months=True)
        else:
            if year == 0 or month == 0:
                today = date.today()
                year = settings.etl_default_year or today.year
                month = settings.etl_default_month or today.month

            run_gold_etl(year=year, month=month)

        os.makedirs("logs/markers", exist_ok=True)

        with self.output().open("w") as f:
            if self.all_months:
                f.write("done_all_months")
            else:
                f.write(f"done_{year}_{month}")

        msg = "Gold layer complete for full history" if self.all_months else f"Gold layer complete for {year}-{month:02d}"
        logger.info(msg)


class LoadAllGold(luigi.Task):
    year = luigi.IntParameter(default=0)
    month = luigi.IntParameter(default=0)
    all_months = luigi.BoolParameter(
        default=False, description="Process full history across all months")
    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        return TransformGoldLayer(
            year=self.year,
            month=self.month,
            all_months=self.all_months,
            source=self.source,
            prefix=self.prefix,
        )

    def output(self):
        h = _path_hash(self.input().path)
        return luigi.LocalTarget(f"logs/markers/gold_all_{h}.done")

    def run(self):
        os.makedirs("logs/markers", exist_ok=True)

        with self.output().open("w") as f:
            f.write("done")

        logger.info("All gold layer processing complete")
