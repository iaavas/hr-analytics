import logging
import os

import luigi

from src.etl.gold.loader import run_gold_etl
from src.etl.silver.tasks import LoadAllSilver

logger = logging.getLogger(__name__)


class TransformGoldLayer(luigi.Task):
    year = luigi.IntParameter(default=0)
    month = luigi.IntParameter(default=0)

    def requires(self):
        return LoadAllSilver()

    def output(self):
        return luigi.LocalTarget(f"logs/markers/gold_{self.year}_{self.month}.done")

    def run(self):
        year = self.year
        month = self.month

        if year == 0 or month == 0:
            from datetime import date

            today = date.today()
            year = year or today.year
            month = month or today.month

        run_gold_etl(year=year, month=month)

        os.makedirs("logs/markers", exist_ok=True)

        with self.output().open("w") as f:
            f.write(f"done_{year}_{month}")

        logger.info(f"Gold layer complete for {year}-{month:02d}")


class LoadAllGold(luigi.Task):
    year = luigi.IntParameter(default=0)
    month = luigi.IntParameter(default=0)

    def requires(self):
        return TransformGoldLayer(year=self.year, month=self.month)

    def output(self):
        return luigi.LocalTarget("logs/markers/gold_all.done")

    def run(self):
        os.makedirs("logs/markers", exist_ok=True)

        with self.output().open("w") as f:
            f.write("done")

        logger.info("All gold layer processing complete")
