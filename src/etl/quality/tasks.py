import hashlib
import logging
import os

import luigi

from src.etl.bronze.tasks import LoadAllBronze
from src.etl.quality.checks import validate_bronze, validate_silver, CheckResult
from src.etl.quality.report import write_report

logger = logging.getLogger(__name__)


def _all_passed(results: list[CheckResult]) -> bool:
    return all(r.passed for r in results)


def _path_hash(path: str) -> str:
    return hashlib.sha256(path.encode()).hexdigest()[:12]


class ValidateBronze(luigi.Task):
    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        return LoadAllBronze(source=self.source, prefix=self.prefix)

    def output(self):
        h = _path_hash(self.input().path)
        return luigi.LocalTarget(f"logs/markers/quality_bronze_{h}.done")

    def run(self):
        results = validate_bronze()
        for r in results:
            logger.info("[%s] %s: %s", "PASS" if r.passed else "FAIL", r.name, r.detail)
        if not _all_passed(results):
            raise ValueError("Bronze validation failed")
        os.makedirs("logs/markers", exist_ok=True)
        with self.output().open("w") as f:
            f.write("ok")


class ValidateSilver(luigi.Task):
    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        from src.etl.silver.tasks import LoadAllSilver
        return LoadAllSilver(source=self.source, prefix=self.prefix)

    def output(self):
        h = _path_hash(self.input().path)
        return luigi.LocalTarget(f"logs/markers/quality_silver_{h}.done")

    def run(self):
        results = validate_silver()
        for r in results:
            logger.info("[%s] %s: %s", "PASS" if r.passed else "FAIL", r.name, r.detail)
        if not _all_passed(results):
            raise ValueError("Silver validation failed")
        os.makedirs("logs/markers", exist_ok=True)
        with self.output().open("w") as f:
            f.write("ok")


class RunQCReport(luigi.Task):
    year = luigi.IntParameter(default=0)
    month = luigi.IntParameter(default=0)
    all_months = luigi.BoolParameter(default=False)
    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        from src.etl.gold.tasks import LoadAllGold
        return LoadAllGold(
            year=self.year,
            month=self.month,
            all_months=self.all_months,
            source=self.source,
            prefix=self.prefix,
        )

    def output(self):
        h = _path_hash(self.input().path)
        return luigi.LocalTarget(f"logs/markers/qc_report_{h}.done")

    def run(self):
        path = write_report()
        logger.info("QC report written: %s", path)
        os.makedirs("logs/markers", exist_ok=True)
        with self.output().open("w") as f:
            f.write(path)
