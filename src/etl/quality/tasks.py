import hashlib
import logging
import os
import subprocess
import sys
from pathlib import Path

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
        failed = [r for r in results if not r.passed]
        for r in results:
            level = logging.WARNING if not r.passed else logging.INFO
            logger.log(level, "[%s] %s: %s",
                      "PASS" if r.passed else "FAIL", r.name, r.detail)
        if failed:
            details = "; ".join(f"{r.name}: {r.detail}" for r in failed)
            raise ValueError(f"Bronze validation failed: {details}")
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
        failed = [r for r in results if not r.passed]
        for r in results:
            level = logging.WARNING if not r.passed else logging.INFO
            logger.log(level, "[%s] %s: %s",
                      "PASS" if r.passed else "FAIL", r.name, r.detail)
        if failed:
            details = "; ".join(f"{r.name}: {r.detail}" for r in failed)
            raise ValueError(f"Silver validation failed: {details}")
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


class RunDashboards(luigi.Task):
    year = luigi.IntParameter(default=0)
    month = luigi.IntParameter(default=0)
    all_months = luigi.BoolParameter(default=False)
    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        return RunQCReport(
            year=self.year,
            month=self.month,
            all_months=self.all_months,
            source=self.source,
            prefix=self.prefix,
        )

    def output(self):
        h = _path_hash(self.input().path)
        return luigi.LocalTarget(f"logs/markers/dashboards_{h}.done")

    def run(self):
        project_root = Path(__file__).resolve().parent.parent.parent
        script = project_root / "dashboard" / "visualize.py"
        if not script.exists():
            raise FileNotFoundError(f"Dashboard script not found: {script}")
        subprocess.run(
            [sys.executable, str(script)],
            cwd=str(project_root),
            env=os.environ.copy(),
            check=True,
        )
        os.makedirs("logs/markers", exist_ok=True)
        with self.output().open("w") as f:
            f.write("done")
        logger.info(
            "Dashboards written to project root (workforce_trend.html, work_hours_overtime.html, attendance_discipline.html)")
