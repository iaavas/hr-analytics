import luigi
import pandas as pd
import logging
import os

from src.etl.bronze.extract import (
    ExtractFromMinIO,
    ExtractFromLocal,
    DiscoverMinIOFiles,
)
from src.etl.bronze.loader import BronzeLoader
from src.app.config import settings

logger = logging.getLogger(__name__)

ENTITY_MAP = {
    "employee": "employee",
    "timesheet": "timesheet",
}


class LoadSingleFileBronze(luigi.Task):
    """Load one CSV into the correct bronze table based on entity type."""
    entity = luigi.Parameter()
    filename = luigi.Parameter()
    source = luigi.Parameter(default="minio")

    def requires(self):
        if self.source == "minio":
            return ExtractFromMinIO(filename=self.filename)
        return ExtractFromLocal(filename=self.filename)

    def output(self):
        safe_name = self.filename.replace(".csv", "")
        return luigi.LocalTarget(
            f"logs/markers/bronze_{self.entity}_{safe_name}.done"
        )

    def run(self):
        df = pd.read_csv(self.input().path,
                         on_bad_lines="skip", engine="python", sep="|",
                         na_values=["[NULL]", "NULL", ""])

        with BronzeLoader() as loader:
            if self.entity == "employee":
                count = loader.load_employee_data(
                    df, source_file=self.filename)
            elif self.entity == "timesheet":
                count = loader.load_timesheet_data(
                    df, source_file=self.filename)
            else:
                raise ValueError(f"Unknown entity type: {self.entity}")

        logger.info(f"Bronze load: {count} records from {self.filename}")

        os.makedirs("logs/markers", exist_ok=True)
        with self.output().open("w") as f:
            f.write(f"loaded {count} records from {self.filename}")


class LoadAllBronze(luigi.Task):
    """
    Entry point — discovers all CSVs and loads them all into bronze.
    Handles any number of employee_* and timesheet_* files automatically.
    Safe to rerun — skips files that already have .done markers.
    """
    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        if self.source == "minio":
            return DiscoverMinIOFiles(prefix=self.prefix)
        return []

    def output(self):
        return luigi.LocalTarget("logs/markers/bronze_all.done")

    def _get_filenames(self):
        if self.source == "minio":
            with self.input().open("r") as f:
                return [
                    os.path.basename(line.strip())
                    for line in f if line.strip()
                ]
        else:
            raw_dir = settings.raw_data_dir
            return [f for f in os.listdir(raw_dir) if f.endswith(".csv")]

    def run(self):
        filenames = self._get_filenames()

        if not filenames:
            raise ValueError("No CSV files found to load")

        tasks = []
        skipped = []

        for filename in filenames:
            entity = None
            for prefix, ent in ENTITY_MAP.items():
                if filename.lower().startswith(prefix):
                    entity = ent
                    break

            if entity:
                tasks.append(
                    LoadSingleFileBronze(
                        entity=entity,
                        filename=filename,
                        source=self.source,
                    )
                )
                logger.info(f"Queuing: {filename} → bronze.{entity}")
            else:
                skipped.append(filename)
                logger.warning(f"No entity match for: {filename}, skipping")

        if not tasks:
            raise ValueError("No files matched any known entity prefix")

        yield tasks

        os.makedirs("logs/markers", exist_ok=True)
        with self.output().open("w") as f:
            loaded = [t.filename for t in tasks]
            f.write(f"loaded: {loaded}\nskipped: {skipped}")

        logger.info(
            f"Bronze complete — {len(tasks)} files loaded, {len(skipped)} skipped")
