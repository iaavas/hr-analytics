import hashlib
import logging
import os

import luigi
import pandas as pd
from sqlalchemy import text

from src.app.config import settings
from src.app.database import engine
from src.etl.bronze.extract import (
    DiscoverLocalFiles,
    DiscoverMinIOFiles,
    ExtractFromLocal,
    ExtractFromMinIO,
)
from src.etl.bronze.loader import BronzeLoader

logger = logging.getLogger(__name__)

ENTITY_MAP = {
    "employee": "employee",
    "timesheet": "timesheet",
}


class LoadSingleFileBronze(luigi.Task):
    entity = luigi.Parameter()
    filename = luigi.Parameter()
    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        if self.source == "minio":
            return ExtractFromMinIO(filename=self.filename, prefix=self.prefix)
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
            batch_size = settings.etl_batch_size
            if self.entity == "employee":
                count = loader.load_employee_data(
                    df, source_file=self.filename, batch_size=batch_size)
            elif self.entity == "timesheet":
                count = loader.load_timesheet_data(
                    df, source_file=self.filename, batch_size=batch_size)
            else:
                raise ValueError(f"Unknown entity type: {self.entity}")

        logger.info(f"Bronze load: {count} records from {self.filename}")

        os.makedirs("logs/markers", exist_ok=True)
        with self.output().open("w") as f:
            f.write(f"loaded {count} records from {self.filename}")


def _manifest_hash(manifest_path: str) -> str:
    with open(manifest_path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()[:12]


def _manifest_marker_key(manifest_path: str) -> str:
    if os.path.exists(manifest_path):
        return _manifest_hash(manifest_path)
    return hashlib.sha256(manifest_path.encode()).hexdigest()[:12]


def _manifest_filenames(manifest_path: str):
    with open(manifest_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            name = line.split("\t")[0].strip()
            yield os.path.basename(name)


class LoadAllBronze(luigi.Task):
    source = luigi.Parameter(default="minio")
    prefix = luigi.Parameter(default="")

    def requires(self):
        if self.source == "minio":
            return DiscoverMinIOFiles(prefix=self.prefix)
        return DiscoverLocalFiles()

    def output(self):
        manifest_path = self.input().path
        h = _manifest_marker_key(manifest_path)
        return luigi.LocalTarget(
            f"logs/markers/bronze_all_{self.source}_{h}.done"
        )

    def _get_filenames(self):
        return list(_manifest_filenames(self.input().path))

    def run(self):

        filenames = self._get_filenames()

        if not filenames:
            raise ValueError("No CSV files found to load")

        tasks = []
        skipped = []

        for filename in filenames:
            entity = None
            for name_prefix, ent in ENTITY_MAP.items():
                if filename.lower().startswith(name_prefix):
                    entity = ent
                    break

            if entity:
                tasks.append(
                    LoadSingleFileBronze(
                        entity=entity,
                        filename=filename,
                        source=self.source,
                        prefix=self.prefix,
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
