import luigi
import pandas as pd
import logging
import os

from src.etl.tasks.extract import ExtractFileFromMinIO, DiscoverMinIOFiles
from src.etl.bronze.loader import BronzeLoader

logger = logging.getLogger(__name__)


class LoadSingleFileBronze(luigi.Task):
  
    entity = luigi.Parameter()       
    object_name = luigi.Parameter()  

    def requires(self):
        return ExtractFileFromMinIO(object_name=self.object_name)

    def output(self):
        safe_name = os.path.basename(self.object_name).replace(".csv", "")
        return luigi.LocalTarget(f"logs/markers/bronze_{self.entity}_{safe_name}.done")

    def run(self):
        local_path = self.input().path

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Extracted file not found: {local_path}")

        df = pd.read_csv(local_path)
        source_file = os.path.basename(self.object_name)

        with BronzeLoader() as loader:
            if self.entity == "employee":
                count = loader.load_employee_data(df, source_file=source_file)
            elif self.entity == "timesheet":
                count = loader.load_timesheet_data(df, source_file=source_file)
            else:
                raise ValueError(f"Unknown entity: {self.entity}")

        logger.info(f"Bronze load complete: {count} records from {source_file}")

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            f.write(f"loaded {count} records from {source_file}")


class LoadAllBronze(luigi.Task):
  
    prefix = luigi.Parameter(default="")

    ENTITY_MAP = {
        "employee": "employee",
        "timesheet": "timesheet",
    }

    def requires(self):
        return DiscoverMinIOFiles(prefix=self.prefix)

    def output(self):
        return luigi.LocalTarget("logs/markers/bronze_all.done")

    def run(self):
        with self.input().open("r") as f:
            object_names = [line.strip() for line in f if line.strip()]

        if not object_names:
            raise ValueError("No CSV files discovered in MinIO")

        tasks = []
        unmatched = []

        for object_name in object_names:
            basename = os.path.basename(object_name).lower()
            matched_entity = None

            for prefix, entity in self.ENTITY_MAP.items():
                if basename.startswith(prefix):
                    matched_entity = entity
                    break

            if matched_entity:
                tasks.append(
                    LoadSingleFileBronze(
                        entity=matched_entity,
                        object_name=object_name,
                    )
                )
                logger.info(f"Routing {object_name} → {matched_entity}")
            else:
                unmatched.append(object_name)
                logger.warning(f"No entity mapping for file: {object_name}, skipping")

        if not tasks:
            raise ValueError("No files matched any known entity prefix")

        yield tasks

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            f.write(f"loaded {len(tasks)} files\nskipped: {unmatched}")