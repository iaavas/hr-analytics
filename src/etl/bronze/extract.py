import luigi
import os
import logging
from src.etl.extract_minio import MinIOExtractor
from src.app.config import settings

logger = logging.getLogger(__name__)


class ExtractFileFromMinIO(luigi.Task):
    object_name = luigi.Parameter() 

    def output(self):
        local_path = f"data/raw/{os.path.basename(self.object_name)}"
        return luigi.LocalTarget(local_path)

    def run(self):
        os.makedirs("data/raw", exist_ok=True)
        extractor = MinIOExtractor(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            bucket_name=settings.minio_bucket,
        )
        extractor.download_to_file(self.object_name, self.output().path)
        logger.info(f"Extracted {self.object_name} → {self.output().path}")


class DiscoverMinIOFiles(luigi.Task):
    prefix = luigi.Parameter(default="")

    def output(self):
        safe_prefix = self.prefix.replace("/", "_") or "root"
        return luigi.LocalTarget(f"logs/manifests/{safe_prefix}_manifest.txt")

    def run(self):
        extractor = MinIOExtractor(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            bucket_name=settings.minio_bucket,
        )
        objects = extractor.list_objects(prefix=self.prefix)
        csv_files = [o for o in objects if o.endswith(".csv")]

        os.makedirs("logs/manifests", exist_ok=True)
        with self.output().open("w") as f:
            for name in csv_files:
                f.write(name + "\n")

        logger.info(f"Discovered {len(csv_files)} CSV files under prefix '{self.prefix}'")