import hashlib
import logging
import os
import shutil

import luigi

from src.app.config import settings
from src.etl.extract_minio import MinIOExtractor

logger = logging.getLogger(__name__)


class ExtractFileFromMinIO(luigi.Task):

    object_name = luigi.Parameter()

    def output(self):
        local_path = os.path.join(
            settings.raw_data_dir, os.path.basename(self.object_name)
        )
        return luigi.LocalTarget(local_path)

    def run(self):
        os.makedirs(settings.raw_data_dir, exist_ok=True)
        extractor = MinIOExtractor(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
            bucket_name=settings.minio_bucket,
        )
        extractor.download_to_file(self.object_name, self.output().path)
        logger.info(f"Extracted {self.object_name} → {self.output().path}")


class DiscoverMinIOFiles(luigi.Task):
    prefix = luigi.Parameter(default="")

    def complete(self):
        return False

    def output(self):
        safe_prefix = self.prefix.replace("/", "_") or "root"
        return luigi.LocalTarget(
            os.path.join(settings.manifests_dir, f"{safe_prefix}_manifest.txt")
        )

    def run(self):
        extractor = MinIOExtractor(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
            bucket_name=settings.minio_bucket,
        )
        objects = [
            (name, etag)
            for name, etag in extractor.list_objects_with_etag(prefix=self.prefix)
            if name.endswith(".csv")
        ]
        objects.sort(key=lambda x: x[0])

        os.makedirs(settings.manifests_dir, exist_ok=True)
        with self.output().open("w") as f:
            for name, etag in objects:
                f.write(f"{name}\t{etag}\n")

        logger.info(
            f"Discovered {len(objects)} CSV files under prefix '{self.prefix}'")


def _file_content_hash(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


class DiscoverLocalFiles(luigi.Task):
    def output(self):
        os.makedirs(settings.raw_data_dir, exist_ok=True)
        entries = []
        for f in sorted(os.listdir(settings.raw_data_dir)):
            if not f.endswith(".csv"):
                continue
            path = os.path.join(settings.raw_data_dir, f)
            if os.path.isfile(path):
                entries.append(f"{f}\t{_file_content_hash(path)}")
        content = "\n".join(entries)
        h = hashlib.sha256(content.encode()).hexdigest()[:12]
        return luigi.LocalTarget(
            os.path.join(settings.manifests_dir, f"local_{h}.txt")
        )

    def run(self):
        os.makedirs(settings.manifests_dir, exist_ok=True)
        entries = []
        for f in sorted(os.listdir(settings.raw_data_dir)):
            if not f.endswith(".csv"):
                continue
            path = os.path.join(settings.raw_data_dir, f)
            if os.path.isfile(path):
                entries.append(f"{f}\t{_file_content_hash(path)}")
        with self.output().open("w") as out:
            out.write("\n".join(entries) + "\n")
        logger.info(f"Discovered {len(entries)} local CSV files")


class ExtractFromMinIO(luigi.Task):
    """Luigi wrapper to pull a CSV from MinIO to local raw storage."""

    filename = luigi.Parameter()
    prefix = luigi.Parameter(default="")

    def output(self):
        local_path = os.path.join(settings.raw_data_dir, self.filename)
        return luigi.LocalTarget(local_path)

    def run(self):
        object_name = os.path.join(
            self.prefix, self.filename) if self.prefix else self.filename
        os.makedirs(settings.raw_data_dir, exist_ok=True)

        extractor = MinIOExtractor(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
            bucket_name=settings.minio_bucket,
        )
        extractor.download_to_file(object_name, self.output().path)
        logger.info(f"Extracted {object_name} → {self.output().path}")


class ExtractFromLocal(luigi.Task):

    filename = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(os.path.join(settings.raw_data_dir, self.filename))

    def run(self):
        source_path = self.output().path
        if os.path.exists(source_path):
            logger.info(f"Found local file for bronze: {source_path}")
            return

        candidate_paths = [
            os.path.join("data", os.path.basename(self.filename)),
            os.path.join("data", "seed", os.path.basename(self.filename)),
        ]
        for candidate in candidate_paths:
            if os.path.exists(candidate):
                os.makedirs(settings.raw_data_dir, exist_ok=True)
                shutil.copy2(candidate, source_path)
                logger.info(
                    f"Copied local seed file {candidate} → {source_path}")
                return

        raise FileNotFoundError(
            f"Local file not found for bronze extract: {source_path}. "
            "Place the CSV in data/raw or provide minio source."
        )
