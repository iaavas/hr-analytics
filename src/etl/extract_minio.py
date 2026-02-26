import os
import io
import logging
from typing import List, Optional, Tuple
from datetime import datetime

import pandas as pd
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


class MinIOExtractor:
    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        secure: bool = False,
        bucket_name: str = "hr-insights",
    ):
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )
        self.bucket_name = bucket_name
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            else:
                logger.info(f"Bucket already exists: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error ensuring bucket exists: {e}")
            raise

    def upload_file(self, file_path: str, object_name: Optional[str] = None):
        if object_name is None:
            object_name = os.path.basename(file_path)

        try:
            self.client.fput_object(
                self.bucket_name,
                object_name,
                file_path,
            )
            logger.info(
                f"Uploaded {file_path} to {self.bucket_name}/{object_name}")
        except S3Error as e:
            logger.error(f"Error uploading file: {e}")
            raise

    def upload_directory(self, directory: str, prefix: str = ""):
        uploaded = []
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".csv"):
                    file_path = os.path.join(root, file)
                    object_name = os.path.join(
                        prefix, os.path.basename(file_path))
                    self.upload_file(file_path, object_name)
                    uploaded.append(object_name)
        return uploaded

    def list_objects(self, prefix: str = "") -> List[str]:
        return [name for name, _ in self.list_objects_with_etag(prefix)]

    def list_objects_with_etag(self, prefix: str = "") -> List[Tuple[str, str]]:
        try:
            objects = self.client.list_objects(
                self.bucket_name,
                prefix=prefix,
                recursive=True,
            )
            return [(obj.object_name, getattr(obj, "etag", "") or "") for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects: {e}")
            raise

    def download_object(self, object_name: str) -> pd.DataFrame:
        response = None
        try:
            response = self.client.get_object(self.bucket_name, object_name)
            data = response.read()
            df = pd.read_csv(io.BytesIO(data), on_bad_lines="skip")
            logger.info(f"Downloaded and parsed {object_name}")
            return df
        except S3Error as e:
            logger.error(f"Error downloading object {object_name}: {e}")
            raise
        finally:
            if response:
                response.close()
                response.release_conn()

    def download_all_csv(self, prefix: str = "") -> dict:
        objects = self.list_objects(prefix)
        csv_files = [obj for obj in objects if obj.endswith(".csv")]

        dataframes = {}
        for obj in csv_files:
            try:
                df = self.download_object(obj)
                dataframes[obj] = df
            except Exception as e:
                logger.warning(f"Skipping {obj}: {e}")

        return dataframes

    def download_to_file(self, object_name: str, file_path: str):
        try:
            self.client.fget_object(
                self.bucket_name,
                object_name,
                file_path,
            )
            logger.info(f"Downloaded {object_name} to {file_path}")
        except S3Error as e:
            logger.error(f"Error downloading object: {e}")
            raise


if __name__ == "__main__":
    extractor = MinIOExtractor()
    print(extractor.list_objects())
