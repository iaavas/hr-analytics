import argparse
import os
import sys

from src.etl.extract_minio import MinIOExtractor
from src.app.config import settings


def upload_path(path: str, prefix: str = "", object_name: str | None = None) -> list[str]:
    extractor = MinIOExtractor(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
        bucket_name=settings.minio_bucket,
    )

    if os.path.isdir(path):
        return extractor.upload_directory(path, prefix=prefix)

    if object_name is None:
        object_name = os.path.join(prefix, os.path.basename(
            path)) if prefix else os.path.basename(path)

    extractor.upload_file(path, object_name=object_name)
    return [object_name]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload CSVs from local disk to the configured MinIO bucket."
    )
    parser.add_argument(
        "path",
        help="File or directory to upload. Directories upload all *.csv files recursively.",
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Optional object prefix (folder) inside the bucket.",
    )
    parser.add_argument(
        "--object-name",
        default=None,
        help="Override object name when uploading a single file.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    uploaded = upload_path(args.path, prefix=args.prefix,
                           object_name=args.object_name)
    if uploaded:
        print(f"Uploaded {len(uploaded)} object(s):")
        for obj in uploaded:
            print(f" - {obj}")
    else:
        print("No files were uploaded.")


if __name__ == "__main__":
    sys.exit(main())
