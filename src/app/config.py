from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_secure: bool = False
    minio_bucket: str = "hr-insights"

    raw_data_dir: str = "data/raw"
    manifests_dir: str = "logs/manifests"

    class Config:
        env_file = ".env"
        env_prefix = "HR_INSIGHTS_"


settings = Settings()
