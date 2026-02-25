"""API server configuration (host, port, debug, env). Loaded from environment with API_ prefix and optional .env file."""

from pydantic_settings import BaseSettings


class APISettings(BaseSettings):
    """Settings for the FastAPI server. Use env_prefix API_ and .env for overrides."""

    api_host: str = "0.0.0.0"
    api_port: int = 5173
    debug: bool = False
    env_name: str = "development"
    reload: bool = True

    class Config:
        env_prefix = "API_"
        env_file = ".env"
        extra = "ignore"


api_settings = APISettings()
