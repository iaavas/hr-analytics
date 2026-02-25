from pydantic_settings import BaseSettings


class APISettings(BaseSettings):

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
