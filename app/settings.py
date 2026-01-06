from typing import Optional

from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    API_BASE_URL: AnyHttpUrl | str

    AUTH_TYPE: str = "none"  # none|basic|bearer
    AUTH_USERNAME: Optional[str] = None
    AUTH_PASSWORD: Optional[str] = None
    AUTH_BEARER_TOKEN: Optional[str] = None

    DATABASE_URL: str

    GROUP_KEY_NAME: str = "label_uid"
    TIMEZONE: str = "Europe/Berlin"

    HOST: str = "0.0.0.0"
    PORT: int = 8000

    SAVE_CONCURRENCY: int = 5
    QUEUE_MAXSIZE: int = 1000

    DEFAULT_WINDOW_SECONDS: int = 300
    DEFAULT_POLL_SECONDS: int = 30

    # <<< FIX: analysis_loop erwartet das
    ANALYSIS_INTERVAL_SEC: float = 300.0
    ANALYSIS_WORKERS: int = 1

    class Config:
        env_file = ".env.example"


settings = Settings()
