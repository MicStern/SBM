from pydantic import BaseSettings, AnyHttpUrl
from typing import Optional

class Settings(BaseSettings):
    API_BASE_URL: AnyHttpUrl | str
    API_POLL_INTERVAL_SEC: float = 10.0

    AUTH_TYPE: str = "none"     # none|basic|bearer
    AUTH_USERNAME: Optional[str] = None
    AUTH_PASSWORD: Optional[str] = None
    AUTH_BEARER_TOKEN: Optional[str] = None

    DATABASE_URL: str
    GROUP_KEY_NAME: str = "labelUUID"

    HOST: str = "0.0.0.0"
    PORT: int = 8000

    FETCH_CONCURRENCY: int = 1
    SAVE_CONCURRENCY: int = 5
    QUEUE_MAXSIZE: int = 1000

    ANALYSIS_INTERVAL_SEC: float = 300.0
    ANALYSIS_WORKERS: int = 1

    class Config:
        env_file = ".env"

settings = Settings()
