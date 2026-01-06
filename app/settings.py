from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # API
    API_BASE_URL: str
    API_POLL_INTERVAL_SEC: float = 30.0
    API_WINDOW_SEC: int = 300  # 5 Minuten Fenster pro Fetch

    # Auth
    AUTH_TYPE: str = "none"  # none|basic|bearer
    AUTH_USERNAME: Optional[str] = None
    AUTH_PASSWORD: Optional[str] = None
    AUTH_BEARER_TOKEN: Optional[str] = None

    # DB
    DATABASE_URL: str

    # Server
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    # Workers
    SAVE_CONCURRENCY: int = 5
    QUEUE_MAXSIZE: int = 1000

    # Analyse (damit analysis_loop nicht crasht)
    ANALYSIS_INTERVAL_SEC: float = 300.0

    class Config:
        env_file = ".env.example"
        extra = "ignore"


settings = Settings()
