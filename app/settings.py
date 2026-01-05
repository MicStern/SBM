from typing import Optional

from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    API_BASE_URL: AnyHttpUrl | str
    API_POLL_INTERVAL_SEC: float = 10.0  # legacy (nicht mehr zentral genutzt, ok)

    AUTH_TYPE: str = "none"  # none|basic|bearer
    AUTH_USERNAME: Optional[str] = None
    AUTH_PASSWORD: Optional[str] = None
    AUTH_BEARER_TOKEN: Optional[str] = None

    DATABASE_URL: str

    # NEU: packet id key aus neuer JSON Struktur
    GROUP_KEY_NAME: str = "label_uid"

    HOST: str = "0.0.0.0"
    PORT: int = 8000

    FETCH_CONCURRENCY: int = 1
    SAVE_CONCURRENCY: int = 5
    QUEUE_MAXSIZE: int = 1000

    ANALYSIS_INTERVAL_SEC: float = 300.0
    ANALYSIS_WORKERS: int = 1

    # Defaults für FetchConfig (UI kann überschreiben)
    DEFAULT_WINDOW_SECONDS: int = 300
    DEFAULT_POLL_SECONDS: int = 30

    class Config:
        env_file = ".env.example"  # <-- wie du gesagt hast


settings = Settings()
