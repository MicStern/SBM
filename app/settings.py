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

    # Gruppierung: final = label_uid
    GROUP_KEY_NAME: str = "label_uid"

    # Zeitzone (fix für “1h zu früh wartet”)
    TIMEZONE: str = "Europe/Berlin"

    HOST: str = "0.0.0.0"
    PORT: int = 8000

    SAVE_CONCURRENCY: int = 5
    QUEUE_MAXSIZE: int = 1000

    DEFAULT_WINDOW_SECONDS: int = 300
    DEFAULT_POLL_SECONDS: int = 30

    class Config:
        env_file = ".env.example"


settings = Settings()
