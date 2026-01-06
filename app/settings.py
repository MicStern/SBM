# app/settings.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- DB ---
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@db:5432/postgres"

    # --- Queue/Workers ---
    QUEUE_MAXSIZE: int = 2000
    SAVE_CONCURRENCY: int = 2

    # --- Fetch defaults ---
    DEFAULT_WINDOW_SEC: int = 300
    DEFAULT_POLL_SEC: int = 2

    # Minimaler Overlap, damit du keine Boundary-Daten verlierst (z.B. wenn end_date exklusiv ist)
    FETCH_OVERLAP_SEC: int = 1

    # --- Theta API ---
    THETA_BASE_URL: str = "http://theta:8000"
    THETA_PROBE_PATH: str = "/basic-api/probes/hsl"

    # WICHTIG: Theta erwartet Strings ohne TZ ("YYYY-MM-DD HH:MM:SS").
    # Diese Zeitzone ist die Interpretation, die auch Postman "gefühlt" nutzt.
    # Für Deutschland/5micron in Jan ist das sehr wahrscheinlich Europe/Berlin.
    THETA_TIMEZONE: str = "Europe/Berlin"

    # --- Auth ---
    # "none" | "basic" | "bearer"
    AUTH_TYPE: str = "basic"
    AUTH_USERNAME: str | None = None
    AUTH_PASSWORD: str | None = None
    AUTH_BEARER_TOKEN: str | None = None

    # --- HTTP ---
    HTTP_TIMEOUT_SEC: float = 30.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
