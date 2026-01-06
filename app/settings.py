# app/settings.py
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # --- DB ---
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@db:5432/postgres"

    # --- Queue/Workers ---
    QUEUE_MAXSIZE: int = 2000
    SAVE_CONCURRENCY: int = 2

    # --- Fetch defaults (für UI & Start ohne Auswahl) ---
    DEFAULT_WINDOW_SEC: int = 300
    DEFAULT_POLL_SEC: int = 30

    # --- Theta endpoint ---
    # Base = Host (ohne Pfad)
    THETA_BASE_URL: str = "https://theta-v2-server.5micron.net"
    # Pfad = API path
    THETA_PROBE_PATH: str = "/basic-api/probes/hsl"

    # Zeitformat der Query-Params (der Server-Beispiel-URL nutzt lokale Zeitstrings ohne TZ)
    # -> Default Berlin
    THETA_TIMEZONE: str = "Europe/Berlin"

    # --- Auth ---
    # AUTH_TYPE: "none" | "basic" | "bearer"
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
