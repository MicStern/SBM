from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Settings werden aus ENV geladen (Docker) und optional aus .env.
    main.py/db.py erwarten einige Felder (z.B. DATABASE_URL, DEFAULT_WINDOW_SEC).
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # --------------------
    # Database
    # --------------------
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@db:5432/postgres"

    # --------------------
    # Runtime / Queue
    # --------------------
    QUEUE_MAXSIZE: int = 1000
    SAVE_CONCURRENCY: int = 2

    # --------------------
    # Fetch defaults (wird von main.py genutzt!)
    # --------------------
    DEFAULT_WINDOW_SEC: int = 1800
    DEFAULT_POLL_SEC: int = 30

    # Optional: wenn du Limits im UI willst
    MIN_WINDOW_SEC: int = 30
    MAX_WINDOW_SEC: int = 7200
    MIN_POLL_SEC: int = 5
    MAX_POLL_SEC: int = 300

    # --------------------
    # Theta Endpoint
    # --------------------
    THETA_BASE_URL: str = "http://theta:8000"
    THETA_TIMEOUT_SEC: int = 30

    # --------------------
    # Theta Auth
    # --------------------
    AUTH_TYPE: str = "none"  # "basic" | "none"
    AUTH_USERNAME: str | None = None
    AUTH_PASSWORD: str | None = None


settings = Settings()
