from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # DB
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@db:5432/postgres"

    # Queue + saving
    QUEUE_MAXSIZE: int = 5000
    SAVE_CONCURRENCY: int = 2

    # Fetch API
    THETA_BASE_URL: str = "https://theta-v2-server.5micron.net/basic-api/probes/hsl"

    # Default fetch parameters (können über UI überschrieben werden)
    DEFAULT_WINDOW_SEC: int = 300
    DEFAULT_POLL_SEC: int = 30

    # Optional: falls du ein extra analysis loop hast
    ANALYSIS_INTERVAL_SEC: int = 5


settings = Settings()
