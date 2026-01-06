from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Lädt ENV Variablen aus Docker/OS und optional aus .env (falls vorhanden).
    """

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # --- Fetch/Runtime ---
    QUEUE_MAXSIZE: int = 1000
    SAVE_CONCURRENCY: int = 2

    # --- Auth für Theta ---
    AUTH_TYPE: str = "none"  # "basic" | "none"
    AUTH_USERNAME: str | None = None
    AUTH_PASSWORD: str | None = None

    # --- Theta Endpoint ---
    THETA_BASE_URL: str = "http://theta:8000"  # ggf. anpassen
    THETA_TIMEOUT_SEC: int = 30


settings = Settings()
