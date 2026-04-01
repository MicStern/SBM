import os
from sqlalchemy import create_engine


def get_engine():
    user = os.getenv("POSTGRES_USER", "app")
    password = os.getenv("POSTGRES_PASSWORD", "app")
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "appdb")

    url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)
