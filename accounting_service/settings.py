import os
from functools import lru_cache

from pydantic_settings import BaseSettings
from sqlalchemy import URL

app_env = os.getenv("APP_ENV", "")

# `.env` when APP_ENV is unset, `.<APP_ENV>.env` otherwise. The name is built here rather than
# in model_config so the empty case does not produce `..env`.
env_file = f".{app_env}.env" if app_env else ".env"


class Settings(BaseSettings):
    SQL_DRIVER: str = "postgresql+psycopg"
    SQL_PORT: int | None = None
    SQL_PASSWORD: str | None = None
    SQL_USER: str | None = None
    SQL_DATABASE: str = "accounting"
    SQL_HOST: str | None = None
    SQL_SCHEMA: str = "public"

    USAGE_CACHE_TIMEOUT: int = 5
    GLOBAL_CACHE_TIMEOUT: int = 300

    model_config = {"env_file": env_file}


@lru_cache
def get_settings() -> Settings:
    """The process-wide settings, read on first use.

    Call `get_settings.cache_clear()` after changing the environment.
    """
    return Settings()


def get_db_url() -> URL:
    settings = get_settings()

    return URL.create(
        settings.SQL_DRIVER,
        username=settings.SQL_USER,
        password=settings.SQL_PASSWORD,
        host=settings.SQL_HOST,
        port=settings.SQL_PORT,
        database=settings.SQL_DATABASE,
        # timezone is pinned so the offset the driver applies does not vary by server. Values
        # are stored as timestamptz, so the instant is never wrong, only its labelling.
        query={"options": f"-c search_path={settings.SQL_SCHEMA} -c timezone=UTC"},
    )
