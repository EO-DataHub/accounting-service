from functools import lru_cache

from pydantic_settings import BaseSettings
from sqlalchemy import URL


class Settings(BaseSettings):
    SQL_DRIVER: str = "postgresql+psycopg"
    SQL_PORT: int | None = None
    SQL_PASSWORD: str | None = None
    SQL_USER: str | None = None
    SQL_DATABASE: str = "accounting"
    SQL_HOST: str | None = None
    SQL_SCHEMA: str = "public"

    model_config = {"env_file": "./.env"}


@lru_cache
def get_settings() -> Settings:
    """
    The process-wide settings, read on first use.

    Deliberately a cached function rather than a module-level instance. A module-level
    instance is read while this module is being imported, which means importing anything
    that reaches it needs a resolvable configuration, and nothing can point the process
    somewhere else without reassigning a global. This is the pattern FastAPI documents for
    exactly that reason - see the "Settings and Environment Variables" page.

    Call `get_settings.cache_clear()` after changing the environment.
    """
    return Settings()


def get_db_url() -> URL:
    """
    The database URL.

    PostgreSQL only. SQLite used to be supported for the unit tests, which cost five
    dialect branches in models.py and meant the tests exercised different SQL from
    production. The tests now run against a throwaway PostgreSQL container instead, so
    nothing needs the SQLite path and leaving it in would only offer a configuration the
    models cannot run on - the expression indexes on billing_event have no SQLite form.
    """
    settings = get_settings()

    return URL.create(
        settings.SQL_DRIVER,
        username=settings.SQL_USER,
        password=settings.SQL_PASSWORD,
        host=settings.SQL_HOST,
        port=settings.SQL_PORT,
        database=settings.SQL_DATABASE,
        query={"options": f"-c search_path={settings.SQL_SCHEMA}"},
    )
