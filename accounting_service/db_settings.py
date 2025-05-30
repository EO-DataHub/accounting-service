from typing import Optional

from pydantic_settings import BaseSettings
from sqlalchemy import URL


class Settings(BaseSettings):
    SQL_DRIVER: str = "sqlite+pysqlite"
    SQL_PORT: Optional[int] = None
    SQL_PASSWORD: Optional[str] = None
    SQL_USER: Optional[str] = None
    SQL_DATABASE: str = "accounting"
    SQL_HOST: Optional[str] = None
    SQL_SCHEMA: str = "public"

    class Config:
        env_file = "./.env"


settings = Settings()


def get_db_url() -> URL:
    return URL.create(
        settings.SQL_DRIVER,
        username=settings.SQL_USER,
        password=settings.SQL_PASSWORD,
        host=settings.SQL_HOST,
        port=settings.SQL_PORT,
        database=settings.SQL_DATABASE,
        query={"options": f"-c search_path={settings.SQL_SCHEMA}"},
    )


if settings.SQL_DRIVER.startswith("sqlite"):
    connect_args = {"check_same_thread": False}
else:
    connect_args = {}


def is_sqlite():
    return settings.SQL_DRIVER.startswith("sqlite")
