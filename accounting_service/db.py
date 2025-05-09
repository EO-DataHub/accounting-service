import logging
from typing import Optional, TextIO

import yaml
from pydantic_settings import BaseSettings
from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session
from yaml.error import YAMLError

from accounting_service import models


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

engine = create_engine(get_db_url(), connect_args=connect_args)


def create_db_and_tables():
    with engine.begin() as conn:
        models.Base.metadata.create_all(conn)


def drop_tables():
    with engine.begin() as conn:
        models.Base.metadata.drop_all(conn)


def get_session():
    with Session(engine) as session:
        yield session


def insert_configuration(config: TextIO):
    """
    This updates the database of prices and items based on the configuration given.

    Example config (YAML format):
    items:
      - sku: "my-sku"
        name: "my product"
        unit: "GB-s"
    prices:
      - sku: "my-sku"
        valid_from: "2025-01-01T00:00:00Z"
        price: 12.34
    """
    try:
        config_obj = yaml.safe_load(config)
        if not isinstance(config_obj, dict):
            raise YAMLError("Expected a YAML dictionary in config file - check the format")
    except YAMLError:
        logging.fatal("accounting-service configuration file is not valid - check the format")
        raise

    with Session(engine) as session:
        for item in config_obj.get("items", []):
            models.BillingItem.upsert_configured_item(session, item)

        for price in config_obj.get("prices", []):
            models.BillingItemPrice.upsert_configured_price(session, price)

        session.commit()
