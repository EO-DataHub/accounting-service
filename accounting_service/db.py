import logging
from typing import TextIO

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from yaml.error import YAMLError

from accounting_service import models
from accounting_service.db_settings import connect_args, get_db_url

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
