from collections.abc import Iterator
from functools import cache
from typing import TextIO

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from accounting_service import models
from accounting_service.configuration import load_configuration
from accounting_service.settings import get_db_url


@cache
def get_engine() -> Engine:
    """The process-wide engine, created on first use.

    Cached because connection pooling wants one engine per process. Call
    `get_engine.cache_clear()` after changing the configuration.
    """
    return create_engine(get_db_url())


@cache
def get_sessionmaker() -> sessionmaker[Session]:
    """The process-wide session factory. Pass this where a component needs to open sessions."""
    return sessionmaker(bind=get_engine())


def get_session() -> Iterator[Session]:
    with get_sessionmaker()() as session:
        yield session


def insert_configuration(session: Session, config: TextIO) -> None:
    """
    Apply a configuration document: the billing items it defines, then its pricing policy.

    The caller owns the transaction, so this does not commit. Raises ConfigurationError for a
    bad document, before anything is applied.

    Items are applied first, so a document may introduce an item and rate it in the same pass.
    The policy is mint-or-match: a document whose numbers are already in force writes nothing.

    Example config (YAML format):
    items:
      - sku: "my-sku"
        name: "my product"
        unit: "GB-s"
    pricing_policy:
      valid_from: "2025-01-01T00:00:00Z"
      default_category: standard
      rates:
        - sku: "my-sku"
          credits_per_unit: 12.34
      category_multipliers:
        - category: standard
          multiplier: 1
    """
    configuration = load_configuration(config)

    for item in configuration.items:
        models.BillingItem.upsert_configured_item(session, item)

    # After the items, so a policy may rate an item the same document introduces.
    if configuration.pricing_policy is not None:
        models.PricingPolicy.load_configured_policy(session, configuration.pricing_policy)
