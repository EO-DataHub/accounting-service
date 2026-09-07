from collections.abc import Iterator
from functools import cache
from typing import TextIO

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from accounting_service import models
from accounting_service.configuration import load_configuration
from accounting_service.db_settings import get_db_url

# There is deliberately no create_all or drop_all here. Alembic owns the deployed schema, so
# the application neither creates nor drops a table, and a drop_all reachable from application
# code is a hazard with no caller. The tests build their own schema - see tests/conftest.py.


@cache
def get_engine() -> Engine:
    """
    The process-wide engine, created on first use.

    Deliberately not created while this module is imported. An engine at module scope means
    importing anything that reaches models.py needs a resolvable database URL, so a missing
    or stale ./.env fails at import rather than in the code that needs a database. It also
    leaves no seam: nothing can be pointed at a different database without reassigning a
    global, which is what tests ended up doing.

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
    This updates the database of prices and items based on the configuration given.

    The caller supplies the session and owns the transaction, so this does not commit. That
    lets several configuration loads share one transaction, and lets a caller retry when two
    replicas load the same configuration at once.

    The document is validated whole before any of it is applied, so a bad entry raises
    ConfigurationError instead of leaving the earlier entries written. See
    accounting_service.configuration.

    Items are applied before prices, so a document may introduce an item and its first price
    together.

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
    configuration = load_configuration(config)

    for item in configuration.items:
        models.BillingItem.upsert_configured_item(session, item)

    for price in configuration.prices:
        models.BillingItemPrice.upsert_configured_price(session, price)
