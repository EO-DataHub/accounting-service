import io
from datetime import datetime
from decimal import Decimal

from faker import Faker
from pytest import Session
from sqlalchemy import select

import accounting_service.db
from accounting_service.models import BillingItem, BillingItemPrice


def test_model_creation():
    accounting_service.db.create_db_and_tables()


def test_item_and_price_creation_via_config_file_results_in_correct_object_in_db(
    db_session: Session,
):
    faker = Faker()
    test_sku = faker.name()

    test_config = f"""---
items:
  - sku: "{test_sku}"
    name: "my product"
    unit: "GB-s"
prices:
  - sku: "{test_sku}"
    valid_from: "2025-01-01T00:00:00Z"
    price: 12.34
"""

    accounting_service.db.insert_configuration(io.StringIO(test_config))

    bi = BillingItem.find_billing_item(db_session, test_sku)
    assert bi.name == "my product"
    assert bi.unit == "GB-s"
    assert bi.sku == test_sku

    prices = (
        db_session.execute(select(BillingItemPrice).where(BillingItemPrice.item == bi))
        .scalars()
        .all()
    )
    prices = list(prices)

    assert len(prices) == 1

    price = prices[0]
    assert price.price == Decimal("12.34")
    assert price.valid_from == datetime(2025, 1, 1, 0, 0, 0)
    assert price.valid_until is None
    assert price.item_id == bi.uuid


def test_item_and_price_update_via_config_file_results_in_correct_object_in_db(db_session: Session):
    faker = Faker()
    test_sku = faker.name()

    test_config = f"""---
items:
  - sku: "{test_sku}"
    name: "my product"
    unit: "GB-s"
prices:
  - sku: "{test_sku}"
    valid_from: "2025-01-01T00:00:00Z"
    price: 12.34
"""

    test_config_update = f"""---
items:
  - sku: "{test_sku}"
    name: "my product 2"
    unit: "GB-s 2"
prices:
  - sku: "{test_sku}"
    valid_from: "2025-01-01T00:00:00Z"
    price: 12.35
  - sku: "{test_sku}"
    valid_from: "2025-01-02T00:00:00Z"
    price: 11.0
"""

    accounting_service.db.insert_configuration(io.StringIO(test_config))
    accounting_service.db.insert_configuration(io.StringIO(test_config_update))

    bi = BillingItem.find_billing_item(db_session, test_sku)
    assert bi.name == "my product 2"
    assert bi.unit == "GB-s 2"
    assert bi.sku == test_sku

    prices = (
        db_session.execute(select(BillingItemPrice).where(BillingItemPrice.item == bi))
        .scalars()
        .all()
    )
    prices = list(prices)

    assert len(prices) == 2

    price = prices[0]
    assert price.price == Decimal("12.35")
    assert price.valid_from == datetime(2025, 1, 1, 0, 0, 0)
    assert price.valid_until == datetime(2025, 1, 2, 0, 0, 0)
    assert price.item_id == bi.uuid

    price = prices[1]
    assert price.price == Decimal("11.0")
    assert price.valid_from == datetime(2025, 1, 2, 0, 0, 0)
    assert price.valid_until is None
    assert price.item_id == bi.uuid
