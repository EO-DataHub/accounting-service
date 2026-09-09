"""End-to-end config loading: a document goes in, rows come out.

The document models and the mint-or-match rule are tested without a database in
tests/test_configuration.py and tests/test_pricing.py. What needs one is the whole path -
`insert_configuration` parsing a document, applying the items, then handing the policy to
the loader - because the ordering between those steps is what lets a policy rate an item the
same document introduces.
"""

import io
from datetime import UTC, datetime
from decimal import Decimal

from faker import Faker
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlmodel import col

import accounting_service.db
from accounting_service.models import BillingItem, PricingPolicy


def a_document(sku: str, *, rate: str, valid_from: str = "2025-01-01T00:00:00Z") -> io.StringIO:
    return io.StringIO(
        f"""---
items:
  - sku: "{sku}"
    name: "my product"
    unit: "GB-s"
pricing_policy:
  valid_from: "{valid_from}"
  default_category: standard
  rates:
    - sku: "{sku}"
      credits_per_unit: {rate}
  category_multipliers:
    - category: standard
      multiplier: 1
"""
    )


def test_a_document_creates_the_item_and_rates_it(db_session: Session) -> None:
    test_sku = Faker().name()

    accounting_service.db.insert_configuration(db_session, a_document(test_sku, rate="12.34"))

    item = BillingItem.find_billing_item(db_session, test_sku)
    assert item is not None
    assert item.name == "my product"
    assert item.unit == "GB-s"

    policy = PricingPolicy.resolve(db_session, datetime(2025, 6, 1, tzinfo=UTC))
    assert policy is not None
    assert policy.version == 1
    assert [(rate.item.sku, rate.credits_per_unit) for rate in policy.rates] == [(test_sku, Decimal("12.34"))]


def test_reloading_a_changed_document_updates_the_item_and_mints_a_policy(db_session: Session) -> None:
    """The item is updated in place; the policy is appended. A calibration is a new version,
    and the one it replaces stays exactly as it was."""
    test_sku = Faker().name()

    accounting_service.db.insert_configuration(db_session, a_document(test_sku, rate="12.34"))

    changed = a_document(test_sku, rate="11.00").getvalue().replace("my product", "my product 2")
    accounting_service.db.insert_configuration(db_session, io.StringIO(changed))

    item = BillingItem.find_billing_item(db_session, test_sku)
    assert item is not None
    assert item.name == "my product 2"

    versions = sorted(db_session.execute(select(col(PricingPolicy.version))).scalars().all())
    assert versions == [1, 2]

    in_force = PricingPolicy.resolve(db_session, datetime(2025, 6, 1, tzinfo=UTC))
    assert in_force is not None
    assert in_force.rates[0].credits_per_unit == Decimal("11.00")
