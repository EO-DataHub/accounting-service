"""Tests for the pricing policy tables.

These need a database because every one of them is about something only PostgreSQL can
answer: whether a constraint refuses a write, and whether a relationship loads. What the
models *declare* is asserted without a database in tests/test_schema.py.

Nothing reads these tables yet. T4 loads policies, T5 resolves one for a usage time and T7
prices from them, so these tests describe the shape those tasks will rely on.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from accounting_service.models import (
    BillingItem,
    PricingPolicy,
    PricingPolicyCategoryMultiplier,
    PricingPolicyRate,
)

JANUARY = datetime(2025, 1, 1, tzinfo=UTC)


def a_policy(
    *,
    version: int = 1,
    valid_from: datetime = JANUARY,
    valid_until: datetime | None = None,
    corrects_id: UUID | None = None,
    credit_to_currency_rate: Decimal = Decimal(100),
    default_category: str = "standard",
    reason: str | None = None,
) -> PricingPolicy:
    """Spelled out rather than taking **overrides, so the arguments keep their types.

    `uuid` and `configured_at` are left off deliberately: both have defaults, and passing
    configured_at=None explicitly would write a null into a NOT NULL column instead of
    letting func.now() apply.
    """
    return PricingPolicy(
        version=version,
        valid_from=valid_from,
        valid_until=valid_until,
        corrects_id=corrects_id,
        credit_to_currency_rate=credit_to_currency_rate,
        default_category=default_category,
        reason=reason,
    )


def a_rate(item_id: UUID, credits_per_unit: str) -> PricingPolicyRate:
    """A rate to be attached through `policy.rates`, which is how T4 will build one.

    The suppression is the SQLModel friction described in the header of models.py: the
    generated __init__ knows nothing about relationships, so it reports `policy_id` as
    missing even though SQLAlchemy fills it in on flush. Wrapped here so the tests below
    carry one suppression between them instead of one each.
    """
    return PricingPolicyRate(  # pyright: ignore[reportCallIssue]
        item_id=item_id,
        credits_per_unit=Decimal(credits_per_unit),
    )


def a_multiplier(category: str, multiplier: str) -> PricingPolicyCategoryMultiplier:
    return PricingPolicyCategoryMultiplier(  # pyright: ignore[reportCallIssue]
        category=category,
        multiplier=Decimal(multiplier),
    )


@pytest.fixture
def item(db_session: Session) -> BillingItem:
    billing_item = BillingItem(sku="cpu-seconds", name="CPU time", unit="s")
    db_session.add(billing_item)
    db_session.flush()

    return billing_item


class TestAPolicyRoundTrip:
    def test_a_policy_with_its_rates_and_multipliers_reads_back(self, db_session: Session, item: BillingItem) -> None:
        """The shape T7 prices from: a policy, one rate per SKU, one multiplier per category."""
        policy = a_policy()
        policy.rates = [a_rate(item.uuid, "0.25")]
        policy.category_multipliers = [
            a_multiplier("standard", "1"),
            a_multiplier("academic", "0.5"),
        ]
        db_session.add(policy)
        db_session.commit()
        db_session.expire_all()

        stored = db_session.execute(select(PricingPolicy).where(PricingPolicy.version == 1)).scalars().one()

        assert stored.rates[0].credits_per_unit == Decimal("0.25")
        assert {m.category: m.multiplier for m in stored.category_multipliers} == {
            "standard": Decimal(1),
            "academic": Decimal("0.5"),
        }

    def test_configured_at_defaults_to_now(self, db_session: Session) -> None:
        """The decision time, as distinct from the validity time. Set by the database, so a
        replica with a skewed clock cannot claim an earlier decision than it made."""
        policy = a_policy()
        db_session.add(policy)
        db_session.commit()
        db_session.refresh(policy)

        assert policy.configured_at is not None

    def test_the_current_policy_has_no_end(self, db_session: Session) -> None:
        policy = a_policy()
        db_session.add(policy)
        db_session.commit()

        assert policy.valid_until is None

    def test_a_correcting_policy_points_at_the_one_it_corrects(self, db_session: Session) -> None:
        """D8. The correction is a new row, and the original is left exactly as it was."""
        original = a_policy(version=1)
        db_session.add(original)
        db_session.flush()

        correction = a_policy(version=2, corrects_id=original.uuid)
        db_session.add(correction)
        db_session.commit()

        assert correction.corrects_id == original.uuid
        assert original.corrects_id is None

    def test_the_timestamps_come_back_aware(self, db_session: Session) -> None:
        """Naive columns are the defect revision 9b4e2c81a7d3 exists to repair, and a naive
        value read back here would misprice by the connection's offset."""
        policy = a_policy(valid_until=JANUARY + timedelta(days=365))
        db_session.add(policy)
        db_session.commit()
        db_session.expire_all()

        stored = db_session.execute(select(PricingPolicy)).scalars().one()

        assert stored.valid_from.tzinfo is not None
        assert stored.valid_until is not None
        assert stored.valid_until.tzinfo is not None
        assert stored.configured_at.tzinfo is not None


class TestTheDatabaseRefusesAMalformedPolicy:
    def test_two_policies_cannot_share_a_version(self, db_session: Session) -> None:
        """The loader runs on every ingester pod start, so two replicas can try to mint the
        same version at once. T4 has to handle the conflict rather than failing the pod."""
        db_session.add(a_policy(version=7))
        db_session.flush()
        db_session.add(a_policy(version=7))

        with pytest.raises(IntegrityError):
            db_session.flush()

    def test_a_policy_cannot_price_the_same_sku_twice(self, db_session: Session, item: BillingItem) -> None:
        policy = a_policy()
        policy.rates = [
            a_rate(item.uuid, "1"),
            a_rate(item.uuid, "2"),
        ]
        db_session.add(policy)

        with pytest.raises(IntegrityError):
            db_session.flush()

    def test_a_policy_cannot_multiply_the_same_category_twice(self, db_session: Session) -> None:
        policy = a_policy()
        policy.category_multipliers = [
            a_multiplier("academic", "0.5"),
            a_multiplier("academic", "0.9"),
        ]
        db_session.add(policy)

        with pytest.raises(IntegrityError):
            db_session.flush()

    def test_validity_cannot_end_before_it_starts(self, db_session: Session) -> None:
        """The `validity_order` check constraint, matching the one on billing_item_price."""
        db_session.add(a_policy(valid_until=JANUARY - timedelta(days=1)))

        with pytest.raises(IntegrityError):
            db_session.flush()

    def test_two_policies_may_share_a_sku(self, db_session: Session, item: BillingItem) -> None:
        """The constraint is per policy. Every policy prices every SKU, so the same SKU
        appearing in the next calibration pass is the normal case rather than a clash."""
        for version in (1, 2):
            policy = a_policy(version=version)
            policy.rates = [a_rate(item.uuid, str(version))]
            db_session.add(policy)

        db_session.commit()

        assert len(db_session.execute(select(PricingPolicyRate)).scalars().all()) == 2

    def test_a_rate_must_name_a_real_item(self, db_session: Session) -> None:
        policy = a_policy()
        policy.rates = [a_rate(uuid4(), "1")]
        db_session.add(policy)

        with pytest.raises(IntegrityError):
            db_session.flush()
