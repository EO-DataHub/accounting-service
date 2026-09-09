"""Tests for the pricing policy tables.

These need a database because every one of them is about something only PostgreSQL can
answer: whether a constraint refuses a write, and whether a relationship loads. What the
models *declare* is asserted without a database in tests/test_schema.py.

Nothing reads these tables yet. T4 loads policies, T5 resolves one for a usage time and T7
prices from them, so these tests describe the shape those tasks will rely on.
"""

import io
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlmodel import col

from accounting_service import db, models
from accounting_service.models import (
    BillingItem,
    PricingPolicy,
    PricingPolicyCategoryMultiplier,
    PricingPolicyRate,
)
from accounting_service.pricing import ConfiguredPolicy

JANUARY = datetime(2025, 1, 1, tzinfo=UTC)
SKU = "cpu-seconds"


def a_policy(
    *,
    version: int = 1,
    valid_from: datetime = JANUARY,
    valid_until: datetime | None = None,
    corrects_id: UUID | None = None,
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
    billing_item = BillingItem(sku=SKU, name="CPU time", unit="s")
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

        stored = db_session.execute(select(PricingPolicy).where(col(PricingPolicy.version) == 1)).scalars().one()

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


def a_document(
    *,
    valid_from: str = "2025-01-01T00:00:00Z",
    rate: str = "0.5",
    default_category: str = "standard",
    reason: str = "initial calibration",
    sku: str = SKU,
    include_item: bool = True,
) -> io.StringIO:
    items = f'items:\n  - {{sku: "{sku}", name: "n", unit: "u"}}\n' if include_item else ""

    return io.StringIO(
        f"""---
{items}pricing_policy:
  valid_from: "{valid_from}"
  default_category: {default_category}
  reason: "{reason}"
  rates:
    - sku: {sku}
      credits_per_unit: {rate}
  category_multipliers:
    - category: standard
      multiplier: 1
    - category: academic
      multiplier: 0.5
"""
    )


def stored_versions(session: Session) -> list[int]:
    # col() because `version` is a plain `int` annotation under SQLModel, so selecting the
    # bare attribute looks to a type checker like `select(int)`.
    return sorted(session.execute(select(col(PricingPolicy.version))).scalars().all())


class TestMintOrMatch:
    """The loader runs on every ingester pod start, so not minting is the common case."""

    def test_the_first_load_mints_version_one(self, db_session: Session) -> None:
        db.insert_configuration(db_session, a_document())

        policy = PricingPolicy.current(db_session)
        assert policy is not None
        assert policy.version == 1
        assert policy.rates[0].credits_per_unit == Decimal("0.5")
        assert {m.category for m in policy.category_multipliers} == {"standard", "academic"}

    def test_loading_the_same_document_again_mints_nothing(self, db_session: Session) -> None:
        """Every pod start reloads the file. A restart is not a calibration."""
        db.insert_configuration(db_session, a_document())
        db.insert_configuration(db_session, a_document())

        assert stored_versions(db_session) == [1]

    def test_rewording_the_reason_mints_nothing(self, db_session: Session) -> None:
        db.insert_configuration(db_session, a_document(reason="first pass"))
        db.insert_configuration(db_session, a_document(reason="same numbers, better words"))

        assert stored_versions(db_session) == [1]

    @pytest.mark.parametrize(
        "changed",
        [
            lambda: a_document(rate="0.9"),
            lambda: a_document(default_category="academic"),
            lambda: a_document(valid_from="2025-06-01T00:00:00Z"),
        ],
        ids=["a-rate", "the-default-category", "valid-from"],
    )
    def test_changing_the_numbers_mints_a_version(
        self, db_session: Session, changed: Callable[[], io.StringIO]
    ) -> None:
        """A callable rather than a dict of overrides, so a misspelled keyword is a type
        error here instead of a silently ignored argument."""
        db.insert_configuration(db_session, a_document())
        db.insert_configuration(db_session, changed())

        assert stored_versions(db_session) == [1, 2]

    def test_the_new_version_is_the_one_in_force(self, db_session: Session) -> None:
        db.insert_configuration(db_session, a_document(rate="0.5"))
        db.insert_configuration(db_session, a_document(rate="0.9"))

        policy = PricingPolicy.current(db_session)
        assert policy is not None
        assert policy.version == 2
        assert policy.rates[0].credits_per_unit == Decimal("0.9")

    def test_nothing_is_written_to_the_earlier_policy(self, db_session: Session) -> None:
        """The loader appends. A stored policy keeps its open range, which is what lets a
        correction be backdated without rewriting what it corrects."""
        db.insert_configuration(db_session, a_document())
        db.insert_configuration(db_session, a_document(rate="0.9"))

        first = db_session.execute(select(PricingPolicy).where(col(PricingPolicy.version) == 1)).scalars().one()
        assert first.valid_until is None
        assert first.rates[0].credits_per_unit == Decimal("0.5")

    def test_a_policy_may_rate_an_item_the_same_document_introduces(self, db_session: Session) -> None:
        """Items load before the policy, so the SKU exists by the time the rate needs it."""
        db.insert_configuration(db_session, a_document(sku="brand-new-sku"))

        assert stored_versions(db_session) == [1]

    def test_a_rate_for_an_unknown_sku_is_refused(self, db_session: Session) -> None:
        with pytest.raises(ValueError, match="do not exist"):
            db.insert_configuration(db_session, a_document(sku="never-configured", include_item=False))

    def test_a_document_with_no_policy_leaves_the_policy_alone(self, db_session: Session) -> None:
        db.insert_configuration(db_session, a_document())
        db.insert_configuration(db_session, io.StringIO('items:\n  - {sku: "other", name: "n", unit: "u"}\n'))

        assert stored_versions(db_session) == [1]


class TestWhichPolicyIsInForce:
    def test_the_most_recently_configured_wins_not_the_latest_valid_from(self, db_session: Session) -> None:
        """A correction is configured after the policy it corrects and backdated over it
        (D8). It has to win, so resolution cannot order on valid_from.

        Both are minted in one transaction here, so `configured_at` is identical and the
        tie-break on version descending is what decides it - which is the same situation two
        loads in one pod start produce.
        """
        db.insert_configuration(db_session, a_document(valid_from="2025-06-01T00:00:00Z", rate="0.5"))
        db.insert_configuration(db_session, a_document(valid_from="2025-01-01T00:00:00Z", rate="0.9"))

        policy = PricingPolicy.current(db_session)
        assert policy is not None
        assert policy.version == 2
        assert policy.valid_from == datetime(2025, 1, 1, tzinfo=UTC)

    def test_no_policy_at_all_is_not_an_error(self, db_session: Session) -> None:
        assert PricingPolicy.current(db_session) is None


class TestTwoReplicasRacing:
    """Both pods start together, both see the same current policy, both mint the same
    version. The unique constraint refuses the second and the loader recovers.

    `_next_version` is patched to return a version already taken, because a single
    connection cannot otherwise be made to lose the race.
    """

    def test_the_loser_retries_and_mints(self, db_session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        db.insert_configuration(db_session, a_document(rate="0.5"))

        versions = iter([1, 2])
        monkeypatch.setattr(models, "_next_version", lambda _session: next(versions))

        db.insert_configuration(db_session, a_document(rate="0.9"))

        assert stored_versions(db_session) == [1, 2]

    def test_the_loser_stops_when_the_winner_minted_the_same_policy(
        self, db_session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The document is already in force by the time the loser looks again, so there is
        nothing left to mint and the pod carries on."""
        db.insert_configuration(db_session, a_document(rate="0.5"))
        monkeypatch.setattr(models, "_next_version", lambda _session: 1)

        loaded = PricingPolicy.load_configured_policy(
            db_session,
            ConfiguredPolicy.model_validate(
                {
                    "valid_from": "2025-01-01T00:00:00Z",
                    "default_category": "standard",
                    "rates": [{"sku": SKU, "credits_per_unit": "0.5"}],
                    "category_multipliers": [
                        {"category": "standard", "multiplier": "1"},
                        {"category": "academic", "multiplier": "0.5"},
                    ],
                }
            ),
        )

        assert loaded is None
        assert stored_versions(db_session) == [1]

    def test_it_gives_up_rather_than_looping(self, db_session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
        """A version that is always taken, and a document that never matches, has to end in
        a raise rather than a spin."""
        db.insert_configuration(db_session, a_document(rate="0.5"))
        monkeypatch.setattr(models, "_next_version", lambda _session: 1)

        with pytest.raises(IntegrityError):
            db.insert_configuration(db_session, a_document(rate="0.9"))
