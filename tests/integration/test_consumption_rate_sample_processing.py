from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from eodhp_utils.pulsar import messages
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

from accounting_service import models
from accounting_service.ingester.messager import ConsumptionSampleRateIngesterMessager
from accounting_service.models import BillingItem, PricingPolicy, PricingPolicyCategoryMultiplier, PricingPolicyRate
from accounting_service.pricing import exact_decimal
from tests.integration.conftest import msg_to_pulsar_msg

RATE = Decimal("0.5")


@pytest.fixture
def item(db_session: Session) -> BillingItem:
    billing_item = BillingItem(sku="test-consumption-sku", name="test", unit="GB-h")
    db_session.add(billing_item)
    db_session.flush()

    return billing_item


@pytest.fixture
def policy(db_session: Session, item: BillingItem) -> PricingPolicy:
    """A policy in force since January, rating `item` under `standard` and `commercial`."""
    stored = PricingPolicy(version=1, valid_from=datetime(2025, 1, 1, tzinfo=UTC), default_category="standard")
    stored.rates = [
        PricingPolicyRate(item_id=item.uuid, credits_per_unit=RATE)  # pyright: ignore[reportCallIssue]
    ]
    stored.category_multipliers = [
        PricingPolicyCategoryMultiplier(category="standard", multiplier=Decimal(1)),  # pyright: ignore[reportCallIssue]
        PricingPolicyCategoryMultiplier(category="commercial", multiplier=Decimal(2)),  # pyright: ignore[reportCallIssue]
    ]
    db_session.add(stored)
    db_session.commit()

    return stored


def two_samples(sku: str, workspace: str) -> tuple[messages.BillingResourceConsumptionRateSample, ...]:
    """Two consumption-rate samples an hour apart, chosen so `_generate_new_estimates`
    produces exactly one BillingEvent covering 01:00-02:00 with a known quantity."""
    return (
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T01:30:00Z", rate=2, sku=sku, workspace=workspace
        ),
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T02:30:00Z", rate=4, sku=sku, workspace=workspace
        ),
    )


def test_message_results_in_sample_in_db(db_session: Session, db_session_factory: sessionmaker[Session]) -> None:
    ############# Setup
    crs = messages.BillingResourceConsumptionRateSample.get_fake()
    msg = msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs)

    db_session.add(models.BillingItem(sku=crs.sku, name="test", unit="GB-h"))
    db_session.flush()
    db_session.commit()

    ############# Test
    messager = ConsumptionSampleRateIngesterMessager(session_factory=db_session_factory)
    failures = messager.consume(msg)

    ############# Behaviour check
    assert not failures.any_permanent()
    assert not failures.any_temporary()

    obj = db_session.get(models.BillableResourceConsumptionRateSample, UUID(str(crs.uuid)))
    assert obj is not None
    assert str(obj.uuid) == crs.uuid
    assert obj.sample_time_utc == datetime.fromisoformat(str(crs.sample_time))
    assert str(obj.user) == crs.user
    assert obj.workspace == crs.workspace
    assert obj.rate == crs.rate
    assert obj.item.sku == crs.sku


def test_messages_across_two_hours_generates_appropriate_billing_events(
    db_session: Session, db_session_factory: sessionmaker[Session]
) -> None:
    ############# Setup

    crs1 = messages.BillingResourceConsumptionRateSample.get_fake(sample_time="2025-01-01T01:30:00Z", rate=2)
    crs2 = messages.BillingResourceConsumptionRateSample.get_fake(
        sample_time="2025-01-01T03:30:00Z", rate=4, sku=crs1.sku, workspace=crs1.workspace
    )

    db_session.add(models.BillingItem(sku=crs1.sku, name="test", unit="GB-h"))
    db_session.flush()
    db_session.commit()

    msg1 = msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs1)
    msg2 = msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs2)

    ############# Test
    messager = ConsumptionSampleRateIngesterMessager(session_factory=db_session_factory)
    failures1 = messager.consume(msg1)
    failures2 = messager.consume(msg2)

    ############# Behaviour check
    assert not failures1.any_permanent()
    assert not failures1.any_temporary()
    assert not failures2.any_permanent()
    assert not failures2.any_temporary()

    bes = [row.event for row in models.BillingEvent.find_billing_events(db_session, str(crs1.workspace))]
    assert len(bes) == 2

    assert bes[0].event_start_utc == datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC)
    assert bes[0].event_end_utc == datetime(2025, 1, 1, 2, 0, 0, tzinfo=UTC)
    assert bes[1].event_start_utc == datetime(2025, 1, 1, 2, 0, 0, tzinfo=UTC)
    assert bes[1].event_end_utc == datetime(2025, 1, 1, 3, 0, 0, tzinfo=UTC)

    assert bes[0].item.sku == crs1.sku
    assert bes[1].item.sku == crs1.sku

    assert bes[0].workspace == crs1.workspace
    assert bes[1].workspace == crs1.workspace

    # Interpolated/known consumption rates are:
    #  01:00:00: 0
    #  01:30:00: 2
    #  02:00:00: 2.5 (interpolated)
    #  03:00:00: 3.5 (interpolated)
    #  03:30:00: 4
    assert bes[0].quantity == 1800 * (2 + 2.5) / 2
    assert bes[1].quantity == 3600 * (2.5 + 3.5) / 2


class TestGeneratedEventsAreCharged:
    """Regression coverage for the estimator writing BillingEvent rows straight to the
    database without ever calling `_charge_event`, so consumption-derived usage recorded a
    quantity but never a credit charge, no matter what the pricing policy said. Mirrors
    `TestTheIngesterCharges` in test_credit_ledger.py, which covers the same `_charge_event`
    call reached from the direct-BillingEvent path.
    """

    def test_generated_billing_events_are_charged_under_the_active_pricing_policy(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy, item: BillingItem
    ) -> None:
        crs1, crs2 = two_samples(item.sku, "test-workspace")

        ############# Test
        messager = ConsumptionSampleRateIngesterMessager(session_factory=db_session_factory)
        failures1 = messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs1))
        failures2 = messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs2))

        ############# Behaviour check
        assert not failures1.any_permanent()
        assert not failures1.any_temporary()
        assert not failures2.any_permanent()
        assert not failures2.any_temporary()

        (usage,) = list(models.BillingEvent.find_billing_events(db_session, "test-workspace"))
        assert usage.credits > 0
        assert usage.credits == exact_decimal(usage.event.quantity) * RATE

    def test_a_redelivered_sample_does_not_charge_the_estimate_twice(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy, item: BillingItem
    ) -> None:
        """The case the partial unique index on the ledger exists for. Pulsar can redeliver a
        consumption-rate-sample message, and `_generate_new_estimates` runs again on delivery -
        it must not mint a second debit for a window it already estimated and charged."""
        crs1, crs2 = two_samples(item.sku, "test-workspace")
        messager = ConsumptionSampleRateIngesterMessager(session_factory=db_session_factory)

        messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs1))
        messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs2))

        # Redeliver the second sample: same uuid, so `_record_event` is a no-op, but
        # `_generate_new_estimates` runs again regardless.
        failures = messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs2))

        ############# Behaviour check
        assert not failures.any_permanent()
        assert not failures.any_temporary()

        usage = list(models.BillingEvent.find_billing_events(db_session, "test-workspace"))
        assert len(usage) == 1
        assert usage[0].credits > 0

    def test_the_workspace_category_multiplies_the_charge(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy, item: BillingItem
    ) -> None:
        models.WorkspaceCategory.assign(db_session, "test-workspace", "commercial")
        db_session.commit()

        crs1, crs2 = two_samples(item.sku, "test-workspace")
        messager = ConsumptionSampleRateIngesterMessager(session_factory=db_session_factory)
        messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs1))
        messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs2))

        (usage,) = list(models.BillingEvent.find_billing_events(db_session, "test-workspace"))
        assert usage.credits == exact_decimal(usage.event.quantity) * RATE * 2

    def test_an_unrated_sku_records_the_event_but_no_charge(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        """A collector emitting a SKU the last calibration pass did not cover."""
        unrated_item = models.BillingItem(sku="unrated-consumption-sku", name="Unrated", unit="GB-h")
        db_session.add(unrated_item)
        db_session.commit()

        crs1, crs2 = two_samples(unrated_item.sku, "test-workspace")
        messager = ConsumptionSampleRateIngesterMessager(session_factory=db_session_factory)
        failures1 = messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs1))
        failures2 = messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs2))

        ############# Behaviour check
        assert not failures1.any_permanent()
        assert not failures1.any_temporary()
        assert not failures2.any_permanent()
        assert not failures2.any_temporary()

        (usage,) = list(models.BillingEvent.find_billing_events(db_session, "test-workspace"))
        assert usage.credits == 0

    def test_no_policy_at_all_records_the_event_but_no_charge(
        self, db_session: Session, db_session_factory: sessionmaker[Session], item: BillingItem
    ) -> None:
        """The state of a fresh installation before any configuration is loaded."""
        crs1, crs2 = two_samples(item.sku, "test-workspace")
        messager = ConsumptionSampleRateIngesterMessager(session_factory=db_session_factory)
        failures1 = messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs1))
        failures2 = messager.consume(msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs2))

        ############# Behaviour check
        assert not failures1.any_permanent()
        assert not failures1.any_temporary()
        assert not failures2.any_permanent()
        assert not failures2.any_temporary()

        (usage,) = list(models.BillingEvent.find_billing_events(db_session, "test-workspace"))
        assert usage.credits == 0
