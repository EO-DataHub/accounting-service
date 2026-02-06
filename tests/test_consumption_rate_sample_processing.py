from datetime import UTC, datetime
from uuid import UUID

from eodhp_utils.pulsar import messages
from sqlalchemy.orm.session import Session

from accounting_service import models
from accounting_service.ingester.messager import ConsumptionSampleRateIngesterMessager
from tests.conftest import msg_to_pulsar_msg


def test_message_results_in_sample_in_db(db_session: Session) -> None:
    ############# Setup
    crs = messages.BillingResourceConsumptionRateSample.get_fake()
    msg = msg_to_pulsar_msg(ConsumptionSampleRateIngesterMessager, crs)

    db_session.add(models.BillingItem(sku=crs.sku, name="test", unit="GB-h"))
    db_session.flush()
    db_session.commit()

    ############# Test
    messager = ConsumptionSampleRateIngesterMessager()
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


def test_messages_across_two_hours_generates_appropriate_billing_events(db_session: Session) -> None:
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
    messager = ConsumptionSampleRateIngesterMessager()
    failures1 = messager.consume(msg1)
    failures2 = messager.consume(msg2)

    ############# Behaviour check
    assert not failures1.any_permanent()
    assert not failures1.any_temporary()
    assert not failures2.any_permanent()
    assert not failures2.any_temporary()

    bes = list(models.BillingEvent.find_billing_events(db_session, str(crs1.workspace)))
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
