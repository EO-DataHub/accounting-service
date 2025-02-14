from unittest import mock
from uuid import UUID

from eodhp_utils.pulsar import messages
from sqlalchemy import create_engine
from sqlalchemy.orm.session import Session

from accounting_service import models
from accounting_service.ingester.messager import AccountingIngesterMessager
from tests.conftest import bemsg_to_pulsar_msg, fake_event_known_times


def test_message_results_in_billingevent_in_db(db_session: Session):
    ############# Setup
    bemsg, start, end = fake_event_known_times()
    db_session.add(models.BillingItem(sku=bemsg.sku, name="test", unit="GB-h"))
    db_session.flush()
    db_session.commit()

    ############# Test
    messager = AccountingIngesterMessager()
    failures = messager.consume(bemsg_to_pulsar_msg(bemsg))

    ############# Behaviour check
    assert not failures.any_permanent()
    assert not failures.any_temporary()

    beobj = db_session.get(models.BillingEvent, UUID(bemsg.uuid))
    assert str(beobj.uuid) == bemsg.uuid
    assert beobj.event_start == start
    assert beobj.event_end == end
    assert str(beobj.user) == bemsg.user
    assert beobj.workspace == bemsg.workspace
    assert beobj.quantity == bemsg.quantity
    assert beobj.item.sku == bemsg.sku


def test_message_with_no_user_results_in_billingevent_in_db(db_session: Session):
    ############# Setup
    bemsg = messages.BillingEvent.get_fake()
    bemsg.user = None

    db_session.add(models.BillingItem(sku=bemsg.sku, name="test", unit="GB-h"))
    db_session.flush()
    db_session.commit()

    ############# Test
    messager = AccountingIngesterMessager()
    failures = messager.consume(bemsg_to_pulsar_msg(bemsg))

    ############# Behaviour check
    assert not failures.any_permanent()
    assert not failures.any_temporary()

    beobj = db_session.get(models.BillingEvent, UUID(bemsg.uuid))
    assert str(beobj.uuid) == bemsg.uuid
    assert beobj.user is None


def test_message_with_unknown_sku_creates_billingitem(db_session):
    ############# Setup
    bemsg = messages.BillingEvent.get_fake()

    ############# Test
    messager = AccountingIngesterMessager()
    failures = messager.consume(bemsg_to_pulsar_msg(bemsg))

    ############# Behaviour check
    assert not failures.any_permanent()
    assert not failures.any_temporary()

    beobj = db_session.get(models.BillingEvent, UUID(bemsg.uuid))
    assert beobj.item.sku == bemsg.sku


def test_message_with_invalid_uuid_produces_permanent_failure():
    ############# Setup
    bemsg = messages.BillingEvent.get_fake()
    bemsg.uuid = "abc"

    ############# Test
    messager = AccountingIngesterMessager()
    failures = messager.consume(bemsg_to_pulsar_msg(bemsg))

    ############# Behaviour check
    assert failures.any_permanent()


def test_db_operational_error_produces_temporary_failure():
    engine = create_engine("postgresql+psycopg://localhost:1/nonexistent")
    with mock.patch("accounting_service.ingester.messager.db.engine", engine):
        # session = scoped_session(sessionmaker(bind=engine))
        ############# Setup
        bemsg = messages.BillingEvent.get_fake()

        ############# Test
        messager = AccountingIngesterMessager()
        failures = messager.consume(bemsg_to_pulsar_msg(bemsg))

        ############# Behaviour check
        assert not failures.any_permanent()
        assert failures.any_temporary()
