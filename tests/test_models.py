from sqlalchemy.orm.session import Session

from accounting_service import models
from tests.conftest import fake_event_known_times


def test_round_trip_billingevent_insertfrommessage_retrieve(db_session: Session):
    ############# Setup
    bemsg, start, end = fake_event_known_times()
    db_session.add(models.BillingItem(sku=bemsg.sku, name="test", unit="GB-h"))

    ############# Test
    beuuid = models.BillingEvent.insert_from_message(db_session, bemsg)

    ############# Behaviour check
    beobj = db_session.get(models.BillingEvent, beuuid)
    assert str(beobj.uuid) == bemsg.uuid
    assert beobj.event_start == start
    assert beobj.event_end == end
    assert str(beobj.user) == bemsg.user
    assert beobj.workspace == bemsg.workspace
    assert beobj.quantity == bemsg.quantity
    assert beobj.item.sku == bemsg.sku
