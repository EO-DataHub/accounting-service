import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, Sequence

from faker import Faker
from sqlalchemy import delete
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


def test_dup_billingevent_uuid_only_added_once(db_session: Session):
    ############# Setup
    bemsg, start, end = fake_event_known_times()
    db_session.add(models.BillingItem(sku=bemsg.sku, name="test", unit="GB-h"))

    ############# Test
    bemsg.quantity = 1
    beuuid1 = models.BillingEvent.insert_from_message(db_session, bemsg)

    bemsg.quantity = 2
    beuuid2 = models.BillingEvent.insert_from_message(db_session, bemsg)

    ############# Behaviour check
    beobj = db_session.get(models.BillingEvent, beuuid1)
    assert str(beobj.uuid) == bemsg.uuid
    assert beobj.quantity == 1

    assert beuuid2 is None


def gen_billingitem_data(
    db_session: Session, events: Sequence[dict], ws_accounts: Optional[Dict[str, str]] = None
):
    """
    This generates test BillingEvents, BillingItems and WorkspaceAccounts based on a spec.
    Example:
      events=[{"workspace": "workspace1", event_start=datetime(2024, 1, 16, 6, 10, 0), sku="abc"}]
      ws_accounts={"workspace1": "account1"}

    Any value in any of these dicts can be omitted to get a default.
    """
    accounts_created: dict[str, uuid.UUID] = {}
    event_uuids: list[uuid.UUID] = []
    item_uuids: dict[str, uuid.UUID] = {}

    ws_accounts = ws_accounts or {}

    fake = Faker()

    for workspace, account in ws_accounts.items():
        account_uuid = accounts_created.setdefault(account, uuid.uuid4())
        db_session.add(models.WorkspaceAccount(workspace=workspace, account=account_uuid))

    item_uuids["testsku"] = uuid.uuid4()
    db_session.add(
        models.BillingItem(uuid=item_uuids["testsku"], sku="testsku", name="test", unit="GB-h")
    )

    for event in events:
        event_uuid = uuid.uuid4()

        start = event.get("event_start", fake.past_datetime("-30d"))
        end = event.get("event_end", start + timedelta(minutes=5))

        item_sku = event.get("sku", "testsku")
        item_uuid = item_uuids.get(item_sku)
        if not item_uuid:
            item_uuid = uuid.uuid4()
            item_uuids[item_sku] = item_uuid
            db_session.add(
                models.BillingItem(uuid=item_uuid, sku=item_sku, name="test", unit="GB-h")
            )

        db_session.add(
            models.BillingEvent(
                uuid=event_uuid,
                event_start=start,
                event_end=end,
                workspace=event.get("workspace", "testworkspace"),
                item_id=item_uuid,
                user=event.get("user", uuid.uuid4()),
                quantity=event.get("quantity", 1.1),
            )
        )
        event_uuids.append(event_uuid)

    return (event_uuids, accounts_created, item_uuids)


def test_finding_all_billing_events_for_workspace_returns_correct_number(db_session: Session):
    gen_billingitem_data(
        db_session,
        [{"workspace": "workspace1"}, {"workspace": "workspace1"}, {"workspace": "workspace2"}],
    )

    bes = models.BillingEvent.find_billing_events(db_session, workspace="workspace1")
    assert len(list(bes)) == 2


def test_finding_all_billing_events_for_account_returns_correct_number(db_session: Session):
    event_uuids, account_uuids, item_uuids = gen_billingitem_data(
        db_session,
        [
            {"workspace": "workspace1"},
            {"workspace": "workspace1"},
            {"workspace": "workspace2"},
            {"workspace": "workspace3"},
        ],
        {"workspace1": "account1", "workspace2": "account1", "workspace3": "account2"},
    )

    bes = models.BillingEvent.find_billing_events(db_session, account=account_uuids["account1"])
    assert len(list(bes)) == 3

    bes = models.BillingEvent.find_billing_events(db_session, account=account_uuids["account2"])
    assert len(list(bes)) == 1


def test_finding_billing_events_for_workspace(db_session: Session):
    ############# Setup
    event_uuids, account_uuids, item_uuids = gen_billingitem_data(
        db_session,
        [
            {"workspace": "workspace1", "event_start": datetime(2024, 1, 16, 6, 10, 0)},
            {"workspace": "workspace1", "event_start": datetime(2024, 1, 16, 7, 10, 0)},
            {"workspace": "workspace1", "event_start": datetime(2024, 1, 16, 8, 10, 0)},
            {"workspace": "workspace1", "event_start": datetime(2024, 1, 16, 9, 10, 0)},
            {"workspace": "workspace2", "event_start": datetime(2024, 1, 16, 7, 5, 0)},
            {"workspace": "workspace3", "event_start": datetime(2024, 1, 17, 7, 5, 0)},
        ],
    )

    ############# Test
    bes = models.BillingEvent.find_billing_events(
        db_session,
        workspace="workspace1",
        start=datetime(2024, 1, 16, 7, 5, 0),
        end=datetime(2024, 1, 16, 9, 5, 0),
    )

    ############# Behaviour check
    bes = list(bes)

    print(repr(bes))

    assert len(bes) == 2
    assert bes[0].uuid == event_uuids[1]
    assert bes[1].uuid == event_uuids[2]


def test_paging_billing_events_produces_all_events_once(db_session: Session):
    ############# Setup
    db_session.execute(delete(models.BillingEvent))
    event_uuids, account_uuids, item_uuids = gen_billingitem_data(
        db_session,
        [
            {"workspace": "workspace1", "event_start": datetime(2024, 1, 16, 6, 10, 0)},
            {"workspace": "workspace2", "event_start": datetime(2024, 1, 16, 7, 5, 0)},
            {"workspace": "workspace3", "event_start": datetime(2024, 1, 16, 7, 5, 0)},
            {"workspace": "workspace1", "event_start": datetime(2024, 1, 16, 7, 10, 0)},
            {"workspace": "workspace1", "event_start": datetime(2024, 1, 16, 8, 10, 0)},
        ],
    )

    ############# Test
    assert len(list(models.BillingEvent.find_billing_events(db_session, limit=200))) == 5
    bes1 = list(models.BillingEvent.find_billing_events(db_session, limit=2))
    bes2 = list(models.BillingEvent.find_billing_events(db_session, limit=2, after=bes1[-1].uuid))
    bes3 = list(models.BillingEvent.find_billing_events(db_session, limit=2, after=bes2[-1].uuid))

    ############# Behaviour check
    assert len(bes1) == 2
    assert bes1[0].uuid == event_uuids[0]
    assert bes1[1].uuid == event_uuids[1]

    assert len(bes2) == 2
    assert bes2[0].uuid == event_uuids[2]
    assert bes2[1].uuid == event_uuids[3]

    assert len(bes3) == 1
    assert bes3[0].uuid == event_uuids[4]
