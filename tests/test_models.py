import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from eodhp_utils.pulsar import messages
from faker import Faker
from sqlalchemy import delete
from sqlalchemy.orm.session import Session

from accounting_service import models
from tests.conftest import fake_event_known_times


def test_round_trip_billingevent_insertfrommessage_retrieve(db_session: Session) -> None:
    ############# Setup
    bemsg, start, end = fake_event_known_times()
    db_session.add(models.BillingItem(sku=bemsg.sku, name="test", unit="GB-h"))

    ############# Test
    beuuid = models.BillingEvent.insert_from_message(db_session, bemsg)

    ############# Behaviour check
    beobj = db_session.get(models.BillingEvent, beuuid)
    assert beobj is not None
    assert str(beobj.uuid) == bemsg.uuid
    assert beobj.event_start_utc == start
    assert beobj.event_end_utc == end
    assert str(beobj.user) == bemsg.user
    assert beobj.workspace == bemsg.workspace
    assert beobj.quantity == bemsg.quantity
    assert beobj.item.sku == bemsg.sku


def test_dup_billingevent_uuid_only_added_once(db_session: Session) -> None:
    ############# Setup
    bemsg, _start, _end = fake_event_known_times()
    db_session.add(models.BillingItem(sku=bemsg.sku, name="test", unit="GB-h"))

    ############# Test
    bemsg.quantity = float(1)
    beuuid1 = models.BillingEvent.insert_from_message(db_session, bemsg)

    bemsg.quantity = float(2)
    beuuid2 = models.BillingEvent.insert_from_message(db_session, bemsg)

    ############# Behaviour check
    beobj = db_session.get(models.BillingEvent, beuuid1)
    assert beobj is not None
    assert str(beobj.uuid) == bemsg.uuid
    assert beobj.quantity == 1

    assert beuuid2 is None


def gen_billingitem_data(
    db_session: Session, events: Sequence[dict[str, Any]], ws_accounts: dict[str, str] | None = None
) -> tuple[list[uuid.UUID], dict[str, uuid.UUID], dict[str, uuid.UUID]]:
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
    db_session.add(models.BillingItem(uuid=item_uuids["testsku"], sku="testsku", name="test", unit="GB-h"))

    for event in events:
        event_uuid = uuid.uuid4()

        start = event.get("event_start", fake.past_datetime("-30d", tzinfo=UTC))
        end = event.get("event_end", start + timedelta(minutes=5))

        item_sku = event.get("sku", "testsku")
        item_uuid = item_uuids.get(item_sku)
        if not item_uuid:
            item_uuid = uuid.uuid4()
            item_uuids[item_sku] = item_uuid
            db_session.add(models.BillingItem(uuid=item_uuid, sku=item_sku, name="test", unit="GB-h"))

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


def test_finding_all_billing_events_for_workspace_returns_correct_number(db_session: Session) -> None:
    gen_billingitem_data(
        db_session,
        [{"workspace": "workspace1"}, {"workspace": "workspace1"}, {"workspace": "workspace2"}],
    )

    bes = models.BillingEvent.find_billing_events(db_session, workspace="workspace1")
    assert len(list(bes)) == 2


def test_finding_all_billing_events_for_account_returns_correct_number(db_session: Session) -> None:
    _event_uuids, account_uuids, _item_uuids = gen_billingitem_data(
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


def test_finding_billing_events_for_workspace(db_session: Session) -> None:
    ############# Setup
    event_uuids, _account_uuids, _item_uuids = gen_billingitem_data(
        db_session,
        [
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 6, 10, 0, tzinfo=UTC),
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 7, 10, 0, tzinfo=UTC),
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 8, 10, 0, tzinfo=UTC),
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 9, 10, 0, tzinfo=UTC),
            },
            {
                "workspace": "workspace2",
                "event_start": datetime(2024, 1, 16, 7, 5, 0, tzinfo=UTC),
            },
            {
                "workspace": "workspace3",
                "event_start": datetime(2024, 1, 17, 7, 5, 0, tzinfo=UTC),
            },
        ],
    )

    ############# Test
    bes = models.BillingEvent.find_billing_events(
        db_session,
        workspace="workspace1",
        start=datetime(2024, 1, 16, 7, 5, 0, tzinfo=UTC),
        end=datetime(2024, 1, 16, 9, 5, 0, tzinfo=UTC),
    )

    ############# Behaviour check
    bes = list(bes)

    print(repr(bes))

    assert len(bes) == 2
    assert bes[0].uuid == event_uuids[1]
    assert bes[1].uuid == event_uuids[2]


def test_paging_billing_events_produces_all_events_once(db_session: Session) -> None:
    ############# Setup
    db_session.execute(delete(models.BillingEvent))
    event_uuids, _account_uuids, _item_uuids = gen_billingitem_data(
        db_session,
        [
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 6, 10, 0, tzinfo=UTC),
            },
            {
                "workspace": "workspace2",
                "event_start": datetime(2024, 1, 16, 7, 5, 0, tzinfo=UTC),
            },
            {
                "workspace": "workspace3",
                "event_start": datetime(2024, 1, 16, 7, 5, 0, tzinfo=UTC),
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 7, 10, 0, tzinfo=UTC),
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 8, 10, 0, tzinfo=UTC),
            },
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


@pytest.fixture
def fake_rate_samples(db_session: Session) -> list[messages.BillingResourceConsumptionRateSample]:
    db_session.add(models.BillingItem(sku="testsku", name="test", unit="GB-h"))
    db_session.add(models.BillingItem(sku="nottestsku", name="test", unit="GB-h"))
    return [
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T00:45:00Z",
            workspace="workspace1",
            rate=1,
            sku="testsku",
        ),
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T00:55:00Z",
            workspace="workspace1",
            rate=2,
            sku="testsku",
        ),
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T01:15:00Z",
            workspace="workspace1",
            rate=3,
            sku="testsku",
        ),
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T01:25:00Z",
            workspace="workspace1",
            rate=4,
            sku="testsku",
        ),
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T01:50:00Z",
            workspace="workspace1",
            rate=2,
            sku="testsku",
        ),
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T02:05:00Z",
            workspace="workspace1",
            rate=1,
            sku="testsku",
        ),
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T02:55:00Z",
            workspace="workspace1",
            rate=90,
            sku="testsku",
        ),
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T01:35:00Z",
            workspace="workspace2",
            rate=900,
            sku="testsku",
        ),
        messages.BillingResourceConsumptionRateSample.get_fake(
            sample_time="2025-01-01T01:35:00Z",
            workspace="workspace1",
            rate=900,
            sku="nottestsku",
        ),
    ]


def test_round_trip_billingresourceconsumptionratesample_insertfrommessage_retrieve(
    db_session: Session,
) -> None:
    ############# Setup
    msg = messages.BillingResourceConsumptionRateSample.get_fake()
    db_session.add(models.BillingItem(sku=msg.sku, name="test", unit="GB-h"))

    ############# Test
    bruuid = models.BillableResourceConsumptionRateSample.insert_from_message(db_session, msg)

    ############# Behaviour check
    brobj = db_session.get(models.BillableResourceConsumptionRateSample, bruuid)
    assert brobj is not None
    assert str(brobj.uuid) == msg.uuid
    assert brobj.sample_time_utc.isoformat() == msg.sample_time
    assert str(brobj.user) == msg.user
    assert brobj.workspace == msg.workspace
    assert brobj.rate == msg.rate
    assert brobj.item.sku == msg.sku


def test_round_trip_billingresourceconsumptionratesample_insertfrommessage_retrieve_interval(
    db_session: Session, fake_rate_samples: list[messages.BillingResourceConsumptionRateSample]
) -> None:
    ############# Setup
    # This creates several samples around our window of interest, 1am-2am 2025-01-01, to send to
    # the data store.
    for sample in fake_rate_samples:
        models.BillableResourceConsumptionRateSample.insert_from_message(db_session, sample)

    ############# Test
    found_samples = list(
        models.BillableResourceConsumptionRateSample.find_data_for_interval(
            db_session,
            "workspace1",
            "testsku",
            datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC),
            datetime(2025, 1, 1, 2, 0, 0, tzinfo=UTC),
        )
    )

    ############# Behaviour check
    # The data found should be the last sample before, the last sample after and all samples during
    # the test period.
    assert len(found_samples) == 5

    assert found_samples[0].rate == 2
    assert found_samples[1].rate == 3
    assert found_samples[2].rate == 4
    assert found_samples[3].rate == 2
    assert found_samples[4].rate == 1


@pytest.mark.parametrize(
    ("start", "end", "expected_consumption"),
    [
        # Samples for this 10 min window should be:
        #   * 1:15: 3 (exact start)
        #   * 1:25: 4 (exact end)
        # Consumption estimate is (3+4)/2 * 600
        pytest.param(
            datetime(2025, 1, 1, 1, 15, 0, tzinfo=UTC),
            datetime(2025, 1, 1, 1, 25, 0, tzinfo=UTC),
            3.5 * 600,
        ),
        # Samples for this 1h window should be:
        #   * 1:00: 2.25 (interpolated between 2 and 3)
        #   * 1:15: 3
        #   * 1:25: 4
        #   * 1:50: 2
        #   * 2:00: 1.3333 (interpolated between 2 and 1)
        # Consumption estimate is 900*(2.25+3)/2 + 600*(3+4)/2 + 1500*(4+2)/2 + 600*(2+1.3333)/2
        #  = 9962.5
        pytest.param(
            datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC),
            datetime(2025, 1, 1, 2, 0, 0, tzinfo=UTC),
            9962.5,
        ),
        # Samples for this 2 min window should be:
        #   * 1:15: 3 (before window)
        #   * 1:19: 3.4 (interpolated window start)
        #   * 1:21: 3.6 (interpolated window end)
        #   * 1:25: 4 (after window)
        # Consumption estimate is 120 * (3.6+3.4)/2
        pytest.param(
            datetime(2025, 1, 1, 1, 19, 0, tzinfo=UTC),
            datetime(2025, 1, 1, 1, 21, 0, tzinfo=UTC),
            420.0,
        ),
        # Samples for this 50m window should be:
        #   * 0:00: No samples
        #   * 0:45: 1
        #   * 0:50: 1.5 (interpolated at window end)
        #   * 0:55: 2 (after window)
        #
        # Consumption estimate is 300*(1+1.5)/2 = 375
        # Note: counted as zero up to first sample
        pytest.param(
            datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
            datetime(2025, 1, 1, 0, 50, 0, tzinfo=UTC),
            375,
        ),
        # Samples for this 1h window should be:
        #   * 2:05: 1 (before window)
        #   * 2:30: 45.5 (interpolated)
        #   * 2:55: 90
        #   * 2:55: 0 (resource assumed destroyed - no later samples)
        #   * 3:30: 0 (window end)
        #   * no later samples
        # Consumption estimate is 25*60*(45.5+90)/2 = 101625.0
        pytest.param(
            datetime(2025, 1, 1, 2, 30, 0, tzinfo=UTC),
            datetime(2025, 1, 1, 3, 30, 0, tzinfo=UTC),
            101625.0,
        ),
    ],
)
def test_consumption_estimation_from_billingresourceconsumptionratesamples(
    db_session: Session,
    fake_rate_samples: list[messages.BillingResourceConsumptionRateSample],
    start: datetime,
    end: datetime,
    expected_consumption: float,
) -> None:
    ############# Setup
    # This creates several samples around our window of interest, 1am-2am 2025-01-01, to send to
    # the data store.
    for sample in fake_rate_samples:
        models.BillableResourceConsumptionRateSample.insert_from_message(db_session, sample)

    ############# Test
    consumption = models.BillableResourceConsumptionRateSample.calculate_consumption_for_interval(
        db_session, "workspace1", "testsku", start, end
    )

    ############# Behaviour check
    assert consumption == expected_consumption
