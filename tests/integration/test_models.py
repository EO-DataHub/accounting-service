import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from eodhp_utils.pulsar import messages
from faker import Faker
from sqlalchemy import text
from sqlalchemy.orm.session import Session

from accounting_service import models
from tests.integration.conftest import fake_event_known_times


def test_dup_billingevent_uuid_only_added_once(db_session: Session) -> None:
    """A redelivered message is ignored, and insert_from_message says so by returning None.

    Kept rather than folded into the messager tests, which assert the row is not overwritten
    but never see the return value. That None is how AccountingIngesterMessager tells a
    recorded event from a duplicate, so it is a contract worth its own test.
    """
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
        event_uuid = event.get("uuid", uuid.uuid4())

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
    bes = [row.event for row in bes]

    print(repr(bes))

    assert len(bes) == 2
    assert bes[0].uuid == event_uuids[1]
    assert bes[1].uuid == event_uuids[2]


def test_paging_billing_events_produces_all_events_once(db_session: Session) -> None:
    ############# Setup
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
    bes1 = [row.event for row in models.BillingEvent.find_billing_events(db_session, limit=2)]
    bes2 = [row.event for row in models.BillingEvent.find_billing_events(db_session, limit=2, after=bes1[-1].uuid)]
    bes3 = [row.event for row in models.BillingEvent.find_billing_events(db_session, limit=2, after=bes2[-1].uuid)]

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


def test_consumption_estimation_reads_the_samples_and_hands_them_to_the_estimator(
    db_session: Session,
    fake_rate_samples: list[messages.BillingResourceConsumptionRateSample],
) -> None:
    """calculate_consumption_for_interval joins the query to the arithmetic correctly.

    Reduced from five parametrised windows, which were all verifying the arithmetic:
    interpolation at the window edges, a resource appearing part-way through, a resource
    destroyed before the end. Every one of those now has a test in tests/test_consumption.py
    that needs no database, against estimate_consumption directly.

    Which samples the query selects is covered by the test above, which asserts the exact
    five it returns for this same window. What is left, and what needs both halves present,
    is that the two are wired together.

    The expected figure is the same 1:00-2:00 window: interpolated 2.25 at the start, then
    3, 4, 2 at 1:15, 1:25, 1:50, then interpolated 1.3333 at the end.

        900*(2.25+3)/2 + 600*(3+4)/2 + 1500*(4+2)/2 + 600*(2+1.3333)/2 = 9962.5

    It also stands in for the filtering: workspace2 and nottestsku each hold a sample with a
    rate of 900, so a leak would not be a near miss.
    """
    ############# Setup
    for sample in fake_rate_samples:
        models.BillableResourceConsumptionRateSample.insert_from_message(db_session, sample)

    ############# Test
    consumption = models.BillableResourceConsumptionRateSample.calculate_consumption_for_interval(
        db_session,
        "workspace1",
        "testsku",
        datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2025, 1, 1, 2, 0, 0, tzinfo=UTC),
    )

    ############# Behaviour check
    assert consumption == 9962.5


# 2025-03-31 is a Monday and the last day of Q1, so these three events fall in two weeks and in
# two quarters, split differently: the week boundary sits between the first and second, the
# quarter boundary between the second and third.
PERIOD_BOUNDARY_EVENTS = [
    {"event_start": datetime(2025, 3, 30, 23, 0, 0, tzinfo=UTC), "quantity": 1},
    {"event_start": datetime(2025, 3, 31, 0, 0, 0, tzinfo=UTC), "quantity": 2},
    {"event_start": datetime(2025, 4, 1, 0, 0, 0, tzinfo=UTC), "quantity": 4},
]


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        pytest.param(
            models.TimeAggregation.WEEK,
            [
                (datetime(2025, 3, 24, tzinfo=UTC), datetime(2025, 3, 31, tzinfo=UTC), 1),
                (datetime(2025, 3, 31, tzinfo=UTC), datetime(2025, 4, 7, tzinfo=UTC), 6),
            ],
            id="week",
        ),
        pytest.param(
            models.TimeAggregation.QUARTER,
            [
                (datetime(2025, 1, 1, tzinfo=UTC), datetime(2025, 4, 1, tzinfo=UTC), 3),
                (datetime(2025, 4, 1, tzinfo=UTC), datetime(2025, 7, 1, tzinfo=UTC), 4),
            ],
            id="quarter",
        ),
    ],
)
def test_weeks_start_on_monday_and_quarters_are_calendar_quarters(
    db_session: Session, period: models.TimeAggregation, expected: list[tuple[datetime, datetime, float]]
) -> None:
    gen_billingitem_data(db_session, PERIOD_BOUNDARY_EVENTS)
    db_session.flush()

    rows = models.BillingEvent.find_billing_events(db_session, time_aggregation=period)

    assert [
        (row.event.event_start_utc, row.event.event_end_utc, float(row.event.quantity)) for row in rows
    ] == expected


# One event on the 10th of each month from December 2025 to July 2026, so that every month and
# quarter a range could touch has something in it, and so do those either side.
MONTHLY_EVENTS = [
    {"event_start": datetime(year, month, 10, tzinfo=UTC), "quantity": 1}
    for year, month in [(2025, 12), *((2026, month) for month in range(1, 8))]
]


def period_starts(
    db_session: Session, period: models.TimeAggregation, start: datetime | None, end: datetime | None
) -> list[tuple[datetime, float]]:
    rows = models.BillingEvent.find_billing_events(db_session, time_aggregation=period, start=start, end=end)

    return [(row.event.event_start_utc, float(row.event.quantity)) for row in rows]


@pytest.mark.parametrize(
    ("period", "expected"),
    [
        pytest.param(
            models.TimeAggregation.MONTH,
            [(datetime(2026, month, 1, tzinfo=UTC), 1) for month in range(2, 6)],
            id="month",
        ),
        pytest.param(
            models.TimeAggregation.QUARTER,
            [(datetime(2026, 1, 1, tzinfo=UTC), 3), (datetime(2026, 4, 1, tzinfo=UTC), 3)],
            id="quarter",
        ),
    ],
)
def test_a_range_includes_every_period_it_overlaps_whole(
    db_session: Session, period: models.TimeAggregation, expected: list[tuple[datetime, float]]
) -> None:
    gen_billingitem_data(db_session, MONTHLY_EVENTS)
    db_session.flush()

    assert (
        period_starts(db_session, period, datetime(2026, 2, 14, tzinfo=UTC), datetime(2026, 5, 23, tzinfo=UTC))
        == expected
    )


def test_a_range_ending_on_a_period_boundary_excludes_the_period_it_starts(db_session: Session) -> None:
    gen_billingitem_data(db_session, MONTHLY_EVENTS)
    db_session.flush()

    assert period_starts(db_session, models.TimeAggregation.QUARTER, None, datetime(2026, 4, 1, tzinfo=UTC)) == [
        (datetime(2025, 10, 1, tzinfo=UTC), 1),
        (datetime(2026, 1, 1, tzinfo=UTC), 3),
    ]


def test_a_range_starting_on_a_period_boundary_excludes_the_period_before(db_session: Session) -> None:
    gen_billingitem_data(db_session, MONTHLY_EVENTS)
    db_session.flush()

    assert period_starts(db_session, models.TimeAggregation.QUARTER, datetime(2026, 4, 1, tzinfo=UTC), None) == [
        (datetime(2026, 4, 1, tzinfo=UTC), 3),
        (datetime(2026, 7, 1, tzinfo=UTC), 1),
    ]


def test_period_bounds_do_not_depend_on_the_session_time_zone(db_session: Session) -> None:
    """The period columns carry no zone. Compared against an aware bound, PostgreSQL would read
    them in the session's TimeZone, and in New York January would end at 05:00 UTC on
    1 February and so overlap a range starting at midnight.
    """
    gen_billingitem_data(db_session, MONTHLY_EVENTS)
    db_session.flush()
    # LOCAL, so that it ends with the test's transaction.
    db_session.execute(text("SET LOCAL TIME ZONE 'America/New_York'"))

    assert period_starts(
        db_session, models.TimeAggregation.MONTH, datetime(2026, 2, 1, tzinfo=UTC), datetime(2026, 3, 1, tzinfo=UTC)
    ) == [(datetime(2026, 2, 1, tzinfo=UTC), 1)]


def test_an_event_is_included_if_any_of_it_falls_in_the_range(db_session: Session) -> None:
    def at(hour: int) -> datetime:
        return datetime(2026, 1, 1, hour, tzinfo=UTC)

    event_uuids, _account_uuids, _item_uuids = gen_billingitem_data(
        db_session,
        [
            {"event_start": at(5), "event_end": at(7)},  # ends exactly at start
            {"event_start": at(6), "event_end": at(8)},  # straddles start
            {"event_start": at(8), "event_end": at(9)},  # inside
            {"event_start": at(9), "event_end": at(11)},  # straddles end
            {"event_start": at(9), "event_end": at(10)},  # ends exactly at end
            {"event_start": at(10), "event_end": at(11)},  # starts exactly at end
        ],
    )
    db_session.flush()

    rows = models.BillingEvent.find_billing_events(db_session, start=at(7), end=at(10))

    assert {row.event.uuid for row in rows} == set(event_uuids[1:5])
