"""Paging through the usage reads with `after` inside a bounded range.

Here rather than in tests/ because what is asserted is which rows PostgreSQL finds for the
cursor, and which plan it uses to find them.
"""

import json
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, text
from sqlmodel import Session

from accounting_service import db
from accounting_service.models import BillingEvent, TimeAggregation, UsageDimension
from tests.integration.test_models import gen_billingitem_data

USERS = [
    uuid.UUID("aaaaaaaa-0000-4000-8000-000000000001"),
    uuid.UUID("bbbbbbbb-0000-4000-8000-000000000002"),
    None,
]
FIRST_DAY = datetime(2024, 1, 1, tzinfo=UTC)

# Inside the data, and on no period boundary, so the first and last periods are partial.
RANGE = "&start=2024-02-10T06:00:00Z&end=2024-05-20T00:00:00Z"
OTHER_RANGE = "&start=2024-06-01T00:00:00Z&end=2024-07-01T00:00:00Z"
EARLIER_RANGE = "&start=2024-01-01T00:00:00Z&end=2024-01-20T00:00:00Z"

GROUP_BYS = ["sku,workspace", "workspace", "user", ""]


@pytest.fixture
def usage(db_session: Session) -> None:
    """Events every four days through the first half of 2024, two a day, spread over three
    workspaces, two SKUs and three users (one of them nobody), so every period type and every
    grouping gives several rows."""
    events: list[dict[str, Any]] = []
    for number in range(46):
        when = FIRST_DAY + timedelta(days=4 * number, hours=number % 7)
        for offset in range(2):
            index = number + offset
            events.append(
                {
                    "workspace": f"ws{index % 3}",
                    "sku": f"sku{index % 2}",
                    "user": USERS[index % 3],
                    "event_start": when + timedelta(hours=offset),
                    "event_end": when + timedelta(hours=offset, minutes=30),
                    "quantity": float(2 * number + offset + 1),
                }
            )
    gen_billingitem_data(db_session, events)
    db_session.commit()


def read(client: TestClient, query: str) -> list[dict[str, Any]]:
    response = client.get(f"/accounting/usage-data?{query}")
    assert response.status_code == 200, response.text
    return response.json()


def paged(client: TestClient, query: str, page_size: int) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    pages = 0
    page = read(client, f"{query}&limit={page_size}")
    while page:
        pages += 1
        rows.extend(page)
        page = read(client, f"{query}&limit={page_size}&after={page[-1]['uuid']}")
    return rows, pages


@pytest.mark.usefixtures("usage")
class TestStage2AggregatedPaging:
    @pytest.mark.parametrize("group_by", GROUP_BYS)
    @pytest.mark.parametrize("period", list(TimeAggregation))
    def test_stage2_pages_joined_equal_one_read(
        self, client: TestClient, period: TimeAggregation, group_by: str
    ) -> None:
        query = f"time-aggregation={period.value}&group-by={group_by}{RANGE}"
        whole = read(client, f"{query}&limit=10000")
        page_size = max(1, len(whole) // 4)

        rows, pages = paged(client, query, page_size)

        assert len(whole) >= 2, "the range should hold more than one row"
        assert pages >= 2
        assert rows == whole


@pytest.mark.usefixtures("usage")
class TestStage2AfterOutsideTheRange:
    @pytest.mark.parametrize("period", list(TimeAggregation))
    def test_stage2_an_after_from_another_range_is_a_404(self, client: TestClient, period: TimeAggregation) -> None:
        query = f"time-aggregation={period.value}&group-by=workspace"
        (cursor, *_) = read(client, f"{query}{RANGE}&limit=1")

        response = client.get(f"/accounting/usage-data?{query}{OTHER_RANGE}&after={cursor['uuid']}")

        assert response.status_code == 404, response.text

    def test_stage2_an_after_from_another_range_is_a_404_per_workspace(self, client: TestClient) -> None:
        query = "time-aggregation=month"
        (cursor, *_) = read(client, f"{query}{RANGE}&limit=1")

        response = client.get(
            f"/workspaces/{cursor['workspace']}/accounting/usage-data?{query}{OTHER_RANGE}&after={cursor['uuid']}"
        )

        assert response.status_code == 404, response.text


@pytest.mark.usefixtures("usage")
class TestStage2RawPagingUnchanged:
    def test_stage2_raw_pages_joined_equal_one_read(self, client: TestClient) -> None:
        whole = read(client, f"limit=10000{RANGE}")

        rows, pages = paged(client, RANGE.lstrip("&"), max(1, len(whole) // 4))

        assert pages >= 2
        assert rows == whole

    def test_stage2_raw_after_from_an_earlier_range_still_pages(self, client: TestClient) -> None:
        """A raw read finds its cursor by primary key wherever it lies, as it always has."""
        (cursor, *_) = read(client, f"limit=1{EARLIER_RANGE}")
        whole = read(client, f"limit=10000{RANGE}")

        response = client.get(f"/accounting/usage-data?limit=10000{RANGE}&after={cursor['uuid']}")

        assert response.status_code == 200, response.text
        assert response.json() == whole


def plan_nodes(plan: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield plan
    for child in plan.get("Plans", []):
        yield from plan_nodes(child)


@pytest.mark.usefixtures("usage")
class TestStage2AfterLookupPlan:
    @pytest.mark.parametrize("period", list(TimeAggregation))
    def test_stage2_the_after_lookup_uses_the_aggregate_index_with_the_range_bounds(
        self, db_session: Session, period: TimeAggregation
    ) -> None:
        """The lookup is the first statement a read with `after` issues. A table this small
        would be read whole by preference, so sequential scans are disabled for the EXPLAIN:
        the question is whether the bounds reach the index, which they cannot if the lookup
        does not apply them."""
        start = datetime(2024, 2, 10, 6, tzinfo=UTC)
        end = datetime(2024, 5, 20, tzinfo=UTC)
        group_by = frozenset({UsageDimension.SKU, UsageDimension.WORKSPACE})
        (first, *_) = BillingEvent.find_billing_events(
            db_session, start=start, end=end, time_aggregation=period, group_by=group_by, limit=1
        )

        statements: list[tuple[str, Any]] = []

        def record(_conn: object, _cursor: object, statement: str, parameters: Any, *_: object) -> None:  # noqa: ANN401
            if statement.lstrip().upper().startswith("SELECT"):
                statements.append((statement, parameters))

        engine = db.get_engine()
        event.listen(engine, "before_cursor_execute", record)
        try:
            list(
                BillingEvent.find_billing_events(
                    db_session,
                    start=start,
                    end=end,
                    time_aggregation=period,
                    group_by=group_by,
                    after=first.event.uuid,
                    limit=1,
                )
            )
        finally:
            event.remove(engine, "before_cursor_execute", record)

        lookup, parameters = statements[0]
        connection = db_session.connection()
        connection.execute(text("SET LOCAL enable_seqscan = off"))
        raw = connection.exec_driver_sql(f"EXPLAIN (FORMAT JSON) {lookup}", parameters).scalar_one()
        document = raw[0] if isinstance(raw, list) else json.loads(raw)[0]
        nodes = list(plan_nodes(document["Plan"]))

        index_name = f"billingevent_{period.value}_aggregate_index"
        on_index = [node for node in nodes if node.get("Index Name") == index_name]

        assert on_index, f"{index_name} is not used: {json.dumps(document['Plan'])}"
        conditions = " ".join(node.get("Index Cond", "") for node in on_index)
        assert "date_trunc" in conditions, f"no range bound on the index: {conditions!r}"
        assert "2024-05-20" in conditions, f"the upper bound is not an index condition: {conditions!r}"
        assert ">" in conditions, f"no lower bound is an index condition: {conditions!r}"
