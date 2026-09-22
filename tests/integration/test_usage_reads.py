"""Filtering and grouping on the two usage-data endpoints (T12).

Here rather than in tests/ because the questions are ones only PostgreSQL answers: what a
GROUP BY collapses, what a filter removes before it collapses, and whether paging still
names one place in the order when the ordering key loses a column. The parsing of the
parameters themselves is in tests/test_api_models.py, which needs no database.

Credits are 0 throughout: these rows are never priced, and what a grouped total does to
credits is asserted in test_credit_ledger.py where the policy fixtures live.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from accounting_service import models
from tests.integration.test_models import gen_billingitem_data

DAY = datetime(2024, 3, 5, tzinfo=UTC)

USER_A = uuid.UUID("aaaaaaaa-0000-4000-8000-000000000001")
USER_B = uuid.UUID("bbbbbbbb-0000-4000-8000-000000000002")
USER_C = uuid.UUID("cccccccc-0000-4000-8000-000000000003")

# Ordered by the text of the UUID, which is how a total's MAX(uuid) is taken.
LOW_UUID = uuid.UUID("00000000-0000-4000-8000-000000000001")
CURSOR_UUID = uuid.UUID("00000000-0000-4000-8000-000000000002")
HIGHER_UUID = uuid.UUID("ffffffff-0000-4000-8000-00000000000e")
HIGH_UUID = uuid.UUID("ffffffff-0000-4000-8000-00000000000f")


@pytest.fixture
def usage(db_session: Session) -> list[uuid.UUID]:
    """Three events in one workspace on one day, spanning two SKUs and two users, plus one
    in a second workspace. Every quantity is distinct, so any row's quantity says exactly
    which events were folded into it.
    """
    event_uuids, _accounts, _items = gen_billingitem_data(
        db_session,
        [
            {"workspace": "ws1", "event_start": DAY.replace(hour=1), "sku": "sku1", "user": USER_A, "quantity": 1.0},
            {"workspace": "ws1", "event_start": DAY.replace(hour=2), "sku": "sku2", "user": USER_A, "quantity": 2.0},
            {"workspace": "ws1", "event_start": DAY.replace(hour=3), "sku": "sku1", "user": USER_B, "quantity": 4.0},
            {"workspace": "ws2", "event_start": DAY.replace(hour=4), "sku": "sku1", "user": USER_A, "quantity": 8.0},
        ],
    )
    db_session.commit()

    return event_uuids


def get(client: TestClient, query: str = "", workspace: str = "ws1") -> list[dict[str, Any]]:
    response = client.get(f"/workspaces/{workspace}/accounting/usage-data{query}")

    assert response.status_code == 200, response.text

    return response.json()


class TestTheSkuFilter:
    def test_it_selects_one_item(self, client: TestClient, usage: list[uuid.UUID]) -> None:
        rows = get(client, "?sku=sku1")

        assert sorted(row["quantity"] for row in rows) == [1.0, 4.0]

    def test_an_unknown_sku_matches_nothing_rather_than_failing(
        self, client: TestClient, usage: list[uuid.UUID]
    ) -> None:
        """A SKU is a string from a table, not a closed set the API can validate against, and
        "no usage of that" is the honest answer to a SKU nothing was ever billed under.
        """
        assert get(client, "?sku=no-such-sku") == []

    def test_it_applies_before_aggregation(self, client: TestClient, usage: list[uuid.UUID]) -> None:
        """The filtered events must be gone from the totals, not merely absent from the rows.
        Filtering afterwards would leave sku2's 2.0 inside a total labelled sku1.
        """
        (row,) = get(client, "?time-aggregation=day&sku=sku1&group-by=")

        assert row["quantity"] == 5.0


class TestTheUserFilter:
    def test_it_selects_one_user(self, client: TestClient, usage: list[uuid.UUID]) -> None:
        rows = get(client, f"?user={USER_A}")

        assert sorted(row["quantity"] for row in rows) == [1.0, 2.0]

    def test_it_applies_before_aggregation(self, client: TestClient, usage: list[uuid.UUID]) -> None:
        (row,) = get(client, f"?time-aggregation=day&user={USER_A}&group-by=")

        assert row["quantity"] == 3.0

    def test_usage_nobody_is_responsible_for_is_excluded(self, client: TestClient, db_session: Session) -> None:
        """Workspace storage is metered against no user. It cannot match a user filter, and
        must not be swept in as a null that compares equal to nothing in particular.
        """
        gen_billingitem_data(
            db_session,
            [
                {"workspace": "ws1", "event_start": DAY, "sku": "sku1", "user": None, "quantity": 9.0},
                {"workspace": "ws1", "event_start": DAY, "sku": "sku1", "user": USER_A, "quantity": 1.0},
            ],
        )
        db_session.commit()

        rows = get(client, f"?user={USER_A}")

        assert [row["quantity"] for row in rows] == [1.0]

    def test_it_combines_with_the_sku_filter(self, client: TestClient, usage: list[uuid.UUID]) -> None:
        rows = get(client, f"?user={USER_A}&sku=sku2")

        assert [row["quantity"] for row in rows] == [2.0]


class TestGroupingDimensions:
    def test_the_default_is_what_it_always_was(self, client: TestClient, usage: list[uuid.UUID]) -> None:
        """One row per SKU per workspace per period. A caller who does not pass `group-by`
        gets the shape it got before there was one.
        """
        rows = get(client, "?time-aggregation=day")

        assert sorted((row["item"], row["quantity"]) for row in rows) == [("sku1", 5.0), ("sku2", 2.0)]
        assert {row["workspace"] for row in rows} == {"ws1"}

    def test_grouping_by_user_merges_across_skus(self, client: TestClient, usage: list[uuid.UUID]) -> None:
        rows = get(client, "?time-aggregation=day&group-by=user")

        assert sorted((row["user"], row["quantity"]) for row in rows) == [
            (str(USER_A), 3.0),
            (str(USER_B), 4.0),
        ]

    @pytest.mark.parametrize(
        ("group_by", "spanned"),
        [
            ("user", ["item", "workspace"]),
            ("sku", ["user", "workspace"]),
            ("workspace", ["item", "user"]),
            ("", ["item", "user", "workspace"]),
        ],
    )
    def test_a_dimension_not_grouped_by_is_null(
        self, client: TestClient, usage: list[uuid.UUID], group_by: str, spanned: list[str]
    ) -> None:
        """Reporting any one of the several values a total spans would be a lie, so the
        answer is null. This is the whole cost of making grouping selectable.
        """
        rows = get(client, f"?time-aggregation=day&group-by={group_by}")

        for row in rows:
            for dimension in spanned:
                assert row[dimension] is None, f"{dimension} should be null when grouping by {group_by!r}"

    def test_the_empty_set_totals_over_the_period_alone(self, client: TestClient, usage: list[uuid.UUID]) -> None:
        (row,) = get(client, "?time-aggregation=day&group-by=")

        assert row["quantity"] == 7.0
        assert row["event_start"] == "2024-03-05T00:00:00Z"
        assert row["event_end"] == "2024-03-06T00:00:00Z"

    def test_grouping_by_workspace_spans_an_account(self, client: TestClient, db_session: Session) -> None:
        """The account read is the one that has more than one workspace to separate."""
        account = uuid.uuid4()
        db_session.add(models.WorkspaceAccount(workspace="ws1", account=account))
        db_session.add(models.WorkspaceAccount(workspace="ws2", account=account))
        gen_billingitem_data(
            db_session,
            [
                {"workspace": "ws1", "event_start": DAY, "sku": "sku1", "quantity": 1.0},
                {"workspace": "ws1", "event_start": DAY, "sku": "sku2", "quantity": 2.0},
                {"workspace": "ws2", "event_start": DAY, "sku": "sku1", "quantity": 4.0},
            ],
        )
        db_session.commit()

        response = client.get(f"/accounts/{account}/accounting/usage-data?time-aggregation=day&group-by=workspace")

        assert response.status_code == 200, response.text
        assert sorted((row["workspace"], row["quantity"]) for row in response.json()) == [("ws1", 3.0), ("ws2", 4.0)]

    def test_grouping_by_everything_keeps_the_events_apart(self, client: TestClient, usage: list[uuid.UUID]) -> None:
        """Nothing here shares all three dimensions, so every event stays its own row."""
        rows = get(client, "?time-aggregation=day&group-by=user,sku,workspace")

        assert sorted(row["quantity"] for row in rows) == [1.0, 2.0, 4.0]


class TestGroupingIsRejectedWhereItMeansNothing:
    def test_without_aggregation(self, client: TestClient) -> None:
        assert client.get("/workspaces/ws1/accounting/usage-data?group-by=user").status_code == 422

    @pytest.mark.parametrize("bad", ["period", "account"])
    def test_a_dimension_that_is_not_one(self, client: TestClient, bad: str) -> None:
        assert client.get(
            f"/workspaces/ws1/accounting/usage-data?time-aggregation=day&group-by={bad}"
        ).status_code == (422)


class TestPagingSurvivesAReducedOrderingKey:
    """The order is (period, workspace, sku, uuid), and a dimension that was not grouped is
    null on every row - it orders nothing, and `after` cannot compare against it. Those
    columns leave the key, so this is the test that what is left still names one place.
    """

    def test_paging_through_a_grouped_read(self, client: TestClient, db_session: Session) -> None:
        gen_billingitem_data(
            db_session,
            [
                {"workspace": "ws1", "event_start": datetime(2024, 3, day, tzinfo=UTC), "quantity": float(day)}
                for day in (5, 6, 7)
            ],
        )
        db_session.commit()

        seen: list[float] = []
        query = "?time-aggregation=day&group-by=&limit=1"

        while page := get(client, query):
            seen.extend(row["quantity"] for row in page)
            query = f"?time-aggregation=day&group-by=&limit=1&after={page[-1]['uuid']}"

        assert seen == [5.0, 6.0, 7.0]

    def test_an_unknown_after_is_still_a_404(self, client: TestClient, usage: list[uuid.UUID]) -> None:
        response = client.get(
            f"/workspaces/ws1/accounting/usage-data?time-aggregation=day&group-by=&after={uuid.uuid4()}"
        )

        assert response.status_code == 404


class TestPagingByUserIsStable:
    """A total's UUID is a MAX over the events folded into it, so it grows as events arrive.
    Every grouped dimension is therefore in the ordering key ahead of it, and user is the one
    of them that can be null.
    """

    def test_an_event_arriving_mid_page_does_not_repeat_a_user(self, client: TestClient, db_session: Session) -> None:
        """Without user in the key the order is the period and then the MAX(uuid) alone. The
        event added here raises user A's maximum past user B's, which moved A behind the
        cursor and returned it on both pages.
        """
        gen_billingitem_data(
            db_session,
            [
                {"workspace": "ws1", "event_start": DAY, "user": USER_A, "quantity": 1.0, "uuid": LOW_UUID},
                {"workspace": "ws1", "event_start": DAY, "user": USER_B, "quantity": 2.0, "uuid": CURSOR_UUID},
                {"workspace": "ws1", "event_start": DAY, "user": USER_C, "quantity": 4.0, "uuid": HIGH_UUID},
            ],
        )
        db_session.commit()

        first = get(client, "?time-aggregation=day&group-by=user&limit=2")

        assert [row["user"] for row in first] == [str(USER_A), str(USER_B)]

        gen_billingitem_data(
            db_session,
            [{"workspace": "ws1", "event_start": DAY, "user": USER_A, "quantity": 8.0, "uuid": HIGHER_UUID}],
        )
        db_session.commit()

        second = get(client, f"?time-aggregation=day&group-by=user&limit=2&after={first[-1]['uuid']}")

        assert [row["user"] for row in second] == [str(USER_C)]

    def test_paging_reaches_the_usage_no_user_is_responsible_for(
        self, client: TestClient, db_session: Session
    ) -> None:
        """A null user sorts last and nothing sorts after it, so it is the page that a
        comparison written for non-null values silently drops.
        """
        gen_billingitem_data(
            db_session,
            [
                {"workspace": "ws1", "event_start": DAY, "user": USER_A, "quantity": 1.0},
                {"workspace": "ws1", "event_start": DAY, "user": USER_B, "quantity": 2.0},
                {"workspace": "ws1", "event_start": DAY, "user": None, "quantity": 4.0},
            ],
        )
        db_session.commit()

        seen: list[float] = []
        query = "?time-aggregation=day&group-by=user&limit=1"

        while page := get(client, query):
            seen.extend(row["quantity"] for row in page)
            query = f"?time-aggregation=day&group-by=user&limit=1&after={page[-1]['uuid']}"

        assert seen == [1.0, 2.0, 4.0]
