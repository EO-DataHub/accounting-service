import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from decimal import Decimal
from http import HTTPStatus
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm.session import Session

from accounting_service import models
from tests.conftest import (
    TOKEN_MEMBER,
    TOKEN_STRANGER,
)
from tests.integration.test_models import gen_billingitem_data

# The `client` fixture authenticates as a hub_admin, which satisfies every tier
# check. Tests below that care about authorisation use `authenticate_as`.

MOCK_TOKEN = "your_mock_jwt_token_here"
AUTH_HEADERS = {"Authorization": f"Bearer {MOCK_TOKEN}"}


def test_workspace_usage_data_returns_correct_items_from_db(db_session: Session, client: TestClient) -> None:
    ############# Setup
    uid = uuid.uuid4()
    event_uuids, _account_uuids, _item_uuids = gen_billingitem_data(
        db_session,
        [
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 6, 10, 0),
                "sku": "sku1",
            },
            {
                "workspace": "workspace2",
                "event_start": datetime(2024, 1, 16, 7, 5, 0),
                "sku": "sku2",
                "quantity": "1.23",
                "user": uid,
            },
        ],
    )

    ############# Test
    response = client.get(
        "/workspaces/workspace2/accounting/usage-data",
        headers=AUTH_HEADERS,
    )

    ############# Behaviour check
    assert response.status_code == 200
    assert response.json() == [
        {
            "uuid": str(event_uuids[1]),
            "event_start": "2024-01-16T07:05:00Z",
            "event_end": "2024-01-16T07:10:00Z",
            "item": "sku2",
            "workspace": "workspace2",
            "quantity": 1.23,
        }
    ]


def test_workspace_usage_data_correctly_paged(db_session: Session, client: TestClient) -> None:
    ############# Setup
    _event_uuids, _account_uuids, _item_uuids = gen_billingitem_data(
        db_session,
        [
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 6, 10, 0),
                "sku": "sku1",
            },
            {
                "workspace": "workspace3",
                "event_start": datetime(2024, 1, 16, 7, 5, 0),
                "sku": "sku3",
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 7, 10, 0),
                "sku": "sku2",
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 8, 10, 0),
                "sku": "sku1",
            },
        ],
    )

    ############# Test
    response_page1 = client.get(
        "/workspaces/workspace1/accounting/usage-data?limit=2",
        headers=AUTH_HEADERS,
    )

    after = response_page1.json()[1]["uuid"]
    response_page2 = client.get(
        f"/workspaces/workspace1/accounting/usage-data?limit=2&after={after}",
        headers=AUTH_HEADERS,
    )

    ############# Behaviour check
    assert response_page1.status_code == 200
    assert response_page2.status_code == 200

    page1 = response_page1.json()
    page2 = response_page2.json()
    assert len(page1) == 2
    assert len(page2) == 1

    # Results should always be in ascending time order.
    assert datetime.fromisoformat(page1[0]["event_start"]) < datetime.fromisoformat(page1[1]["event_start"])
    assert datetime.fromisoformat(page1[1]["event_start"]) < datetime.fromisoformat(page2[0]["event_start"])


def test_page_after_unknown_event_produces_404(db_session: Session, client: TestClient) -> None:
    response = client.get(
        "/workspaces/workspace1/accounting/usage-data?after=a659b597-7522-411d-a2e0-23f7f5629b16",
        headers=AUTH_HEADERS,
    )

    assert response.status_code == HTTPStatus.NOT_FOUND


@pytest.mark.parametrize(
    ("aggregation", "page_size", "results"),
    [
        pytest.param(
            "",
            100,
            [
                [
                    {"event_start": "2025-01-01T00:00:00Z", "item": "sku1", "quantity": 0.01},
                    {"event_start": "2025-01-01T02:00:00Z", "item": "sku1", "quantity": 0.1},
                    {"event_start": "2025-01-01T02:00:00Z", "item": "sku2", "quantity": 0.2},
                    {"event_start": "2025-01-01T23:00:00Z", "item": "sku1", "quantity": 1.0},
                    {"event_start": "2025-01-02T02:00:00Z", "item": "sku1", "quantity": 0.2},
                    {"event_start": "2025-02-02T00:00:00Z", "item": "sku1", "quantity": 0.4},
                ],
                [],
            ],
        ),
        pytest.param(
            "day",
            100,
            [
                [
                    {"event_start": "2025-01-01T00:00:00Z", "item": "sku1", "quantity": 1.11},
                    {"event_start": "2025-01-01T00:00:00Z", "item": "sku2", "quantity": 0.2},
                    {"event_start": "2025-01-02T00:00:00Z", "item": "sku1", "quantity": 0.2},
                    {"event_start": "2025-02-02T00:00:00Z", "item": "sku1", "quantity": 0.4},
                ],
                [],
            ],
        ),
        pytest.param(
            "day",
            3,
            [
                [
                    {"event_start": "2025-01-01T00:00:00Z", "item": "sku1", "quantity": 1.11},
                    {"event_start": "2025-01-01T00:00:00Z", "item": "sku2", "quantity": 0.2},
                    {"event_start": "2025-01-02T00:00:00Z", "item": "sku1", "quantity": 0.2},
                ],
                [
                    {"event_start": "2025-02-02T00:00:00Z", "item": "sku1", "quantity": 0.4},
                ],
            ],
        ),
        pytest.param(
            "day",
            2,
            [
                [
                    {"event_start": "2025-01-01T00:00:00Z", "item": "sku1", "quantity": 1.11},
                    {"event_start": "2025-01-01T00:00:00Z", "item": "sku2", "quantity": 0.2},
                ],
                [
                    {"event_start": "2025-01-02T00:00:00Z", "item": "sku1", "quantity": 0.2},
                    {"event_start": "2025-02-02T00:00:00Z", "item": "sku1", "quantity": 0.4},
                ],
            ],
        ),
        pytest.param(
            "month",
            100,
            [
                [
                    {"event_start": "2025-01-01T00:00:00Z", "item": "sku1", "quantity": 1.31},
                    {"event_start": "2025-01-01T00:00:00Z", "item": "sku2", "quantity": 0.2},
                    {"event_start": "2025-02-01T00:00:00Z", "item": "sku1", "quantity": 0.4},
                ],
                [],
            ],
        ),
    ],
)
def test_workspace_usage_data_correctly_time_aggregated(
    db_session: Session, client: TestClient, aggregation: str, page_size: int, results: list[list[dict[str, Any]]]
) -> None:
    ############# Setup
    _event_uuids, _account_uuids, _item_uuids = gen_billingitem_data(
        db_session,
        [
            {
                "workspace": "workspace1",
                "event_start": datetime(2025, 1, 1, 0, 0, 0),
                "event_end": datetime(2025, 1, 1, 1, 0, 0),
                "quantity": 0.01,
                "sku": "sku1",
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2025, 1, 1, 2, 0, 0),
                "event_end": datetime(2025, 1, 1, 3, 0, 0),
                "quantity": 0.1,
                "sku": "sku1",
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2025, 1, 1, 23, 0, 0),
                "event_end": datetime(2025, 1, 2, 0, 0, 0),
                "quantity": 1,
                "sku": "sku1",
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2025, 1, 2, 2, 0, 0),
                "event_end": datetime(2025, 1, 2, 3, 0, 0),
                "quantity": 0.2,
                "sku": "sku1",
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2025, 2, 2, 0, 0, 0),
                "event_end": datetime(2025, 2, 3, 0, 0, 0),
                "quantity": 0.4,
                "sku": "sku1",
            },
            {
                "workspace": "workspace1",
                "event_start": datetime(2025, 1, 1, 2, 0, 0),
                "event_end": datetime(2025, 1, 1, 3, 0, 0),
                "quantity": 0.2,
                "sku": "sku2",
            },
            {
                "workspace": "workspace2",
                "event_start": datetime(2025, 1, 1, 2, 0, 0),
                "event_end": datetime(2025, 1, 1, 3, 0, 0),
                "quantity": 0.5,
                "sku": "sku2",
            },
        ],
    )

    db_session.flush()

    ############# Test
    # Omitted rather than sent empty. The parameter is a closed enum, so "" is rejected like
    # any other value that is not a period.
    aggregation_param = f"&time-aggregation={aggregation}" if aggregation else ""

    response_pages = [
        client.get(
            f"/workspaces/workspace1/accounting/usage-data?limit={page_size}{aggregation_param}",
            headers=AUTH_HEADERS,
        )
    ]

    after = response_pages[0].json()[-1]["uuid"]
    response_pages.append(
        client.get(
            f"/workspaces/workspace1/accounting/usage-data?limit={page_size}&after={after}{aggregation_param}",
            headers=AUTH_HEADERS,
        )
    )

    ############# Behaviour check
    for page in [0, 1]:
        response_page = response_pages[page]

        assert response_page.status_code == 200

        response_json = response_page.json()
        expected_json = results[page]

        assert len(response_json) == len(expected_json)
        for i in range(len(response_json)):
            print(f"{response_json[i]=}, {expected_json[i]=}")
            assert response_json[i]["item"] == expected_json[i]["item"]
            assert response_json[i]["quantity"] == expected_json[i]["quantity"]
            assert response_json[i]["event_start"] == expected_json[i]["event_start"]


def test_account_usage_data_returns_correct_items_from_db(db_session: Session, client: TestClient) -> None:
    ############# Setup

    account_uuid = uuid.uuid4()
    db_session.add(models.WorkspaceAccount(workspace="workspace1", account=account_uuid))
    db_session.add(models.WorkspaceAccount(workspace="workspace3", account=account_uuid))

    uid = uuid.uuid4()
    event_uuids, _account_uuids, _item_uuids = gen_billingitem_data(
        db_session,
        [
            {
                "workspace": "workspace1",
                "event_start": datetime(2024, 1, 16, 6, 10, 0),
                "sku": "sku1",
                "user": uid,
            },
            {
                "workspace": "workspace2",
                "event_start": datetime(2024, 1, 16, 7, 5, 0),
                "sku": "sku2",
                "quantity": "1.23",
            },
            {
                "workspace": "workspace3",
                "event_start": datetime(2024, 1, 16, 7, 5, 0),
                "sku": "sku3",
                "user": uid,
            },
        ],
    )

    ############# Test
    response = client.get(
        f"/accounts/{account_uuid}/accounting/usage-data",
        headers=AUTH_HEADERS,
    )

    ############# Behaviour check
    # We should get data for workspaces 1 and 3 only, in event_start time order.
    assert response.status_code == 200
    assert response.json() == [
        {
            "uuid": str(event_uuids[0]),
            "event_start": "2024-01-16T06:10:00Z",
            "event_end": "2024-01-16T06:15:00Z",
            "item": "sku1",
            "workspace": "workspace1",
            "quantity": 1.1,
        },
        {
            "uuid": str(event_uuids[2]),
            "event_start": "2024-01-16T07:05:00Z",
            "event_end": "2024-01-16T07:10:00Z",
            "item": "sku3",
            "workspace": "workspace3",
            "quantity": 1.1,
        },
    ]


def test_skus_list_api_returns_items_in_sku_order(db_session: Session, client: TestClient) -> None:
    """The endpoint queries, orders by SKU, and serialises through BillingItemAPIResult.

    Which fields the model maps is tested in tests/test_api_models.py against in-memory
    objects. What only a database can show is the ordering, and that the handler's return
    value passes response validation.
    """
    ############# Setup
    db_session.add(models.BillingItem(uuid=uuid.uuid4(), sku="sku2", name="Item 2", unit="S"))
    db_session.add(models.BillingItem(uuid=uuid.uuid4(), sku="sku1", name="Item 1", unit="GBh"))

    ############# Test
    response = client.get("/accounting/skus")

    ############# Behaviour check
    assert response.status_code == 200
    assert [item["sku"] for item in response.json()] == ["sku1", "sku2"]


def test_skus_api_returns_the_requested_item(db_session: Session, client: TestClient) -> None:
    """Lookup by SKU reaches the right row. Field mapping is covered elsewhere."""
    ############# Setup
    wanted = uuid.uuid4()
    db_session.add(models.BillingItem(uuid=wanted, sku="sku1", name="Item 1", unit="GBh"))
    db_session.add(models.BillingItem(uuid=uuid.uuid4(), sku="sku2", name="Item 2", unit="S"))

    ############# Test
    response = client.get("/accounting/skus/sku1")

    ############# Behaviour check
    assert response.status_code == 200
    assert response.json()["uuid"] == str(wanted)


def test_skus_api_returns_404_for_unknown_item(db_session: Session, client: TestClient) -> None:
    ############# Test
    response = client.get("/accounting/skus/nonexistent-sku")

    ############# Behaviour check
    assert response.status_code == 404
    assert response.json() == {"detail": "SKU not known"}


@pytest.mark.parametrize(
    ("claims", "expected_status"),
    [
        pytest.param(TOKEN_MEMBER, 200, id="a-member-gets-through"),
        pytest.param(TOKEN_STRANGER, 401, id="a-stranger-does-not"),
    ],
)
def test_usage_data_is_behind_the_workspace_dependency(
    client: TestClient,
    authenticate_as: Callable[[dict[str, Any]], None],
    claims: dict[str, Any],
    expected_status: int,
) -> None:
    """The route actually has require_workspace attached, and it both grants and denies.

    Which tier grants what is tested in tests/test_authz.py, against the functions
    directly, with no database. This pair exists only to show the dependency is wired to
    the route - a decorator that was never added would leave those tests passing and the
    endpoint open.
    """
    authenticate_as(claims)

    response = client.get("/workspaces/workspace1/accounting/usage-data", headers=AUTH_HEADERS)

    assert response.status_code == expected_status


def test_account_usage_data_is_behind_the_account_dependency(
    client: TestClient,
    authenticate_as: Callable[[dict[str, Any]], None],
) -> None:
    """require_account is attached to the account route. See the note above."""
    authenticate_as(TOKEN_MEMBER)

    response = client.get(f"/accounts/{uuid.uuid4()}/accounting/usage-data", headers=AUTH_HEADERS)

    assert response.status_code == 401


def test_prices_api_returns_only_the_currently_valid_prices(db_session: Session, client: TestClient) -> None:
    """find_prices excludes a price whose validity has ended, and orders by SKU.

    The decimal formatting and the null valid_until are covered in
    tests/test_api_models.py; the filter and the ordering need a query.
    """
    ############# Setup
    uuid_item_a = uuid.uuid4()
    uuid_item_b = uuid.uuid4()
    db_session.add(models.BillingItem(uuid=uuid_item_a, sku="sku1", name="Item a", unit="GBh"))
    db_session.add(models.BillingItem(uuid=uuid_item_b, sku="sku2", name="Item b", unit="GBh"))

    current_a = uuid.uuid4()
    superseded_a = uuid.uuid4()
    current_b = uuid.uuid4()

    db_session.add(
        models.BillingItemPrice(
            uuid=current_a,
            price=Decimal("2.34"),
            valid_from=datetime(2024, 1, 16, 0, 0, 0),
            configured_at=datetime(2024, 1, 16, 0, 0, 0),
            item_id=uuid_item_a,
        )
    )
    db_session.add(
        models.BillingItemPrice(
            uuid=superseded_a,
            price=Decimal("2.30"),
            valid_from=datetime(2023, 1, 16, 0, 0, 0),
            valid_until=datetime(2024, 1, 16, 0, 0, 0),
            configured_at=datetime(2023, 1, 16, 0, 0, 0),
            item_id=uuid_item_a,
        )
    )
    db_session.add(
        models.BillingItemPrice(
            uuid=current_b,
            price=Decimal("0.000000412"),
            valid_from=datetime(2023, 1, 16, 0, 0, 0),
            configured_at=datetime(2023, 1, 17, 0, 0, 0),
            item_id=uuid_item_b,
        )
    )

    ############# Test
    response = client.get("/accounting/prices")

    ############# Behaviour check
    assert response.status_code == 200
    assert [(p["uuid"], p["sku"]) for p in response.json()] == [
        (str(current_a), "sku1"),
        (str(current_b), "sku2"),
    ]


def test_usage_data_query_count_does_not_grow_with_the_page(
    db_session: Session,
    client: TestClient,
    counting_selects: Callable[[], AbstractContextManager[list[str]]],
) -> None:
    """Reading a page of usage data costs the same number of queries whatever its size.

    The join in find_billing_events exists for the ordering and paging predicates and does
    not populate `item`, so reading the SKU on the way out used to cost one query per row: a
    page of 100 events issued 101 queries. A selectinload fixes it, and nothing else in the
    suite would notice if it were removed, because the responses stay correct either way.

    Asserted as a shape rather than a magic number: the count must not depend on the number
    of rows. Distinct SKUs per event, so a lazy load could not be served from the identity
    map and would show up here.
    """
    ############# Setup
    gen_billingitem_data(
        db_session,
        [
            {
                "workspace": "wsCount",
                "event_start": datetime(2025, 1, 1, 0, 0) + timedelta(minutes=minute),
                "sku": f"count-sku{minute}",
            }
            for minute in range(40)
        ],
    )
    db_session.commit()

    ############# Test
    counts = {}
    for limit in (4, 40):
        with counting_selects() as statements:
            response = client.get(
                f"/workspaces/wsCount/accounting/usage-data?limit={limit}",
                headers=AUTH_HEADERS,
            )
        assert len(response.json()) == limit
        counts[limit] = len(statements)

    ############# Behaviour check
    assert counts[4] == counts[40], f"query count grew with the page size: {counts}"
    assert counts[40] <= 3, f"expected a small constant number of queries, got {counts[40]}"
