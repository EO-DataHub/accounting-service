import pprint
import uuid
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from http import HTTPStatus
from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm.session import Session

from accounting_service import models
from tests.conftest import (
    TOKEN_ADMIN,
    TOKEN_HUB_ADMIN,
    TOKEN_MEMBER,
    TOKEN_NO_REALM_ACCESS,
    TOKEN_OWNER,
    TOKEN_SCALAR_CLAIM,
    TOKEN_STRANGER,
)
from tests.test_models import gen_billingitem_data

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


def test_skus_list_api_returns_items_correctly(db_session: Session, client: TestClient) -> None:
    ############# Setup

    uuid_sku1 = uuid.uuid4()
    uuid_sku2 = uuid.uuid4()

    db_session.add(models.BillingItem(uuid=uuid_sku1, sku="sku1", name="Item 1", unit="GBh"))
    db_session.add(models.BillingItem(uuid=uuid_sku2, sku="sku2", name="Item 2", unit="S"))

    ############# Test
    response = client.get("/accounting/skus")

    ############# Behaviour check
    # Should get a list of all billing items in SKU order.
    pprint.pprint(response.json())
    assert response.status_code == 200
    assert response.json() == [
        {"uuid": str(uuid_sku1), "sku": "sku1", "name": "Item 1", "unit": "GBh"},
        {"uuid": str(uuid_sku2), "sku": "sku2", "name": "Item 2", "unit": "S"},
    ]


def test_skus_api_returns_item_correctly(db_session: Session, client: TestClient) -> None:
    ############# Setup

    uuid_sku1 = uuid.uuid4()

    db_session.add(models.BillingItem(uuid=uuid_sku1, sku="sku1", name="Item 1", unit="GBh"))

    ############# Test
    response = client.get("/accounting/skus/sku1")

    ############# Behaviour check
    assert response.status_code == 200
    assert response.json() == {
        "uuid": str(uuid_sku1),
        "sku": "sku1",
        "name": "Item 1",
        "unit": "GBh",
    }


def test_skus_api_returns_404_for_unknown_item(db_session: Session, client: TestClient) -> None:
    ############# Test
    response = client.get("/accounting/skus/nonexistent-sku")

    ############# Behaviour check
    assert response.status_code == 404
    assert response.json() == {"detail": "SKU not known"}


@pytest.mark.parametrize(
    ("claims", "workspace", "expected_status"),
    [
        # hub_admin overrides every tier, including for a workspace it holds no
        # claim for at all.
        pytest.param(TOKEN_HUB_ADMIN, "workspace1", 200, id="hub-admin"),
        pytest.param(TOKEN_HUB_ADMIN, "never-heard-of-it", 200, id="hub-admin-unknown-workspace"),
        # A plain member reaches its own workspace and no others.
        pytest.param(TOKEN_MEMBER, "workspace1", 200, id="member-own-workspace"),
        pytest.param(TOKEN_MEMBER, "workspace2", 401, id="member-other-workspace"),
        # Tier ordering: neither of these tokens lists the workspace under
        # 'workspaces', so both rely on outranking the MEMBER minimum.
        pytest.param(TOKEN_OWNER, "workspace2", 200, id="owner-outranks-member"),
        pytest.param(TOKEN_ADMIN, "workspace1", 200, id="admin-outranks-member"),
        pytest.param(TOKEN_STRANGER, "workspace1", 401, id="stranger"),
        # 'workspace1' is a substring of the scalar claim 'workspace1-prod'. A
        # bare `in` test against a string would grant owner access here.
        pytest.param(TOKEN_SCALAR_CLAIM, "workspace1", 401, id="scalar-claim-is-not-substring-matched"),
        # A missing realm_access must deny, not raise.
        pytest.param(TOKEN_NO_REALM_ACCESS, "workspace1", 200, id="no-realm-access-still-a-member"),
        pytest.param(TOKEN_NO_REALM_ACCESS, "workspace2", 401, id="no-realm-access-non-member"),
    ],
)
def test_workspace_usage_data_enforces_minimum_tier(
    client: TestClient,
    authenticate_as: Callable[[dict[str, Any]], None],
    claims: dict[str, Any],
    workspace: str,
    expected_status: int,
) -> None:
    """The usage-data endpoint requires MEMBER, so every tier at or above it passes.

    These assert status codes only. An authorised request against a workspace
    with no billing events is a 200 with an empty list, so no fixture data is
    needed and the test stays about authorisation.
    """
    authenticate_as(claims)

    response = client.get(
        f"/workspaces/{workspace}/accounting/usage-data",
        headers=AUTH_HEADERS,
    )

    assert response.status_code == expected_status


def test_account_usage_data_denies_unrelated_account(
    client: TestClient,
    authenticate_as: Callable[[dict[str, Any]], None],
) -> None:
    """Account access comes from the billing-accounts claim, not workspace tiers."""
    authenticate_as(TOKEN_MEMBER)

    response = client.get(
        f"/accounts/{uuid.uuid4()}/accounting/usage-data",
        headers=AUTH_HEADERS,
    )

    assert response.status_code == 401


def test_account_usage_data_allows_listed_account(
    client: TestClient,
    authenticate_as: Callable[[dict[str, Any]], None],
) -> None:
    account_uuid = uuid.uuid4()

    authenticate_as(
        {
            "workspaces": [],
            "billing-accounts": [str(account_uuid)],
            "realm_access": {"roles": ["user"]},
        }
    )

    response = client.get(
        f"/accounts/{account_uuid}/accounting/usage-data",
        headers=AUTH_HEADERS,
    )

    assert response.status_code == 200


def test_prices_api_returns_current_prices_correctly(db_session: Session, client: TestClient) -> None:
    ############# Setup

    uuid_item_a = uuid.uuid4()
    uuid_item_b = uuid.uuid4()
    db_session.add(models.BillingItem(uuid=uuid_item_a, sku="sku1", name="Item a", unit="GBh"))
    db_session.add(models.BillingItem(uuid=uuid_item_b, sku="sku2", name="Item b", unit="GBh"))

    uuid_price1 = uuid.uuid4()
    uuid_price2 = uuid.uuid4()
    uuid_price3 = uuid.uuid4()

    db_session.add(
        models.BillingItemPrice(
            uuid=uuid_price1,
            price=Decimal("2.34"),
            valid_from=datetime(2024, 1, 16, 0, 0, 0),
            configured_at=datetime(2024, 1, 16, 0, 0, 0),
            item_id=uuid_item_a,
        )
    )

    db_session.add(
        models.BillingItemPrice(
            uuid=uuid_price2,
            price=Decimal("2.30"),
            valid_from=datetime(2023, 1, 16, 0, 0, 0),
            valid_until=datetime(2024, 1, 16, 0, 0, 0),
            configured_at=datetime(2023, 1, 16, 0, 0, 0),
            item_id=uuid_item_a,
        )
    )

    db_session.add(
        models.BillingItemPrice(
            uuid=uuid_price3,
            price=Decimal("0.000000412"),
            valid_from=datetime(2023, 1, 16, 0, 0, 0),
            configured_at=datetime(2023, 1, 17, 0, 0, 0),
            item_id=uuid_item_b,
        )
    )

    ############# Test
    response = client.get("/accounting/prices")

    ############# Behaviour check
    # Should return current prices in SKU order.
    assert response.status_code == 200
    assert response.json() == [
        {
            "uuid": str(uuid_price1),
            # An exact decimal string, not a float: the back end keeps the precision and the
            # UI decides how to display it.
            "price": "2.34",
            "valid_from": "2024-01-16T00:00:00Z",
            "valid_until": None,
            "sku": "sku1",
        },
        {
            "uuid": str(uuid_price3),
            # Never scientific notation, which is what Pydantic's own Decimal output gives.
            "price": "0.000000412",
            "valid_from": "2023-01-16T00:00:00Z",
            "valid_until": None,
            "sku": "sku2",
        },
    ]
