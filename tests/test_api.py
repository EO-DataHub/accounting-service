import pprint
import uuid
from datetime import datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlalchemy.orm.session import Session

from accounting_service import app, models
from tests.test_models import gen_billingitem_data


# Mock function for decode_jwt_token
def mock_decode_jwt_token(authorization: str):
    return {
        "workspaces": ["workspace1", "workspace2"],
        "workspaces_owned": ["workspace2"],
        "realm_access": {"roles": ["user", "hub_admin"]},
    }


def test_workspace_usage_data_returns_correct_items_from_db(
    db_session: Session, client: TestClient
):
    ############# Setup
    db_session.execute(delete(models.BillingEvent))
    uid = uuid.uuid4()
    event_uuids, account_uuids, item_uuids = gen_billingitem_data(
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
    with patch.object(app.app, "decode_jwt_token", mock_decode_jwt_token):

        mock_token = "your_mock_jwt_token_here"

        response = client.get(
            "/workspaces/workspace2/accounting/usage-data",
            headers={"Authorization": f"Bearer {mock_token}"},
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


def test_workspace_usage_data_correctly_paged(db_session: Session, client: TestClient):
    ############# Setup
    db_session.execute(delete(models.BillingEvent))
    event_uuids, account_uuids, item_uuids = gen_billingitem_data(
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
    with patch.object(app.app, "decode_jwt_token", mock_decode_jwt_token):
        mock_token = "your_mock_jwt_token_here"

        response_page1 = client.get(
            "/workspaces/workspace1/accounting/usage-data?limit=2",
            headers={"Authorization": f"Bearer {mock_token}"},
        )

        after = response_page1.json()[1]["uuid"]
        response_page2 = client.get(
            f"/workspaces/workspace1/accounting/usage-data?limit=2&after={after}",
            headers={"Authorization": f"Bearer {mock_token}"},
        )

        ############# Behaviour check
        assert response_page1.status_code == 200
        assert response_page2.status_code == 200

        page1 = response_page1.json()
        page2 = response_page2.json()
        assert len(page1) == 2
        assert len(page2) == 1

        # Results should always be in ascending time order.
        assert datetime.fromisoformat(page1[0]["event_start"]) < datetime.fromisoformat(
            page1[1]["event_start"]
        )
        assert datetime.fromisoformat(page1[1]["event_start"]) < datetime.fromisoformat(
            page2[0]["event_start"]
        )


@pytest.mark.parametrize(
    "aggregation,page_size,results",
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
    db_session: Session, client: TestClient, aggregation, page_size, results
):
    ############# Setup
    db_session.execute(delete(models.BillingEvent))
    event_uuids, account_uuids, item_uuids = gen_billingitem_data(
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
    with patch.object(app.app, "decode_jwt_token", mock_decode_jwt_token):
        mock_token = "your_mock_jwt_token_here"

        response_pages = []

        response_pages.append(
            client.get(
                f"/workspaces/workspace1/accounting/usage-data?limit={page_size}&time-aggregation={aggregation}",
                headers={"Authorization": f"Bearer {mock_token}"},
            )
        )

        after = response_pages[0].json()[-1]["uuid"]
        response_pages.append(
            client.get(
                f"/workspaces/workspace1/accounting/usage-data?limit={page_size}&after={after}&time-aggregation={aggregation}",
                headers={"Authorization": f"Bearer {mock_token}"},
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
                print(f"{response_json=}, {expected_json=}")
                assert response_json[i]["item"] == expected_json[i]["item"]
                assert response_json[i]["quantity"] == expected_json[i]["quantity"]
                assert response_json[i]["event_start"] == expected_json[i]["event_start"]


def test_account_usage_data_returns_correct_items_from_db(db_session: Session, client: TestClient):
    ############# Setup
    db_session.execute(delete(models.BillingEvent))

    account_uuid = uuid.uuid4()
    db_session.add(models.WorkspaceAccount(workspace="workspace1", account=account_uuid))
    db_session.add(models.WorkspaceAccount(workspace="workspace3", account=account_uuid))

    uid = uuid.uuid4()
    event_uuids, account_uuids, item_uuids = gen_billingitem_data(
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
    with patch.object(app.app, "decode_jwt_token", mock_decode_jwt_token):
        mock_token = "your_mock_jwt_token_here"
        response = client.get(
            f"/accounts/{account_uuid}/accounting/usage-data",
            headers={"Authorization": f"Bearer {mock_token}"},
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


def test_skus_list_api_returns_items_correctly(db_session: Session, client: TestClient):
    ############# Setup
    db_session.execute(delete(models.BillingEvent))
    db_session.execute(delete(models.BillingItem))

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


def test_skus_api_returns_item_correctly(db_session: Session, client: TestClient):
    ############# Setup
    db_session.execute(delete(models.BillingEvent))
    db_session.execute(delete(models.BillingItem))

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


def test_skus_api_returns_404_for_unknown_item(db_session: Session, client: TestClient):
    ############# Test
    response = client.get("/accounting/skus/nonexistent-sku")

    ############# Behaviour check
    assert response.status_code == 404
    assert response.json() == {"detail": "SKU not known"}


def test_prices_api_returns_current_prices_correctly(db_session: Session, client: TestClient):
    ############# Setup
    db_session.execute(delete(models.BillingItemPrice))

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
            price=2.34,
            valid_from=datetime(2024, 1, 16, 0, 0, 0),
            configured_at=datetime(2024, 1, 16, 0, 0, 0),
            item_id=uuid_item_a,
        )
    )

    db_session.add(
        models.BillingItemPrice(
            uuid=uuid_price2,
            price=2.30,
            valid_from=datetime(2023, 1, 16, 0, 0, 0),
            valid_until=datetime(2024, 1, 16, 0, 0, 0),
            configured_at=datetime(2023, 1, 16, 0, 0, 0),
            item_id=uuid_item_a,
        )
    )

    db_session.add(
        models.BillingItemPrice(
            uuid=uuid_price3,
            price=0.000000412,
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
            "price": 2.34,
            "valid_from": "2024-01-16T00:00:00Z",
            "valid_until": None,
            "sku": "sku1",
        },
        {
            "uuid": str(uuid_price3),
            "price": 0.000000412,
            "valid_from": "2023-01-16T00:00:00Z",
            "valid_until": None,
            "sku": "sku2",
        },
    ]
