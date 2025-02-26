import uuid
from datetime import datetime

from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlalchemy.orm.session import Session

from accounting_service import models
from tests.test_models import gen_billingitem_data


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
    response = client.get("/workspaces/workspace2/accounting/usage-data")

    ############# Behaviour check
    assert response.status_code == 200
    assert response.json() == [
        {
            "uuid": str(event_uuids[1]),
            "event_start": "2024-01-16T07:05:00Z",
            "event_end": "2024-01-16T07:10:00Z",
            "item": "sku2",
            "user": str(uid),
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
    response_page1 = client.get("/workspaces/workspace1/accounting/usage-data?limit=2")

    after = response_page1.json()[1]["uuid"]
    response_page2 = client.get(
        f"/workspaces/workspace1/accounting/usage-data?limit=2&after={after}"
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


def test_account_usage_data_returns_correct_items_from_db(db_session: Session, client: TestClient):
    ############# Setup
    db_session.execute(delete(models.BillingEvent))

    account_uuid = uuid.uuid4()
    db_session.add(models.WorkspaceAccount(workspace="workspace1", account=account_uuid))

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
        ],
    )

    ############# Test
    response = client.get(f"/accounts/{account_uuid}/accounting/usage-data")

    ############# Behaviour check
    assert response.status_code == 200
    assert response.json() == [
        {
            "uuid": str(event_uuids[0]),
            "event_start": "2024-01-16T06:10:00Z",
            "event_end": "2024-01-16T06:15:00Z",
            "item": "sku1",
            "user": str(uid),
            "workspace": "workspace1",
            "quantity": 1.1,
        }
    ]
