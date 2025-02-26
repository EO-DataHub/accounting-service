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
    response = client.get("/workspaces/workspace2/accounting/usage-data")

    from pprint import pprint

    pprint(response.json())
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
