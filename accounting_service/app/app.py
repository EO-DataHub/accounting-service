import logging
import os
from datetime import datetime
from typing import Annotated, Iterator
from uuid import UUID

from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from accounting_service.db import get_session
from accounting_service.models import BillingEvent

logger = logging.getLogger(__name__)

root_path = os.environ.get("ROOT_PATH", "/api/")

SessionDep = Annotated[Session, Depends(get_session)]

app = FastAPI(root_path=root_path)

# This server serves three areas of the API:
#
#   * /api/workspaces/{workspace-id}/accounting/: Data about a specific workspace
#   * /api/accounts/{account-id}/accounting/: Data about all workspaces in a specific account
#   * /api/accounting/: Data not specific to any account or workspace (prices and billing items)
#
# The sub-paths within the first two are the same, we just filter the data differently.


def billingevent_to_api_object(event: BillingEvent):
    return {
        "uuid": event.uuid,
        "event_start": event.event_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event_end": event.event_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "item": event.item.sku,
        "workspace": event.workspace,
        "quantity": event.quantity,
        "user": event.user,
    }


@app.get("/workspaces/{workspace}/accounting/usage-data")
def get_workspace_usage_data(
    session: SessionDep,
    workspace: str,
    start: datetime = None,
    end: datetime = None,
    limit=100,
    after=None,
):
    """This returns usage data for a workspace or account within some given time range (or all)."""
    events: Iterator[BillingEvent] = BillingEvent.find_billing_events(
        session, workspace=workspace, start=start, end=end, limit=limit, after=after
    )

    return list(map(billingevent_to_api_object, events))


@app.get("/accounts/{account_id}/accounting/usage-data")
def get_account_usage_data(
    session: SessionDep,
    account_id: UUID,
    start: datetime = None,
    end: datetime = None,
    limit=100,
    after=None,
):
    """This returns usage data for a workspace or account within some given time range (or all)."""
    events: Iterator[BillingEvent] = BillingEvent.find_billing_events(
        session, account=account_id, start=start, end=end, limit=limit, after=after
    )

    return list(map(billingevent_to_api_object, events))
