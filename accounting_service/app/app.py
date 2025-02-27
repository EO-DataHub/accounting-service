import logging
import os
from datetime import datetime
from typing import Annotated, Iterator
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from accounting_service.db import get_session
from accounting_service.models import BillingEvent, BillingItem

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


def billingitem_to_api_object(item: BillingItem):
    return {
        "uuid": str(item.uuid),
        "sku": item.sku,
        "name": item.name,
        "unit": item.unit,
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


@app.get("/accounting/skus")
def get_item_list(session: SessionDep):
    """This returns all BillingItems in SKU order."""
    items: Iterator[BillingItem] = BillingItem.find_billing_items(session)
    return list(map(billingitem_to_api_object, items))


@app.get("/accounting/skus/{sku}")
def get_item(session: SessionDep, sku: str):
    """This returns a specific BillingItem based on its SKU."""
    item: Iterator[BillingItem] = BillingItem.find_billing_item(session, sku)

    if item is None:
        raise HTTPException(status_code=404, detail="SKU not known")

    return billingitem_to_api_object(item)
