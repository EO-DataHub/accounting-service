import logging
import os
from datetime import datetime, timezone
from typing import Annotated, Iterator, List, Optional
from uuid import UUID

import jwt
from eodhp_utils.runner import log_component_version, setup_logging
from fastapi import Depends, FastAPI, Header, HTTPException, Path, Query, Response
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from pydantic import BaseModel, Field
from sqlalchemy import Result, Row
from sqlalchemy.orm import Session

from accounting_service.db import get_session
from accounting_service.models import (
    BillingEvent,
    BillingItem,
    BillingItemPrice,
    datetime_default_to_utc,
)

logger = logging.getLogger(__name__)

setup_logging(verbosity=1)
log_component_version("accounting-service")


root_path = os.environ.get("ROOT_PATH", "/api/")

SessionDep = Annotated[Session, Depends(get_session)]

app = FastAPI(root_path=root_path)

FastAPIInstrumentor.instrument_app(app)

# This server serves three areas of the API:
#
#   * /api/workspaces/{workspace-id}/accounting/: Data about a specific workspace
#   * /api/accounts/{account-id}/accounting/: Data about all workspaces in a specific account
#   * /api/accounting/: Data not specific to any account or workspace (prices and billing items)
#
# The sub-paths within the first two are the same, we just filter the data differently.


class BillingEventAPIResult(BaseModel):
    """
    Billing events represent the consumption of a chargeable resource, often over some time
    period. Where consumption happens at a single timepoint, the start and end times will
    be identical.

    All consumption happens within a specific workspace and all charges are attributed to
    a single workspace.
    """

    uuid: UUID
    event_start: Annotated[
        datetime,
        Field(description="Start time of resource consumption", examples=["2025-02-12T13:34:22Z"]),
    ]
    event_end: Annotated[
        datetime,
        Field(description="End time of resource consumption", examples=["2025-02-12T13:34:22Z"]),
    ]
    item: Annotated[str, Field(description="Item (SKU) consumed", examples=["wfcpu"])]
    workspace: Annotated[
        str, Field(description="Workspace which consumed the resource", examples=["my-workspace"])
    ]
    quantity: Annotated[
        float,
        Field(
            description="Quantity consumed in the units defined in the item definition",
            examples=["0.42"],
        ),
    ]


def billingevent_to_api_object(event: BillingEvent):
    return {
        "uuid": event.uuid,
        "event_start": event.event_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "event_end": event.event_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "item": event.item.sku,
        "workspace": event.workspace,
        "quantity": event.quantity,
    }


class BillingItemAPIResult(BaseModel):
    """
    A billing item is a product you can buy from EO DataHub, like CPU time.
    """

    uuid: UUID
    sku: Annotated[
        str,
        Field(
            description="Human-readable codename (SKU/stock-keeping unit) for the item",
            examples=["wfcpu"],
        ),
    ]
    name: Annotated[
        str,
        Field(description="Human-readable name for the item", examples=["Workflow CPU seconds"]),
    ]
    unit: Annotated[str, Field(description="Unit the item is priced in", examples=["GB-months"])]


def billingitem_to_api_object(item: BillingItem):
    return {
        "uuid": str(item.uuid),
        "sku": item.sku,
        "name": item.name,
        "unit": item.unit,
    }


class BillingItemPriceAPIResult(BaseModel):
    """
    A billing item price gives the price-per-unit of a billing item which is/was in force between
    certain dates.
    """

    uuid: UUID
    sku: Annotated[str, Field(description="The product this applies to", examples=["wfcpu"])]
    valid_from: datetime
    valid_until: Annotated[
        Optional[datetime], Field(description="Price was in-force until this time")
    ] = None
    price: Annotated[float, Field(description="Price-per-unit in Pounds", examples=["0.001"])]


def billingitemprice_to_api_object(price: Row[tuple[BillingItemPrice, str]]) -> dict:
    result = {
        "uuid": str(price[0].uuid),
        "sku": price[1],
        "valid_from": price[0].valid_from.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "price": price[0].price,
    }

    if price[0].valid_until:
        result["valid_until"] = price[0].valid_until.strftime("%Y-%m-%dT%H:%M:%SZ")

    return result


def add_usage_data_headers(response: Response):
    response.headers["Vary"] = "Cookie,Authorization,Accept-Encoding"
    response.headers["Cache-Control"] = "private,max-age=5"


def add_global_data_headers(response: Response):
    response.headers["Vary"] = "Accept-Encoding"
    response.headers["Cache-Control"] = "private,max-age=300"


def decode_jwt_token(authorization: Optional[str] = Header(...)):
    if authorization is None:
        raise HTTPException(status_code=400, detail="Authorization header missing")

    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=400, detail="Invalid Authorization header format")

    token = authorization[len("Bearer ") :]

    credentials = jwt.decode(token, options={"verify_signature": False}, algorithms=["RS256"])
    return credentials


def workspace_authz(
    workspace: str, token_payload: dict, require_owner: bool = False, allow_hub_admin: bool = False
):
    workspaces = token_payload.get("workspaces", [])
    owned = token_payload.get("workspaces_owned", [])
    roles = token_payload["realm_access"].get("roles", [])

    # Allow if user is hub admin
    if allow_hub_admin and "hub_admin" in roles:
        return workspace

    # Require owner if specified
    if require_owner:
        if workspace not in owned:
            raise HTTPException(status_code=401, detail="Must be workspace owner")
    else:
        # Must be a member of the workspace
        if workspace not in workspaces:
            raise HTTPException(status_code=401, detail="Access to this workspace is not allowed")

    return workspace


def account_authz(account_id: UUID, token_payload: dict, allow_hub_admin: bool = False):
    billing_accounts = token_payload.get("billing-accounts", [])
    roles = token_payload["realm_access"].get("roles", [])

    # Allow if user is hub admin
    if allow_hub_admin and "hub_admin" in roles:
        return account_id

    # Require owner if specified
    if str(account_id) not in billing_accounts:
        raise HTTPException(status_code=401, detail="Must be account owner")

    return account_id


@app.get(
    "/workspaces/{workspace}/accounting/usage-data",
    response_model=List[BillingEventAPIResult],
    summary="Get resource consumption data for a workspace",
)
def get_workspace_usage_data(
    session: SessionDep,
    response: Response,
    workspace: Annotated[
        str,
        Path(
            title="EO DataHub workspace name",
            description="Billing events for this workspace will be returned.",
            examples=["my-workspace"],
        ),
    ],
    authorization: Optional[str] = Header(...),
    start: Annotated[
        Optional[datetime],
        Query(
            title="Start timestamp (RFC8601 timestamp)",
            description="Only billing events which ended after this time are included",
            examples=["2025-02-12T13:34:22Z"],
        ),
    ] = None,
    end: Annotated[
        Optional[datetime],
        Query(
            title="End timestamp (RFC8601 timestamp)",
            description="Only billing events which started before this time are included",
            examples=["2025-02-15T13:34:22Z"],
        ),
    ] = None,
    limit: Annotated[
        Optional[int],
        Query(
            title="Maximum number of results to return",
            description=(
                "When paging, set this to the page size and use 'after' to fetch "
                + "subsequent pages"
            ),
            examples=["200"],
        ),
    ] = 100,
    after: Annotated[
        Optional[UUID],
        Query(
            title="Paging continuation location",
            description=(
                "When paging with 'limit', set this to the UUID of the last billing "
                + "event you saw to get the next page of results."
            ),
            examples=["456e15d1-d01b-4060-8b7b-85b93ecbf050"],
        ),
    ] = None,
    time_aggregation: Annotated[
        Optional[str],
        Query(
            title="Time aggregation of results",
            description=(
                "Optionally ggregate usage information into totals for the given time periods - "
                + "'day' or 'month'"
            ),
            examples=["day", "month"],
        ),
    ] = None,
):
    """
    This returns resource consumption data for a workspace within some given time range (or all).
    Start and end times can be given in which case all consumption which overlaps this, even
    partially, will be returned. Each result describes consumption over some specified time period.

    Consumption data may be aggregated so that the time periods used get longer, but they will
    never be aggregated across day boundaries (midnight UTC).
    """

    # Decode the JWT token
    token_payload = decode_jwt_token(authorization)

    # Check workspace authorization
    workspace = workspace_authz(workspace, token_payload, allow_hub_admin=True)

    start = datetime_default_to_utc(start)
    end = datetime_default_to_utc(end)

    events: Iterator[BillingEvent] = BillingEvent.find_billing_events(
        session,
        workspace=workspace,
        start=start,
        end=end,
        limit=limit or 100,
        after=after,
        time_aggregation=time_aggregation,
    )

    add_usage_data_headers(response)
    return list(map(billingevent_to_api_object, events))


@app.get(
    "/accounts/{account_id}/accounting/usage-data",
    response_model=List[BillingEventAPIResult],
    summary="Get resource consumption data for all workspaces in a billing account",
)
def get_account_usage_data(
    session: SessionDep,
    response: Response,
    account_id: Annotated[
        UUID,
        Path(
            title="EO DataHub account ID",
            description=(
                "Billing events for all workspaces owned by this account will be "
                + "returned. This is a UUID, as found in the 'id' fields at /api/accounts"
            ),
            examples=["4b48ebea-bdb8-4bb9-bce9-a7853ad3965d"],
        ),
    ],
    authorization: Optional[str] = Header(...),
    start: Annotated[
        Optional[datetime],
        Query(
            title="Start timestamp (RFC8601 timestamp)",
            description="Only billing events which ended after this time are included",
            examples=["2025-02-12T13:34:22Z"],
        ),
    ] = None,
    end: Annotated[
        Optional[datetime],
        Query(
            title="End timestamp (RFC8601 timestamp)",
            description="Only billing events which started before this time are included",
            examples=["2025-02-15T13:34:22Z"],
        ),
    ] = None,
    limit: Annotated[
        Optional[int],
        Query(
            title="Maximum number of results to return",
            description=(
                "When paging, set this to the page size and use 'after' to fetch "
                + "subsequent pages"
            ),
            examples=["200"],
        ),
    ] = 100,
    after: Annotated[
        Optional[UUID],
        Query(
            title="Paging continuation location",
            description=(
                "When paging with 'limit', set this to the UUID of the last billing "
                + "event you saw to get the next page of results."
            ),
            examples=["456e15d1-d01b-4060-8b7b-85b93ecbf050"],
        ),
    ] = None,
):
    """
    This returns resource consumption data for all workspaces billed to a specified account an
    within some given time range (or all).
    Start and end times can be given in which case all consumption which overlaps this, even
    partially, will be returned. Each result describes consumption over some specified time period.

    Consumption data may be aggregated so that the time periods used get longer, but they will
    never be aggregated across day boundaries (midnight UTC).
    """

    # Decode the JWT token
    token_payload = decode_jwt_token(authorization)

    # Check authorization
    account_id = account_authz(account_id, token_payload, allow_hub_admin=True)

    start = datetime_default_to_utc(start)
    end = datetime_default_to_utc(end)

    events: Iterator[BillingEvent] = BillingEvent.find_billing_events(
        session, account=account_id, start=start, end=end, limit=limit or 100, after=after
    )

    add_usage_data_headers(response)
    return list(map(billingevent_to_api_object, events))


@app.get(
    "/accounting/skus",
    summary="Describe available billing items (products / stock-keeping units).",
    response_model=List[BillingItemAPIResult],
)
def get_item_list(session: SessionDep, response: Response):
    """
    This returns all available billing items in SKU order. A billing item is a single 'product'
    sold by EO DataHub, such as CPU time or object storage. Note that prices must be fetched
    separately and may vary over time.
    """
    items: Iterator[BillingItem] = BillingItem.find_billing_items(session)
    add_global_data_headers(response)
    return list(map(billingitem_to_api_object, items))


@app.get(
    "/accounting/skus/{sku}",
    summary="Describe a single billing item",
    response_model=BillingItemAPIResult,
)
def get_item(session: SessionDep, response: Response, sku: str):
    """This returns a specific billing item based on its SKU."""
    item: Optional[BillingItem] = BillingItem.find_billing_item(session, sku)

    if item is None:
        raise HTTPException(
            status_code=404, detail="SKU not known", headers={"Cache-Control": "max-age=60"}
        )
    else:
        add_global_data_headers(response)
        return billingitem_to_api_object(item)


@app.get(
    "/accounting/prices",
    summary="Return all current EO DataHub prices",
    response_model=List[BillingItemPriceAPIResult],
)
def get_prices(session: SessionDep, response: Response):
    """
    This returns all current prices in SKU order. Prices which were only valid in the past or will
    be in the future are not returned. The cost is given in Pounds per unit, where the unit is
    defined in the billing item the price relates to.
    """
    prices: Result[tuple[BillingItemPrice, str]] = BillingItemPrice.find_prices(
        session, datetime.now(timezone.utc)
    )

    add_global_data_headers(response)
    return list(map(billingitemprice_to_api_object, prices))
