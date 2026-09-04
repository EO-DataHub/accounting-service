import logging
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from eodhp_utils.runner import log_component_version, setup_logging
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Path,
    Query,
    Request,
)

# noinspection PyPackageRequirements
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from sqlalchemy import Result
from sqlalchemy.orm import Session

from accounting_service.app.authz import MinTier
from accounting_service.app.dependencies import (
    global_data_cache,
    require_account,
    require_workspace,
    usage_data_cache,
)
from accounting_service.db import get_session
from accounting_service.models import (
    AfterBillingEventNotFound,
    BillingEvent,
    BillingItem,
    BillingItemPrice,
)

from .models import (
    BillingEventAPIResult,
    BillingItemAPIResult,
    BillingItemPriceAPIResult,
    UsageQuery,
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


@app.exception_handler(AfterBillingEventNotFound)
def handle_after_billing_event_not_found(_request: Request, exc: AfterBillingEventNotFound) -> JSONResponse:
    """Paging from an event that does not exist is a 404.

    Registered once rather than caught in each handler, so a query endpoint added later gets
    this without having to remember it.
    """
    return JSONResponse(status_code=HTTPStatus.NOT_FOUND, content={"detail": str(exc)})


@app.get(
    "/workspaces/{workspace}/accounting/usage-data",
    summary="Get resource consumption data for a workspace",
    dependencies=[Depends(require_workspace(MinTier.MEMBER)), Depends(usage_data_cache)],
)
def get_workspace_usage_data(
    session: SessionDep,
    workspace: Annotated[
        str,
        Path(
            title="EO DataHub workspace name",
            description="Billing events for this workspace will be returned.",
            examples=["my-workspace"],
        ),
    ],
    query: Annotated[UsageQuery, Query()],
) -> list[BillingEventAPIResult]:
    """
    This returns resource consumption data for a workspace within some given time range (or all).
    Start and end times can be given in which case all consumption which overlaps this, even
    partially, will be returned. Each result describes consumption over some specified time period.

    Consumption data may be aggregated so that the time periods used get longer, but they will
    never be aggregated across day boundaries (midnight UTC).
    """

    events: Iterator[BillingEvent] = BillingEvent.find_billing_events(
        session,
        workspace=workspace,
        start=query.start,
        end=query.end,
        limit=query.limit,
        after=query.after,
        time_aggregation=query.time_aggregation,
    )

    return [BillingEventAPIResult.from_billing_event(event) for event in events]


@app.get(
    "/accounts/{account_id}/accounting/usage-data",
    summary="Get resource consumption data for all workspaces in a billing account",
    dependencies=[Depends(require_account), Depends(usage_data_cache)],
)
def get_account_usage_data(
    session: SessionDep,
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
    query: Annotated[UsageQuery, Query()],
) -> list[BillingEventAPIResult]:
    """
    This returns resource consumption data for all workspaces billed to a specified account an
    within some given time range (or all).
    Start and end times can be given in which case all consumption which overlaps this, even
    partially, will be returned. Each result describes consumption over some specified time period.

    Consumption data may be aggregated so that the time periods used get longer, but they will
    never be aggregated across day boundaries (midnight UTC).
    """

    events: Iterator[BillingEvent] = BillingEvent.find_billing_events(
        session,
        account=account_id,
        start=query.start,
        end=query.end,
        limit=query.limit,
        after=query.after,
        time_aggregation=query.time_aggregation,
    )

    return [BillingEventAPIResult.from_billing_event(event) for event in events]


@app.get(
    "/accounting/skus",
    summary="Describe available billing items (products / stock-keeping units).",
    dependencies=[Depends(global_data_cache)],
)
def get_item_list(session: SessionDep) -> list[BillingItemAPIResult]:
    """
    This returns all available billing items in SKU order. A billing item is a single 'product'
    sold by EO DataHub, such as CPU time or object storage. Note that prices must be fetched
    separately and may vary over time.
    """
    items: Iterator[BillingItem] = BillingItem.find_billing_items(session)
    return [BillingItemAPIResult.from_billing_item(item) for item in items]


@app.get(
    "/accounting/skus/{sku}",
    summary="Describe a single billing item",
    dependencies=[Depends(global_data_cache)],
)
def get_item(session: SessionDep, sku: str) -> BillingItemAPIResult:
    """This returns a specific billing item based on its SKU."""
    item: BillingItem | None = BillingItem.find_billing_item(session, sku)

    if item is None:
        raise HTTPException(status_code=404, detail="SKU not known", headers={"Cache-Control": "max-age=60"})

    return BillingItemAPIResult.from_billing_item(item)


@app.get(
    "/accounting/prices",
    summary="Return all current EO DataHub prices",
    dependencies=[Depends(global_data_cache)],
)
def get_prices(session: SessionDep) -> list[BillingItemPriceAPIResult]:
    """
    This returns all current prices in SKU order. Prices which were only valid in the past or will
    be in the future are not returned. The cost is given in Pounds per unit, where the unit is
    defined in the billing item the price relates to.
    """
    prices: Result[tuple[BillingItemPrice, str]] = BillingItemPrice.find_prices(session, datetime.now(UTC))

    return [BillingItemPriceAPIResult.from_billing_item_price(price, sku) for price, sku in prices]
