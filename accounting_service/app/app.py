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
from fastapi.responses import JSONResponse

# noinspection PyPackageRequirements
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
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
    CreditLedgerTransaction,
    PricingPolicy,
)

from .models import (
    BillingEventAPIResult,
    BillingItemAPIResult,
    BillingItemRateAPIResult,
    CreditBalanceAPIResult,
    LedgerTransactionAPIResult,
    UsageQuery,
)

logger = logging.getLogger(__name__)

setup_logging(verbosity=1)
log_component_version("accounting-service")


root_path = os.environ.get("ROOT_PATH", "/api/")

SessionDep = Annotated[Session, Depends(get_session)]


app = FastAPI(root_path=root_path)

FastAPIInstrumentor.instrument_app(app)


@app.exception_handler(AfterBillingEventNotFound)
def handle_after_billing_event_not_found(_request: Request, exc: AfterBillingEventNotFound) -> JSONResponse:
    """Paging from an event that does not exist is a 404."""

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
    """Returns resource consumption data for a workspace within some given time range (or all)."""

    events: Iterator[BillingEvent] = BillingEvent.find_billing_events(
        session,
        workspace=workspace,
        start=query.start,
        end=query.end,
        limit=query.limit,
        after=query.after,
        time_aggregation=query.time_aggregation,
    )

    return [BillingEventAPIResult.model_validate(event) for event in events]


@app.get(
    "/workspaces/{workspace}/accounting/balance",
    summary="Get a workspace's credit balance",
    dependencies=[Depends(require_workspace(MinTier.MEMBER)), Depends(usage_data_cache)],
)
def get_workspace_balance(
    session: SessionDep,
    workspace: Annotated[
        str,
        Path(
            title="EO DataHub workspace name",
            description="The balance of this workspace will be returned.",
            examples=["my-workspace"],
        ),
    ],
) -> CreditBalanceAPIResult:
    """Returns the credits currently available to a workspace."""
    return CreditBalanceAPIResult(
        workspace=workspace,
        balance=CreditLedgerTransaction.balance(session, workspace),
        as_of=datetime.now(UTC),
    )


@app.get(
    "/workspaces/{workspace}/accounting/ledger/{transaction}",
    summary="Explain one credit transaction",
    dependencies=[Depends(require_workspace(MinTier.MEMBER)), Depends(usage_data_cache)],
)
def get_ledger_transaction(
    session: SessionDep,
    workspace: Annotated[
        str,
        Path(
            title="EO DataHub workspace name",
            description="The workspace the transaction belongs to.",
            examples=["my-workspace"],
        ),
    ],
    transaction: Annotated[
        UUID,
        Path(
            title="Credit transaction ID",
            description="The transaction to explain.",
            examples=["456e15d1-d01b-4060-8b7b-85b93ecbf050"],
        ),
    ],
) -> LedgerTransactionAPIResult:
    """Returns one credit transaction and, for a charge, the arithmetic behind it."""
    found = CreditLedgerTransaction.find_transaction(session, transaction, workspace=workspace)

    if found is None:
        raise HTTPException(status_code=404, detail="Transaction not known")

    return LedgerTransactionAPIResult.of(found)


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
    """Returns resource consumption data for all workspaces billed to a specified account an
    within some given time range (or all).
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

    return [BillingEventAPIResult.model_validate(event) for event in events]


@app.get(
    "/accounting/skus",
    summary="Describe available billing items (products / stock-keeping units).",
    dependencies=[Depends(global_data_cache)],
)
def get_item_list(session: SessionDep) -> list[BillingItemAPIResult]:
    """Returns all available billing items in SKU order. Note that prices must be fetched
    separately and may vary over time.
    """
    items: Iterator[BillingItem] = BillingItem.find_billing_items(session)
    return [BillingItemAPIResult.model_validate(item) for item in items]


@app.get(
    "/accounting/skus/{sku}",
    summary="Describe a single billing item",
    dependencies=[Depends(global_data_cache)],
)
def get_item(session: SessionDep, sku: str) -> BillingItemAPIResult:
    """Returns a specific billing item based on its SKU."""
    item: BillingItem | None = BillingItem.find_billing_item(session, sku)

    if item is None:
        raise HTTPException(status_code=404, detail="SKU not known", headers={"Cache-Control": "max-age=60"})

    return BillingItemAPIResult.model_validate(item)


@app.get(
    "/accounting/prices",
    summary="Return the current EO DataHub credit rates",
    dependencies=[Depends(global_data_cache)],
)
def get_prices(session: SessionDep) -> list[BillingItemRateAPIResult]:
    """Returns the credits charged per unit for every SKU, in SKU order. The unit is defined
    in the billing item the rate relates to.
    """
    policy = PricingPolicy.resolve(session, datetime.now(UTC))

    if policy is None:
        return []

    return sorted(
        (
            BillingItemRateAPIResult(
                sku=rate.item.sku,
                credits_per_unit=rate.credits_per_unit,
                valid_from=policy.valid_from,
                policy_version=policy.version,
            )
            for rate in policy.rates
        ),
        key=lambda rate: rate.sku,
    )
