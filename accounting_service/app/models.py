from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import (
    AfterValidator,
    AliasPath,
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    field_validator,
)

from accounting_service.models import (
    BillingItemBase,
    CreditLedgerTransaction,
    TimeAggregation,
    TransactionType,
)
from accounting_service.pricing import price_usage
from accounting_service.timestamps import as_utc, datetime_default_to_utc


def _plain_decimal(value: Decimal) -> str:
    """Render an exact decimal as a string, never in scientific notation.

    Pydantic would emit "4.12E-7" for a small rate, which is correct and unhelpful.
    format(value, "f") gives "0.000000412", and preserves the stored scale so 0.10 stays
    "0.10".
    """
    return format(value, "f")


# An exact decimal, carried as a string so no precision is lost on the way out. The back end
# does the arithmetic; the front end decides how to display it.
ExactDecimal = Annotated[Decimal, PlainSerializer(_plain_decimal, return_type=str)]

# A timestamp guaranteed to be UTC-aware. Without the validator a naive value serialises with
# no offset at all, which is a silently different wire format.
UtcTimestamp = Annotated[datetime, AfterValidator(as_utc)]


class UsageQuery(BaseModel):
    """The query parameters shared by both usage-data endpoints."""

    model_config = ConfigDict(populate_by_name=True)

    start: Annotated[
        datetime | None,
        Field(
            default=None,
            title="Start timestamp (RFC8601 timestamp)",
            description="Only billing events which ended after this time are included",
            examples=["2025-02-12T13:34:22Z"],
        ),
    ]
    end: Annotated[
        datetime | None,
        Field(
            default=None,
            title="End timestamp (RFC8601 timestamp)",
            description="Only billing events which started before this time are included",
            examples=["2025-02-15T13:34:22Z"],
        ),
    ]
    limit: Annotated[
        int,
        Field(
            default=100,
            ge=1,
            title="Maximum number of results to return",
            description="When paging, set this to the page size and use 'after' to fetch subsequent pages",
            examples=[200],
        ),
    ]
    after: Annotated[
        UUID | None,
        Field(
            default=None,
            title="Paging continuation location",
            description=(
                "When paging with 'limit', set this to the UUID of the last billing event you "
                "saw to get the next page of results."
            ),
            examples=["456e15d1-d01b-4060-8b7b-85b93ecbf050"],
        ),
    ]
    time_aggregation: Annotated[
        TimeAggregation | None,
        Field(
            default=None,
            alias="time-aggregation",
            title="Time aggregation of results",
            description=(
                "Optionally aggregate usage information into totals for the given time periods - "
                "'day' or 'month'. Omit the parameter for no aggregation; any other value, "
                "including an empty one, is rejected."
            ),
            examples=["day", "month"],
        ),
    ]

    @field_validator("start", "end")
    @classmethod
    def _naive_timestamp_means_utc(cls, value: datetime | None) -> datetime | None:
        """A timestamp arriving without an offset is taken to be UTC."""
        return datetime_default_to_utc(value)


# No shared base with BillingEvent: the response exposes `item` as a SKU string where the
# table has a relationship.
class BillingEventAPIResult(BaseModel):
    """
    Billing events represent the consumption of a chargeable resource, often over some time
    period. Where consumption happens at a single timepoint, the start and end times will
    be identical.

    All consumption happens within a specific workspace and all charges are attributed to
    a single workspace.
    """

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    uuid: UUID
    event_start: Annotated[
        UtcTimestamp,
        Field(description="Start time of resource consumption", examples=["2025-02-12T13:34:22Z"]),
    ]
    event_end: Annotated[
        UtcTimestamp,
        Field(description="End time of resource consumption", examples=["2025-02-12T13:34:22Z"]),
    ]
    item: Annotated[
        str,
        Field(
            validation_alias=AliasPath("item", "sku"),
            description="Item (SKU) consumed",
            examples=["wfcpu"],
        ),
    ]
    workspace: Annotated[str, Field(description="Workspace which consumed the resource", examples=["my-workspace"])]
    quantity: Annotated[
        float,
        Field(
            description="Quantity consumed in the units defined in the item definition",
            examples=["0.42"],
        ),
    ]


# Every field comes from BillingItemBase, which the table shares, so the two cannot drift.
class BillingItemAPIResult(BillingItemBase):
    """A billing item is a product you can buy from EO DataHub, like CPU time."""

    # Redeclared to drop the base's default_factory: a generated default makes the field
    # optional in the response schema, and the server always sends one. Pyright objects to an
    # override without a default; at runtime this is the intent.
    uuid: UUID  # pyright: ignore[reportGeneralTypeIssues]


class BillingItemRateAPIResult(BaseModel):
    """What one SKU costs per unit, under the policy that prices usage now.

    Every SKU reports `valid_from` as the date of the calibration that set it, so all of them
    share one date rather than each carrying its own - a policy is versioned as a bundle.
    """

    sku: Annotated[str, Field(description="The product this applies to", examples=["wfcpu"])]
    credits_per_unit: Annotated[
        ExactDecimal,
        Field(
            description="Credits charged per unit, as an exact decimal string",
            examples=["0.001"],
        ),
    ]
    valid_from: Annotated[UtcTimestamp, Field(description="When the calibration that set this took effect")]
    policy_version: Annotated[int, Field(description="Which pricing policy version this rate comes from")]


class CreditBalanceAPIResult(BaseModel):
    """A workspace's credit balance, and the time it was true."""

    workspace: Annotated[str, Field(description="The workspace this balance belongs to", examples=["my-workspace"])]
    balance: Annotated[
        ExactDecimal,
        Field(
            description=(
                "Credits available, as an exact decimal string. Negative if the workspace has "
                "spent more than it holds."
            ),
            examples=["989.2"],
        ),
    ]
    as_of: Annotated[
        UtcTimestamp,
        Field(
            description="When this balance was computed. The ledger is append-only, so it is a value at an instant.",
            examples=["2026-09-09T13:34:22Z"],
        ),
    ]


class PricingExplanation(BaseModel):
    """How a charge was arrived at: quantity x credits per unit x category multiplier.

    Recomputed from what the ledger row stores - the quantity, the policy version and the
    category resolved at the time - rather than read back from it. So the arithmetic can be
    reproduced after the rates have changed and after the workspace has been recategorised.

    `charge` should always equal the magnitude of the transaction's `credits`. Where it does
    not, the stored charge no longer follows from the policy said to have produced it.
    """

    sku: Annotated[str, Field(description="The item consumed", examples=["cpu-seconds"])]
    quantity: Annotated[float, Field(description="Units consumed, in the item's own unit", examples=[3600.0])]
    credits_per_unit: Annotated[
        ExactDecimal,
        Field(description="The rate this SKU carried under the policy below", examples=["0.001"]),
    ]
    category: Annotated[
        str,
        Field(
            description="The workspace category resolved when the charge was priced, not the category it has now",
            examples=["academic"],
        ),
    ]
    multiplier: Annotated[
        ExactDecimal,
        Field(description="The multiplier that category carried under the policy below", examples=["0.5"]),
    ]
    policy_version: Annotated[
        int,
        Field(description="The pricing policy version that priced this charge", examples=[3]),
    ]
    charge: Annotated[
        ExactDecimal,
        Field(
            description=(
                "quantity x credits_per_unit x multiplier, recomputed from the fields above. "
                "Equals the magnitude of the transaction's `credits`."
            ),
            examples=["1.8"],
        ),
    ]


class LedgerTransactionAPIResult(BaseModel):
    """One movement of credits, and how it was reached.

    `credits` is signed as stored: a usage debit is negative, a grant positive.

    `pricing` is null for a grant, which is not priced..
    """

    uuid: UUID
    workspace: Annotated[str, Field(description="The workspace charged or credited", examples=["my-workspace"])]
    user: Annotated[
        UUID | None,
        Field(
            description=(
                "The user whose usage was charged. Null on a grant, which belongs to the "
                "whole workspace pool rather than to one member."
            ),
        ),
    ]
    transaction_type: Annotated[
        TransactionType,
        Field(description="What kind of movement this is", examples=["debit"]),
    ]
    credits: Annotated[
        ExactDecimal,
        Field(
            description="Credits moved, signed: negative for a usage debit, positive for a grant",
            examples=["-1.8"],
        ),
    ]
    occurred_at: Annotated[
        UtcTimestamp,
        Field(description="When the usage happened, or when the grant was made"),
    ]
    recorded_at: Annotated[
        UtcTimestamp,
        Field(
            description=(
                "When this service recorded the movement. Later than `occurred_at` for usage that was backfilled."
            ),
        ),
    ]
    reason: Annotated[
        str | None,
        Field(description="Why a grant or a correction was made. Null on a usage debit, which nobody initiated."),
    ]
    pricing: Annotated[
        PricingExplanation | None,
        Field(description="How the charge was computed. Null for a grant, which is not priced."),
    ]

    @classmethod
    def of(cls, transaction: CreditLedgerTransaction) -> "LedgerTransactionAPIResult":
        """Build the response, recomputing the pricing where there is any."""
        return cls(
            uuid=transaction.uuid,
            workspace=transaction.workspace,
            user=transaction.user,
            transaction_type=transaction.transaction_type,
            credits=transaction.credits,
            occurred_at=transaction.occurred_at_utc,
            recorded_at=transaction.recorded_at_utc,
            reason=transaction.reason,
            pricing=_explain(transaction),
        )


def _explain(transaction: CreditLedgerTransaction) -> PricingExplanation | None:
    """Reproduce the arithmetic behind a charge, or None if there is none to reproduce.

    The four fields are tested together rather than trusting the transaction type, because it
    is their presence that decides whether the arithmetic can be done.

    Recomputed through `price_usage` rather than by multiplying here, so the explanation comes
    from the same function that produced the charge and the two cannot drift.

    `UnratedSKUError` is not caught: a policy is immutable once written, so a policy that no
    longer rates the SKU it priced is a corrupted record rather than a missing explanation.
    """
    policy = transaction.policy
    item = transaction.item

    if policy is None or item is None or transaction.category is None or transaction.quantity is None:
        return None

    priced = price_usage(
        policy.rate_card(),
        sku=item.sku,
        quantity=transaction.quantity,
        category=transaction.category,
    )

    return PricingExplanation(
        sku=priced.sku,
        quantity=priced.quantity,
        credits_per_unit=priced.credits_per_unit,
        # The resolved category rather than the stored one, so the category and the multiplier
        # beside it always agree with each other.
        category=priced.category,
        multiplier=priced.multiplier,
        policy_version=policy.version,
        charge=priced.credits,
    )
