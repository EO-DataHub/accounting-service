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

    Pydantic's own Decimal serialisation would emit "4.12E-7" for a small rate, which is
    correct and unhelpful: a UI showing it verbatim looks broken. format(value, "f") gives
    "0.000000412" instead, and preserves the stored scale, so a price of 0.10 stays "0.10"
    rather than becoming "0.1".
    """
    return format(value, "f")


# An exact decimal, carried as a string so no precision is lost on the way out. The back end
# does the arithmetic; the front end decides how to display it.
ExactDecimal = Annotated[Decimal, PlainSerializer(_plain_decimal, return_type=str)]

# A timestamp guaranteed to be UTC-aware. The validator is the guarantee: a naive value would
# otherwise serialise with no offset at all, which is a silently different wire format. The
# *_utc properties on the stored models already convert, so this normally changes nothing.
UtcTimestamp = Annotated[datetime, AfterValidator(as_utc)]


class UsageQuery(BaseModel):
    """
    The query parameters shared by both usage-data endpoints.

    FastAPI expands these into query parameters and documents them from here, so the two
    handlers declare `query: Annotated[UsageQuery, Query()]` instead of repeating six
    parameters each. The defaults and the timestamp normalisation therefore have one home.

    The workspace and account identifiers are not here. They are path parameters, and they
    are what distinguishes the two endpoints.
    """

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
        """A timestamp arriving without an offset is taken to be UTC.

        A domain rule rather than an HTTP one, and it used to be applied by each handler
        calling datetime_default_to_utc on the way past.
        """
        return datetime_default_to_utc(value)


# No shared base with BillingEvent: the response exposes `item` as a SKU string where the
# table has a relationship, so the two shapes genuinely differ. The mapping is expressed as a
# validation alias rather than a constructor, and the timestamps are converted by the
# UtcTimestamp validator instead of by reading the *_utc properties by hand.
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
# Built with model_validate rather than a hand-written constructor: the shapes are identical,
# so there is nothing to map.
#
# `uuid` is redeclared to drop the base's default_factory. A generated default makes the field
# optional in the response schema, and the server always sends one.
class BillingItemAPIResult(BillingItemBase):
    """
    A billing item is a product you can buy from EO DataHub, like CPU time.
    """

    # No from_attributes config needed: SQLModel's own model_validate reads objects.
    #
    # uuid is redeclared to drop the base's default_factory, because a generated default makes
    # the field optional in the response schema and the server always sends one. Pyright
    # objects to an override without a default; at runtime this is exactly the intent.
    uuid: UUID  # pyright: ignore[reportGeneralTypeIssues]


class BillingItemRateAPIResult(BaseModel):
    """What one SKU costs per unit, under the policy that prices usage now.

    This replaced a response carrying a price in pounds, read from `billing_item_price`.
    Credits are the unit of account in this service and buying them is out of scope, so
    there is nothing to convert: the number here is the number the ledger charges.

    Three fields changed shape with it, and the UI reads this endpoint through
    `InvoicesContext`:

      * `price` became `credits_per_unit`, renamed rather than redefined so a client
        displaying credits as pounds fails visibly instead of showing a wrong number.
      * `valid_until` is gone. The loader never closes a policy, so it was always null.
      * `uuid` is gone. It identified a price row, and a rate row's identity is an
        implementation detail no client has a use for.

    `valid_from` is unchanged in name but not in meaning. Every SKU now reports the date of
    the calibration that set it, so all of them share one date rather than each carrying its
    own - which is the point of versioning a policy as a bundle (D3).
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
    """A workspace's credit balance.

    One number, and the time it was true. Credits are the unit of account and nothing
    converts them to money (D2), so there is no currency here and no second figure.

    A negative balance is a workspace that has spent more than it has been granted. It is
    reported rather than refused: nothing in this service blocks work, and a budget breach
    publishes a message instead of stopping anything (D4).
    """

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

    Recomputed from what the ledger row stores rather than read back from it. The row keeps
    the quantity, the policy version and the category resolved at the time, and this is that
    policy projected over those inputs again - so the arithmetic can be reproduced months
    later, after the rates have changed and after the workspace has been recategorised (D8).

    `charge` is therefore a derived figure, and it is the check on the whole scheme: it
    should always equal the magnitude of the `credits` recorded on the transaction. Where it
    does not, the stored charge no longer follows from the policy that is said to have
    produced it.
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

    `credits` is signed as stored: a usage debit is negative, a grant positive. That is what
    makes a balance a plain sum, and it means the sign here is the direction of the movement
    rather than a formatting choice.

    `pricing` is null for a grant. A grant is not priced - a hub admin adds credits and
    records why (T15) - so there is no arithmetic to show, and nesting the explanation says
    that in the shape of the response rather than in a footnote about which fields are
    meaningful.
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
        """Build the response, recomputing the pricing where there is any to recompute."""
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

    None for a grant, which is not priced. The four fields are tested together rather than
    trusting the transaction type, because it is their presence that decides whether the
    arithmetic can be done - and `ck_credit_ledger_transaction_debit_is_priced` guarantees a
    debit has them.

    The charge is recomputed through `price_usage` and not by multiplying here, so the
    explanation is produced by the same function that produced the charge. Writing the
    multiplication out again would let the two drift, and an explanation that disagrees with
    the ledger is worse than none.

    Nothing catches `UnratedSKUError`. It would mean the policy that priced this row no longer
    holds a rate for the SKU it priced, and a policy is immutable once written, so that is a
    corrupted record rather than a case to report as "no explanation available".
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
        # The resolved category rather than the stored one. They are the same for any row
        # priced under this policy, and reporting the resolved one keeps the category and the
        # multiplier beside it consistent with each other whatever happens.
        category=priced.category,
        multiplier=priced.multiplier,
        policy_version=policy.version,
        charge=priced.credits,
    )
