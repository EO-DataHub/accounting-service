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
    JsonValue,
    PlainSerializer,
    field_validator,
    model_validator,
)

from accounting_service.models import (
    BillingItemBase,
    CreditLedgerTransaction,
    PricingPolicy,
    TimeAggregation,
    TransactionType,
    UsageDimension,
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

_DIMENSIONS = "|".join(UsageDimension)

# What `group-by` accepts, published. Pydantic would generate the repeated spelling alone,
# from the declared type, and an array cannot carry the empty value at all: an empty array
# serialises to no parameter, which is not "the empty set" but "omitted", i.e. the default.
# So a generated client could not ask for a total over the period alone, and the examples
# were not valid instances of what was published. This replaces the generated `anyOf`,
# leaving the title, description and examples beside it alone. The enum is inlined rather
# than $ref'd, because the reference FastAPI would generate goes with the schema it
# replaces. tests/test_api_models.py holds this to what _split_dimensions accepts.
#
# Annotated, because Field takes a JsonDict and the literal below infers as something
# narrower.
GROUPING_SCHEMA: dict[str, JsonValue] = {
    "anyOf": [
        {"type": "string", "pattern": rf"^ *$|^ *({_DIMENSIONS})( *, *({_DIMENSIONS}))* *$"},
        {"type": "array", "uniqueItems": True, "items": {"type": "string", "enum": list(UsageDimension)}},
        {"type": "null"},
    ]
}


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
    user: Annotated[
        UUID | None,
        Field(
            default=None,
            title="Only usage by this user",
            description=(
                "Restrict the result to consumption attributed to one user. Usage that no user "
                "is responsible for, such as workspace storage, is attributed to none and is "
                "excluded by this filter. Filtering is not grouping: a total narrowed to one "
                "user still reports 'user' as null unless 'group-by' names that dimension."
            ),
            examples=["ee3c1c1e-0b0e-4d1a-9c7f-1f2b3c4d5e6f"],
        ),
    ]
    sku: Annotated[
        str | None,
        Field(
            default=None,
            title="Only usage of this item",
            description=(
                "Restrict the result to one billing item, named by its SKU as /accounting/skus "
                "lists them. An unknown SKU is not an error; it simply matches nothing. "
                "Filtering is not grouping, though 'sku' is in the default set, so a total "
                "reports the item only while 'group-by' still names it."
            ),
            examples=["cpu-seconds"],
        ),
    ]
    group_by: Annotated[
        frozenset[UsageDimension] | None,
        Field(
            default=None,
            alias="group-by",
            title="Dimensions to break the totals down by",
            description=(
                "Which dimensions each aggregated total is broken down by, beside the period "
                "itself: any of 'user', 'sku' and 'workspace', comma-separated or repeated. "
                "Omit the parameter for 'sku,workspace'. A dimension left out is reported as "
                "null rather than as one of the several values the total now spans - and also "
                "where a filter has left it spanning one, because this parameter alone decides "
                "what a row reports. Pass an empty value to total over the period alone. Only "
                "meaningful with 'time-aggregation', and rejected without it."
            ),
            examples=["user,sku", "workspace", ""],
            json_schema_extra=GROUPING_SCHEMA,
        ),
    ]

    @field_validator("start", "end")
    @classmethod
    def _naive_timestamp_means_utc(cls, value: datetime | None) -> datetime | None:
        """A timestamp arriving without an offset is taken to be UTC."""
        return datetime_default_to_utc(value)

    @field_validator("group_by", mode="before")
    @classmethod
    def _split_dimensions(cls, value: object) -> object:
        """Accept 'group-by=user,sku' as well as 'group-by=user&group-by=sku'.

        FastAPI hands a set-typed parameter over as a list however it was spelled, so a
        comma-separated value arrives as one element containing commas. Splitting every
        element covers both spellings and the mixture of them. A bare string is accepted
        too, so that the model validates the same way when it is built directly.
        """

        elements: tuple[object, ...]

        if isinstance(value, str):
            elements = (value,)
        elif isinstance(value, list | tuple | set | frozenset):
            elements = tuple(value)
        else:
            return value

        return [part.strip() for element in elements for part in str(element).split(",") if part.strip()]

    @model_validator(mode="after")
    def _grouping_needs_aggregation(self) -> "UsageQuery":
        """`group-by` says how totals are broken down, so it is meaningless without totals.

        Rejected rather than ignored, for the same reason an unknown 'time-aggregation' is:
        a caller who asked for a breakdown and silently got one row per event would read the
        result as the breakdown.
        """

        if self.group_by is not None and self.time_aggregation is None:
            raise ValueError("'group-by' needs 'time-aggregation', which is what produces the totals it groups")

        return self


class LedgerQuery(BaseModel):
    """The query parameters for the ledger list endpoint.

    Deliberately not UsageQuery. They share three parameters by coincidence rather than by
    contract: a ledger row is a movement of credits and has no period to aggregate over, and
    `type` has no counterpart in a usage read.
    """

    model_config = ConfigDict(populate_by_name=True)

    transaction_type: Annotated[
        TransactionType | None,
        Field(
            default=None,
            alias="type",
            title="Kind of movement to return",
            description=(
                "Restrict the result to one kind of movement - 'grant', 'debit' or 'reversal'. "
                "Omit the parameter for all three; any other value is rejected."
            ),
            examples=["grant"],
        ),
    ]
    start: Annotated[
        datetime | None,
        Field(
            default=None,
            title="Start timestamp (RFC8601 timestamp)",
            description="Only transactions which occurred at or after this time are included",
            examples=["2025-02-12T13:34:22Z"],
        ),
    ]
    end: Annotated[
        datetime | None,
        Field(
            default=None,
            title="End timestamp (RFC8601 timestamp)",
            description="Only transactions which occurred before this time are included",
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
                "When paging with 'limit', set this to the UUID of the last transaction you "
                "saw to get the next page of results."
            ),
            examples=["456e15d1-d01b-4060-8b7b-85b93ecbf050"],
        ),
    ]

    @field_validator("start", "end")
    @classmethod
    def _naive_timestamp_means_utc(cls, value: datetime | None) -> datetime | None:
        """A timestamp arriving without an offset is taken to be UTC."""
        return datetime_default_to_utc(value)


# No shared base with BillingEvent: the response exposes `item` as a SKU string where the
# table has a relationship, and `credits` is not on the table at all.
#
# Validated from a `UsageRow` rather than from a BillingEvent, so every alias path reaches
# through `event` and `credits` is read from the row beside it.
class BillingEventAPIResult(BaseModel):
    """
    Billing events represent the consumption of a chargeable resource, often over some time
    period. Where consumption happens at a single timepoint, the start and end times will
    be identical.

    All consumption happens within a specific workspace and all charges are attributed to
    a single workspace.

    Both figures for the same consumption: `quantity` is what was metered, `credits` is what
    it cost.

    `item`, `workspace` and `user` are null on an aggregate that `group-by` did not break
    down by, and, for `user`, where the usage is nobody's in particular. `group-by` is the
    only thing that decides which of them a row reports: a filter selects which events are
    counted, so `?user=` narrows a total to one user without naming that user on the row.
    Ask for `group-by=user` as well to see it. One row per event always carries all three
    that it has.
    """

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    uuid: Annotated[UUID, Field(validation_alias=AliasPath("event", "uuid"))]
    event_start: Annotated[
        UtcTimestamp,
        Field(
            validation_alias=AliasPath("event", "event_start"),
            description="Start time of resource consumption",
            examples=["2025-02-12T13:34:22Z"],
        ),
    ]
    event_end: Annotated[
        UtcTimestamp,
        Field(
            validation_alias=AliasPath("event", "event_end"),
            description="End time of resource consumption",
            examples=["2025-02-12T13:34:22Z"],
        ),
    ]
    item: Annotated[
        str | None,
        Field(
            default=None,
            validation_alias=AliasPath("event", "item", "sku"),
            description=(
                "Item (SKU) consumed. Null on an aggregate that 'group-by' did not break down "
                "by SKU, whether the total spans several of them or a filter has left it "
                "spanning one."
            ),
            examples=["wfcpu"],
        ),
    ]
    user: Annotated[
        UUID | None,
        Field(
            default=None,
            validation_alias=AliasPath("event", "user"),
            description=(
                "User who consumed the resource. Null where no single user is responsible - "
                "workspace storage, for instance - and on an aggregate that 'group-by' did "
                "not break down by user, including one the 'user' filter has narrowed to a "
                "single user."
            ),
            examples=["ee3c1c1e-0b0e-4d1a-9c7f-1f2b3c4d5e6f"],
        ),
    ]
    workspace: Annotated[
        str | None,
        Field(
            default=None,
            validation_alias=AliasPath("event", "workspace"),
            description=(
                "Workspace which consumed the resource. Null on an aggregate that 'group-by' "
                "did not break down by workspace, whether the total spans several of them or "
                "the read is scoped to one."
            ),
            examples=["my-workspace"],
        ),
    ]
    quantity: Annotated[
        float,
        Field(
            validation_alias=AliasPath("event", "quantity"),
            description="Quantity consumed in the units defined in the item definition",
            examples=["0.42"],
        ),
    ]
    credits: Annotated[
        ExactDecimal,
        Field(
            description=(
                "Credits this consumption cost, as an exact decimal string. Positive, unlike "
                "the ledger's own signing, so it reads alongside `quantity`. Net of any "
                "correction, so a fully reversed charge reports 0 - as does usage recorded "
                "before any pricing policy covered it."
            ),
            examples=["1.8"],
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


class PolicyRateAPIResult(BaseModel):
    """What one SKU costs per unit under one policy."""

    sku: Annotated[str, Field(description="The product this applies to", examples=["cpu-seconds"])]
    credits_per_unit: Annotated[
        ExactDecimal,
        Field(description="Credits charged per unit, as an exact decimal string", examples=["0.001"]),
    ]


class CategoryMultiplierAPIResult(BaseModel):
    """What one workspace category multiplies every rate by, under one policy."""

    category: Annotated[str, Field(description="The workspace category", examples=["academic"])]
    multiplier: Annotated[
        ExactDecimal,
        Field(description="Applied to every rate above for a workspace in this category", examples=["0.5"]),
    ]


class PricingPolicyAPIResult(BaseModel):
    """The whole rate card in force: every rate and every multiplier.

    Narrower than the stored row. `reason`, `corrects_id` and `valid_until` are audit fields,
    and the audit path is `billing-admin` rather than this endpoint.

    Whether a caller should see every category's multiplier or only their own workspace's is
    undecided. If it becomes the latter, `of` is where the list is built, and the route's
    `Vary` has to gain `Authorization` in the same change.
    """

    version: Annotated[int, Field(description="The policy version these numbers come from", examples=[3])]
    valid_from: Annotated[UtcTimestamp, Field(description="The usage this policy prices starts here")]
    configured_at: Annotated[UtcTimestamp, Field(description="When this calibration was recorded")]
    default_category: Annotated[
        str,
        Field(
            description="The category a workspace prices under when it has no assignment of its own",
            examples=["standard"],
        ),
    ]
    rates: Annotated[list[PolicyRateAPIResult], Field(description="Every rated SKU, in SKU order")]
    category_multipliers: Annotated[
        list[CategoryMultiplierAPIResult],
        Field(description="Every category multiplier, in category order"),
    ]

    @classmethod
    def of(cls, policy: PricingPolicy) -> "PricingPolicyAPIResult":
        """Project a stored policy into the response, sorted so the order is stable."""
        return cls(
            version=policy.version,
            valid_from=policy.valid_from,
            configured_at=policy.configured_at,
            default_category=policy.default_category,
            rates=sorted(
                (
                    PolicyRateAPIResult(sku=rate.item.sku, credits_per_unit=rate.credits_per_unit)
                    for rate in policy.rates
                ),
                key=lambda rate: rate.sku,
            ),
            category_multipliers=sorted(
                (
                    CategoryMultiplierAPIResult(category=entry.category, multiplier=entry.multiplier)
                    for entry in policy.category_multipliers
                ),
                key=lambda entry: entry.category,
            ),
        )


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
