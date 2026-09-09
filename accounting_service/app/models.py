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
    TimeAggregation,
)
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
