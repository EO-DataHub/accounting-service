from datetime import datetime
from decimal import Decimal
from typing import Annotated, Self
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    field_validator,
)

from accounting_service.models import (
    BillingEvent,
    BillingItem,
    BillingItemPrice,
    TimeAggregation,
    as_utc,
    datetime_default_to_utc,
)


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


def _blank_to_none(value: object) -> object:
    """Treat an empty query parameter as absent.

    A client building a query string from unset state sends `?time-aggregation=` rather than
    omitting the parameter, and that means "no aggregation" rather than "an invalid period".
    Anything else is still rejected, which is the point of the enum.
    """
    return None if value == "" else value


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
        BeforeValidator(_blank_to_none),
        Field(
            default=None,
            alias="time-aggregation",
            title="Time aggregation of results",
            description=(
                "Optionally aggregate usage information into totals for the given time periods - "
                "'day' or 'month'. Any other value is rejected."
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
        UtcTimestamp,
        Field(description="Start time of resource consumption", examples=["2025-02-12T13:34:22Z"]),
    ]
    event_end: Annotated[
        UtcTimestamp,
        Field(description="End time of resource consumption", examples=["2025-02-12T13:34:22Z"]),
    ]
    item: Annotated[str, Field(description="Item (SKU) consumed", examples=["wfcpu"])]
    workspace: Annotated[str, Field(description="Workspace which consumed the resource", examples=["my-workspace"])]
    quantity: Annotated[
        float,
        Field(
            description="Quantity consumed in the units defined in the item definition",
            examples=["0.42"],
        ),
    ]

    @classmethod
    def from_billing_event(cls, event: BillingEvent) -> Self:
        """Build the response from a stored event.

        Reads the *_utc properties rather than the columns, so the Z suffix on the way out
        is earned rather than asserted. The API reported the connection's local time as UTC
        while this mapping used the columns directly.
        """
        return cls(
            uuid=event.uuid,
            event_start=event.event_start_utc,
            event_end=event.event_end_utc,
            item=event.item.sku,
            workspace=event.workspace,
            quantity=event.quantity,
        )


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

    @classmethod
    def from_billing_item(cls, item: BillingItem) -> Self:
        return cls(uuid=item.uuid, sku=item.sku, name=item.name, unit=item.unit)


class BillingItemPriceAPIResult(BaseModel):
    """
    A billing item price gives the price-per-unit of a billing item which is/was in force between
    certain dates.
    """

    uuid: UUID
    sku: Annotated[str, Field(description="The product this applies to", examples=["wfcpu"])]
    valid_from: UtcTimestamp
    valid_until: Annotated[UtcTimestamp | None, Field(description="Price was in-force until this time")] = None
    price: Annotated[
        ExactDecimal,
        Field(
            description="Price-per-unit in Pounds, as an exact decimal string",
            examples=["0.001"],
        ),
    ]

    @classmethod
    def from_billing_item_price(cls, price: BillingItemPrice, sku: str) -> Self:
        """Build the response from a stored price and the SKU it belongs to.

        The SKU is passed separately because find_prices returns it alongside the price
        rather than on it. The Row that query produces is a SQLAlchemy detail, so the
        caller unpacks it rather than this model knowing the shape.
        """
        return cls(
            uuid=price.uuid,
            sku=sku,
            valid_from=price.valid_from_utc,
            valid_until=price.valid_until_utc,
            price=price.price,
        )
