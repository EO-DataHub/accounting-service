# pyright: reportArgumentType=false, reportCallIssue=false, reportAssignmentType=false
# pyright: reportOptionalOperand=false, reportAttributeAccessIssue=false
#
# Pyright cannot type-check SQLModel query expressions, and this is the module where they all
# live. SQLModel declares fields as bare annotations rather than SQLAlchemy's Mapped[...], so
# at class level pyright sees the Python value type instead of a SQL expression. The identical
# query checks clean one way and not the other:
#
#     select(WithMapped).where(WithMapped.valid_until > at)      # no diagnostics
#     select(WithSQLModel).where(WithSQLModel.valid_until > at)  # error + warning
#
# So `where(cls.valid_from <= at)` reports a bool where a ColumnElement is wanted, a nullable
# column compared with > is an invalid operand, `__tablename__ = "..."` is not a declared_attr,
# and constructing a row with `item=obj` looks like a missing item_id because the generated
# __init__ knows nothing about relationships. All of it works; none of it is checkable.
#
# Suppressed here only. Every other module keeps these rules, and the rules that catch real
# mistakes - undefined names, bad returns, unreachable code - stay on everywhere including here.
import logging
import uuid
from collections.abc import Iterator, Sequence
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Self
from uuid import UUID, uuid4

import eodhp_utils.pulsar.messages
from pydantic_core import PydanticUndefined
from sqlalchemy import (
    TIMESTAMP,
    CheckConstraint,
    CursorResult,
    Index,
    MetaData,
    Result,
    and_,
    func,
    or_,
    select,
    text,
    union,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, aliased, selectinload
from sqlmodel import Field as SQLModelField
from sqlmodel import Relationship, SQLModel

from accounting_service.consumption import ConsumptionWindow, RateSample, estimate_consumption
from accounting_service.pricing import ConfiguredPrice, PriceAction, plan_price_change
from accounting_service.timestamps import as_utc, datetime_default_to_utc

# Every table here is a SQLModel. The naming convention is set on SQLModel's own MetaData so
# that indexes, unique constraints, check constraints, foreign keys and primary keys all get
# deterministic names. Alembic matches constraints by name, so without this a later revision
# could not reference one - see the Constraint naming section of the schema note.
#
# Check constraints still need naming in the model, because an anonymous one can never be
# matched against the name PostgreSQL invents for it. Give the bare name only; the convention
# adds the ck_<table>_ prefix.
SQLModel.metadata = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)


def aware_timestamp(
    *,
    default: object = PydanticUndefined,
    index: bool = False,
) -> Any:  # noqa: ANN401 - SQLModel's Field() returns Any so it can be assigned to any field
    """Declare a `timestamptz` column.

    The one place the timezone decision is made. SQLModel maps a bare `datetime` to TIMESTAMP
    WITHOUT TIME ZONE, which discards the offset on write and hands back a naive value on read
    - the failure this codebase has already had three times.

    A function rather than an annotated type. `Annotated[datetime, Field(sa_type=...)]` looks
    tidier but does not compose: both `| None` and an explicit `Field(...)` on the attribute
    discard the annotation's metadata, silently reverting the column to TIMESTAMP WITHOUT TIME
    ZONE. Only the simplest of the six timestamp columns would have been covered.

    `default` takes a value, None, or a SQL function such as func.now(). Left unset it means
    the column has no default, which is what PydanticUndefined signals to SQLModel.
    """
    return SQLModelField(sa_type=TIMESTAMP(timezone=True), default=default, index=index)


class WorkspaceAccount(SQLModel, table=True):
    """
    This records which account contains each workspace.

    This is not the authoritative data, which is held by the workspace service and sent via Pulsar.
    """

    __tablename__ = "workspace_account"

    workspace: str = SQLModelField(index=True, primary_key=True)
    account: UUID = SQLModelField(index=True)

    @staticmethod
    def record_mapping(session: Session, account: UUID, workspace: str) -> bool:
        # We don't allow workspaces to move between accounts, so we only insert a record if
        # there isn't one already.
        result = session.execute(
            text(
                "INSERT INTO workspace_account (workspace, account) "
                + "SELECT cast(:workspace as text), :account "
                + "WHERE NOT EXISTS ("
                + "SELECT 1 FROM workspace_account "
                + "WHERE workspace=:workspace)"
            ),
            [
                {
                    "workspace": workspace,
                    "account": account,
                }
            ],
        )

        assert isinstance(result, CursorResult)  # Makes mypy happy
        return result.rowcount > 0


class BillingItemBase(SQLModel):
    """
    The fields a BillingItem has, shared by the table and the API response.

    A BillingItem is a thing we sell: a unit of CPU time, a unit of bandwidth, etc.

    Declared once because the two shapes are identical. Where a response deliberately differs
    from what is stored - BillingEvent exposes its item as a SKU string rather than a
    relationship - there is no shared base and the response model maps the difference itself.

    The Field arguments carry both concerns: `index` and `primary_key` are acted on only by
    the table subclass, and the descriptions are used only by the OpenAPI schema, so neither
    costs the other anything.

    `uuid` is declared here rather than on the table so that the column order matches what is
    already deployed. Base-class fields are emitted before subclass fields, so declaring it
    below would move it to the end of the table.
    """

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)  # Internal ID

    # User-visible ID like 'cpusecs-computenodes'. 'sku' = 'stock-keeping unit'.
    sku: str = SQLModelField(
        index=True,
        description="Human-readable codename (SKU/stock-keeping unit) for the item",
        schema_extra={"examples": ["wfcpu"]},
    )
    name: str = SQLModelField(
        description="Human-readable name for the item",
        schema_extra={"examples": ["Workflow CPU seconds"]},
    )
    unit: str = SQLModelField(
        description="Unit the item is priced in",
        schema_extra={"examples": ["GB-months"]},
    )


class BillingItem(BillingItemBase, table=True):
    """
    BillingItems should be pre-created, but if we see a BillingEvent referring to an unknown one
    we auto-create it. The name and unit will be empty.
    """

    __tablename__ = "billing_item"

    @classmethod
    def find_billing_items(cls, session: Session) -> Iterator[Self]:
        """Returns all user-visible BillingItems in order of SKU."""
        # This is currently all BillingItems but this could change if we add a 'deleted' flag
        # or some visibility rules.
        query = select(cls).order_by(cls.sku)
        return map(lambda r: r[0], session.execute(query))

    @classmethod
    def find_billing_item(cls, session: Session, sku: str) -> Self | None:
        """Returns a specified BillingItem, assuming it's visible."""
        # This is currently any BillingItem but this could change if we add a 'deleted' flag
        # or some visibility rules.
        query = select(cls).where(cls.sku == sku)
        result = session.execute(query).first()
        return result[0] if result else None

    @classmethod
    def ensure_sku_exists(cls, session: Session, sku: str) -> Self | None:
        """
        This creates a stub BillingItem for an SKU if none already exists.
        """
        rnd_uuid = uuid.uuid4()
        session.execute(
            text(
                "INSERT INTO billing_item (uuid, sku, name, unit) "
                + "SELECT :uuid, cast(:sku as text), '', '' "
                + "WHERE NOT EXISTS ("
                + "    SELECT 1 FROM billing_item "
                + "    WHERE sku=:sku)"
            ),
            [
                {
                    "sku": sku,
                    "uuid": rnd_uuid,
                }
            ],
        )

    @classmethod
    def upsert_configured_item(cls, session: Session, item: dict[str, Any]) -> None:
        """
        This aimed at inserting or updating BillingItems based on a database-independent source
        such as a YAML configuration file. 'item' should have fields 'sku', 'name' and 'unit'.
        An item will be inserted if the SKU isn't known, otherwise name and unit will be updated.
        """
        item_obj = cls.find_billing_item(session, item["sku"])
        if item_obj:
            if "name" in item:
                item_obj.name = item["name"]
            if "unit" in item:
                item_obj.unit = item["unit"]
        else:
            item_obj = BillingItem(**item)
            session.add(item_obj)


class BillingItemPrice(SQLModel, table=True):
    """
    How much we charged for a particular item between a particular time range. `valid_until` will
    be None for the current price.

    To determine the price at time <x> use
        SELECT price FROM BillingItemPrice
            WHERE item=<item>
              AND valid_from <= <x> and valid_until > <x>
              ORDER BY configured_at DESC
              LIMIT 1

    Once created these must not change except for setting `valid_until` to the current time when
    creating a new BillingItemPrice to replace it. If historical prices must be changed then this
    is done by creating a new BillingItemPrice with an overlapping or identical time range but
    setting `configured_at` to the time of configuration. This means we always have a record
    of prices presented to users at any time in the past.

    We support only a single price, not varying prices for different users or workspaces, tiered
    prices, etc.
    """

    __tablename__ = "billing_item_price"

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)
    item_id: UUID = SQLModelField(foreign_key="billing_item.uuid")
    price: Decimal  # This is in pounds.
    valid_from: datetime = aware_timestamp()
    # None for current price, a time in the past otherwise.
    valid_until: datetime | None = aware_timestamp(default=None)
    # Set to the current time at the time this row is added.
    configured_at: datetime = aware_timestamp(default=func.now())

    item: BillingItem = Relationship()

    @property
    def valid_from_utc(self) -> datetime:
        return as_utc(self.valid_from)

    @property
    def valid_until_utc(self) -> datetime | None:
        return as_utc(self.valid_until) if self.valid_until else None

    __table_args__ = (
        Index(
            "billingitemprice_item_validfrom_index",
            "item_id",
            "valid_from",
        ),
        CheckConstraint(
            "valid_until IS NULL OR valid_from <= valid_until",
            name="validity_order",
        ),
    )

    @classmethod
    def find_prices(cls, session: Session, at: datetime) -> Result[tuple[Self, str]]:
        """Returns all prices valid at the specified time. Each result is a tuple containing a
        BillingItemPrice first and the associated SKU second."""
        query = (
            select(cls, BillingItem.sku)
            .join(cls.item)
            .where(cls.valid_from <= at)
            .where(
                or_(
                    cls.valid_until == None,  # noqa: E711
                    cls.valid_until > at,
                )
            )
            .order_by(BillingItem.sku, cls.valid_from)
        )

        return session.execute(query)

    @classmethod
    def _configured_valid_froms(cls, session: Session, item: BillingItem) -> Sequence[datetime]:
        """Every instant a price is already configured to start at, for one item."""
        return session.execute(select(cls.valid_from).where(cls.item_id == item.uuid)).scalars().all()

    @classmethod
    def upsert_configured_price(cls, session: Session, price: dict[str, Any]) -> None:
        """
        This inserts or updates a price based on a database-independent source such as a YAML
        configuration file. 'price' must contain 'sku', 'price' and 'valid_from'.

        'valid_from' must either be newer than the current price, in which case the new price
        replaces it at that time, or must exactly match an existing configured price, in which
        case its amount is updated.

        The rules live in accounting_service.pricing, which decides from values. This reads what
        is stored, asks for a decision, and carries it out.
        """
        entry = ConfiguredPrice.model_validate(price)

        item_obj = BillingItem.find_billing_item(session, entry.sku)
        if not item_obj:
            logging.error("Failed to find item %s when configuring price", entry.sku)
            raise ValueError(f"Attempt to add price for unknown SKU {entry.sku}")

        plan = plan_price_change(entry.valid_from, cls._configured_valid_froms(session, item_obj))

        if plan.action is PriceAction.AMEND:
            session.execute(
                update(cls)
                .where(cls.item_id == item_obj.uuid)
                .where(cls.valid_from == entry.valid_from)
                .values(price=entry.price)
            )
            return

        if plan.action is PriceAction.SUPERSEDE:
            # Close the period this price takes over from. Targeted by valid_from rather than
            # by a null valid_until, so a row that was somehow left open does not get closed
            # by accident.
            session.execute(
                update(cls)
                .where(cls.item_id == item_obj.uuid)
                .where(cls.valid_from == plan.supersedes_valid_from)
                .values(valid_until=entry.valid_from)
            )

        session.add(cls(item=item_obj, valid_from=entry.valid_from, price=entry.price))


class TimeAggregation(StrEnum):
    """
    Periods that usage data can be totalled over.

    This is a closed set for two reasons. An unrecognised value used to be ignored silently,
    so a caller asking for weekly totals got ungrouped rows and a 200. And the value is
    interpolated into SQL in find_billing_events, so the set of permitted values is a
    security boundary as well as a validation rule.
    """

    DAY = "day"
    MONTH = "month"


class AfterBillingEventNotFound(Exception):
    """Raised when paging and specifying the page after an unknown event"""

    pass


class BillingEvent(SQLModel, table=True):
    """
    This records a particular workspace's consumption of a particular BillingItem at a particular
    time or over a particular period. This consumption is priced at its start date.

    BillingEvents can be aggregated over time. A series of billing events can be combined if
    the user, workspace and item are the same and if they occur within the same day. The UUID
    of the first event is kept. They can also be split if the event time period includes
    midnight.

    Note that the 'workspace' field should always refer to a workspace in the WorkspaceAccount
    entity. However, to avoid data loss in the event that messages from the workspace service
    are received too late or not at all, we don't impose a foreign key constraint.
    """

    __tablename__ = "billing_event"

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)
    event_start: datetime = aware_timestamp()
    event_end: datetime = aware_timestamp()
    item_id: UUID = SQLModelField(foreign_key="billing_item.uuid")
    user: UUID | None = SQLModelField(default=None)  # None for, for example, workspace storage.
    workspace: str
    quantity: float  # The units involved are defined in the BillingItem

    item: BillingItem = Relationship()

    @property
    def event_start_utc(self) -> datetime:
        return as_utc(self.event_start)

    @property
    def event_end_utc(self) -> datetime:
        return as_utc(self.event_end)

    __table_args__ = (
        Index(
            "billingevent_workspace_eventstart_index",
            "workspace",
            "event_start",
        ),
        # Named explicitly: Alembic's autogenerate matches check constraints by
        # name, so an anonymous one can never be matched against the name
        # PostgreSQL invents for it.
        CheckConstraint("event_start <= event_end", name="start_before_end"),
        # The next two are listed in UNCOMPARED_INDEXES in alembic/env.py, because
        # PostgreSQL normalises the expressions and Alembic then reports them as
        # changed forever. That exclusion also stops autogenerate emitting them, so
        # they are written by hand in the baseline migration. Change one here and you
        # must change the migration too: no autogenerated revision will do it, and
        # alembic check reports clean when they are missing rather than flagging it.
        # See the comment on UNCOMPARED_INDEXES in alembic/env.py.
        Index(
            "billingevent_month_aggregate_index",
            text("date_trunc('month', event_start AT TIME ZONE 'UTC')"),
            text("(date_trunc('month', event_start AT TIME ZONE 'UTC') + '1 month'::interval)"),
            "workspace",
            "item_id",
        ),
        Index(
            "billingevent_day_aggregate_index",
            text("date_trunc('day', event_start AT TIME ZONE 'UTC')"),
            text("(date_trunc('day', event_start AT TIME ZONE 'UTC') + '1 day'::interval)"),
            "workspace",
            "item_id",
        ),
    )

    @classmethod
    def find_billing_events(
        cls,
        session: Session,
        workspace: str | None = None,
        account: UUID | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        after: UUID | None = None,
        limit: int = 5_000,
        time_aggregation: TimeAggregation | None = None,
    ) -> Iterator[Self]:
        """
        Find and return BillingEvents matching some criteria.

        For paging, `after` should be the UUID of the last billing event on the previous page.

        time_aggregation gives daily or monthly totals for each SKU+workspace pair. Anything
        outside TimeAggregation raises ValueError rather than being ignored.
        """
        # With no time aggregation we use the raw table as the source of rows to filter, sort,
        # page and return.
        #
        # With time aggregation we use a sub-SELECT which calculates aggregated data as the
        # source of rows. The UUID assigned is the lexicographically largest of all rows
        # aggregated. This isn't perfect and can result in errors when fetching the last pages
        # because new BillingEvents can arrive whilst paging and change the maximum UUIDs.
        # This does not happen very often, especially with large page sizes.
        if time_aggregation is not None:
            # Coerced rather than trusted. The value is interpolated into the SQL below, so
            # the closed set has to be enforced at runtime and not only in the type hints.
            # TimeAggregation(...) raises ValueError on anything else.
            period = TimeAggregation(time_aggregation).value

            period_start_expr = f"date_trunc('{period}', event_start AT TIME ZONE 'UTC')"
            period_end_expr = f"{period_start_expr} + '1 {period}'::interval"
            uuid_expr = "CAST(MAX(CAST(uuid AS TEXT)) AS UUID)"

            select_aggregated_events = text(
                f"""
SELECT {uuid_expr} as uuid,
       {period_start_expr} AS event_start,
       {period_end_expr} AS event_end,
       item_id,
       NULL AS user,
       workspace,
       SUM(quantity) AS quantity
FROM {cls.__tablename__}
GROUP BY 2, 3, 4, 6
"""
            )

            select_aggregated_events = select_aggregated_events.columns(
                cls.uuid,
                cls.event_start,
                cls.event_end,
                cls.item_id,
                cls.user,
                cls.workspace,
                cls.quantity,
            )

            billingevent_src = aliased(BillingEvent, select_aggregated_events.subquery())
        else:
            billingevent_src = cls

        # The join is here for the ordering and paging predicates below, which compare
        # BillingItem.sku. It does not populate `item`, so reading event.item.sku on the way
        # out cost one query per row - a page of 100 events issued 101 queries.
        #
        # selectinload rather than contains_eager: contains_eager would reuse the join and
        # need no second query at all, but it would then depend on a join that exists for
        # ordering and could reasonably be removed. selectinload is one extra query for the
        # whole page and is independent of the query's shape.
        all_billing_events = (
            select(billingevent_src)
            .join(BillingItem, BillingItem.uuid == billingevent_src.item_id)
            .options(selectinload(billingevent_src.item))
        )

        # We need a complete and certain order so that the 'after' parameter works.
        query = all_billing_events.order_by(
            billingevent_src.event_start,
            billingevent_src.event_end,
            billingevent_src.workspace,
            BillingItem.sku,
            billingevent_src.uuid,
        )

        query = query.limit(limit)

        if workspace is not None:
            query = query.where(billingevent_src.workspace == workspace)

        if account is not None:
            query = query.join(WorkspaceAccount, WorkspaceAccount.workspace == billingevent_src.workspace).where(
                WorkspaceAccount.account == account
            )

        if start is not None:
            query = query.where(billingevent_src.event_start >= start)

        if end is not None:
            query = query.where(billingevent_src.event_end < end)

        if after is not None:
            # This is equivalent to
            #   after_be = session.get(cls, after)
            # but it works when billingevent_src is an alias rather than an ORM class.
            after_be = session.execute(
                select(billingevent_src).where(billingevent_src.uuid == after)
            ).scalar_one_or_none()

            if after_be is None:
                raise AfterBillingEventNotFound(f"No records matching after={after} found")

            query = query.where(
                billingevent_src.event_start >= after_be.event_start,
                or_(
                    (billingevent_src.event_start > after_be.event_start),
                    and_(
                        billingevent_src.event_start == after_be.event_start,
                        billingevent_src.event_end > after_be.event_end,
                    ),
                    and_(
                        billingevent_src.event_start == after_be.event_start,
                        billingevent_src.event_end == after_be.event_end,
                        billingevent_src.workspace > after_be.workspace,
                    ),
                    and_(
                        billingevent_src.event_start == after_be.event_start,
                        billingevent_src.event_end == after_be.event_end,
                        billingevent_src.workspace == after_be.workspace,
                        BillingItem.sku > after_be.item.sku,
                    ),
                    and_(
                        billingevent_src.event_start == after_be.event_start,
                        billingevent_src.event_end == after_be.event_end,
                        billingevent_src.workspace == after_be.workspace,
                        BillingItem.sku == after_be.item.sku,
                        billingevent_src.uuid > after,
                    ),
                ),
            )

        return map(lambda r: r[0], session.execute(query))

    @classmethod
    def find_latest_billing_event(
        cls,
        session: Session,
        workspace: str | None,
        sku: str | None,
    ) -> Self | None:
        """
        Returns the most recent BillingEvent, optionally constrained by workspace and item.
        """
        query = select(cls).order_by(cls.event_end.desc()).limit(1)

        if workspace is not None:
            query = query.where(cls.workspace == workspace)

        if sku is not None:
            query = query.join(BillingItem).where(BillingItem.sku == sku)

        return session.execute(query).scalar_one_or_none()

    @classmethod
    def insert_from_message(cls, session: Session, msg: eodhp_utils.pulsar.messages.BillingEvent) -> UUID | None:
        """
        Adds a new BillingEvent to the DB based on a Pulsar message.

        Deals with duplicated UUIDs by ignoring the second message and returning None.
        """
        result = session.execute(
            insert(cls)
            .values(
                uuid=UUID(str(msg.uuid)),
                event_start=datetime_default_to_utc(datetime.fromisoformat(str(msg.event_start))),
                event_end=datetime_default_to_utc(datetime.fromisoformat(str(msg.event_end))),
                item_id=select(BillingItem.uuid).where(BillingItem.sku == msg.sku).scalar_subquery(),
                user=UUID(str(msg.user)) if msg.user else None,
                workspace=msg.workspace,
                quantity=msg.quantity,
            )
            .on_conflict_do_nothing(index_elements=["uuid"])
            .returning(BillingEvent.uuid)
        )

        return result.scalar_one_or_none()

    def __repr__(self) -> str:
        return (
            "BillingEvent("
            + f"{self.uuid=}, "
            + f"{self.event_start=}, "
            + f"{self.event_end=}, "
            + f"{self.item_id=}, "
            + f"{self.user=}, "
            + f"{self.workspace=}, "
            + f"{self.quantity=})"
        )


class BillableResourceConsumptionRateSample(SQLModel, table=True):
    """
    A consumption rate sample is a point-in-time sample of the rate at which a user is consuming a
    billed-for resources, typically storage but it could be any other resource where the time it's
    held for is the basis for the charge.

    For example, if we measure storage use at 8GB then the consumption rate sample would be
    '8GB-seconds per second'. The billable resource is measured in GB-seconds, and every second 8
    of them are consumed.

    Samples are used to generate estimated BillingEvents periodically by, effectively, interpolating
    between samples and integrating.
    """

    __tablename__ = "billing_resource_consumption_rate_sample"

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)

    # Typically this is the end of the sampling process, although we pretend here that it was
    # instantaneous.
    sample_time: datetime = aware_timestamp(index=True)

    item_id: UUID = SQLModelField(foreign_key="billing_item.uuid")

    # This is None for, for example, workspace storage.
    user: UUID | None = SQLModelField(default=None)
    workspace: str

    # The units of this are defined in the BillingItem and divided by seconds.
    # eg, storage consumption is measured in GB-seconds, so this is in GB.
    rate: float

    item: BillingItem = Relationship()

    @property
    def sample_time_utc(self) -> datetime:
        return as_utc(self.sample_time)

    __table_args__ = (
        Index(
            "billableresourceconsumptionratesample_workspace_time_index",
            "workspace",
            "sample_time",
        ),
    )

    @classmethod
    def insert_from_message(
        cls, session: Session, msg: eodhp_utils.pulsar.messages.BillingResourceConsumptionRateSample
    ) -> UUID | None:
        result = session.execute(
            insert(cls)
            .values(
                uuid=UUID(str(msg.uuid)),
                sample_time=datetime_default_to_utc(datetime.fromisoformat(str(msg.sample_time))),
                item_id=(select(BillingItem.uuid).where(BillingItem.sku == msg.sku).scalar_subquery()),
                user=UUID(str(msg.user)) if msg.user else None,
                workspace=msg.workspace,
                rate=msg.rate,
            )
            .on_conflict_do_nothing(index_elements=["uuid"])
            .returning(cls.uuid)
        )

        return result.scalar_one_or_none()

    @classmethod
    def find_data_for_interval(
        cls, session: Session, workspace: str, sku: str, start: datetime, end: datetime
    ) -> Sequence[Self]:
        item_subquery = select(BillingItem.uuid).where(BillingItem.sku == sku).scalar_subquery()
        last_before_start = (
            select(cls)
            .where(cls.item_id == item_subquery)
            .where(cls.workspace == workspace)
            .where(cls.sample_time <= start)
            .order_by(cls.sample_time.desc())
            .limit(1)
        )
        first_after_end = (
            select(cls)
            .where(cls.item_id == item_subquery)
            .where(cls.workspace == workspace)
            .where(cls.sample_time >= end)
            .order_by(cls.sample_time)
            .limit(1)
        )
        in_period = (
            select(cls)
            .where(cls.item_id == item_subquery)
            .where(cls.workspace == workspace)
            .where(cls.sample_time > start)
            .where(cls.sample_time < end)
        )

        query = select(cls).from_statement(
            union(last_before_start, first_after_end, in_period).order_by("sample_time")
        )
        return session.execute(query).scalars().all()

    @classmethod
    def calculate_consumption_for_interval(
        cls, session: Session, workspace: str, sku: str, start: datetime, end: datetime
    ) -> float | None:
        """
        This calculates estimated consumption within a time interval, using linear interpolation
        to estimate consumption rates from samples and then (effectively) integrating.

        It's assumed that the resource did not exist (zero consumption rate) before the first
        sample and after the last sample. Callers should endeavour not to call this for an interval
        until sample collection has got as far as at least one sample after the end of the
        interval. If no sample exists after the end of the interval then, if one is later
        collected, the answer given by this method will change.

        This reads the samples and hands the arithmetic to accounting_service.consumption, which
        owns no database and is tested without one.
        """
        samples = cls.find_data_for_interval(session, workspace, sku, start, end)

        return estimate_consumption(
            [RateSample(at=sample.sample_time_utc, rate=sample.rate) for sample in samples],
            ConsumptionWindow(start=start, end=end),
        )

    @classmethod
    def find_earliest(
        cls,
        session: Session,
        workspace: str | None,
        item_id: UUID | None,
    ) -> Self | None:
        """
        Returns the first observed sample for the given constraints.
        """
        query = select(cls).order_by(cls.sample_time).limit(1)

        if workspace is not None:
            query = query.where(cls.workspace == workspace)

        if item_id is not None:
            query = query.where(cls.item_id == item_id)

        return session.execute(query).scalar_one_or_none()

    def __repr__(self) -> str:
        return (
            "BillableResourceConsumptionRateSample("
            + f"{self.uuid=}, "
            + f"{self.sample_time=}, "
            + f"{self.item_id=}, "
            + f"{self.user=}, "
            + f"{self.workspace=}, "
            + f"{self.rate=})"
        )
