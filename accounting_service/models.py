"""Wrap a column in `col()` before reaching for a suppression. SQLModel declares fields as plain
annotations, so `cls.sku == sku` types as a `bool` rather than as a SQL expression.
"""

import logging
import uuid
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
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
    UniqueConstraint,
    and_,
    func,
    or_,
    select,
    text,
    union,
)
from sqlalchemy import Enum as SAEnum
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, aliased, selectinload
from sqlalchemy.orm.strategy_options import _AbstractLoad
from sqlmodel import Field as SQLModelField
from sqlmodel import Relationship, SQLModel, col

from accounting_service.configuration import ConfiguredItem
from accounting_service.consumption import ConsumptionWindow, RateSample, estimate_consumption
from accounting_service.pricing import ConfiguredPolicy, PolicyFingerprint, PricedUsage, RateCard
from accounting_service.timestamps import as_utc, datetime_default_to_utc

# The naming convention is set on SQLModel's own MetaData so that indexes, unique constraints,
# check constraints, foreign keys and primary keys all get deterministic names. Alembic matches
# constraints by name, so a later revision cannot reference one that was named by PostgreSQL.
SQLModel.metadata = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)


metadata = SQLModel.metadata

# How many times to retry a version another replica claimed first.
_MINT_ATTEMPTS = 3


def aware_timestamp(
    *,
    default: object = PydanticUndefined,
    index: bool = False,
) -> Any:  # noqa: ANN401 - SQLModel's Field() returns Any so it can be assigned to any field
    """Declare a `timestamptz` column."""
    return SQLModelField(
        sa_type=TIMESTAMP(timezone=True),  # pyright: ignore[reportArgumentType]
        default=default,
        index=index,
    )


def pg_enum(values: "type[StrEnum]", name: str) -> Any:  # noqa: ANN401 - as aware_timestamp, SQLModel's Field() returns Any
    """Declare a native PostgreSQL enum column over a `StrEnum`."""
    return SQLModelField(
        sa_type=SAEnum(values, name=name, values_callable=lambda enum: [member.value for member in enum]),  # pyright: ignore[reportArgumentType]
    )


class WorkspaceAccount(SQLModel, table=True):
    """
    This records which account contains each workspace.

    This is not the authoritative data, which is held by the workspace service and sent via Pulsar.
    """

    __tablename__ = "workspace_account"  # pyright: ignore[reportAssignmentType]

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


class WorkspaceCategory(SQLModel, table=True):
    """Which pricing category a workspace is charged under."""

    __tablename__ = "workspace_category"  # pyright: ignore[reportAssignmentType]

    workspace: str = SQLModelField(primary_key=True)
    category: str
    updated_at: datetime = aware_timestamp(default=func.now())
    updated_by: UUID | None = SQLModelField(default=None)

    @classmethod
    def category_for(cls, session: Session, workspace: str) -> str | None:
        """The category assigned to this workspace, or None if it has none."""
        return session.execute(select(col(cls.category)).where(col(cls.workspace) == workspace)).scalar_one_or_none()

    @classmethod
    def assign(cls, session: Session, workspace: str, category: str, updated_by: UUID | None = None) -> None:
        """Set this workspace's category, replacing any existing assignment."""
        session.execute(
            insert(cls)
            .values(workspace=workspace, category=category, updated_at=func.now(), updated_by=updated_by)
            .on_conflict_do_update(
                index_elements=["workspace"],
                set_={"category": category, "updated_at": func.now(), "updated_by": updated_by},
            )
        )


class BillingItemBase(SQLModel):
    """
    The fields a BillingItem has, shared by the table and the API response.

    A BillingItem is a thing we sell: a unit of CPU time, a unit of bandwidth, etc.
    """

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)  # Internal ID

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
    __tablename__ = "billing_item"  # pyright: ignore[reportAssignmentType]

    @classmethod
    def find_billing_items(cls, session: Session) -> Iterator[Self]:
        """Returns all user-visible BillingItems in order of SKU."""
        query = select(cls).order_by(cls.sku)
        return map(lambda r: r[0], session.execute(query))

    @classmethod
    def find_billing_item(cls, session: Session, sku: str) -> Self | None:
        """Returns a specified BillingItem, assuming it's visible."""
        query = select(cls).where(col(cls.sku) == sku)
        result = session.execute(query).first()
        return result[0] if result else None

    @classmethod
    def ensure_sku_exists(cls, session: Session, sku: str) -> Self | None:
        """This creates a stub BillingItem for an SKU if none already exists."""
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
    def upsert_configured_item(cls, session: Session, entry: ConfiguredItem) -> None:
        """Insert or update a BillingItem from a validated configuration entry.

        The item is inserted when its SKU is unknown, otherwise its name and unit are updated.
        """
        item_obj = cls.find_billing_item(session, entry.sku)

        if item_obj:
            item_obj.name = entry.name
            item_obj.unit = entry.unit
        else:
            session.add(BillingItem(sku=entry.sku, name=entry.name, unit=entry.unit))


def _next_version(session: Session) -> int:
    """One past the highest version stored."""
    return (session.execute(select(func.max(col(PricingPolicy.version)))).scalar() or 0) + 1


class PricingPolicy(SQLModel, table=True):
    """Rows are immutable once written. A correction adds a new policy pointing at the one it
    corrects through `corrects_id`, which is what lets an already-charged period be re-priced
    without destroying the record of what was charged at the time.

    Bi-temporal. `valid_from` and `valid_until` say which usage the policy applies to;
    `configured_at` says when the decision was taken. Resolution orders by `configured_at`
    descending, so a correcting policy wins over the policy it corrects.
    """

    __tablename__ = "pricing_policy"  # pyright: ignore[reportAssignmentType]

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)

    version: int = SQLModelField(unique=True)

    valid_from: datetime = aware_timestamp()
    valid_until: datetime | None = aware_timestamp(default=None)
    configured_at: datetime = aware_timestamp(default=func.now())

    # Set when this policy corrects an earlier one.
    corrects_id: UUID | None = SQLModelField(default=None, foreign_key="pricing_policy.uuid")

    # Applied to a workspace that has no category assignment yet.
    default_category: str

    # Why this calibration happened. Feeds the audit log.
    reason: str | None = None

    rates: list["PricingPolicyRate"] = Relationship(back_populates="policy")
    category_multipliers: list["PricingPolicyCategoryMultiplier"] = Relationship(back_populates="policy")

    __table_args__ = (
        CheckConstraint(
            "valid_until IS NULL OR valid_from <= valid_until",
            name="validity_order",
        ),
    )

    @classmethod
    def current(cls, session: Session) -> Self | None:
        """The policy in force, or None when none has been loaded yet."""
        return (
            session.execute(
                select(cls)
                .options(*_policy_load_options())
                .order_by(col(cls.configured_at).desc(), col(cls.version).desc())
                .limit(1)
            )
            .scalars()
            .first()
        )

    @classmethod
    def resolve(cls, session: Session, at: datetime) -> Self | None:
        """The policy that prices usage occurring at `at`, or None when none is stored.

        The most recently configured policy whose `valid_from` is at or before `at`, ties
        breaking on `version` descending.

        When `at` predates every policy, the earliest policy prices it.

        Distinct from `current()`, which ignores validity and answers "what did we configure
        last" for the loader. A policy dated next month is the last configured and prices
        nothing today.
        """
        applicable = (
            select(cls)
            .options(*_policy_load_options())
            .where(col(cls.valid_from) <= at)
            .order_by(col(cls.configured_at).desc(), col(cls.version).desc())
            .limit(1)
        )

        if policy := session.execute(applicable).scalars().first():
            return policy

        earliest = (
            select(cls).options(*_policy_load_options()).order_by(col(cls.valid_from), col(cls.version)).limit(1)
        )

        return session.execute(earliest).scalars().first()

    def fingerprint(self) -> PolicyFingerprint:
        """What this policy would have to match for a document to leave it alone."""
        return PolicyFingerprint.of(
            valid_from=self.valid_from,
            default_category=self.default_category,
            rates=[(rate.item.sku, rate.credits_per_unit) for rate in self.rates],
            category_multipliers=[(entry.category, entry.multiplier) for entry in self.category_multipliers],
        )

    def rate_card(self) -> RateCard:
        """This policy's numbers, in the form pricing needs them."""
        return RateCard.of(
            default_category=self.default_category,
            rates=[(rate.item.sku, rate.credits_per_unit) for rate in self.rates],
            category_multipliers=[(entry.category, entry.multiplier) for entry in self.category_multipliers],
        )

    @classmethod
    def load_configured_policy(cls, session: Session, entry: ConfiguredPolicy) -> Self | None:
        """Mint a new version, or return None when the document is already in force.

        Called on every ingester pod start, so the common case is an unchanged document and
        this does nothing. Only a change to the numbers, the default category or `valid_from`
        mints a version - see PolicyFingerprint.

        Two replicas starting together both see the same current policy and both try to mint
        the same version. The unique constraint refuses the second, and this recovers rather
        than failing the pod: the write happens inside a savepoint so the caller's transaction
        survives, and the loser re-reads.

        Raises ValueError for a rate naming a SKU that does not exist. Items load before the
        policy, so a document may introduce an item and rate it in the same pass.
        """
        wanted = entry.fingerprint

        current = cls.current(session)
        if current is not None and current.fingerprint() == wanted:
            logging.debug("Pricing policy version %s is already in force", current.version)
            return None

        items = cls._resolve_rated_items(session, entry)

        for remaining in reversed(range(_MINT_ATTEMPTS)):
            version = _next_version(session)

            try:
                with session.begin_nested():
                    policy = cls(
                        version=version,
                        valid_from=entry.valid_from,
                        default_category=entry.default_category,
                        reason=entry.reason,
                    )
                    # The generated __init__ knows nothing about relationships, so it reports
                    # policy_id as missing even though SQLAlchemy fills it in on flush.
                    policy.rates = [
                        PricingPolicyRate(  # pyright: ignore[reportCallIssue]
                            item_id=items[rate.sku], credits_per_unit=rate.credits_per_unit
                        )
                        for rate in entry.rates
                    ]
                    policy.category_multipliers = [
                        PricingPolicyCategoryMultiplier(  # pyright: ignore[reportCallIssue]
                            category=item.category, multiplier=item.multiplier
                        )
                        for item in entry.category_multipliers
                    ]
                    session.add(policy)
                    session.flush()
            except IntegrityError:
                logging.info("Pricing policy version %s was taken while loading; re-reading", version)
                session.expire_all()

                current = cls.current(session)
                if current is not None and current.fingerprint() == wanted:
                    return None

                if not remaining:
                    raise

                continue

            logging.info("Minted pricing policy version %s", version)

            return policy

        # Unreachable: the last attempt either returns or re-raises.
        raise AssertionError

    @classmethod
    def _resolve_rated_items(cls, session: Session, entry: ConfiguredPolicy) -> dict[str, UUID]:
        skus = [rate.sku for rate in entry.rates]

        found = {
            sku: uuid
            for sku, uuid in session.execute(
                select(col(BillingItem.sku), col(BillingItem.uuid)).where(col(BillingItem.sku).in_(skus))
            ).all()
        }

        if missing := sorted(set(skus) - set(found)):
            raise ValueError(f"`pricing_policy.rates` names SKUs which do not exist: {', '.join(missing)}")

        return found


class PricingPolicyRate(SQLModel, table=True):
    """The credits charged per unit of one SKU under one policy."""

    __tablename__ = "pricing_policy_rate"  # pyright: ignore[reportAssignmentType]

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)
    policy_id: UUID = SQLModelField(foreign_key="pricing_policy.uuid")
    item_id: UUID = SQLModelField(foreign_key="billing_item.uuid")
    credits_per_unit: Decimal

    policy: PricingPolicy = Relationship(back_populates="rates")
    item: BillingItem = Relationship()

    __table_args__ = (UniqueConstraint("policy_id", "item_id"),)


class PricingPolicyCategoryMultiplier(SQLModel, table=True):
    """The multiplier applied to every rate in one policy, for one workspace category.

    An unknown category resolves to `default_category`, so a value nobody has configured is a pricing
    decision rather than a validation failure.
    """

    __tablename__ = "pricing_policy_category_multiplier"  # pyright: ignore[reportAssignmentType]

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)
    policy_id: UUID = SQLModelField(foreign_key="pricing_policy.uuid")
    category: str
    multiplier: Decimal

    policy: PricingPolicy = Relationship(back_populates="category_multipliers")

    __table_args__ = (UniqueConstraint("policy_id", "category"),)


def _policy_load_options() -> tuple[_AbstractLoad, ...]:
    """Eager-load a policy's rates, their items, and its category multipliers."""
    return (
        selectinload(PricingPolicy.rates).selectinload(  # pyright: ignore[reportArgumentType]
            PricingPolicyRate.item  # pyright: ignore[reportArgumentType]
        ),
        selectinload(PricingPolicy.category_multipliers),  # pyright: ignore[reportArgumentType]
    )


class TimeAggregation(StrEnum):
    """Periods that usage data can be totalled over."""

    DAY = "day"
    MONTH = "month"


class AfterBillingEventNotFound(Exception):
    """Raised when paging and specifying the page after an unknown event"""

    pass


class BillingEvent(SQLModel, table=True):
    """This records a particular workspace's consumption of a particular BillingItem at a particular
    time or over a particular period. This consumption is priced at its start date.

    BillingEvents can be aggregated over time. A series of billing events can be combined if
    the user, workspace and item are the same and if they occur within the same day. The UUID
    of the first event is kept. They can also be split if the event time period includes
    midnight.

    The 'workspace' field should always refer to a workspace in WorkspaceAccount, but there is
    no foreign key: messages from the workspace service may arrive late or never, and losing a
    billing event is worse than holding a dangling reference.
    """

    __tablename__ = "billing_event"  # pyright: ignore[reportAssignmentType]

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
        CheckConstraint("event_start <= event_end", name="start_before_end"),
        # The next two are listed in UNCOMPARED_INDEXES in alembic/env.py, because PostgreSQL
        # normalises the expressions and Alembic then reports them as changed forever. That
        # exclusion also stops autogenerate emitting them, so they are written by hand in the
        # baseline migration: change one here and you must change the migration too, because
        # `alembic check` reports clean when they are missing.
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
        # With no aggregation the raw table is the source of rows to filter, sort, page and
        # return. With aggregation it is a sub-SELECT computing the totals, and the UUID
        # assigned is the lexicographically largest of the rows aggregated. That can misbehave
        # on the last pages, because events arriving while paging change the maximum UUIDs.
        if time_aggregation is not None:
            # Coerced rather than trusted: the value is interpolated into the SQL below, so the
            # closed set has to be enforced at runtime and not only in the type hints.
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

            # The table's own columns, not the ORM attributes. `.columns()` describes the result
            # of the text above, so a Column is what it wants, and col() hands back `Mapped[...]`.
            table = SQLModel.metadata.tables[str(cls.__tablename__)]
            select_aggregated_events = select_aggregated_events.columns(
                table.c.uuid,
                table.c.event_start,
                table.c.event_end,
                table.c.item_id,
                table.c.user,
                table.c.workspace,
                table.c.quantity,
            )

            billingevent_src = aliased(BillingEvent, select_aggregated_events.subquery())
        else:
            billingevent_src = cls

        # The join exists for the ordering and paging predicates below, which compare
        # BillingItem.sku. It does not populate `item`, so reading event.item.sku on the way out
        # cost one query per row. selectinload rather than contains_eager, which would reuse the
        # join and then depend on a join that exists only for ordering.
        #
        # Column handles for everything below, named once so the paging comparison reads as the
        # tuple comparison it is.
        event_start = col(billingevent_src.event_start)
        event_end = col(billingevent_src.event_end)
        event_workspace = col(billingevent_src.workspace)
        event_uuid = col(billingevent_src.uuid)
        item_sku = col(BillingItem.sku)

        all_billing_events = (
            select(billingevent_src)
            .join(BillingItem, col(BillingItem.uuid) == col(billingevent_src.item_id))
            .options(selectinload(billingevent_src.item))  # pyright: ignore[reportArgumentType]
        )

        # We need a complete and certain order so that the 'after' parameter works.
        query = all_billing_events.order_by(
            event_start,
            event_end,
            event_workspace,
            item_sku,
            event_uuid,
        )

        query = query.limit(limit)

        if workspace is not None:
            query = query.where(event_workspace == workspace)

        if account is not None:
            query = query.join(WorkspaceAccount, col(WorkspaceAccount.workspace) == event_workspace).where(
                col(WorkspaceAccount.account) == account
            )

        if start is not None:
            query = query.where(event_start >= start)

        if end is not None:
            query = query.where(event_end < end)

        if after is not None:
            # Equivalent to session.get(cls, after), but works when billingevent_src is an alias.
            after_be = session.execute(select(billingevent_src).where(event_uuid == after)).scalar_one_or_none()

            if after_be is None:
                raise AfterBillingEventNotFound(f"No records matching after={after} found")

            # Everything strictly after `after_be` in the ordering above: a lexicographic
            # comparison over (event_start, event_end, workspace, sku, uuid), spelled out
            # because PostgreSQL cannot use the index for a row-value comparison here.
            query = query.where(
                event_start >= after_be.event_start,
                or_(
                    event_start > after_be.event_start,
                    and_(
                        event_start == after_be.event_start,
                        event_end > after_be.event_end,
                    ),
                    and_(
                        event_start == after_be.event_start,
                        event_end == after_be.event_end,
                        event_workspace > after_be.workspace,
                    ),
                    and_(
                        event_start == after_be.event_start,
                        event_end == after_be.event_end,
                        event_workspace == after_be.workspace,
                        item_sku > after_be.item.sku,
                    ),
                    and_(
                        event_start == after_be.event_start,
                        event_end == after_be.event_end,
                        event_workspace == after_be.workspace,
                        item_sku == after_be.item.sku,
                        event_uuid > after,
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
        """Returns the most recent BillingEvent, optionally constrained by workspace and item."""
        query = select(cls).order_by(col(cls.event_end).desc()).limit(1)

        if workspace is not None:
            query = query.where(col(cls.workspace) == workspace)

        if sku is not None:
            query = query.join(BillingItem).where(col(BillingItem.sku) == sku)

        return session.execute(query).scalar_one_or_none()

    @classmethod
    def insert_from_message(cls, session: Session, msg: eodhp_utils.pulsar.messages.BillingEvent) -> UUID | None:
        """Adds a new BillingEvent to the DB based on a Pulsar message.

        Deals with duplicated UUIDs by ignoring the second message and returning None.
        """
        result = session.execute(
            insert(cls)
            .values(
                uuid=UUID(str(msg.uuid)),
                event_start=datetime_default_to_utc(datetime.fromisoformat(str(msg.event_start))),
                event_end=datetime_default_to_utc(datetime.fromisoformat(str(msg.event_end))),
                item_id=select(col(BillingItem.uuid)).where(col(BillingItem.sku) == msg.sku).scalar_subquery(),
                user=UUID(str(msg.user)) if msg.user else None,
                workspace=msg.workspace,
                quantity=msg.quantity,
            )
            .on_conflict_do_nothing(index_elements=["uuid"])
            .returning(col(BillingEvent.uuid))
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
    """A consumption rate sample is a point-in-time sample of the rate at which a user is consuming a
    billed-for resources, typically storage but it could be any other resource where the time it's
    held for is the basis for the charge.

    For example, if we measure storage use at 8GB then the consumption rate sample would be
    '8GB-seconds per second'. The billable resource is measured in GB-seconds, and every second 8
    of them are consumed.

    Samples are used to generate estimated BillingEvents periodically by, effectively, interpolating
    between samples and integrating.

    If we go for a separate billing system for non-transient items such as storage, this table can go.
    """

    __tablename__ = "billing_resource_consumption_rate_sample"  # pyright: ignore[reportAssignmentType]

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
                item_id=(select(col(BillingItem.uuid)).where(col(BillingItem.sku) == msg.sku).scalar_subquery()),
                user=UUID(str(msg.user)) if msg.user else None,
                workspace=msg.workspace,
                rate=msg.rate,
            )
            .on_conflict_do_nothing(index_elements=["uuid"])
            .returning(col(cls.uuid))
        )

        return result.scalar_one_or_none()

    @classmethod
    def find_data_for_interval(
        cls, session: Session, workspace: str, sku: str, start: datetime, end: datetime
    ) -> Sequence[Self]:
        """The samples covering an interval: the last one before it, those inside it, and the
        first one after it. The bracketing samples are what make interpolation possible.
        """
        item_subquery = select(col(BillingItem.uuid)).where(col(BillingItem.sku) == sku).scalar_subquery()
        last_before_start = (
            select(cls)
            .where(col(cls.item_id) == item_subquery)
            .where(col(cls.workspace) == workspace)
            .where(col(cls.sample_time) <= start)
            .order_by(col(cls.sample_time).desc())
            .limit(1)
        )
        first_after_end = (
            select(cls)
            .where(col(cls.item_id) == item_subquery)
            .where(col(cls.workspace) == workspace)
            .where(col(cls.sample_time) >= end)
            .order_by(col(cls.sample_time))
            .limit(1)
        )
        in_period = (
            select(cls)
            .where(col(cls.item_id) == item_subquery)
            .where(col(cls.workspace) == workspace)
            .where(col(cls.sample_time) > start)
            .where(col(cls.sample_time) < end)
        )

        query = select(cls).from_statement(
            union(last_before_start, first_after_end, in_period).order_by("sample_time")
        )
        return session.execute(query).scalars().all()

    @classmethod
    def calculate_consumption_for_interval(
        cls, session: Session, workspace: str, sku: str, start: datetime, end: datetime
    ) -> float | None:
        """This calculates estimated consumption within a time interval, using linear interpolation
        to estimate consumption rates from samples and then (effectively) integrating.

        It's assumed that the resource did not exist (zero consumption rate) before the first
        sample and after the last sample. Callers should endeavour not to call this for an interval
        until sample collection has got as far as at least one sample after the end of the
        interval. If no sample exists after the end of the interval then, if one is later
        collected, the answer given by this method will change.

        The arithmetic is in accounting_service.consumption, which owns no database.
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
        """Returns the first observed sample for the given constraints."""
        query = select(cls).order_by(col(cls.sample_time)).limit(1)

        if workspace is not None:
            query = query.where(col(cls.workspace) == workspace)

        if item_id is not None:
            query = query.where(col(cls.item_id) == item_id)

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


class TransactionType(StrEnum):
    """What kind of act a ledger row records.

    Adding a value later is straightforward; removing or renaming one needs a replacement type
    and a swap of every column using it, so the value set is worth choosing deliberately.
    """

    DEBIT = "debit"
    GRANT = "grant"
    REVERSAL = "reversal"


class CreditLedgerTransaction(SQLModel, table=True):
    """One movement of credits: a usage debit, an admin grant, or a correction.

    Append-only. A correction is a new row referencing the one it corrects. So concurrent
    debits never contend for a row, a balance needs no lock and no mutable total, and every row
    records what was actually charged at the time - which is what lets a period be re-priced
    without destroying that record.
    """

    __tablename__ = "credit_ledger_transaction"  # pyright: ignore[reportAssignmentType]

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)

    workspace: str

    # Denormalised from the billing event rather than joined through it. Per-user budgets
    # and the per-user usage filter both need it, and a grant has no billing event to join
    # through, so under a join every grant would have no user at all. Null on a grant marks it
    # as belonging to the whole workspace pool.
    user: UUID | None = SQLModelField(default=None)

    transaction_type: TransactionType = pg_enum(TransactionType, "transaction_type")

    credits: Decimal

    # Set for a usage debit and for a correction of one. Null for a grant, which prices nothing
    # and meters nothing.
    billing_event_id: UUID | None = SQLModelField(default=None, foreign_key="billing_event.uuid")
    item_id: UUID | None = SQLModelField(default=None, foreign_key="billing_item.uuid")
    quantity: float | None = SQLModelField(default=None)

    # How this was priced: a reference to the policy, not a copy of its numbers, so the charge
    # can be recomputed rather than merely re-read. Nullable because a grant is not priced, and
    # a non-null column would have to be filled with a policy that did not apply to it. The
    # invariant that does hold is the `debit_is_priced` constraint below.
    policy_id: UUID | None = SQLModelField(default=None, foreign_key="pricing_policy.uuid")
    category: str | None = SQLModelField(default=None)

    # Period filters want when the usage happened; audit and reconciliation want when this
    # service learned of it. A backfilled event carries an old `occurred_at` and a recent
    # `recorded_at`, and one column cannot answer both.
    occurred_at: datetime = aware_timestamp()
    recorded_at: datetime = aware_timestamp(default=func.now())

    # Corrections. `correction_batch_id` has no foreign key yet - adds the
    # `correction_batch` table and the constraint with it - but the column exists now because
    # the idempotency index below tests it.
    reverses_id: UUID | None = SQLModelField(default=None, foreign_key="credit_ledger_transaction.uuid")
    correction_batch_id: UUID | None = SQLModelField(default=None)

    # The hub_admin responsible for a grant or a correction. Null on a usage debit.
    created_by: UUID | None = SQLModelField(default=None)
    reason: str | None = None

    item: BillingItem | None = Relationship()
    policy: PricingPolicy | None = Relationship()

    @property
    def occurred_at_utc(self) -> datetime:
        return as_utc(self.occurred_at)

    @property
    def recorded_at_utc(self) -> datetime:
        return as_utc(self.recorded_at)

    __table_args__ = (
        # One original debit per billing event, while still allowing correction rows against
        # that same event: a plain unique constraint on billing_event_id would block re-pricing
        # entirely.
        #
        # The WHERE clause is load-bearing in both directions. PostgreSQL treats NULLs as
        # distinct in a unique index, so without it correction rows would not be constrained at
        # all and original debits would not be protected from each other.
        Index(
            "credit_ledger_original_debit_index",
            "billing_event_id",
            unique=True,
            postgresql_where=text("correction_batch_id IS NULL AND transaction_type = 'debit'"),
        ),
        Index(
            "credit_ledger_workspace_recorded_index",
            "workspace",
            "recorded_at",
        ),
        # A debit is priced, so it has a policy and a category. Stated as a rule about debits
        # rather than as an equivalence with "is a grant", because a reversal of a debit carries
        # the original's policy while a reversal of a grant would carry none.
        CheckConstraint(
            "transaction_type <> 'debit' OR (policy_id IS NOT NULL AND category IS NOT NULL)",
            name="debit_is_priced",
        ),
    )

    @classmethod
    def record_usage_debit(
        cls,
        session: Session,
        event: "BillingEvent",
        priced: PricedUsage,
        policy_id: UUID,
    ) -> UUID | None:
        """Charge a billing event, or return None if it has already been charged.

        The charge arrives positive from `price_usage` and is stored negated.

        Idempotent through the partial unique index rather than through a prior SELECT.
        Checking first and inserting second leaves a race between the two, and this runs on a
        consumer that can redeliver a message and be restarted mid-transaction, so
        `on_conflict_do_nothing` collapses both into one statement the database arbitrates.

        `BillingEvent.insert_from_message` deduplicates on the message UUID; this protects
        against the same stored event being priced twice.

        Does not commit, so the event and its debit land together or not at all.
        """
        result = session.execute(
            insert(cls)
            .values(
                workspace=event.workspace,
                user=event.user,
                transaction_type=TransactionType.DEBIT,
                credits=-priced.credits,
                billing_event_id=event.uuid,
                item_id=event.item_id,
                quantity=priced.quantity,
                policy_id=policy_id,
                category=priced.category,
                occurred_at=event.event_start_utc,
            )
            .on_conflict_do_nothing(
                index_elements=["billing_event_id"],
                index_where=text("correction_batch_id IS NULL AND transaction_type = 'debit'"),
            )
            .returning(col(cls.uuid))
        )

        return result.scalar_one_or_none()

    @classmethod
    def record_grant(
        cls,
        session: Session,
        workspace: str,
        credits: Decimal,
        reason: str,
        created_by: UUID | None = None,
        occurred_at: datetime | None = None,
    ) -> Self:
        """Add credits to a workspace's pool.

        Positive, and carrying no policy or category: a grant is not priced. `user` is left
        null, which marks the grant as belonging to the whole workspace rather than to one
        member.

        `reason` is required. A grant is a privileged write with no payment behind it, so
        the audit log has nothing to show but the reason somebody gave.

        Not idempotent: two identical grants are two grants, because unlike a redelivered
        billing event there is no natural key saying they are the same act.

        Does not commit.
        """
        transaction = cls(
            workspace=workspace,
            transaction_type=TransactionType.GRANT,
            credits=credits,
            reason=reason,
            created_by=created_by,
            occurred_at=occurred_at or datetime.now(UTC),
        )

        session.add(transaction)
        session.flush()

        return transaction

    @classmethod
    def balance(cls, session: Session, workspace: str, user: UUID | None = None) -> Decimal:
        """The workspace's credit balance, or one user's net spend within it.

        The latest snapshot plus every row recorded after it. With no snapshot stored this reads
        the whole ledger and is still correct, which is what lets a snapshot be rebuilt or
        discarded at any time.

        The delta is taken on `recorded_at`, never `occurred_at`. A backfilled event carries an
        old `occurred_at`, so on that column it would fall after the snapshot's cut and also
        outside the delta, and vanish from the balance.

        Passing `user` gives that user's net spend against the shared pool rather than an
        allowance of their own: grants carry no user, so they are not in the sum.
        """
        snapshot = CreditBalanceSnapshot.latest(session, workspace, user)
        opening = snapshot.balance if snapshot else Decimal(0)

        delta = select(func.coalesce(func.sum(col(cls.credits)), Decimal(0))).where(col(cls.workspace) == workspace)

        if snapshot:
            delta = delta.where(col(cls.recorded_at) > snapshot.as_of_utc)

        if user is not None:
            delta = delta.where(col(cls.user) == user)

        return opening + session.execute(delta).scalar_one()

    @classmethod
    def find_transaction(cls, session: Session, uuid_: UUID, workspace: str | None = None) -> Self | None:
        """One transaction, with everything needed to explain the charge loaded.

        The policy comes with its rates and multipliers, because the explanation recomputes the
        charge rather than reading it back: the row stores the quantity, the policy and the
        resolved category but not the rate or the multiplier.

        `workspace` scopes the lookup. A transaction UUID is not a capability, so an endpoint
        under /workspaces/{workspace}/ must not hand back a row belonging to another workspace
        merely because the UUID was right.
        """
        query = (
            select(cls)
            .options(
                selectinload(cls.item),  # pyright: ignore[reportArgumentType]
                selectinload(cls.policy).options(  # pyright: ignore[reportArgumentType]
                    *_policy_load_options()
                ),
            )
            .where(col(cls.uuid) == uuid_)
        )

        if workspace is not None:
            query = query.where(col(cls.workspace) == workspace)

        return session.execute(query).scalars().first()

    @classmethod
    def recent_transactions(cls, session: Session, workspace: str, limit: int = 50) -> Sequence[Self]:
        """The workspace's most recent transactions, newest first.

        Ordered on `recorded_at`, the order they were written rather than the order the usage
        happened in, so a backfilled event appears at the top where it can be noticed.

        For the admin CLI. The API's ledger endpoint is a different query: it groups and
        nets, because an individual reversal must not appear there as an event of its own.
        """
        return (
            session.execute(
                select(cls)
                .options(selectinload(cls.item))  # pyright: ignore[reportArgumentType]
                .where(col(cls.workspace) == workspace)
                .order_by(col(cls.recorded_at).desc())
                .limit(limit)
            )
            .scalars()
            .all()
        )

    def __repr__(self) -> str:
        return (
            "CreditLedgerTransaction("
            + f"{self.uuid=}, "
            + f"{self.workspace=}, "
            + f"{self.transaction_type=}, "
            + f"{self.credits=}, "
            + f"{self.occurred_at=})"
        )


class CreditBalanceSnapshot(SQLModel, table=True):
    """A workspace's balance as at one instant, so a balance read need not sum every row.

    An optimisation and not a source of truth: the ledger alone always gives the right answer,
    so a snapshot can be rebuilt or thrown away at any time. Nothing writes one yet. What this
    table buys is a cheap balance for the budget checks that run on every billing event.

    `as_of` is compared against `recorded_at`, never `occurred_at`. A backfilled event carries
    an old `occurred_at`, and on that column it would be counted in neither the snapshot nor
    the delta.

    Keyed on workspace and user the same way budgets are. A null `user` is the whole-pool total
    that the balance endpoint reads; a row naming a user serves a per-user threshold check. The
    surrogate primary key is because a primary key column cannot be null, and the unique
    constraint declares NULLS NOT DISTINCT because PostgreSQL otherwise counts two null users
    as different values and would let one instant be snapshotted twice.

    Whatever writes a snapshot must take `as_of` from the `recorded_at` of the newest row it
    included, not from the clock: `func.now()` is the transaction timestamp, so rows written
    together share it and a cut at that instant would drop all of them from the delta.
    """

    __tablename__ = "credit_balance_snapshot"  # pyright: ignore[reportAssignmentType]

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)
    workspace: str
    user: UUID | None = SQLModelField(default=None)
    as_of: datetime = aware_timestamp()
    balance: Decimal

    __table_args__ = (
        UniqueConstraint("workspace", "user", "as_of", postgresql_nulls_not_distinct=True),
        Index("credit_balance_snapshot_lookup_index", "workspace", "user", "as_of"),
    )

    @property
    def as_of_utc(self) -> datetime:
        return as_utc(self.as_of)

    @classmethod
    def latest(cls, session: Session, workspace: str, user: UUID | None = None) -> Self | None:
        """The most recent snapshot for this workspace, or for one user within it.

        `user=None` means the whole-pool row, which is not the same as "any user", so this
        matches IS NULL rather than leaving the filter off.
        """
        by_user = col(cls.user) == user if user is not None else col(cls.user).is_(None)

        return (
            session.execute(
                select(cls)
                .where(col(cls.workspace) == workspace)
                .where(by_user)
                .order_by(col(cls.as_of).desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
