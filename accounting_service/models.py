"""The tables, and the queries over them.

Every table lives here. `alembic/env.py` imports `metadata` from this module, and defining
the classes is what populates it, so a table declared elsewhere would need its own import in
env.py to be seen by autogenerate - which an import cleanup once removed, leaving
autogenerate ready to drop the whole schema.

Two conventions worth knowing before adding a query.

**Wrap a column in `col()`.** SQLModel declares fields as plain annotations rather than
SQLAlchemy's `Mapped[...]`, so at class level a checker sees the Python value type: `cls.sku
== sku` is a `bool`, `select(cls.version)` is `select(int)`, and `cls.event_end.desc()` is an
attribute error on `datetime`. `col()` hands back the column, so the expression types as the
SQL it always was. This module used to carry a file-level suppression of five rules instead;
`col()` replaced it, and the remaining suppressions are inline and specific.

**Three things `col()` cannot fix**, each suppressed on the line it occurs:

  * `__tablename__ = "..."`, because SQLModel's base declares it as a `declared_attr`. No
    spelling avoids this - annotating it or making it a `ClassVar` trades one diagnostic for
    another.
  * `selectinload(...)`, which wants the attribute itself. `col()` returns `Mapped[...]`, and
    SQLModel types a `Relationship()` attribute as the related class, so nothing satisfies
    both the checker and SQLAlchemy. `.columns()` on a text query is the mirror image: it
    wants a real `Column`, which the metadata can supply.
  * Building a row through a relationship, such as `PricingPolicyRate(item_id=...)` attached
    via `policy.rates`. The generated `__init__` knows nothing about relationships, so it
    reports the foreign key as missing even though SQLAlchemy fills it in on flush.
"""

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
    UniqueConstraint,
    and_,
    func,
    or_,
    select,
    text,
    union,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, aliased, selectinload
from sqlalchemy.orm.strategy_options import _AbstractLoad
from sqlmodel import Field as SQLModelField
from sqlmodel import Relationship, SQLModel, col

from accounting_service.configuration import ConfiguredItem
from accounting_service.consumption import ConsumptionWindow, RateSample, estimate_consumption
from accounting_service.pricing import ConfiguredPolicy, PolicyFingerprint, RateCard
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


# Re-exported so that alembic/env.py depends on importing this module rather than on someone
# remembering to. Defining the table classes below is what populates the metadata, so
# `target_metadata = SQLModel.metadata` in env.py left the import of this module unused - and
# an automated import cleanup then emptied it. Autogenerate reported every table as removed and
# would have generated a revision dropping the whole schema.
metadata = SQLModel.metadata

# How many times to retry a version that another replica claimed first. More than two
# replicas racing on the same load is not a case worth designing for: the loser re-reads and
# usually finds nothing left to do.
_MINT_ATTEMPTS = 3


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
    # sa_type is annotated as `type[Any]` but SQLAlchemy wants the configured instance, which
    # is the whole point of passing TIMESTAMP(timezone=True) rather than TIMESTAMP.
    return SQLModelField(
        sa_type=TIMESTAMP(timezone=True),  # pyright: ignore[reportArgumentType]
        default=default,
        index=index,
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

    __tablename__ = "billing_item"  # pyright: ignore[reportAssignmentType]

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
        query = select(cls).where(col(cls.sku) == sku)
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
    def upsert_configured_item(cls, session: Session, entry: ConfiguredItem) -> None:
        """
        Insert or update a BillingItem from a validated configuration entry.

        The item is inserted when its SKU is unknown, otherwise its name and unit are updated.
        Both are written unconditionally, because an entry describes the item completely.

        This used to take a bare dict and update only the keys it found, which is what let
        `BillingItem(**item)` construct a row from unvalidated YAML: BillingItem is a table
        class, so Pydantic validation is off and the dict was spread in unchecked. The partial
        update that behaviour supported belongs to the admin CLI, which fills the fields the
        operator omitted from the stored row before building a document.
        """
        item_obj = cls.find_billing_item(session, entry.sku)

        if item_obj:
            item_obj.name = entry.name
            item_obj.unit = entry.unit
        else:
            session.add(BillingItem(sku=entry.sku, name=entry.name, unit=entry.unit))


def _next_version(session: Session) -> int:
    """One past the highest version stored.

    A function rather than inline so a test can make it collide on purpose; there is no way
    to provoke the race from a single connection otherwise.
    """
    return (session.execute(select(func.max(col(PricingPolicy.version)))).scalar() or 0) + 1


class PricingPolicy(SQLModel, table=True):
    """
    One calibration pass, covering every rate at once (D3).

    Rows are immutable once written. A correction does not edit a policy; it adds a new one
    pointing at the policy it corrects through `corrects_id`. That is what lets a period which
    has already been charged be re-priced without destroying the record of what was charged at
    the time (D8).

    Bi-temporal. `valid_from` and `valid_until` say
    which usage the policy applies to; `configured_at` says when the decision was taken.
    Resolving a policy for a usage time selects on the validity range and orders by
    `configured_at` descending, so a correcting policy wins over the policy it corrects (T5).

    There is no index on the validity columns. A policy is one deliberate calibration pass, so
    this table holds a handful of rows where `billing_item_price` holds one per price change
    per SKU, and the index that table needs would only be overhead here.
    """

    __tablename__ = "pricing_policy"  # pyright: ignore[reportAssignmentType]

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)

    # Human-usable identifier, minted as max + 1 by the loader (T4). Unique is the part a
    # database can enforce; monotonic is a property of how the loader assigns it. The
    # constraint matters because the loader runs on every ingester pod start, so two replicas
    # can try to mint the same version at once and one of them has to lose.
    version: int = SQLModelField(unique=True)

    valid_from: datetime = aware_timestamp()
    valid_until: datetime | None = aware_timestamp(default=None)
    configured_at: datetime = aware_timestamp(default=func.now())

    # Set when this policy corrects an earlier one (D8). Deliberately a bare foreign key with
    # no relationship attribute: following it is an audit path (T19) that can query by uuid,
    # and a self-referential relationship needs a `remote_side` that would earn its keep only
    # once something walks the chain.
    corrects_id: UUID | None = SQLModelField(default=None, foreign_key="pricing_policy.uuid")

    # Applied to a workspace that has no category assignment yet (D6).
    default_category: str

    # Why this calibration happened. Feeds the audit log (T19).
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
        """The policy in force, or None when none has been loaded yet.

        The most recently configured policy, not the one with the latest `valid_from`. A
        correction is configured after the policy it corrects but is backdated to cover the
        same period, so `configured_at` is what decides which of two overlapping policies
        wins (D8).

        Ties are broken on `version` descending, because `configured_at` defaults to
        `func.now()` and that is the transaction timestamp: two policies minted in one
        transaction carry the same value.

        Rates are loaded with their items, because the fingerprint is keyed on SKU and
        walking `rate.item` per row would be a query each.
        """
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

        The most recently configured policy whose `valid_from` is at or before `at`. A
        correction is configured after the policy it corrects and backdated over the same
        period, so `configured_at` is what decides between two policies that both cover the
        time (D8). Ties break on `version` descending, because `configured_at` defaults to
        the transaction timestamp and two policies minted together share it.

        When `at` predates every policy, the earliest policy prices it (D10). That happens
        for an event backfilled from before the first calibration. The alternatives were to
        skip the event, losing a charge silently, or to park it for retry, which needs a
        queue and a way to notice the queue filling up.

        This differs from `current()`, which ignores validity entirely and answers "what did
        we configure last" for the loader. A policy dated next month is what we configured
        last, and is not what prices anything today.
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
        """This policy's numbers, in the form pricing needs them (T7).

        The counterpart to `fingerprint`, and the same projection: a stored policy and the
        document that minted it price identically, because both become the same value object
        before any arithmetic happens. `_policy_load_options` has already loaded the rates and
        their items, so this walks no relationship that costs a query.
        """
        return RateCard.of(
            default_category=self.default_category,
            rates=[(rate.item.sku, rate.credits_per_unit) for rate in self.rates],
            category_multipliers=[(entry.category, entry.multiplier) for entry in self.category_multipliers],
        )

    @classmethod
    def load_configured_policy(cls, session: Session, entry: ConfiguredPolicy) -> Self | None:
        """Mint a new version, or return None when the document is already in force.

        Called on every ingester pod start, so the common case is a document that has not
        changed and this does nothing. Only a change to the numbers, the exchange rate, the
        default category or `valid_from` mints a version - see PolicyFingerprint.

        Two replicas starting together both see the same current policy and both try to mint
        the same version. The unique constraint on `version` refuses the second, and this
        recovers rather than failing the pod: the write happens inside a savepoint so the
        caller's transaction survives, and the loser re-reads. If the winner minted what
        this document describes, there is nothing left to do.

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
    """
    The credits charged per unit of one SKU under one policy.

    One row per SKU per policy. T7 prices an event from this rate, the event's quantity and
    the multiplier for the workspace's category.

    The unique constraint on `(policy_id, item_id)` is what makes a policy well formed: a
    second rate for the same SKU would make the price of an event depend on which row a query
    happened to return first.
    """

    __tablename__ = "pricing_policy_rate"  # pyright: ignore[reportAssignmentType]

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)
    policy_id: UUID = SQLModelField(foreign_key="pricing_policy.uuid")
    item_id: UUID = SQLModelField(foreign_key="billing_item.uuid")
    credits_per_unit: Decimal

    policy: PricingPolicy = Relationship(back_populates="rates")
    item: BillingItem = Relationship()

    __table_args__ = (UniqueConstraint("policy_id", "item_id"),)


class PricingPolicyCategoryMultiplier(SQLModel, table=True):
    """
    The multiplier applied to every rate in one policy, for one workspace category.

    A category is a plain string rather than an enum, because the set is defined by the
    configuration document and by whatever the workspace service sends, not by this service.
    An unknown category resolves to the policy's `default_category` (D6), so a value nobody
    has configured is a pricing decision rather than a validation failure.
    """

    __tablename__ = "pricing_policy_category_multiplier"  # pyright: ignore[reportAssignmentType]

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)
    policy_id: UUID = SQLModelField(foreign_key="pricing_policy.uuid")
    category: str
    multiplier: Decimal

    policy: PricingPolicy = Relationship(back_populates="category_multipliers")

    __table_args__ = (UniqueConstraint("policy_id", "category"),)


def _policy_load_options() -> tuple[_AbstractLoad, ...]:
    """Eager-load a policy's rates, their items, and its category multipliers.

    Every read of a policy needs all three: the fingerprint is keyed on SKU, so walking
    `rate.item` per row would be a query each.

    The two suppressions live here rather than at each call site. `col()` is no help for a
    loader option: it returns `Mapped[...]`, and `selectinload` wants the attribute itself.
    SQLModel types a `Relationship()` attribute as the related class, so there is nothing to
    hand it that satisfies both the checker and SQLAlchemy.
    """
    return (
        selectinload(PricingPolicy.rates).selectinload(  # pyright: ignore[reportArgumentType]
            PricingPolicyRate.item  # pyright: ignore[reportArgumentType]
        ),
        selectinload(PricingPolicy.category_multipliers),  # pyright: ignore[reportArgumentType]
    )


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

            # The table's own columns, not the ORM attributes. `.columns()` is describing the
            # result of the text above, so a Column is what it wants - and col() cannot supply
            # one, because it hands back `Mapped[...]`.
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

        # The join is here for the ordering and paging predicates below, which compare
        # BillingItem.sku. It does not populate `item`, so reading event.item.sku on the way
        # out cost one query per row - a page of 100 events issued 101 queries.
        #
        # selectinload rather than contains_eager: contains_eager would reuse the join and
        # need no second query at all, but it would then depend on a join that exists for
        # ordering and could reasonably be removed. selectinload is one extra query for the
        # whole page and is independent of the query's shape.
        # Column handles for everything below. col() is needed because SQLModel declares fields
        # as plain annotations, so `billingevent_src.event_start >= start` reads as a `bool` to
        # a type checker rather than a SQL predicate. Naming them once also lets the paging
        # comparison below read as the tuple comparison it is, instead of drowning in prefixes.
        event_start = col(billingevent_src.event_start)
        event_end = col(billingevent_src.event_end)
        event_workspace = col(billingevent_src.workspace)
        event_uuid = col(billingevent_src.uuid)
        item_sku = col(BillingItem.sku)

        all_billing_events = (
            select(billingevent_src)
            .join(BillingItem, col(BillingItem.uuid) == col(billingevent_src.item_id))
            # selectinload takes the attribute itself, which col() cannot supply: it hands back
            # `Mapped[...]`, and SQLModel types a Relationship() attribute as the related class.
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
            # This is equivalent to
            #   after_be = session.get(cls, after)
            # but it works when billingevent_src is an alias rather than an ORM class.
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
        """
        Returns the most recent BillingEvent, optionally constrained by workspace and item.
        """
        query = select(cls).order_by(col(cls.event_end).desc()).limit(1)

        if workspace is not None:
            query = query.where(col(cls.workspace) == workspace)

        if sku is not None:
            query = query.join(BillingItem).where(col(BillingItem.sku) == sku)

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
