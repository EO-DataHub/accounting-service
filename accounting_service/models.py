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


def pg_enum(values: "type[StrEnum]", name: str) -> Any:  # noqa: ANN401 - as aware_timestamp, SQLModel's Field() returns Any
    """Declare a native PostgreSQL enum column over a `StrEnum`.

    The sibling of `aware_timestamp()`, and it exists for the same reason: the obvious
    spelling is silently wrong. `SAEnum(SomeEnum)` persists the *member names* - DEBIT, GRANT,
    REVERSAL - because SQLAlchemy reads `.name` off each member by default. A `StrEnum` exists
    precisely so its values are the wire and storage form, so what reaches the database would
    be the one spelling nothing else in the codebase uses. `values_callable` is what asks for
    the values instead.

    That fault does not present as a type error or a failed write. It presents as a check
    constraint or a literal comparing against a label the type does not have, which
    PostgreSQL rejects at DDL time with a message about the enum rather than about the
    spelling. The first instance cost the ledger's `debit_is_priced` constraint.

    `name` is given explicitly rather than derived from the class, so a revision has a stable
    type name to create and drop. Renaming the class must not rename a deployed type.

    Two things a revision using this must do by hand, because Alembic autogenerates neither:
    create the type before the table that uses it, and drop it after. `create_table` creates
    it as a side effect, but `drop_table` does not drop it, so without an explicit drop the
    type outlives the table and a downgrade-then-upgrade cycle fails.
    """
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
    """Which pricing category a workspace is charged under (T6, D6).

    A separate table from `workspace_account` rather than a column on it, because
    `WorkspaceAccount.record_mapping` is insert-only by design and silently ignores a change.
    That is right for the account mapping - a workspace does not move between accounts - and
    wrong for a category, which D6 requires to be changeable.

    No history is kept here. History lives on the ledger, which records the category resolved
    at pricing time, so recategorising a workspace never rewrites what it was charged.

    A row being absent is not an error. An unassigned workspace prices under the policy's
    `default_category` (D6), and that is where every workspace sits until the workspace
    service sends the field.
    """

    __tablename__ = "workspace_category"  # pyright: ignore[reportAssignmentType]

    workspace: str = SQLModelField(primary_key=True)
    category: str
    updated_at: datetime = aware_timestamp(default=func.now())
    updated_by: UUID | None = SQLModelField(default=None)

    @classmethod
    def category_for(cls, session: Session, workspace: str) -> str | None:
        """The category assigned to this workspace, or None if it has none.

        None rather than the default, because the default belongs to the policy and this
        table does not know which policy is being applied. `RateCard.multiplier_for` takes
        None and resolves it (D6).
        """
        return session.execute(select(col(cls.category)).where(col(cls.workspace) == workspace)).scalar_one_or_none()

    @classmethod
    def assign(cls, session: Session, workspace: str, category: str, updated_by: UUID | None = None) -> None:
        """Set this workspace's category, replacing any existing assignment.

        An upsert rather than an insert, which is the whole reason this is not a column on
        `workspace_account`. Does not commit: the caller owns the transaction, as it does for
        `insert_configuration`.
        """
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


class TransactionType(StrEnum):
    """What kind of act a ledger row records.

    Metadata, not arithmetic. The sign on `credits` carries the arithmetic, so a balance is a
    plain SUM and nothing has to know the type to total correctly. The type is what an audit
    surface reports (T19) and what the idempotency index keys on.

    A native PostgreSQL enum rather than a string, so an invalid value fails at write time.
    That costs something in the migrations: Alembic does not autogenerate enum changes, so a
    revision must call `SAEnum(...).create(bind)` on upgrade and `drop()` on downgrade or the
    type outlives the table and a downgrade-then-upgrade cycle fails. The value set is
    therefore worth choosing deliberately - adding a value later is easy, removing or
    renaming one needs a replacement type and a swap of every column using it.
    """

    DEBIT = "debit"
    GRANT = "grant"
    REVERSAL = "reversal"


class CreditLedgerTransaction(SQLModel, table=True):
    """One movement of credits: a usage debit, an admin grant, or a correction (T8).

    Append-only. Rows are never updated or deleted; a correction is a new row referencing the
    one it corrects (D7). Two things follow. Concurrent debits never contend for a row, so a
    balance needs no lock and no mutable total - which is why this carries none of the risk
    the original sizing assumed. And every row records what was actually charged at the time,
    so a period can be re-priced without destroying the record of what it cost (D8).

    A balance is `SUM(credits)`. Debits are stored negative and grants positive, so the sign
    carries the arithmetic and `transaction_type` decides nothing about a total. Pricing
    returns a positive charge and this is where it acquires its sign.

    Every input to the charge is stored beside the result: `quantity`, `policy_id` and
    `category`. That is what makes a row replayable months later, and it is what the
    explainable-pricing endpoint (T13), the audit log (T19) and historical re-pricing (T18)
    read. `category` is the one resolved at pricing time, so recategorising a workspace does
    not rewrite its past charges.

    `credits` is an unconstrained NUMERIC. Nothing is rounded on the way in - see the
    docstring on `price_usage` - so fixing a scale here would be the same decision made in
    the schema instead of in the code, and would make a replay reproduce the scale in force
    when the replay ran rather than the policy.
    """

    __tablename__ = "credit_ledger_transaction"  # pyright: ignore[reportAssignmentType]

    uuid: UUID = SQLModelField(default_factory=uuid4, primary_key=True)

    workspace: str

    # Denormalised from the billing event rather than joined through it. The duplication earns
    # its keep twice: per-user budgets (T16) and the per-user usage filter (T12) both need it,
    # and a grant has no billing event to join through, so under a join every grant would
    # have no user at all. Null on a grant marks it as belonging to the whole workspace pool.
    user: UUID | None = SQLModelField(default=None)

    transaction_type: TransactionType = pg_enum(TransactionType, "transaction_type")

    credits: Decimal

    # Set for a usage debit and for a correction of one. Null for a grant, which prices
    # nothing and meters nothing.
    billing_event_id: UUID | None = SQLModelField(default=None, foreign_key="billing_event.uuid")
    item_id: UUID | None = SQLModelField(default=None, foreign_key="billing_item.uuid")
    quantity: float | None = SQLModelField(default=None)

    # How this was priced: a reference to the policy, not a copy of its numbers, so the charge
    # can be recomputed rather than merely re-read.
    #
    # Nullable, where the schema note had both of these NOT NULL. A grant is not priced, so it
    # has no policy and no category, and a non-null column would have to be filled with a
    # policy that did not apply to it. The invariant that actually holds is the check
    # constraint below: a debit has both.
    policy_id: UUID | None = SQLModelField(default=None, foreign_key="pricing_policy.uuid")
    category: str | None = SQLModelField(default=None)

    # Both, because the two questions differ. Period filters want when the usage happened;
    # audit and reconciliation want when this service learned of it. A backfilled event
    # carries an old `occurred_at` and a recent `recorded_at`, and collapsing them into one
    # column makes both queries wrong. Balance snapshots key on `recorded_at` for exactly
    # this reason.
    occurred_at: datetime = aware_timestamp()
    recorded_at: datetime = aware_timestamp(default=func.now())

    # Corrections (T17, T18). `correction_batch_id` has no foreign key yet: T17 adds the
    # `correction_batch` table and the constraint with it. The column exists now because the
    # idempotency index below tests it, and an index cannot reference a column that is not
    # there.
    reverses_id: UUID | None = SQLModelField(default=None, foreign_key="credit_ledger_transaction.uuid")
    correction_batch_id: UUID | None = SQLModelField(default=None)

    # The hub_admin responsible, for a grant or a correction. Null on a usage debit, which no
    # person initiated.
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
        # that same event. A plain unique constraint on billing_event_id would block
        # re-pricing entirely (T18).
        #
        # The WHERE clause is load-bearing in both directions. PostgreSQL treats NULLs as
        # distinct in a unique index, so without it grants would be unconstrained anyway,
        # correction rows would not be constrained at all, and original debits would not be
        # protected from each other.
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
        # Named explicitly: Alembic matches check constraints by name, so an anonymous one can
        # never be matched against the name PostgreSQL invents for it.
        #
        # A debit is priced, so it has a policy and a category. A grant is not, so it has
        # neither. Stated as a rule about debits rather than as an equivalence, because a
        # reversal of a debit carries the original's policy (D7) while a reversal of a grant
        # would carry none, and an equivalence would refuse the second.
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
        """Charge a billing event, or return None if it has already been charged (T9).

        The charge arrives positive from `price_usage` and is stored negated, because the sign
        is the ledger's business rather than the price's.

        Idempotent through the partial unique index, not through a prior SELECT. Checking
        first and inserting second is two statements with a race between them, and the race
        is the case that matters: this runs on a Pulsar consumer that can redeliver a message
        and can be restarted mid-transaction. `on_conflict_do_nothing` collapses both into one
        statement the database arbitrates.

        `BillingEvent.insert_from_message` already deduplicates on the message UUID. This
        protects a different failure: the same stored event being priced twice, after a crash
        between writing the event and writing the debit.

        Does not commit. The caller owns the transaction, and the point of not committing here
        is that the event and its debit land together or not at all.
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

        Positive, and carrying no policy or category: a grant is not priced, so there is
        nothing to record about how it was priced. `user` is left null, which is what marks a
        grant as belonging to the whole workspace rather than to one member (D5).

        `reason` is required rather than optional. A grant is a privileged write with no
        payment behind it (D2), so the audit log (T19) has nothing to show but the reason
        somebody gave.

        Not idempotent, and deliberately so. Two identical grants are two grants: unlike a
        redelivered billing event, there is no natural key saying they are the same act.

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
        """The workspace's credit balance, or one user's net spend within it (T10).

        The latest snapshot plus every row recorded after it. Two statements rather than the
        one join in the schema note, which does not survive contact with the data: reading
        `FROM credit_balance_snapshot` returns no rows at all for a workspace with no
        snapshot, so the balance of a workspace nobody has snapshotted comes back empty
        instead of as the ledger sum. It also returns one row per snapshot, where only the
        latest is wanted.

        The delta is taken on `recorded_at`, never `occurred_at`. A backfilled event carries
        an old `occurred_at`, so on that column it would fall after the snapshot's cut and
        also outside the delta, and vanish from the balance entirely.

        A snapshot is an optimisation and not a source of truth. With none stored this reads
        the whole ledger and is still correct, which is what lets a snapshot be rebuilt or
        discarded at any time.

        Passing `user` gives that user's net spend against the shared pool rather than an
        allowance of their own: grants carry no user, so they are not in the sum. That is what
        a per-user threshold checks against (D5, T16).
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
        """One transaction, with everything needed to explain the charge loaded (T13).

        The policy comes with its rates and multipliers, because the explanation recomputes
        the charge rather than reading it back. The row stores the quantity, the policy and the
        resolved category but not the rate or the multiplier, so reproducing the arithmetic
        means projecting that policy into a rate card again - which is the property the design
        is built on (D8), and the only way to show that the stored number still follows from
        its inputs.

        `workspace` scopes the lookup where the caller has one. A transaction UUID is not a
        capability, so an endpoint under /workspaces/{workspace}/ must not hand back a row
        belonging to another workspace merely because the UUID was right.
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

        Ordered on `recorded_at`, which is the order they were written rather than the order
        the usage happened in: this answers "what has this workspace done lately", and a
        backfilled event is news even though its `occurred_at` is old.

        For the admin CLI. The API's ledger endpoint (T12) is a different query - it groups
        and nets rather than listing rows, because an individual reversal must not appear
        there as an event of its own (D12).
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

    An optimisation and not a source of truth. The ledger alone always gives the right
    answer, so a snapshot can be rebuilt or thrown away at any time. Nothing writes one yet -
    `CreditLedgerTransaction.balance` reads the whole ledger when there is none, and stays
    correct doing it. What this table buys is a cheap balance for the budget checks that run
    on every billing event (T16).

    Keyed on `as_of` against `recorded_at`, never `occurred_at`. A backfilled event carries an
    old `occurred_at`: on that column it would sort before a snapshot taken after it arrived,
    and so be counted in neither the snapshot nor the delta.

    Keyed on workspace and user the same way budgets are, because budgets are what need a
    balance to be cheap. A null `user` is the whole-pool total that the workspace balance
    endpoint reads; a row naming a user serves a per-user threshold check.

    The schema note specified `(workspace, user, as_of)` as the primary key with `user`
    nullable, which PostgreSQL will not have: a primary key column is NOT NULL, so the
    whole-pool row could not exist. A surrogate key with a unique constraint instead, and the
    constraint declares NULLS NOT DISTINCT so that the pool row stays unique - by default
    PostgreSQL counts two null users as different values and would let the same instant be
    snapshotted twice.
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

        `user=None` means the whole-pool row, which is a different thing from "any user", so
        this matches IS NULL rather than leaving the filter off.
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
