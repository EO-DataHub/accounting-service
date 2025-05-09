import logging
import uuid
from collections import namedtuple
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterator, Optional, Self, Sequence
from uuid import UUID, uuid4

import eodhp_utils.pulsar.messages
from sqlalchemy import (
    TIMESTAMP,
    CheckConstraint,
    CursorResult,
    ForeignKey,
    Index,
    Result,
    Uuid,
    and_,
    func,
    or_,
    select,
    text,
    union,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship

from accounting_service import db


class Base(DeclarativeBase):
    pass


class WorkspaceAccount(Base):
    """
    This records which account contains each workspace.

    This is not the authoritative data, which is held by the workspace service and sent via Pulsar.
    """

    __tablename__ = "workspace_account"

    workspace: Mapped[str] = mapped_column(index=True, primary_key=True)
    account: Mapped[UUID] = mapped_column(index=True)

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
                    "account": (
                        account.hex if db.settings.SQL_DRIVER.startswith("sqlite") else account
                    ),
                }
            ],
        )

        assert isinstance(result, CursorResult)  # Makes mypy happy
        return result.rowcount > 0


class BillingItem(Base):
    """
    A BillingItem is a thing we sell: a unit of CPU time, a unit of bandwidth, etc.

    BillingItems should be pre-created, but if we see a BillingEvent referring to an unknown one
    we auto-create it. The name and unit will be empty.
    """

    __tablename__ = "billing_item"

    uuid: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)  # Internal ID
    sku: Mapped[str] = mapped_column(
        index=True
    )  # User-visible ID like 'cpusecs-computenodes'. 'sku' = 'stock-keeping unit'.
    name: Mapped[str]  # User-visible name like 'CPU time in notebooks and workflows'
    unit: Mapped[str]  # Units, like seconds or GB-hours

    @classmethod
    def find_billing_items(cls, session: Session) -> Iterator[Self]:
        """Returns all user-visible BillingItems in order of SKU."""
        # This is currently all BillingItems but this could change if we add a 'deleted' flag
        # or some visibility rules.
        query = select(cls).order_by(cls.sku)
        return map(lambda r: r[0], session.execute(query))

    @classmethod
    def find_billing_item(cls, session: Session, sku: str) -> Optional[Self]:
        """Returns a specified BillingItem, assuming it's visible."""
        # This is currently any BillingItem but this could change if we add a 'deleted' flag
        # or some visibility rules.
        query = select(cls).where(cls.sku == sku)
        result = session.execute(query).first()
        return result[0] if result else None

    @classmethod
    def ensure_sku_exists(cls, session: Session, sku: str) -> Optional[Self]:
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
                    "uuid": (
                        rnd_uuid.hex if db.settings.SQL_DRIVER.startswith("sqlite") else rnd_uuid
                    ),
                }
            ],
        )

    @classmethod
    def upsert_configured_item(cls, session: Session, item: dict):
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


class BillingItemPrice(Base):
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

    uuid: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    item_id: Mapped[UUID] = mapped_column(ForeignKey(BillingItem.uuid))
    item: Mapped["BillingItem"] = relationship(foreign_keys=item_id)
    price: Mapped[Decimal]  # This is in pounds.
    valid_from: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    valid_until: Mapped[Optional[datetime]] = mapped_column(
        TIMESTAMP(timezone=True)
    )  # None for current price, a time in the past otherwise.
    configured_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), default=func.now()
    )  # Set to the current time at the time this row is added.

    __table_args__ = (
        Index(
            "billingitemprice_item_validfrom_index",
            "item_id",
            "valid_from",
        ),
        CheckConstraint("valid_until IS NULL OR valid_from <= valid_until"),
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
    def upsert_configured_price(cls, session: Session, price: dict):
        """
        This aimed at inserting or updating prices based on a database-independent source
        such as a YAML configuration file. 'price' must contain 'sku', 'price' and 'valid_from'.

        'valid_from' must either be newer than the current price, in which case the new price
        will replace it at that time, or must exactly match an existing configured price, in
        which case its price will be updated.
        """
        item_obj = BillingItem.find_billing_item(session, price["sku"])
        if not item_obj:
            logging.error("Failed to find item %s when configuring price", price["sku"])
            raise ValueError(f"Attempt to add price for unknown SKU {price["sku"]}")

        valid_from = datetime.fromisoformat(price["valid_from"]).astimezone(timezone.utc)

        existing_prices_updated = session.execute(
            update(cls)
            .where(cls.item == item_obj)
            .where(cls.valid_from == valid_from)
            .values(price=price["price"])
        )

        if existing_prices_updated.rowcount > 0:
            return

        latest_price = (
            session.execute(
                select(cls).where(cls.item == item_obj).order_by(cls.valid_from.desc()).limit(1)
            )
            .scalars()
            .one_or_none()
        )

        if latest_price:
            if latest_price.valid_from.astimezone(timezone.utc) > valid_from:
                raise ValueError(
                    f"Attempt to add price {price["sku"]} where valid_from is earlier "
                    + f"than the latest existing price, {latest_price.valid_from}."
                )

            latest_price.valid_until = valid_from

        price_obj = cls(
            item=item_obj,
            valid_from=valid_from,
            price=price["price"],
        )
        session.add(price_obj)


def datetime_default_to_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt and not dt.tzinfo:
        return dt.replace(tzinfo=timezone.utc)

    return dt


class BillingEvent(Base):
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

    uuid: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    event_start: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), index=True)
    event_end: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True))
    item_id: Mapped[UUID] = mapped_column(ForeignKey(BillingItem.uuid))
    item: Mapped["BillingItem"] = relationship(foreign_keys=item_id)
    user: Mapped[Optional[UUID]]  # This is None for, for example, workspace storage.
    workspace: Mapped[str]
    quantity: Mapped[float]  # The units involved are defined in the BillingItem

    @property
    def event_start_utc(self):
        # In PostgreSQL we always have a sample_time with a timezone attached and it should always
        # be UTC. In SQLite, used for tests, we get datetimes with no timezone, creating problems
        # when we do comparisons.
        #
        # This harmonizes this.
        return self.event_start.astimezone(timezone.utc)

    @property
    def event_end_utc(self):
        return self.event_end.astimezone(timezone.utc)

    __table_args__ = (
        Index(
            "billingevent_workspace_eventstart_index",
            "workspace",
            "event_start",
        ),
        CheckConstraint("event_start <= event_end"),
    )

    @classmethod
    def find_billing_events(
        cls,
        session: Session,
        workspace: Optional[str] = None,
        account: Optional[UUID] = None,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        after: Optional[UUID] = None,
        limit: int = 5_000,
    ) -> Iterator[Self]:
        """
        Find and return BillingEvents matching some criteria.

        For paging, `after` should be the UUID of the last billing event on the previous page.
        """
        query = select(cls).limit(limit)

        # We need a complete and certain order so that the 'after' parameter works.
        query = query.order_by(
            cls.event_start,
            cls.event_end,
            cls.workspace,
            cls.uuid,
        )

        if workspace is not None:
            query = query.where(cls.workspace == workspace)

        if account is not None:
            query = query.join(WorkspaceAccount, WorkspaceAccount.workspace == cls.workspace).where(
                WorkspaceAccount.account == account
            )

        if start is not None:
            query = query.where(cls.event_start >= start)

        if end is not None:
            query = query.where(cls.event_end < end)

        if after is not None:
            after_be = session.get(cls, after)

            if after_be is not None:
                query = query.where(
                    or_(
                        (cls.event_start > after_be.event_start),
                        and_(
                            cls.event_start == after_be.event_start,
                            cls.event_end > after_be.event_end,
                        ),
                        and_(
                            cls.event_start == after_be.event_start,
                            cls.event_end == after_be.event_end,
                            cls.workspace > after_be.workspace,
                        ),
                        and_(
                            cls.event_start == after_be.event_start,
                            cls.event_end == after_be.event_end,
                            cls.workspace == after_be.workspace,
                            cls.uuid > after,
                        ),
                    )
                )

        return map(lambda r: r[0], session.execute(query))

    @classmethod
    def find_latest_billing_event(
        cls,
        session: Session,
        workspace: Optional[str],
        sku: Optional[str],
    ) -> Optional[Self]:
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
    def insert_from_message(
        cls, session: Session, msg: eodhp_utils.pulsar.messages.BillingEvent
    ) -> Optional[UUID]:
        """
        Adds a new BillingEvent to the DB based on a Pulsar message.

        Deals with duplicated UUIDs by ignoring the second message and returning None.
        """
        result = session.execute(
            insert(cls)
            .values(
                uuid=UUID(msg.uuid),
                event_start=datetime_default_to_utc(datetime.fromisoformat(msg.event_start)),
                event_end=datetime_default_to_utc(datetime.fromisoformat(msg.event_end)),
                item_id=select(BillingItem.uuid)
                .where(BillingItem.sku == msg.sku)
                .scalar_subquery(),
                user=UUID(msg.user) if msg.user else None,
                workspace=msg.workspace,
                quantity=msg.quantity,
            )
            .on_conflict_do_nothing(index_elements=["uuid"])
            .returning(BillingEvent.uuid)
        )

        return result.scalar_one_or_none()

    def __repr__(self):
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


class BillableResourceConsumptionRateSample(Base):
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

    uuid: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)

    # Typically this is the end of the sampling process, although we pretend here that it was
    # instantaneous.
    sample_time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), index=True)

    item_id: Mapped[UUID] = mapped_column(ForeignKey(BillingItem.uuid))
    item: Mapped["BillingItem"] = relationship(foreign_keys=item_id)

    user: Mapped[Optional[UUID]]  # This is None for, for example, workspace storage.
    workspace: Mapped[str]

    # The units of this are defined in the BillingItem and divided by seconds.
    # eg, storage consumption is measured in GB-seconds, so this is in GB.
    rate: Mapped[float]

    @property
    def sample_time_utc(self):
        # In PostgreSQL we always have a sample_time with a timezone attached and it should always
        # be UTC. In SQLite, used for tests, we get datetimes with no timezone, creating problems
        # when we do comparisons.
        #
        # This harmonizes this.
        return self.sample_time.astimezone(timezone.utc)

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
    ) -> Optional[UUID]:
        result = session.execute(
            insert(cls)
            .values(
                uuid=UUID(msg.uuid),
                sample_time=datetime_default_to_utc(datetime.fromisoformat(msg.sample_time)),
                item_id=(
                    select(BillingItem.uuid).where(BillingItem.sku == msg.sku).scalar_subquery()
                ),
                user=UUID(msg.user) if msg.user else None,
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

        if db.settings.SQL_DRIVER.startswith("sqlite"):
            # Only used in tests (but the tests can be run with PostgreSQL as well).
            # SQLite can't cope with the UNION syntax used by SQLAlchemy.
            return (
                list(session.execute(last_before_start).scalars().all())
                + list(session.execute(in_period).scalars().all())
                + list(session.execute(first_after_end).scalars().all())
            )

        query = select(cls).from_statement(
            union(last_before_start, first_after_end, in_period).order_by("sample_time")
        )
        return session.execute(query).scalars().all()

    @classmethod
    def calculate_consumption_for_interval(
        cls, session: Session, workspace: str, sku: str, start: datetime, end: datetime
    ) -> Optional[float]:
        """
        This calculates estimated consumption within a time interval, using linear interpolation
        to estimate consumption rates from samples and then (effectively) integrating.

        It's assumed that the resource did not exist (zero consumption rate) before the first
        sample and after the last sample. Callers should endeavour not to call this for an interval
        until sample collection has got as far as at least one sample after the end of the
        interval. If no sample exists after the end of the interval then, if one is later
        collected, the answer given by this method will change.
        """
        rate_samples = list(cls.find_data_for_interval(session, workspace, sku, start, end))

        if not rate_samples or len(rate_samples) <= 1:
            # No record of any consumption at all.
            #
            # If there is one sample then this is equivalent to no consumption. THis is because
            # we assume that the resource didn't exist until the first sample and didn't exist
            # after the last one, so we act as if it existed for zero time.
            return None

        # We need an estimate of consumption rate at the start and end of the interval.
        # We use interpolation.
        def interpolate(at: datetime, s0, s1):
            assert at >= s0.sample_time_utc
            assert at <= s1.sample_time_utc

            proportion: float = (at - s0.sample_time_utc) / (
                s1.sample_time_utc - s0.sample_time_utc
            )

            return s0.rate + proportion * (s1.rate - s0.rate)

        RateTime = namedtuple("RateTime", ["at", "rate"])

        starting_ratetime = (
            # If no samples exist before the window then it may not have existed yet.
            # To avoid awkward questions, we treat consumption as zero up until the first
            # sample.
            RateTime(at=rate_samples[0].seconds_after(start), rate=0)
            if rate_samples[0].sample_time_utc > start
            else RateTime(at=0, rate=interpolate(start, rate_samples[0], rate_samples[1]))
        )

        ending_ratetime = (
            # If there are no samples after the window we assume the resource was destroyed
            # sometime after the last sample. Again, to avoid awkward questions we assume
            # this happened exactly at the last sample.
            RateTime(at=rate_samples[-1].seconds_after(start), rate=0)
            if rate_samples[-1].sample_time_utc < end
            else RateTime(
                at=(end - start).seconds, rate=interpolate(end, rate_samples[-2], rate_samples[-1])
            )
        )

        mid_samples = filter(lambda s: s.after(start) and not s.after(end), rate_samples)
        mid_ratetimes = map(lambda s: RateTime(at=s.seconds_after(start), rate=s.rate), mid_samples)

        # Form a list of RateTimes tuples covering exactly the window, clipped to a shorter period
        # only if we've assumed the resource was created/destroyed during the window.
        ratelist: list[RateTime] = [starting_ratetime] + list(mid_ratetimes) + [ending_ratetime]

        # Now imagine a linear interpolation between the points in ratelist being integrated to
        # produce our answer.
        total_consumption: float = 0.0
        for s0, s1 in zip(ratelist, ratelist[1:], strict=False):
            assert s1.at >= s0.at

            duration = s1.at - s0.at
            rate = (s0.rate + s1.rate) / 2.0
            total_consumption += duration * rate

        return total_consumption

    @classmethod
    def find_earliest(
        cls,
        session: Session,
        workspace: Optional[str],
        item_id: Optional[UUID],
    ) -> Optional[Self]:
        """
        Returns the first observed sample for the given constraints.
        """
        query = select(cls).order_by(cls.sample_time).limit(1)

        if workspace is not None:
            query = query.where(cls.workspace == workspace)

        if item_id is not None:
            query = query.where(cls.item_id == item_id)

        return session.execute(query).scalar_one_or_none()

    def seconds_after(self, after: datetime) -> float:
        return (self.sample_time_utc - after).seconds

    def after(self, t: datetime) -> bool:
        return self.sample_time_utc > t

    def __repr__(self):
        return (
            "BillableResourceConsumptionRateSample("
            + f"{self.uuid=}, "
            + f"{self.sample_time=}, "
            + f"{self.item_id=}, "
            + f"{self.user=}, "
            + f"{self.workspace=}, "
            + f"{self.rate=})"
        )
