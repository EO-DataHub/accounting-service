from datetime import datetime
from decimal import Decimal
from typing import Iterator, Optional, Self
from uuid import UUID, uuid4

import eodhp_utils.pulsar.messages
from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Index,
    Uuid,
    and_,
    insert,
    or_,
    select,
    text,
)
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

        return not not result


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

    def find_billing_items(session: Session) -> Iterator[Self]:
        """Returns all user-visible BillingItems in order of SKU."""
        # This is currently all BillingItems but this could change if we add a 'deleted' flag
        # or some visibility rules.
        query = select(BillingItem).order_by(BillingItem.sku)
        return map(lambda r: r[0], session.execute(query))

    def find_billing_item(session: Session, sku: str) -> Optional[Self]:
        """Returns a specified BillingItem, assuming it's visible."""
        # This is currently any BillingItem but this could change if we add a 'deleted' flag
        # or some visibility rules.
        query = select(BillingItem).where(BillingItem.sku == sku)
        result = session.execute(query).first()
        return result[0] if result else None


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
    valid_from: Mapped[datetime]
    valid_until: Mapped[Optional[datetime]]  # None for current price, a time in the past otherwise.
    configured_at: Mapped[datetime]  # Set to the current time at the time this row is added.

    __table_args__ = (
        Index(
            "item",
            "valid_from",
        ),
        CheckConstraint("valid_from <= valid_until"),
    )

    def find_prices(session: Session, at: datetime) -> Iterator[tuple[Self, str]]:  # type: ignore
        """Returns all prices valid at the specified time. Each result is a tuple containing a
        BillingItemPrice first and the associated SKU second."""
        query = (
            select(BillingItemPrice, BillingItem.sku)
            .join(BillingItemPrice.item)
            .where(BillingItemPrice.valid_from < at)
            .where(
                or_(
                    BillingItemPrice.valid_until == None,  # noqa: E711
                    BillingItemPrice.valid_until > at,
                )
            )
            .order_by(BillingItem.sku, BillingItemPrice.valid_from)
        )
        print(query)
        return session.execute(query)


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
    event_start: Mapped[datetime] = mapped_column(index=True)
    event_end: Mapped[datetime]
    item_id: Mapped[UUID] = mapped_column(ForeignKey(BillingItem.uuid))
    item: Mapped["BillingItem"] = relationship(foreign_keys=item_id)
    user: Mapped[Optional[UUID]]  # This is None for, for example, workspace storage.
    workspace: Mapped[str]
    quantity: Mapped[float]  # The units involved are defined in the BillingItem

    __table_args__ = (
        Index(
            "workspace",
            "event_start",
        ),
        CheckConstraint("event_start <= event_end"),
    )

    @staticmethod
    def find_billing_events(
        session: Session,
        workspace: str = None,
        account: UUID = None,
        start: datetime = None,
        end: datetime = None,
        after: UUID = None,
        limit: int = 5_000,
    ) -> Iterator[Self]:
        """
        Find and return BillingEvents matching some criteria.

        For paging, `after` should be the UUID of the last billing event on the previous page.
        """
        query = select(BillingEvent).limit(limit)

        # We need a complete and certain order so that the 'after' parameter works.
        query = query.order_by(
            BillingEvent.event_start,
            BillingEvent.event_end,
            BillingEvent.workspace,
            BillingEvent.uuid,
        )

        if workspace is not None:
            query = query.where(BillingEvent.workspace == workspace)

        if account is not None:
            query = query.join(
                WorkspaceAccount, WorkspaceAccount.workspace == BillingEvent.workspace
            ).where(WorkspaceAccount.account == account)

        if start is not None:
            query = query.where(BillingEvent.event_start >= start)

        if end is not None:
            query = query.where(BillingEvent.event_end < end)

        if after is not None:
            after_be = session.get(BillingEvent, after)

            if after_be is not None:
                query = query.where(
                    or_(
                        (BillingEvent.event_start > after_be.event_start),
                        and_(
                            BillingEvent.event_start == after_be.event_start,
                            BillingEvent.event_end > after_be.event_end,
                        ),
                        and_(
                            BillingEvent.event_start == after_be.event_start,
                            BillingEvent.event_end == after_be.event_end,
                            BillingEvent.workspace > after_be.workspace,
                        ),
                        and_(
                            BillingEvent.event_start == after_be.event_start,
                            BillingEvent.event_end == after_be.event_end,
                            BillingEvent.workspace == after_be.workspace,
                            BillingEvent.uuid > after,
                        ),
                    )
                )

        return map(lambda r: r[0], session.execute(query))

    @classmethod
    def insert_from_message(
        cls, session: Session, msg: eodhp_utils.pulsar.messages.BillingEvent
    ) -> UUID:
        result = session.execute(
            insert(cls)
            .values(
                uuid=UUID(msg.uuid),
                event_start=datetime.fromisoformat(msg.event_start),
                event_end=datetime.fromisoformat(msg.event_end),
                item_id=select(BillingItem.uuid)
                .where(BillingItem.sku == msg.sku)
                .scalar_subquery(),
                user=UUID(msg.user) if msg.user else None,
                workspace=msg.workspace,
                quantity=msg.quantity,
            )
            .returning(BillingEvent.uuid)
        )

        return result.first()[0]

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
