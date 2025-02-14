from datetime import datetime
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

import eodhp_utils.pulsar.messages
from sqlalchemy import CheckConstraint, ForeignKey, Index, Uuid, insert, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlmodel import Session


class Base(DeclarativeBase):
    pass


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
    # item: Mapped["BillingItem"] = relationship(foreign_keys="billing_item.uuid")
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


class BillingEvent(Base):
    """
    This records a particular workspace's consumption of a particular BillingItem at a particular
    time or over a particular period. This consumption is priced at its start date.

    BillingEvents can be aggregated over time. A series of billing events can be combined if
    the user, workspace and item are the same and if they occur within the same day. The UUID
    of the first event is kept. They can also be split if the event time period includes
    midnight.
    """

    __tablename__ = "billing_event"

    uuid: Mapped[UUID] = mapped_column(Uuid, primary_key=True, default=uuid4)
    event_start: Mapped[datetime] = mapped_column(index=True)
    event_end: Mapped[datetime]
    item_id: Mapped[UUID] = mapped_column(ForeignKey(BillingItem.uuid))
    item: Mapped["BillingItem"] = relationship(foreign_keys=item_id)
    user: Mapped[Optional[UUID]]  # This is None for, for example, workspace storage.
    workspace: Mapped[str] = mapped_column(index=True)
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
        workspace: str = None,
        account: UUID = None,
        start: datetime = None,
        end: datetime = None,
        after: UUID = None,
        limit: int = 5_000,
    ):
        """
        Find and return BillingEvents matching some criteria.

        For paging, `after` should be the UUID of the last billing event on the previous page.
        """

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
