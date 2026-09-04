"""Rules for the prices loaded from configuration.

A price applies from a point in time until the next one supersedes it, so loading one is not
a plain insert: it has to work out whether the entry amends a price already configured or
starts a new period, and reject an entry that would slot in behind the current one.

Those rules used to be expressed as a sequence of queries, with the decision inferred from
an UPDATE's row count. They are stated here as a decision over values instead, so the
queries carry it out rather than making it, and so the rules can be tested without a
database.

The pricing policy in the credits work replaces `billing_item_price` (D3), but the same
question arises there: a policy load either matches what is already configured or mints a
new version. This is where that decision should live too.
"""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from accounting_service.timestamps import as_utc


class ConfiguredPrice(BaseModel):
    """One `prices:` entry from the configuration file.

    Validated rather than read out of a dict, so a missing or misspelled key fails at load
    with a message naming the field, instead of raising a KeyError somewhere downstream.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    sku: str
    price: Decimal
    valid_from: datetime

    @field_validator("valid_from")
    @classmethod
    def _must_be_utc(cls, value: datetime) -> datetime:
        """A configuration timestamp with no offset means UTC.

        The previous code called `.astimezone(UTC)` on the parsed value, which reads a naive
        datetime as the machine's local time. On a host in Europe/London a summer date such
        as 2025-07-01T00:00:00 was stored as 23:00 on 30 June, and a winter one was not, so
        the same configuration meant different things depending on the date and the host.
        """
        return as_utc(value)


class PriceAction(StrEnum):
    """What loading a configured price should do to the prices already stored."""

    AMEND = "amend"
    """A price is already configured for exactly this instant. Change its amount and leave
    the periods alone. This is how a mistake in a configured price gets corrected."""

    SUPERSEDE = "supersede"
    """This price starts a new period. Close the current one at this instant and add it."""

    APPEND = "append"
    """No prices exist for this item yet, so there is nothing to close."""


class BackdatedPriceError(ValueError):
    """Raised when a configured price would take effect before the current one started.

    Prices form a single unbroken sequence of periods, so an entry landing behind the latest
    would either overlap it or need it split. Correcting history is done by adding a price
    with a matching `valid_from` and a later `configured_at`, which is what AMEND covers.
    """


class PricePlan(BaseModel):
    """What to do about one configured price, and to which stored row."""

    model_config = ConfigDict(frozen=True)

    action: PriceAction
    supersedes_valid_from: datetime | None = None
    """The `valid_from` of the price this one takes over from, so the caller closes that row
    rather than working out which is current. Set only for SUPERSEDE."""

    @model_validator(mode="after")
    def _superseding_names_a_predecessor(self) -> Self:
        if (self.action is PriceAction.SUPERSEDE) != (self.supersedes_valid_from is not None):
            raise ValueError(f"{self.action} must name a predecessor if and only if it supersedes one")

        return self


def plan_price_change(valid_from: datetime, configured_valid_froms: Sequence[datetime]) -> PricePlan:
    """Decide what a configured price should do, given the instants already configured.

    `configured_valid_froms` is every `valid_from` already stored for the item, in any order.
    Only the instants matter; the amounts do not affect the decision.
    """
    if not configured_valid_froms:
        return PricePlan(action=PriceAction.APPEND)

    normalised = [as_utc(existing) for existing in configured_valid_froms]
    wanted = as_utc(valid_from)

    if wanted in normalised:
        return PricePlan(action=PriceAction.AMEND)

    latest = max(normalised)

    if latest > wanted:
        raise BackdatedPriceError(
            f"a price valid from {wanted.isoformat()} cannot be added behind the current one, "
            f"which starts at {latest.isoformat()}"
        )

    return PricePlan(action=PriceAction.SUPERSEDE, supersedes_valid_from=latest)
