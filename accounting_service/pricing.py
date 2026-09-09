"""Rules for the pricing policy: what a document is worth, and what usage costs under it.

A policy is one calibration pass covering every rate at once (D3), so loading one is not a
plain insert: it either matches the policy already in force, in which case nothing is
written, or it mints a new version. That decision is stated here as a comparison over values
- see `PolicyFingerprint` - so the queries carry it out rather than making it, and so the
rule can be tested without a database.

Charging usage is the same shape. `RateCard` is a policy's numbers over values and
`price_usage` is the arithmetic over them (T7), so pricing an event needs no session and the
part that D8's replayability rests on can be tested without one. `PricingPolicy` in models.py
projects a stored row into a `RateCard`, exactly as it projects one into a fingerprint.

Credits are the unit of account in this service and there is no conversion to money. Buying
credits is out of scope: a hub admin grants them (T15). Whatever invoicing arrives later
brings its own rates and its own rules, so nothing here asserts what a credit is worth.

This module used to hold the per-SKU price rules for `billing_item_price` - amend, supersede
or append, decided from a set of configured instants. The policy replaced that table, and a
policy is versioned as a bundle rather than per SKU, so those rules went with it.
"""

import math
from collections import Counter
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal
from typing import Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from accounting_service.timestamps import as_utc


class ConfiguredRate(BaseModel):
    """One entry under `pricing_policy.rates`: what one SKU costs in credits per unit."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    sku: str
    credits_per_unit: Decimal


class ConfiguredCategoryMultiplier(BaseModel):
    """One entry under `pricing_policy.category_multipliers`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    category: str
    multiplier: Decimal


class ConfiguredPolicy(BaseModel):
    """The `pricing_policy` section of the configuration document.

    One calibration pass: every rate and every category multiplier together (D3). The loader either recognises this as the policy already in force or mints
    a new version of it - see `PolicyFingerprint` for what "recognises" means.

    `valid_until` is deliberately absent. The loader never closes a policy it did not create,
    so every stored policy has an open range and resolution orders by `configured_at`
    instead. That is what lets a correction be backdated without rewriting the ranges of
    policies it corrects (D8).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    valid_from: datetime
    default_category: str
    reason: str | None = None
    rates: tuple[ConfiguredRate, ...] = ()
    category_multipliers: tuple[ConfiguredCategoryMultiplier, ...] = ()

    @field_validator("valid_from")
    @classmethod
    def _must_be_utc(cls, value: datetime) -> datetime:
        """A configuration timestamp with no offset means UTC, as for a configured price."""
        return as_utc(value)

    @model_validator(mode="after")
    def _each_sku_is_rated_once(self) -> Self:
        if duplicated := repeated(rate.sku for rate in self.rates):
            raise ValueError(f"`pricing_policy.rates` rates these SKUs more than once: {', '.join(duplicated)}")

        return self

    @model_validator(mode="after")
    def _each_category_is_multiplied_once(self) -> Self:
        if duplicated := repeated(entry.category for entry in self.category_multipliers):
            raise ValueError(
                f"`pricing_policy.category_multipliers` names these categories more than once: {', '.join(duplicated)}"
            )

        return self

    @model_validator(mode="after")
    def _the_default_category_has_a_multiplier(self) -> Self:
        """A workspace with no assignment is priced under `default_category` (D6).

        Without a multiplier for it, every uncategorised workspace is unpriceable, which is
        a whole class of workspaces rather than an edge case. Required rather than defaulted
        to 1, so the number in force is the number someone wrote down.
        """
        configured = {entry.category for entry in self.category_multipliers}

        if self.default_category not in configured:
            raise ValueError(
                f"`pricing_policy.default_category` is {self.default_category!r}, which has no entry in "
                f"category_multipliers: {sorted(configured)}"
            )

        return self

    @property
    def fingerprint(self) -> "PolicyFingerprint":
        return PolicyFingerprint.of(
            valid_from=self.valid_from,
            default_category=self.default_category,
            rates=[(rate.sku, rate.credits_per_unit) for rate in self.rates],
            category_multipliers=[(entry.category, entry.multiplier) for entry in self.category_multipliers],
        )


class PolicyFingerprint(BaseModel):
    """Everything that makes one policy different from another.

    Two policies with equal fingerprints are the same calibration, so loading a document
    whose fingerprint matches the policy in force mints nothing. This is the whole of the
    "match" half of the mint-or-match loader, expressed over values so it needs no database.

    What is deliberately not here:

      * `reason`. Re-wording the note explaining a calibration is not a calibration.
      * `version`, `configured_at`, `corrects_id`. These describe a stored policy rather than
        the numbers it holds, and every mint would differ by them.
      * `valid_until`, which the loader never sets.

    `valid_from` *is* here, so re-dating a calibration mints a new version. The document then
    describes the policy completely, and correcting a date is a recordable act instead of a
    silent no-op.

    Rates and multipliers are sorted, so the order entries appear in the document does not
    matter. Amounts are compared as `Decimal`, which compares numerically: rewriting `0.5` as
    `0.50` is not a new policy.
    """

    model_config = ConfigDict(frozen=True)

    valid_from: datetime
    default_category: str
    rates: tuple[tuple[str, Decimal], ...]
    category_multipliers: tuple[tuple[str, Decimal], ...]

    @classmethod
    def of(
        cls,
        *,
        valid_from: datetime,
        default_category: str,
        rates: Iterable[tuple[str, Decimal]],
        category_multipliers: Iterable[tuple[str, Decimal]],
    ) -> Self:
        """Build a fingerprint from the parts, whether they came from a document or a row."""
        return cls(
            valid_from=as_utc(valid_from),
            default_category=default_category,
            rates=tuple(sorted(rates)),
            category_multipliers=tuple(sorted(category_multipliers)),
        )


class UnratedSKUError(LookupError):
    """A policy that has to price a SKU it holds no rate for.

    A type of its own because the caller has a real choice and it differs by caller. The
    ingester (T9) meets this when a collector emits a SKU that the last calibration pass did
    not cover, and dropping the charge silently is the one thing it must not do. The
    pre-execution estimate (T14) meets it for a SKU nobody can be charged for yet, where
    answering "no price" is a fine answer. So this reports the fact and decides nothing.
    """

    def __init__(self, sku: str) -> None:
        super().__init__(f"the pricing policy holds no rate for SKU {sku!r}")

        self.sku = sku


class RateCard(BaseModel):
    """A policy's numbers, in the form pricing needs them.

    The counterpart to `PolicyFingerprint`. Both are one policy projected over values, so a
    stored row and a configuration document price identically and neither needs a session.
    Where the fingerprint answers "is this the same calibration", this answers "what does
    this cost".

    `default_category` belongs here rather than beside the multipliers, because resolving a
    workspace's category is part of pricing rather than part of looking a number up: a
    workspace with no assignment, or with one this service has never been configured for,
    prices under the default (D6).
    """

    model_config = ConfigDict(frozen=True)

    default_category: str

    # Credits per unit, by SKU, and the multiplier applied to all of them, by category.
    rates: dict[str, Decimal]
    category_multipliers: dict[str, Decimal]

    @classmethod
    def of(
        cls,
        *,
        default_category: str,
        rates: Iterable[tuple[str, Decimal]],
        category_multipliers: Iterable[tuple[str, Decimal]],
    ) -> Self:
        """Build a rate card from the parts, whether they came from a document or a row."""
        return cls(
            default_category=default_category,
            rates=dict(rates),
            category_multipliers=dict(category_multipliers),
        )

    def multiplier_for(self, category: str | None) -> tuple[str, Decimal]:
        """The category this usage is priced under, and the multiplier that applies.

        Both, because they travel together: the category returned is the one the charge was
        actually computed under, and that is what goes on the ledger row so a recategorised
        workspace does not rewrite the past.

        `None` is a workspace with no assignment. A category with no multiplier is one this
        service has not been configured for, which D6 treats the same way rather than as a
        validation failure, because the set of categories is defined by the workspace service
        and the configuration document, not here.
        """
        if category is not None and (multiplier := self.category_multipliers.get(category)) is not None:
            return category, multiplier

        # `ConfiguredPolicy` refuses a document whose `default_category` has no multiplier, so
        # for any policy that came through the loader this always lands. A card built by hand
        # without one is a programming error and the KeyError says so.
        return self.default_category, self.category_multipliers[self.default_category]


class PricedUsage(BaseModel):
    """One metered quantity, and how it came to cost what it cost.

    Every input to the arithmetic, not the result alone. The ledger stores these fields on
    each row so the charge can be recomputed from first principles months later, which is what
    the explainable-pricing endpoint (T13), the audit log (T19) and historical re-pricing (T18)
    all read.

    `category` is the resolved one, which is not necessarily the one that was asked for.
    """

    model_config = ConfigDict(frozen=True)

    sku: str
    quantity: float
    credits_per_unit: Decimal
    category: str
    multiplier: Decimal
    credits: Decimal


def price_usage(card: RateCard, *, sku: str, quantity: float, category: str | None) -> PricedUsage:
    """Price `quantity` units of `sku` for a workspace in `category` (T7).

    The charge is `quantity x credits_per_unit x multiplier`, and it is positive. The ledger
    signs it: a debit is stored negative so a balance is a plain SUM, and that is the ledger's
    business rather than the price's.

    Raises `UnratedSKUError` when the policy holds no rate for the SKU. Raises `ValueError` for
    a quantity that is negative or not finite: a NaN would multiply out to a NaN credit that
    every later sum inherits, and a negative quantity would make a debit into a silent grant.
    Neither is a measurement, so neither is priced.

    **Nothing is rounded here.** The product is exact and is stored as it comes out. Read paths
    round for display, which `ExactDecimal` in `app/models.py` already does without dropping
    scale or falling into exponent notation. Rounding at this point would round every event
    separately, so a great many small charges would each lose their tail and the total would
    drift away from the quantities that produced it. It would also put D8's replay at the mercy
    of whichever rounding rule was in force when the replay ran rather than of the policy. The
    credits column is therefore an unconstrained NUMERIC (T8), not one with a fixed scale.

    Decimal multiplication rounds to the context precision, 28 significant digits, which is far
    beyond anything a metered quantity carries.
    """
    if not math.isfinite(quantity):
        raise ValueError(f"cannot price a quantity of {quantity} of {sku!r}")

    if quantity < 0:
        raise ValueError(f"cannot price a negative quantity {quantity} of {sku!r}")

    credits_per_unit = card.rates.get(sku)

    if credits_per_unit is None:
        raise UnratedSKUError(sku)

    resolved, multiplier = card.multiplier_for(category)

    return PricedUsage(
        sku=sku,
        quantity=quantity,
        credits_per_unit=credits_per_unit,
        category=resolved,
        multiplier=multiplier,
        credits=exact_decimal(quantity) * credits_per_unit * multiplier,
    )


def exact_decimal(quantity: float) -> Decimal:
    """A metered quantity as the decimal number that was measured.

    Through `str` rather than `Decimal(quantity)`. The latter converts the float's binary value
    exactly, so a measured 0.1 arrives as 0.10000000000000000555111512312578270211815834045
    and every charge derived from it carries that tail into the ledger. `str` gives the
    shortest decimal that round trips to the same float, which is the number the collector
    reported.

    A quantity is a float and stays one - it is a measurement, and the column matching it
    predates this work. Credits are exact, and this is the boundary between the two.
    """
    return Decimal(str(quantity))


def repeated(values: Iterable[str]) -> list[str]:
    """The values appearing more than once, sorted.

    Public and living here rather than in configuration.py because the dependency runs that
    way: configuration imports pricing, not the reverse.
    """
    return sorted(value for value, count in Counter(values).items() if count > 1)
