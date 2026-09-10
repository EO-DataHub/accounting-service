"""The pricing rules: what a configuration document is worth, and what usage costs under it."""

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

    `valid_until` is absent because the loader never closes a policy. Every stored policy has
    an open range and resolution orders by `configured_at` instead, which is what lets a
    correction be backdated without rewriting the ranges of the policies it corrects.
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
        """A configuration timestamp with no offset means UTC."""
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
        """Required rather than defaulted to 1.

        Every workspace with no category assignment prices under `default_category`, so
        without a multiplier for it a whole class of workspaces is unpriceable. Requiring it
        means the number in force is a number somebody wrote down.
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

    Equal fingerprints mean the same calibration, so a document matching the policy in force
    mints nothing. Deliberately excluded: `reason`, because rewording a note is not a
    calibration; `version`, `configured_at` and `corrects_id`, which describe a stored row
    rather than its numbers; and `valid_until`, which the loader never sets.

    `valid_from` is included, so re-dating a calibration mints a new version and correcting a
    date is a recordable act rather than a silent no-op.

    Rates and multipliers are sorted, so document order does not matter, and amounts compare
    as `Decimal`, so `0.50` is not a change from `0.5`.
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
    """A policy was asked to price a SKU it holds no rate for."""

    def __init__(self, sku: str) -> None:
        super().__init__(f"the pricing policy holds no rate for SKU {sku!r}")

        self.sku = sku


class RateCard(BaseModel):
    """A policy's numbers, in the form pricing needs them."""

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
        """The category this usage is priced under, and the multiplier that applies."""

        if category is not None and (multiplier := self.category_multipliers.get(category)) is not None:
            return category, multiplier

        return self.default_category, self.category_multipliers[self.default_category]


class PricedUsage(BaseModel):
    """One metered quantity, and every input to what it cost.

    The ledger stores these fields on each row so the charge can be recomputed from first
    principles months later.

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
    """Price `quantity` units of `sku` for a workspace in `category`.

    The charge is `quantity x credits_per_unit x multiplier`, and it is positive: the ledger
    signs it, storing a debit negative so a balance is a plain SUM.

    **Nothing is rounded.** The product is exact and is stored as it comes out.
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

    Through `str`, not `Decimal(quantity)`. The latter converts the float's binary value
    exactly, so a measured 0.1 arrives as 0.1000000000000000055511151231257827 and every
    charge derived from it carries that tail into the ledger. `str` gives the shortest decimal
    that round trips to the same float, which is the number the collector reported.

    This is the boundary between measurements, which are floats, and credits, which are exact.
    """
    return Decimal(str(quantity))


def repeated(values: Iterable[str]) -> list[str]:
    """The values appearing more than once, sorted."""
    return sorted(value for value, count in Counter(values).items() if count > 1)
