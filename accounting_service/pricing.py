"""Rules for the pricing policy loaded from configuration.

A policy is one calibration pass covering every rate at once (D3), so loading one is not a
plain insert: it either matches the policy already in force, in which case nothing is
written, or it mints a new version. That decision is stated here as a comparison over values
- see `PolicyFingerprint` - so the queries carry it out rather than making it, and so the
rule can be tested without a database.

Credits are the unit of account in this service and there is no conversion to money. Buying
credits is out of scope: a hub admin grants them (T15). Whatever invoicing arrives later
brings its own rates and its own rules, so nothing here asserts what a credit is worth.

This module used to hold the per-SKU price rules for `billing_item_price` - amend, supersede
or append, decided from a set of configured instants. The policy replaced that table, and a
policy is versioned as a bundle rather than per SKU, so those rules went with it.
"""

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


def repeated(values: Iterable[str]) -> list[str]:
    """The values appearing more than once, sorted.

    Public and living here rather than in configuration.py because the dependency runs that
    way: configuration imports pricing, not the reverse.
    """
    return sorted(value for value, count in Counter(values).items() if count > 1)
