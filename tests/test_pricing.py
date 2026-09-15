"""Tests for the pricing policy rules and the pricing arithmetic.

No database. What makes two policies the same calibration, and therefore what mints a
version, is a comparison over values, and so is what a metered quantity costs.

`price_usage` is the highest-value target in this file. Every ledger row is its output, and
D8 says a charge must be reproducible from the quantity, the policy and the category stored
beside it, so a change in what this function returns is a change in what the ledger means.

This file used to hold the per-SKU price-period rules for `billing_item_price` - amend,
supersede or append. Credits are the unit of account now, the policy replaced that table,
and a policy is versioned as a bundle rather than per SKU, so those tests went with the
rules they covered.
"""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from accounting_service.pricing import (
    ConfiguredPolicy,
    PolicyFingerprint,
    RateCard,
    UnratedSKUError,
    exact_decimal,
    price_usage,
)


class TestConfiguredPolicy:
    """The `pricing_policy` section, validated before anything is applied."""

    @staticmethod
    def a_policy(**overrides: object) -> dict[str, object]:
        return {
            "valid_from": "2025-01-01T00:00:00Z",
            "default_category": "standard",
            "rates": [{"sku": "cpu-seconds", "credits_per_unit": "0.5"}],
            "category_multipliers": [{"category": "standard", "multiplier": "1"}],
        } | overrides

    def test_a_complete_section_validates(self) -> None:
        policy = ConfiguredPolicy.model_validate(self.a_policy())

        assert policy.rates[0].sku == "cpu-seconds"
        assert policy.category_multipliers[0].multiplier == Decimal(1)

    def test_valid_from_without_an_offset_is_taken_as_utc(self) -> None:
        policy = ConfiguredPolicy.model_validate(self.a_policy(valid_from="2025-07-01T00:00:00"))

        assert policy.valid_from == datetime(2025, 7, 1, tzinfo=UTC)

    def test_the_reason_is_optional(self) -> None:
        assert ConfiguredPolicy.model_validate(self.a_policy()).reason is None

    def test_an_unknown_field_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="valid_untill"):
            ConfiguredPolicy.model_validate(self.a_policy(valid_untill="2026-01-01T00:00:00Z"))

    def test_valid_until_is_not_a_field(self) -> None:
        """The loader never closes a policy, so a document cannot ask it to.

        Every stored policy keeps an open range and resolution orders by `configured_at`,
        which is what lets a correction be backdated without rewriting the policies it
        corrects.
        """
        assert "valid_until" not in ConfiguredPolicy.model_fields

    def test_a_repeated_sku_is_rejected(self) -> None:
        rates = [
            {"sku": "cpu-seconds", "credits_per_unit": "0.5"},
            {"sku": "cpu-seconds", "credits_per_unit": "0.9"},
        ]

        with pytest.raises(ValidationError, match="more than once"):
            ConfiguredPolicy.model_validate(self.a_policy(rates=rates))

    def test_a_repeated_category_is_rejected(self) -> None:
        multipliers = [
            {"category": "standard", "multiplier": "1"},
            {"category": "standard", "multiplier": "2"},
        ]

        with pytest.raises(ValidationError, match="more than once"):
            ConfiguredPolicy.model_validate(self.a_policy(category_multipliers=multipliers))

    def test_the_default_category_must_have_a_multiplier(self) -> None:
        """Otherwise every workspace with no category assignment is unpriceable (D6)."""
        with pytest.raises(ValidationError, match="has no entry in category_multipliers"):
            ConfiguredPolicy.model_validate(self.a_policy(default_category="academic"))


class TestPolicyFingerprint:
    """What makes two policies the same calibration, and therefore what mints a version."""

    @staticmethod
    def a_fingerprint(**overrides: object) -> PolicyFingerprint:
        parts: dict[str, object] = {
            "valid_from": datetime(2025, 1, 1, tzinfo=UTC),
            "default_category": "standard",
            "rates": [("cpu-seconds", Decimal("0.5")), ("memory-gb-seconds", Decimal("0.1"))],
            "category_multipliers": [("standard", Decimal(1))],
        }

        return PolicyFingerprint.of(**(parts | overrides))  # pyright: ignore[reportArgumentType]

    def test_the_same_numbers_fingerprint_equal(self) -> None:
        assert self.a_fingerprint() == self.a_fingerprint()

    def test_the_order_rates_are_written_in_does_not_matter(self) -> None:
        """A document listing SKUs in a different order is the same calibration."""
        reversed_rates = [("memory-gb-seconds", Decimal("0.1")), ("cpu-seconds", Decimal("0.5"))]

        assert self.a_fingerprint(rates=reversed_rates) == self.a_fingerprint()

    def test_a_rewritten_decimal_scale_is_not_a_new_policy(self) -> None:
        """0.50 credits is 0.5 credits. Decimal compares numerically, not as written."""
        rescaled = [("cpu-seconds", Decimal("0.500")), ("memory-gb-seconds", Decimal("0.10"))]

        assert self.a_fingerprint(rates=rescaled) == self.a_fingerprint()

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("default_category", "academic"),
            ("valid_from", datetime(2025, 2, 1, tzinfo=UTC)),
            ("rates", [("cpu-seconds", Decimal("0.6")), ("memory-gb-seconds", Decimal("0.1"))]),
            ("category_multipliers", [("standard", Decimal("0.9"))]),
        ],
        ids=["default-category", "valid-from", "a-rate", "a-multiplier"],
    )
    def test_changing_any_of_these_is_a_new_policy(self, field: str, value: object) -> None:
        assert self.a_fingerprint(**{field: value}) != self.a_fingerprint()

    def test_removing_a_rate_is_a_new_policy(self) -> None:
        assert self.a_fingerprint(rates=[("cpu-seconds", Decimal("0.5"))]) != self.a_fingerprint()

    def test_valid_from_is_compared_as_an_instant(self) -> None:
        """Stored timestamps come back in the connection's timezone. The same instant
        written with a different offset is the same policy."""
        one_am_utc = datetime(2025, 1, 1, 1, tzinfo=UTC)
        two_am_plus_one = datetime(2025, 1, 1, 2, tzinfo=timezone(timedelta(hours=1)))

        assert self.a_fingerprint(valid_from=two_am_plus_one) == self.a_fingerprint(valid_from=one_am_utc)

    def test_a_naive_valid_from_is_read_as_utc(self) -> None:
        naive = datetime(2025, 7, 1, 12, 0)

        assert self.a_fingerprint(valid_from=naive) == self.a_fingerprint(
            valid_from=datetime(2025, 7, 1, 12, tzinfo=UTC)
        )

    def test_the_reason_is_not_part_of_the_fingerprint(self) -> None:
        """Re-wording the note explaining a calibration is not a calibration."""
        assert "reason" not in PolicyFingerprint.model_fields

    def test_a_configured_policy_fingerprints_itself(self) -> None:
        document = TestConfiguredPolicy.a_policy(reason="first pass")
        other = TestConfiguredPolicy.a_policy(reason="reworded, same numbers")

        assert ConfiguredPolicy.model_validate(document).fingerprint == (
            ConfiguredPolicy.model_validate(other).fingerprint
        )


def a_rate_card(**overrides: object) -> RateCard:
    """A two-SKU, two-category policy. `standard` is the default and multiplies by 1."""
    parts: dict[str, object] = {
        "default_category": "standard",
        "rates": [("cpu-seconds", Decimal("0.5")), ("memory-gb-seconds", Decimal("0.1"))],
        "category_multipliers": [("standard", Decimal(1)), ("academic", Decimal("0.25"))],
    }

    return RateCard.of(**(parts | overrides))  # pyright: ignore[reportArgumentType]


class TestRateCard:
    """Resolving the category a workspace is priced under (D6)."""

    def test_a_configured_category_prices_under_itself(self) -> None:
        assert a_rate_card().multiplier_for("academic") == ("academic", Decimal("0.25"))

    def test_a_workspace_with_no_category_prices_under_the_default(self) -> None:
        """Which is every workspace until T6 lands, and any workspace after it that nobody
        has assigned."""
        assert a_rate_card().multiplier_for(None) == ("standard", Decimal(1))

    def test_a_category_with_no_multiplier_prices_under_the_default(self) -> None:
        """The workspace service defines the set of categories, not this service, so a value
        nobody has configured a multiplier for is a pricing decision rather than an error."""
        assert a_rate_card().multiplier_for("commercial") == ("standard", Decimal(1))

    def test_the_default_needs_a_multiplier_of_its_own(self) -> None:
        """`ConfiguredPolicy` refuses a document without one, so reaching this means a card
        was built from something that never went through the loader."""
        card = a_rate_card(default_category="academic", category_multipliers=[("standard", Decimal(1))])

        with pytest.raises(KeyError):
            card.multiplier_for(None)


class TestPriceUsage:
    """The arithmetic every ledger row is the output of."""

    def test_the_charge_is_quantity_times_rate_times_multiplier(self) -> None:
        priced = price_usage(a_rate_card(), sku="cpu-seconds", quantity=3600.0, category="academic")

        assert priced.credits == Decimal(3600) * Decimal("0.5") * Decimal("0.25")

    def test_the_charge_is_positive(self) -> None:
        """The ledger signs it: a debit is stored negative so a balance is a plain SUM. That
        is the ledger's business rather than the price's."""
        priced = price_usage(a_rate_card(), sku="cpu-seconds", quantity=10.0, category=None)

        assert priced.credits > 0

    def test_the_result_records_every_input(self) -> None:
        """D8 replays a charge from what was stored beside it, so all of this goes on the row."""
        priced = price_usage(a_rate_card(), sku="memory-gb-seconds", quantity=2.0, category="academic")

        assert (priced.sku, priced.quantity) == ("memory-gb-seconds", 2.0)
        assert (priced.credits_per_unit, priced.multiplier) == (Decimal("0.1"), Decimal("0.25"))

    def test_the_resolved_category_is_reported_not_the_requested_one(self) -> None:
        """The ledger stores this one, so a recategorised workspace does not rewrite the past."""
        priced = price_usage(a_rate_card(), sku="cpu-seconds", quantity=1.0, category="commercial")

        assert priced.category == "standard"

    def test_an_unrated_sku_is_refused_and_names_itself(self) -> None:
        """A collector emitting a SKU the last calibration did not cover. What to do about it
        differs by caller, so this reports the fact and decides nothing."""
        with pytest.raises(UnratedSKUError) as raised:
            price_usage(a_rate_card(), sku="gpu-seconds", quantity=1.0, category=None)

        assert raised.value.sku == "gpu-seconds"

    def test_a_zero_quantity_costs_nothing(self) -> None:
        """A measurement of nothing, which is not the same as no measurement."""
        priced = price_usage(a_rate_card(), sku="cpu-seconds", quantity=0.0, category=None)

        assert priced.credits == 0

    @pytest.mark.parametrize("quantity", [float("nan"), float("inf"), float("-inf")], ids=["nan", "inf", "-inf"])
    def test_a_quantity_that_is_not_finite_is_refused(self, quantity: float) -> None:
        """A NaN would multiply out to a NaN credit that every later balance inherits."""
        with pytest.raises(ValueError, match="cannot price a quantity"):
            price_usage(a_rate_card(), sku="cpu-seconds", quantity=quantity, category=None)

    def test_a_negative_quantity_is_refused(self) -> None:
        """It would turn a debit into a silent grant."""
        with pytest.raises(ValueError, match="negative quantity"):
            price_usage(a_rate_card(), sku="cpu-seconds", quantity=-1.0, category=None)

    def test_nothing_is_rounded(self) -> None:
        """Read paths round for display. Rounding here would round every event separately, so
        a great many small charges would drift away from the quantities that produced them."""
        card = a_rate_card(rates=[("cpu-seconds", Decimal("0.333333"))])

        priced = price_usage(card, sku="cpu-seconds", quantity=3.0, category=None)

        assert priced.credits == Decimal("0.999999")

    def test_a_measured_quantity_does_not_bring_a_binary_tail_with_it(self) -> None:
        """`Decimal(0.1)` is 0.1000000000000000055511151231257827..., and unrounded credits
        would carry that into the ledger for every event."""
        card = a_rate_card(rates=[("cpu-seconds", Decimal(1))])

        priced = price_usage(card, sku="cpu-seconds", quantity=0.1, category=None)

        assert priced.credits == Decimal("0.1")


class TestExactDecimal:
    """The boundary between a float measurement and exact credits."""

    def test_a_quantity_becomes_the_number_that_was_measured(self) -> None:
        assert exact_decimal(0.1) == Decimal("0.1")

    def test_not_the_float_binary_value(self) -> None:
        """This is what `Decimal(quantity)` would produce: the float's binary value, exactly,
        tail and all. Spelled out rather than computed, because ruff rejects both ways of
        writing it and because the tail is the whole point."""
        binary_value = Decimal("0.1000000000000000055511151231257827021181583404541015625")

        assert exact_decimal(0.1) != binary_value

    def test_a_whole_quantity_survives_unchanged(self) -> None:
        assert exact_decimal(3600.0) == Decimal(3600)
