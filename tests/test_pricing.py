"""Tests for the pricing policy rules.

No database. What makes two policies the same calibration, and therefore what mints a
version, is a comparison over values.

This file used to hold the per-SKU price-period rules for `billing_item_price` - amend,
supersede or append. Credits are the unit of account now, the policy replaced that table,
and a policy is versioned as a bundle rather than per SKU, so those tests went with the
rules they covered.
"""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from accounting_service.pricing import ConfiguredPolicy, PolicyFingerprint


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
