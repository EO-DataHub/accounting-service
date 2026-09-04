"""Tests for the configured-price rules.

No database. These are the decisions that used to be inferred from an UPDATE's row count,
which is why they had none of their own.
"""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from pydantic import ValidationError

from accounting_service.pricing import (
    BackdatedPriceError,
    ConfiguredPrice,
    PriceAction,
    plan_price_change,
)

JAN1 = datetime(2025, 1, 1, tzinfo=UTC)
JAN2 = datetime(2025, 1, 2, tzinfo=UTC)
JAN3 = datetime(2025, 1, 3, tzinfo=UTC)


def test_the_first_price_for_an_item_is_appended() -> None:
    plan = plan_price_change(JAN1, [])

    assert plan.action is PriceAction.APPEND
    assert plan.supersedes_valid_from is None


def test_a_later_price_supersedes_the_current_one() -> None:
    plan = plan_price_change(JAN2, [JAN1])

    assert plan.action is PriceAction.SUPERSEDE
    assert plan.supersedes_valid_from == JAN1


def test_the_price_superseded_is_the_latest_not_the_only() -> None:
    """With several periods stored, the new price closes the most recent one."""
    plan = plan_price_change(JAN3, [JAN1, JAN2])

    assert plan.supersedes_valid_from == JAN2


def test_the_order_of_the_stored_instants_does_not_matter() -> None:
    """The caller reads them with no ORDER BY, so the decision must not depend on it."""
    plan = plan_price_change(JAN3, [JAN2, JAN1])

    assert plan.supersedes_valid_from == JAN2


def test_an_exact_match_amends_rather_than_adding_a_period() -> None:
    """Reloading the same configuration corrects the amount instead of splitting the period.

    This is what makes the loader idempotent: the ingester runs it on every start, and an
    unchanged configuration must not accumulate periods.
    """
    plan = plan_price_change(JAN1, [JAN1])

    assert plan.action is PriceAction.AMEND
    assert plan.supersedes_valid_from is None


def test_an_exact_match_amends_even_when_it_is_not_the_latest() -> None:
    """Correcting an older configured price leaves the later periods alone."""
    plan = plan_price_change(JAN1, [JAN1, JAN2])

    assert plan.action is PriceAction.AMEND


def test_a_backdated_price_is_rejected() -> None:
    with pytest.raises(BackdatedPriceError, match="cannot be added behind"):
        plan_price_change(JAN1, [JAN2])


def test_the_rejection_names_both_instants() -> None:
    """The message has to be actionable: which entry, and what it collided with."""
    with pytest.raises(BackdatedPriceError) as raised:
        plan_price_change(JAN1, [JAN2])

    assert "2025-01-01" in str(raised.value)
    assert "2025-01-02" in str(raised.value)


def test_instants_are_compared_in_utc_not_as_written() -> None:
    """The same moment written with a different offset is the same period, so it amends."""
    one_am_utc = datetime(2025, 1, 1, 1, 0, tzinfo=UTC)
    two_am_plus_one = datetime(2025, 1, 1, 2, 0, tzinfo=timezone(timedelta(hours=1)))

    plan = plan_price_change(two_am_plus_one, [one_am_utc])

    assert plan.action is PriceAction.AMEND


def test_a_naive_stored_instant_is_read_as_utc() -> None:
    """A naive value can reach here from an object built in Python before a round trip."""
    plan = plan_price_change(JAN1, [datetime(2025, 1, 1)])

    assert plan.action is PriceAction.AMEND


class TestConfiguredPrice:
    """Validation of one `prices:` entry.

    Built with model_validate on a dict, which is how the loader does it: the entries come
    from YAML, so the values arrive as whatever YAML produced - a float for a price, a string
    for a timestamp - and the coercion is the thing being tested.
    """

    def test_a_yaml_float_becomes_an_exact_decimal(self) -> None:
        """12.34 must not arrive as the binary approximation of 12.34."""
        entry = ConfiguredPrice.model_validate({"sku": "s", "price": 12.34, "valid_from": "2025-01-01T00:00:00Z"})

        assert entry.price == Decimal("12.34")

    def test_a_zoneless_timestamp_means_utc(self) -> None:
        """Previously this was read as the host's local time.

        On a host in Europe/London a summer date was stored an hour early and a winter one
        was not, so the same configuration meant different things depending on the date.
        """
        entry = ConfiguredPrice.model_validate({"sku": "s", "price": 1, "valid_from": "2025-07-01T00:00:00"})

        assert entry.valid_from == datetime(2025, 7, 1, tzinfo=UTC)

    def test_an_offset_timestamp_is_converted(self) -> None:
        entry = ConfiguredPrice.model_validate({"sku": "s", "price": 1, "valid_from": "2025-07-01T01:00:00+01:00"})

        assert entry.valid_from == datetime(2025, 7, 1, tzinfo=UTC)

    def test_a_missing_field_is_named(self) -> None:
        """A KeyError from a dict lookup did not say which entry or which field."""
        with pytest.raises(ValidationError, match="valid_from"):
            ConfiguredPrice.model_validate({"sku": "s", "price": 1})

    def test_an_unexpected_field_is_rejected(self) -> None:
        """A misspelled key in the configuration should fail rather than being ignored."""
        with pytest.raises(ValidationError, match="prise"):
            ConfiguredPrice.model_validate({"sku": "s", "price": 1, "valid_from": "2025-01-01T00:00:00Z", "prise": 2})

    def test_entries_are_immutable(self) -> None:
        entry = ConfiguredPrice.model_validate({"sku": "s", "price": 1, "valid_from": "2025-01-01T00:00:00Z"})

        with pytest.raises(ValidationError):
            entry.price = Decimal(2)
