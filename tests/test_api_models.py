"""Tests for the response models.

No database. The models are Pydantic, so a stored object can be built in memory and mapped
without a query; the mapping is a pure function of the object handed to it.

What is tested here is the mapping, including the parts the models deliberately do
differently from the tables: `item` is a SKU string where BillingEvent has a relationship,
`price` is an exact decimal string where the column is NUMERIC, and timestamps are converted
rather than merely labelled.

tests/test_api.py still exercises these through HTTP against real rows, which is what proves
the handlers use them and that response_model validation passes. The field-level questions
are here.
"""

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from accounting_service.app.models import (
    BillingEventAPIResult,
    BillingItemAPIResult,
    BillingItemRateAPIResult,
)
from accounting_service.models import BillingEvent, BillingItem


def an_item(sku: str = "cpu-seconds") -> BillingItem:
    return BillingItem(uuid=uuid4(), sku=sku, name="CPU time", unit="s")


def an_event(
    *,
    item: BillingItem | None = None,
    event_start: datetime = datetime(2025, 6, 15, 12, 0, tzinfo=UTC),
    event_end: datetime = datetime(2025, 6, 15, 12, 15, tzinfo=UTC),
    workspace: str = "my-workspace",
    quantity: float = 1.5,
) -> BillingEvent:
    """Spelled out rather than taking **overrides, so the arguments keep their types.

    A `**overrides: object` signature widened every field to `object` and produced eight
    pyright warnings inside the constructor call.
    """
    item = item or an_item()

    return BillingEvent(
        uuid=uuid4(),
        event_start=event_start,
        event_end=event_end,
        item_id=item.uuid,
        workspace=workspace,
        quantity=quantity,
        item=item,
    )


class TestBillingItemAPIResult:
    """Shares BillingItemBase with the table, so there is nothing to map."""

    def test_every_field_comes_across(self) -> None:
        item = an_item(sku="memory-gb-seconds")

        result = BillingItemAPIResult.model_validate(item)

        assert result.uuid == item.uuid
        assert result.sku == "memory-gb-seconds"
        assert result.name == "CPU time"
        assert result.unit == "s"

    def test_uuid_is_required_rather_than_generated(self) -> None:
        """The base gives uuid a default_factory so the table can generate one.

        The response redeclares it without that default. A generated value would make the
        field optional in the OpenAPI schema, and the server always sends one.
        """
        assert BillingItemAPIResult.model_fields["uuid"].is_required()


class TestBillingEventAPIResult:
    def test_item_is_mapped_to_the_sku(self) -> None:
        """The table has a relationship; the response has a string.

        Expressed as a validation alias rather than a constructor, so this is the test that
        the alias path still reaches through the relationship.
        """
        result = BillingEventAPIResult.model_validate(an_event(item=an_item(sku="EFS-STORAGE-STD")))

        assert result.item == "EFS-STORAGE-STD"

    def test_the_scalar_fields_come_across(self) -> None:
        event = an_event(workspace="other-workspace", quantity=42.5)

        result = BillingEventAPIResult.model_validate(event)

        assert result.uuid == event.uuid
        assert result.workspace == "other-workspace"
        assert result.quantity == 42.5

    def test_a_timestamp_with_an_offset_is_converted_to_utc(self) -> None:
        """Not relabelled. The connection can hand back an aware value in any timezone, and
        the response has always claimed UTC with a Z suffix."""
        one_am_utc_as_two_am_plus_one = datetime(2025, 6, 15, 2, 0, tzinfo=timezone(timedelta(hours=1)))

        result = BillingEventAPIResult.model_validate(an_event(event_start=one_am_utc_as_two_am_plus_one))

        assert result.event_start == datetime(2025, 6, 15, 1, 0, tzinfo=UTC)

    def test_a_naive_timestamp_is_taken_as_utc(self) -> None:
        """Reachable from an object built in Python before any round trip.

        Without the validator it would serialise with no offset at all, which is a silently
        different wire format from every other timestamp the API emits.
        """
        result = BillingEventAPIResult.model_validate(an_event(event_start=datetime(2025, 6, 15, 12, 0)))

        assert result.event_start == datetime(2025, 6, 15, 12, 0, tzinfo=UTC)

    @pytest.mark.parametrize(
        ("stored", "emitted"),
        [
            (datetime(2025, 6, 15, 12, 0, tzinfo=UTC), "2025-06-15T12:00:00Z"),
            (datetime(2025, 6, 15, 12, 0, 0, 654321, tzinfo=UTC), "2025-06-15T12:00:00.654321Z"),
        ],
        ids=["whole-seconds", "with-microseconds"],
    )
    def test_the_serialised_form(self, stored: datetime, emitted: str) -> None:
        """Z rather than +00:00, and sub-second precision is kept.

        The truncation to whole seconds was dropped deliberately: billing event timestamps
        arrive from Pulsar with microseconds, so it was discarding real precision.
        """
        result = BillingEventAPIResult.model_validate(an_event(event_start=stored))

        assert result.model_dump(mode="json")["event_start"] == emitted


class TestBillingItemRateAPIResult:
    """The rates response, which replaced a price in pounds read from billing_item_price."""

    @staticmethod
    def a_rate(credits_per_unit: str = "2.34") -> BillingItemRateAPIResult:
        return BillingItemRateAPIResult(
            sku="cpu-seconds",
            credits_per_unit=Decimal(credits_per_unit),
            valid_from=datetime(2025, 1, 1, tzinfo=UTC),
            policy_version=3,
        )

    def test_every_field_comes_across(self) -> None:
        emitted = self.a_rate().model_dump(mode="json")

        assert emitted == {
            "sku": "cpu-seconds",
            "credits_per_unit": "2.34",
            "valid_from": "2025-01-01T00:00:00Z",
            "policy_version": 3,
        }

    @pytest.mark.parametrize(
        ("stored", "emitted"),
        [
            ("2.34", "2.34"),
            # Pydantic's own Decimal output would give "4.12E-7" here, which a UI showing it
            # verbatim renders as something that looks broken.
            ("0.000000412", "0.000000412"),
            # Scale is preserved: 0.10 is not 0.1, for anything formatting a quantity.
            ("0.10", "0.10"),
            ("1E+2", "100"),
        ],
        ids=["ordinary", "very-small", "trailing-zero", "exponent-in-storage"],
    )
    def test_the_rate_is_an_exact_decimal_string(self, stored: str, emitted: str) -> None:
        assert self.a_rate(stored).model_dump(mode="json")["credits_per_unit"] == emitted

    @pytest.mark.parametrize("gone", ["price", "uuid", "valid_until"])
    def test_the_fields_that_went_are_gone(self, gone: str) -> None:
        """`price` was renamed rather than redefined, so a client displaying credits as
        pounds fails visibly. `valid_until` was always null once the loader stopped closing
        policies, and `uuid` identified a price row nothing asks about.
        """
        assert gone not in BillingItemRateAPIResult.model_fields
