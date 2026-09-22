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

import re
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest

from accounting_service.app.app import app
from accounting_service.app.models import (
    BillingEventAPIResult,
    BillingItemAPIResult,
    BillingItemRateAPIResult,
    PricingPolicyAPIResult,
    UsageQuery,
)
from accounting_service.models import (
    BillingEvent,
    BillingItem,
    PricingPolicy,
    PricingPolicyCategoryMultiplier,
    PricingPolicyRate,
    UsageDimension,
    UsageRow,
)


def an_item(sku: str = "cpu-seconds") -> BillingItem:
    return BillingItem(uuid=uuid4(), sku=sku, name="CPU time", unit="s")


def an_event(
    *,
    item: BillingItem | None = None,
    event_start: datetime = datetime(2025, 6, 15, 12, 0, tzinfo=UTC),
    event_end: datetime = datetime(2025, 6, 15, 12, 15, tzinfo=UTC),
    workspace: str = "my-workspace",
    quantity: float = 1.5,
    user: UUID | None = None,
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
        user=user,
        workspace=workspace,
        quantity=quantity,
        item=item,
    )


def a_usage_row(event: BillingEvent | None = None, credits: Decimal = Decimal("1.8")) -> UsageRow:
    """What the usage endpoints map: an event, and what the ledger charged for it.

    The response is validated from the pair rather than from the event, because `credits`
    is not on the table.
    """
    return UsageRow(event=event or an_event(), credits=credits)


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
        result = BillingEventAPIResult.model_validate(a_usage_row(an_event(item=an_item(sku="EFS-STORAGE-STD"))))

        assert result.item == "EFS-STORAGE-STD"

    def test_the_scalar_fields_come_across(self) -> None:
        event = an_event(workspace="other-workspace", quantity=42.5)

        result = BillingEventAPIResult.model_validate(a_usage_row(event))

        assert result.uuid == event.uuid
        assert result.workspace == "other-workspace"
        assert result.quantity == 42.5

    def test_the_user_comes_across(self) -> None:
        user = uuid4()

        result = BillingEventAPIResult.model_validate(a_usage_row(an_event(user=user)))

        assert result.user == user

    def test_usage_nobody_is_responsible_for_reports_no_user(self) -> None:
        """Workspace storage, for instance: metered against the workspace and against no
        one in it. Null is the answer, not an omission.
        """
        result = BillingEventAPIResult.model_validate(a_usage_row(an_event(user=None)))

        assert result.user is None

    @pytest.mark.parametrize("ungrouped", ["item", "workspace", "user"])
    def test_a_dimension_an_aggregate_spans_is_null_rather_than_missing(self, ungrouped: str) -> None:
        """What `group-by` leaves out comes back from the query as NULL, and has to survive
        response validation as null. Before grouping was selectable, `item` and `workspace`
        were required, so this is the assertion that they are not any more.
        """
        event = an_event()
        setattr(event, ungrouped, None)

        result = BillingEventAPIResult.model_validate(a_usage_row(event))

        assert getattr(result, ungrouped) is None

    def test_a_timestamp_with_an_offset_is_converted_to_utc(self) -> None:
        """Not relabelled. The connection can hand back an aware value in any timezone, and
        the response has always claimed UTC with a Z suffix."""
        one_am_utc_as_two_am_plus_one = datetime(2025, 6, 15, 2, 0, tzinfo=timezone(timedelta(hours=1)))

        result = BillingEventAPIResult.model_validate(a_usage_row(an_event(event_start=one_am_utc_as_two_am_plus_one)))

        assert result.event_start == datetime(2025, 6, 15, 1, 0, tzinfo=UTC)

    def test_a_naive_timestamp_is_taken_as_utc(self) -> None:
        """Reachable from an object built in Python before any round trip.

        Without the validator it would serialise with no offset at all, which is a silently
        different wire format from every other timestamp the API emits.
        """
        result = BillingEventAPIResult.model_validate(a_usage_row(an_event(event_start=datetime(2025, 6, 15, 12, 0))))

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
        result = BillingEventAPIResult.model_validate(a_usage_row(an_event(event_start=stored)))

        assert result.model_dump(mode="json")["event_start"] == emitted

    def test_credits_come_from_the_row_rather_than_the_event(self) -> None:
        """The only field with no counterpart on the table."""
        result = BillingEventAPIResult.model_validate(a_usage_row(credits=Decimal("1.8")))

        assert result.credits == Decimal("1.8")

    def test_credits_serialise_as_an_exact_decimal_string(self) -> None:
        """A float here would round a charge on the way out."""
        result = BillingEventAPIResult.model_validate(a_usage_row(credits=Decimal("0.0000001")))

        assert result.model_dump(mode="json")["credits"] == "0.0000001"

    def test_usage_that_was_never_charged_reports_zero_credits(self) -> None:
        """An event with no ledger row keeps its quantity and reports no cost.

        `find_billing_events` coalesces the missing sum to zero, so the field is never null
        and the front end has no third case to handle. The distinction between "cost nothing"
        and "was never priced" is not on this endpoint by design - see `UsageRow`.
        """
        result = BillingEventAPIResult.model_validate(a_usage_row(credits=Decimal(0)))

        assert result.credits == Decimal(0)
        assert result.quantity == 1.5


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


class TestPricingPolicyAPIResult:
    """The whole rate card in force, projected from a stored policy."""

    @staticmethod
    def a_policy(
        *,
        rates: tuple[tuple[str, str], ...] = (("cpu-seconds", "0.001"),),
        multipliers: tuple[tuple[str, str], ...] = (("standard", "1"),),
    ) -> PricingPolicy:
        policy = PricingPolicy(
            version=3,
            valid_from=datetime(2025, 1, 1, tzinfo=UTC),
            configured_at=datetime(2024, 12, 20, 9, 30, tzinfo=UTC),
            default_category="standard",
        )
        policy.rates = [
            PricingPolicyRate(  # pyright: ignore[reportCallIssue]
                item_id=uuid4(),
                credits_per_unit=Decimal(credits_per_unit),
                item=an_item(sku),
            )
            for sku, credits_per_unit in rates
        ]
        policy.category_multipliers = [
            PricingPolicyCategoryMultiplier(  # pyright: ignore[reportCallIssue]
                category=category, multiplier=Decimal(multiplier)
            )
            for category, multiplier in multipliers
        ]

        return policy

    def test_every_field_comes_across(self) -> None:
        emitted = PricingPolicyAPIResult.of(self.a_policy()).model_dump(mode="json")

        assert emitted == {
            "version": 3,
            "valid_from": "2025-01-01T00:00:00Z",
            "configured_at": "2024-12-20T09:30:00Z",
            "default_category": "standard",
            "rates": [{"sku": "cpu-seconds", "credits_per_unit": "0.001"}],
            "category_multipliers": [{"category": "standard", "multiplier": "1"}],
        }

    def test_rates_and_multipliers_are_sorted(self) -> None:
        """Relationship order is whatever the database returned. The response is not."""
        policy = self.a_policy(
            rates=(("memory-gb-seconds", "0.002"), ("cpu-seconds", "0.001")),
            multipliers=(("standard", "1"), ("academic", "0.5")),
        )

        emitted = PricingPolicyAPIResult.of(policy).model_dump(mode="json")

        assert [rate["sku"] for rate in emitted["rates"]] == ["cpu-seconds", "memory-gb-seconds"]
        assert [entry["category"] for entry in emitted["category_multipliers"]] == ["academic", "standard"]

    @pytest.mark.parametrize(("stored", "emitted"), [("0.000000412", "0.000000412"), ("0.50", "0.50")])
    def test_amounts_are_exact_decimal_strings(self, stored: str, emitted: str) -> None:
        """Both amounts go through ExactDecimal, not just the rate."""
        policy = self.a_policy(rates=(("cpu-seconds", stored),), multipliers=(("standard", stored),))

        rendered = PricingPolicyAPIResult.of(policy).model_dump(mode="json")

        assert rendered["rates"][0]["credits_per_unit"] == emitted
        assert rendered["category_multipliers"][0]["multiplier"] == emitted

    def test_a_naive_stored_timestamp_is_taken_as_utc(self) -> None:
        """configured_at defaults to func.now() and comes back naive from some drivers."""
        policy = self.a_policy()
        policy.configured_at = datetime(2024, 12, 20, 9, 30)

        assert PricingPolicyAPIResult.of(policy).model_dump(mode="json")["configured_at"] == "2024-12-20T09:30:00Z"

    @pytest.mark.parametrize("gone", ["uuid", "reason", "corrects_id", "valid_until"])
    def test_the_audit_fields_are_not_served(self, gone: str) -> None:
        """The endpoint answers "what am I charged", not "who changed it and why".
        `billing-admin` is the audit path, and T19 is where an audit trail lands.
        """
        assert gone not in PricingPolicyAPIResult.model_fields


class TestUsageQueryGrouping:
    """`group-by` is the parameter that decides an aggregate's shape, so its parsing is the
    contract. Pydantic is what rejects a bad one, and FastAPI turns that into a 422.
    """

    def test_omitting_it_is_not_the_same_as_asking_for_nothing(self) -> None:
        """None means "the default set"; the empty set means "total over the period alone".
        Collapsing the two would make the default unexpressible.
        """
        # model_validate({}) rather than UsageQuery(): the defaults live inside Field()
        # within Annotated, which pyright does not read as a default for the constructor.
        assert UsageQuery.model_validate({}).group_by is None

        query = UsageQuery.model_validate({"time-aggregation": "day", "group-by": ""})

        assert query.group_by == frozenset()

    def test_comma_separated(self) -> None:
        query = UsageQuery.model_validate({"time-aggregation": "day", "group-by": "user,sku"})

        assert query.group_by == frozenset({UsageDimension.USER, UsageDimension.SKU})

    def test_repeated(self) -> None:
        """How FastAPI spells a set-typed query parameter by default."""
        query = UsageQuery.model_validate({"time-aggregation": "day", "group-by": ["user", "sku"]})

        assert query.group_by == frozenset({UsageDimension.USER, UsageDimension.SKU})

    def test_repeated_and_comma_separated_together(self) -> None:
        query = UsageQuery.model_validate({"time-aggregation": "day", "group-by": ["user,sku", "workspace"]})

        assert query.group_by == frozenset({UsageDimension.USER, UsageDimension.SKU, UsageDimension.WORKSPACE})

    def test_surrounding_space_is_ignored(self) -> None:
        query = UsageQuery.model_validate({"time-aggregation": "day", "group-by": "user, sku"})

        assert query.group_by == frozenset({UsageDimension.USER, UsageDimension.SKU})

    @pytest.mark.parametrize("bad", ["period", "account", "SKU"])
    def test_a_dimension_that_is_not_one_is_rejected(self, bad: str) -> None:
        """A closed set, like `time-aggregation`. Silently ignoring an unknown dimension
        would hand back a breakdown by something other than what was asked for.
        """
        with pytest.raises(ValueError, match="group-by"):
            UsageQuery.model_validate({"time-aggregation": "day", "group-by": bad})

    def test_grouping_without_aggregation_is_rejected(self) -> None:
        """There are no totals to group. Rejected rather than ignored, because one row per
        event looks enough like a breakdown to be read as one.
        """
        with pytest.raises(ValueError, match="time-aggregation"):
            UsageQuery.model_validate({"group-by": "user"})

    def test_aggregation_without_grouping_is_the_default(self) -> None:
        assert UsageQuery.model_validate({"time-aggregation": "day"}).group_by is None


def published_group_by_schema() -> dict[str, Any]:
    """The `group-by` parameter as /openapi.json publishes it.

    No database: building the spec reads the routes and the models, nothing else. The
    parameter is the same on both usage-data endpoints, so the first one found will do.
    """

    for path in app.openapi()["paths"].values():
        for operation in path.values():
            for parameter in operation.get("parameters", []):
                if parameter["name"] == "group-by":
                    return parameter["schema"]

    raise AssertionError("no group-by parameter is published")


class TestGroupingIsPublishedAsItIsAccepted:
    """A generated client knows only the schema, and the declared type published an array of
    dimensions alone. An array cannot carry the empty value - an empty one serialises to no
    parameter at all, which reads as omitted, i.e. the default - so "total over the period
    alone" was reachable only by writing the request by hand.
    """

    @staticmethod
    def string_spelling() -> str:
        (branch,) = (b for b in published_group_by_schema()["anyOf"] if b.get("type") == "string")

        return branch["pattern"]

    @pytest.mark.parametrize("spelling", ["", "user", "user,sku", "user, sku", "user,sku,workspace"])
    def test_what_the_schema_admits_is_accepted(self, spelling: str) -> None:
        """Soundness, which is the direction a generated client depends on. Not the converse:
        the validator also forgives a trailing comma, and publishing that would be odd.
        """
        assert re.fullmatch(self.string_spelling(), spelling), f"{spelling!r} is not published as valid"

        UsageQuery.model_validate({"time-aggregation": "day", "group-by": spelling})

    @pytest.mark.parametrize("bad", ["period", "account", "SKU"])
    def test_a_dimension_that_is_not_one_is_not_expressible_either(self, bad: str) -> None:
        assert re.fullmatch(self.string_spelling(), bad) is None

    def test_the_examples_are_instances_of_it(self) -> None:
        """They were not: an array schema with string examples, one of them not even a
        dimension. Swagger UI renders those into its array widget, and a generator that
        validates its examples rejects them.
        """
        for example in published_group_by_schema()["examples"]:
            assert re.fullmatch(self.string_spelling(), example), f"example {example!r} is not valid"

    def test_the_repeated_spelling_still_lists_every_dimension(self) -> None:
        """Inlined rather than $ref'd, so this is what holds it to UsageDimension."""
        (branch,) = (b for b in published_group_by_schema()["anyOf"] if b.get("type") == "array")

        assert branch["items"]["enum"] == list(UsageDimension)


class TestUsageQueryFilters:
    def test_user_and_sku_default_to_unfiltered(self) -> None:
        query = UsageQuery.model_validate({})

        assert query.user is None
        assert query.sku is None

    def test_they_parse(self) -> None:
        user = uuid4()

        query = UsageQuery.model_validate({"user": str(user), "sku": "cpu-seconds"})

        assert query.user == user
        assert query.sku == "cpu-seconds"

    def test_a_user_that_is_not_a_uuid_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="user"):
            UsageQuery.model_validate({"user": "somebody"})

    def test_they_need_no_aggregation(self) -> None:
        """Unlike `group-by`: filtering selects which events are counted, which means the
        same thing whether or not they are then totalled.
        """
        query = UsageQuery.model_validate({"user": str(uuid4()), "sku": "cpu-seconds"})

        assert query.time_aggregation is None
