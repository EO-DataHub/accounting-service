"""Tests for the configuration document.

No database. `load_configuration` parses and validates, and nothing about that needs a
session. The two tests proving a document reaches the tables are in
tests/integration/test_package.py.

The emphasis is on rejection. Every case here was accepted before the document was
validated as a whole: a misspelled top-level key loaded and changed nothing, an item entry
was spread into a table class unchecked, and a repeated price entry quietly meant whichever
of the two was written last.
"""

import io
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from accounting_service.configuration import (
    ConfigurationError,
    ConfiguredItem,
    load_configuration,
)

COMPLETE = """---
items:
  - sku: "cpu-seconds"
    name: "CPU time"
    unit: "s"
prices:
  - sku: "cpu-seconds"
    valid_from: "2025-01-01T00:00:00Z"
    price: 12.34
"""


class TestAWellFormedDocument:
    def test_the_items_are_parsed(self) -> None:
        configuration = load_configuration(COMPLETE)

        assert configuration.items == (ConfiguredItem(sku="cpu-seconds", name="CPU time", unit="s"),)

    def test_the_prices_are_parsed(self) -> None:
        (price,) = load_configuration(COMPLETE).prices

        assert price.sku == "cpu-seconds"
        assert price.valid_from == datetime(2025, 1, 1, tzinfo=UTC)

    def test_a_price_becomes_an_exact_decimal(self) -> None:
        """YAML parses 12.34 as a float, and money that has been through a float is not the
        figure that was written down."""
        (price,) = load_configuration(COMPLETE).prices

        assert price.price == Decimal("12.34")

    def test_a_valid_from_without_an_offset_is_taken_as_utc(self) -> None:
        """The documented rule, and the reason ConfiguredPrice has a validator: calling
        astimezone on a naive value reads it as the host's local time instead."""
        document = 'prices:\n  - {sku: "s", price: 1, valid_from: "2025-07-01T00:00:00"}\n'

        (price,) = load_configuration(document).prices

        assert price.valid_from == datetime(2025, 7, 1, tzinfo=UTC)

    @pytest.mark.parametrize(
        "document",
        [
            'items:\n  - {sku: "s", name: "n", unit: "u"}\n',
            'prices:\n  - {sku: "s", price: 1, valid_from: "2025-01-01T00:00:00Z"}\n',
        ],
        ids=["items-only", "prices-only"],
    )
    def test_either_collection_may_be_absent(self, document: str) -> None:
        """A document holding only prices is what set-price sends, and one holding only items
        is what update-item sends."""
        load_configuration(document)

    def test_a_stream_is_accepted_as_well_as_a_string(self) -> None:
        """The ingester passes an open file; the admin CLI passes a StringIO."""
        assert load_configuration(io.StringIO(COMPLETE)) == load_configuration(COMPLETE)

    def test_the_result_cannot_be_edited_afterwards(self) -> None:
        configuration = load_configuration(COMPLETE)

        with pytest.raises(ValueError, match="frozen"):
            configuration.items = ()  # type: ignore[misc]


class TestTheDocumentAsAWhole:
    def test_a_misspelled_top_level_key_is_rejected(self) -> None:
        """`item` for `items`. This used to load successfully and do nothing at all, because
        the apply loop asked for each key by name and defaulted it to an empty list."""
        with pytest.raises(ConfigurationError, match="item"):
            load_configuration('item:\n  - {sku: "s", name: "n", unit: "u"}\n')

    def test_an_empty_document_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="found nothing"):
            load_configuration("")

    def test_a_document_that_is_not_a_mapping_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="must be a YAML mapping"):
            load_configuration("- one\n- two\n")

    def test_unparseable_yaml_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="not valid YAML"):
            load_configuration('items: [{sku: "unclosed"\n')

    def test_a_kubernetes_configmap_is_rejected_rather_than_ignored(self) -> None:
        """The hazard dev/accounting.conf's header warns about.

        The production config lives inside a ConfigMap, under data."accounting.conf".
        Mounting that file whole rather than its inner document gave a mapping with no
        `items` and no `prices` key, so the load succeeded and configured nothing - the
        service then priced events against whatever the last good load had left.
        """
        configmap = 'data:\n  accounting.conf: |\n    items:\n      - {sku: "s", name: "n", unit: "u"}\n'

        with pytest.raises(ConfigurationError, match="data"):
            load_configuration(configmap)

    def test_the_failure_is_a_valueerror(self) -> None:
        """dev/billing_admin.py's handle_errors catches ValueError to print a red line and
        exit non-zero. A different base class there means a raw traceback."""
        assert issubclass(ConfigurationError, ValueError)


class TestItemEntries:
    @pytest.mark.parametrize("omitted", ["sku", "name", "unit"])
    def test_an_incomplete_entry_is_rejected(self, omitted: str) -> None:
        """The defect this task was for. BillingItem is a table class, so `table=True` turns
        Pydantic validation off and `BillingItem(**item)` accepted whatever the YAML held.
        A missing unit reached a NOT NULL column; a missing name did too.
        """
        fields = {"sku": "s", "name": "n", "unit": "u"}
        del fields[omitted]

        with pytest.raises(ConfigurationError, match=omitted):
            load_configuration(f"items:\n  - {fields}\n")

    @pytest.mark.parametrize("blanked", ["sku", "name", "unit"])
    def test_a_blank_field_is_rejected(self, blanked: str) -> None:
        """ "" satisfies `str`. Rejected rather than coerced, because a blank SKU is the key
        every price would then be looked up by."""
        fields = {"sku": "s", "name": "n", "unit": "u"} | {blanked: ""}

        with pytest.raises(ConfigurationError, match="at least 1 character"):
            load_configuration(f"items:\n  - {fields}\n")

    def test_an_unknown_field_is_rejected(self) -> None:
        """A field the loader would ignore is more likely a typo than a comment."""
        with pytest.raises(ConfigurationError, match="unti"):
            load_configuration('items:\n  - {sku: "s", name: "n", unit: "u", unti: "u"}\n')

    def test_every_fault_is_reported_at_once(self) -> None:
        """An operator fixing a hand-edited file should not have to reload once per mistake."""
        with pytest.raises(ConfigurationError) as raised:
            load_configuration('items:\n  - {sku: "s"}\n  - {name: "n", unit: "u"}\n')

        assert "name" in str(raised.value)
        assert "sku" in str(raised.value)


class TestRepeatedEntries:
    def test_a_repeated_item_sku_is_rejected(self) -> None:
        """Applying both means the first is written and then overwritten, which nothing in
        the document suggests is happening."""
        document = 'items:\n  - {sku: "s", name: "one", unit: "u"}\n  - {sku: "s", name: "two", unit: "u"}\n'

        with pytest.raises(ConfigurationError, match="more than once"):
            load_configuration(document)

    def test_a_repeated_price_for_the_same_instant_is_rejected(self) -> None:
        """Worse than a repeated item, because it is order-dependent: applying the first entry
        makes the second an amendment of it, so the document means whatever was written last.
        """
        document = (
            "prices:\n"
            '  - {sku: "s", price: 1, valid_from: "2025-01-01T00:00:00Z"}\n'
            '  - {sku: "s", price: 2, valid_from: "2025-01-01T00:00:00Z"}\n'
        )

        with pytest.raises(ConfigurationError, match="more than once"):
            load_configuration(document)

    def test_the_same_instant_written_two_ways_is_still_a_duplicate(self) -> None:
        """Compared after parsing, so an offset and a Z form of the same instant collide."""
        document = (
            "prices:\n"
            '  - {sku: "s", price: 1, valid_from: "2025-01-01T00:00:00Z"}\n'
            '  - {sku: "s", price: 2, valid_from: "2025-01-01T01:00:00+01:00"}\n'
        )

        with pytest.raises(ConfigurationError, match="more than once"):
            load_configuration(document)

    def test_the_same_sku_at_different_times_is_a_price_history(self) -> None:
        document = (
            "prices:\n"
            '  - {sku: "s", price: 1, valid_from: "2025-01-01T00:00:00Z"}\n'
            '  - {sku: "s", price: 2, valid_from: "2025-02-01T00:00:00Z"}\n'
        )

        assert len(load_configuration(document).prices) == 2

    def test_the_message_names_the_offender(self) -> None:
        """Only the repeated one. Pydantic echoes the whole input alongside the message, so
        the assertion is on the sentence rather than on the absence of the other SKUs.
        """
        document = (
            'items:\n  - {sku: "fine", name: "n", unit: "u"}\n'
            '  - {sku: "twice", name: "n", unit: "u"}\n  - {sku: "twice", name: "n", unit: "u"}\n'
        )

        with pytest.raises(ConfigurationError) as raised:
            load_configuration(document)

        assert "`items` names these SKUs more than once: twice" in str(raised.value)


def test_the_development_configuration_is_valid() -> None:
    """dev/accounting.conf is mounted into the ingester by docker-compose.

    A hand edit that breaks it now stops the container at start-up rather than at the first
    price lookup, so this fails in the suite instead.
    """
    document = Path(__file__).parent.parent / "dev" / "accounting.conf"

    configuration = load_configuration(document.read_text())

    priced = {price.sku for price in configuration.prices}
    assert priced <= {item.sku for item in configuration.items}, "a price names a SKU the file does not define"
