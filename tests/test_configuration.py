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
from decimal import Decimal
from pathlib import Path

import pytest

from accounting_service.configuration import (
    ConfigurationError,
    ConfiguredItem,
    load_configuration,
)

POLICY_ONLY = """---
pricing_policy:
  valid_from: "2025-01-01T00:00:00Z"
  default_category: standard
  rates: []
  category_multipliers:
    - category: standard
      multiplier: 1
"""

COMPLETE = """---
items:
  - sku: "cpu-seconds"
    name: "CPU time"
    unit: "s"
"""


class TestAWellFormedDocument:
    def test_the_items_are_parsed(self) -> None:
        configuration = load_configuration(COMPLETE)

        assert configuration.items == (ConfiguredItem(sku="cpu-seconds", name="CPU time", unit="s"),)

    @pytest.mark.parametrize(
        "document",
        [
            'items:\n  - {sku: "s", name: "n", unit: "u"}\n',
            POLICY_ONLY,
        ],
        ids=["items-only", "policy-only"],
    )
    def test_either_section_may_be_absent(self, document: str) -> None:
        """A document holding only items is what the admin CLI sends, and one holding only a
        policy is a calibration against SKUs already defined."""
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

    defined = {item.sku for item in configuration.items}

    policy = configuration.pricing_policy
    assert policy is not None, "the local config should exercise the policy loader"
    assert {rate.sku for rate in policy.rates} <= defined, "a rate names a SKU the file does not define"
    assert defined <= {rate.sku for rate in policy.rates}, "an item the file defines has no rate"


POLICY = """---
items:
  - sku: "cpu-seconds"
    name: "CPU time"
    unit: "s"
pricing_policy:
  valid_from: "2025-01-01T00:00:00Z"
  default_category: standard
  reason: "initial calibration"
  rates:
    - sku: cpu-seconds
      credits_per_unit: 0.5
  category_multipliers:
    - category: standard
      multiplier: 1
    - category: academic
      multiplier: 0.5
"""


class TestThePolicySection:
    def test_it_is_parsed(self) -> None:
        policy = load_configuration(POLICY).pricing_policy

        assert policy is not None
        assert policy.rates[0].credits_per_unit == Decimal("0.5")
        assert len(policy.category_multipliers) == 2

    def test_it_is_optional(self) -> None:
        """A document that only adds an item or corrects a price carries no policy, which is
        what the admin CLI sends."""
        assert load_configuration(COMPLETE).pricing_policy is None

    def test_a_fault_inside_it_is_reported_as_a_configuration_error(self) -> None:
        """Nested validation runs through the same load, so the whole document is rejected
        rather than the policy being skipped."""
        broken = POLICY.replace("default_category: standard", "default_category: nonexistent")

        with pytest.raises(ConfigurationError, match="has no entry in category_multipliers"):
            load_configuration(broken)

    def test_an_unknown_key_inside_it_is_rejected(self) -> None:
        with pytest.raises(ConfigurationError, match="rate"):
            load_configuration(POLICY.replace("  rates:", "  rate:"))
