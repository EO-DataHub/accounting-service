"""The configuration document that defines billing items and the pricing policy.

The ingester reads /etc/eodh/accounting.conf on every pod start and pushes it through
db.insert_configuration, so this document is how items and rates enter the system.
dev/billing_admin.py builds one in memory rather than taking a second route in.
"""

from typing import Annotated, Self, TextIO

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from yaml.error import YAMLError

from accounting_service.pricing import ConfiguredPolicy, repeated

# "" satisfies `str`, and an item's SKU is the key every rate is looked up by.
NonEmpty = Annotated[str, Field(min_length=1)]


class ConfigurationError(ValueError):
    """A document that cannot be applied, whether it is unparseable or merely wrong."""

    pass


class ConfiguredItem(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    sku: NonEmpty
    name: NonEmpty
    unit: NonEmpty


class Configuration(BaseModel):
    """A whole configuration document."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Tuples rather than lists: frozen=True stops the field being reassigned, not the list
    # being appended to.
    items: tuple[ConfiguredItem, ...] = ()

    pricing_policy: ConfiguredPolicy | None = None

    @model_validator(mode="after")
    def _each_item_appears_once(self) -> Self:
        """A repeated SKU means the last entry silently wins over the earlier one."""

        if duplicated := repeated(entry.sku for entry in self.items):
            raise ValueError(f"`items` names these SKUs more than once: {', '.join(duplicated)}")

        return self


def load_configuration(document: TextIO | str) -> Configuration:
    """Parse and validate a configuration document, or raise ConfigurationError.

    Every invalid field is reported at once rather than stopping at the first, so an operator
    fixing a hand-edited file sees the whole list.

    What this cannot check is whether a rate names a SKU that exists, since that needs a query.
    `PricingPolicy.load_configured_policy` raises on an unknown SKU, and a document may
    legitimately introduce an item and rate it together.
    """
    try:
        parsed = yaml.safe_load(document)
    except YAMLError as error:
        raise ConfigurationError(f"Configuration is not valid YAML: {error}") from error

    if not isinstance(parsed, dict):
        # Covers an empty file, which parses to None, and a document that is a bare list.
        found = "nothing" if parsed is None else type(parsed).__name__
        raise ConfigurationError(
            f"Configuration must be a YAML mapping with `items` and `pricing_policy` keys, found {found}"
        )

    try:
        return Configuration.model_validate(parsed)
    except ValidationError as error:
        raise ConfigurationError(f"Configuration is not valid:\n{error}") from error
