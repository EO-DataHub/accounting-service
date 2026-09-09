"""The configuration document that defines items and their prices.

The ingester reads /etc/eodh/accounting.conf on every pod start and pushes it through
db.insert_configuration, so this document is how items and prices enter the system;
dev/billing_admin.py builds one in memory rather than taking a second route in.

Validated here, in one pass, before any of it reaches a session. Each entry used to be read
as a bare dict inside the apply loop, which had two consequences. A typo was found one entry
at a time, so a document could be part applied before the bad entry raised - and because the
ingester loads the file before it starts consuming, that left the pod running against a
half-configured price list. And an item entry was never validated at all: BillingItem is a
SQLModel table class, and `table=True` turns Pydantic validation off, so `BillingItem(**item)`
accepted whatever the YAML held and left the complaint to the database, or to nothing.

`ConfiguredPolicy` is deliberately not defined here. It lives in accounting_service.pricing
next to the rule that consumes it, because whether a document is a new calibration or the one
already in force is a pricing question rather than a property of the document.
"""

from typing import Annotated, Self, TextIO

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from yaml.error import YAMLError

from accounting_service.pricing import ConfiguredPolicy, repeated

# "" satisfies `str`, and an item's SKU is the key every price is looked up by. Rejected
# rather than coerced to None, which is the same choice made for the API models.
NonEmpty = Annotated[str, Field(min_length=1)]


class ConfigurationError(ValueError):
    """A document that cannot be applied, whether it is unparseable or merely wrong.

    One type for both, because the caller's response to either is the same: stop. A
    ValueError so that dev/billing_admin.py's handle_errors reports it as a red line and a
    non-zero exit rather than a traceback.
    """


class ConfiguredItem(BaseModel):
    """One entry under `items`.

    All three fields are required, because an entry describes an item completely. Updating
    one field of a stored item is a thing the admin CLI does, not something the document
    expresses - see update-item in dev/billing_admin.py, which reads the stored row and
    fills in what the operator left out.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    sku: NonEmpty
    name: NonEmpty
    unit: NonEmpty


class Configuration(BaseModel):
    """A whole configuration document.

    `extra="forbid"` is the point of validating at this level: `item:` for `items:` used to
    load successfully and change nothing, because the apply loop asked for keys by name and
    defaulted each to an empty list.

    Both sections are optional. A document holding only items is what the admin CLI sends,
    and a document holding only a policy is a calibration against SKUs already defined.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Tuples rather than lists: frozen=True stops the field being reassigned, not the list
    # being appended to.
    items: tuple[ConfiguredItem, ...] = ()

    # One calibration pass (D3). Absent in a document that only adds an item, which is what
    # the admin CLI sends.
    pricing_policy: ConfiguredPolicy | None = None

    @model_validator(mode="after")
    def _each_item_appears_once(self) -> Self:
        """A repeated SKU means the last entry wins and the earlier one is applied then
        overwritten, which nothing about the document suggests."""
        if duplicated := repeated(entry.sku for entry in self.items):
            raise ValueError(f"`items` names these SKUs more than once: {', '.join(duplicated)}")

        return self


def load_configuration(document: TextIO | str) -> Configuration:
    """Parse and validate a configuration document, or raise ConfigurationError.

    Everything wrong with the document is reported before anything is applied. Pydantic
    reports every invalid field at once rather than stopping at the first, so an operator
    fixing a hand-edited file sees the whole list.

    What this cannot check is whether a price names a SKU that exists, since that needs a
    query. upsert_configured_price raises on an unknown SKU, and a document may legitimately
    introduce an item and its first price together.
    """
    try:
        parsed = yaml.safe_load(document)
    except YAMLError as error:
        raise ConfigurationError(f"Configuration is not valid YAML: {error}") from error

    if not isinstance(parsed, dict):
        # Covers an empty file, which parses to None, and a document that is a bare list.
        found = "nothing" if parsed is None else type(parsed).__name__
        raise ConfigurationError(f"Configuration must be a YAML mapping with `items` and `prices` keys, found {found}")

    try:
        return Configuration.model_validate(parsed)
    except ValidationError as error:
        raise ConfigurationError(f"Configuration is not valid:\n{error}") from error
