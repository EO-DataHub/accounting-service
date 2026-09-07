"""Tests for the admin CLI's update-item command.

Only that one, because it is the only command the strict configuration document forced a
change on: add-item and set-price already sent complete entries. Neither of those is covered
here, and neither was before. The commands are driven through a click Context carrying the test's session, rather than
through CliRunner: the `cli` group opens its own Session on the process-wide engine, so
invoking through it would write outside the test's transaction and leave the rows behind.
"""

import io
from collections.abc import Callable

import click
import pytest
from sqlalchemy.orm import Session

from accounting_service import db
from accounting_service.models import BillingItem
from dev import billing_admin

SKU = "cli-test-sku"

CONFIG = f"""---
items:
  - sku: "{SKU}"
    name: "original name"
    unit: "GB-s"
prices:
  - sku: "{SKU}"
    valid_from: "2025-01-01T00:00:00Z"
    price: 1.00
"""


@pytest.fixture
def run_command(db_session: Session) -> Callable[..., None]:
    """Invoke a command's callback with the test's session as the click object.

    `@click.pass_obj` wraps the callback and reads the session from the active context, so
    the context has to exist and the session cannot simply be passed as an argument.
    """

    def _run(command: click.Command, **arguments: object) -> None:
        with click.Context(command, obj=db_session):
            command.callback(**arguments)  # pyright: ignore[reportOptionalCall]

    return _run


@pytest.fixture
def stored_item(db_session: Session) -> BillingItem:
    db.insert_configuration(db_session, io.StringIO(CONFIG))
    item = BillingItem.find_billing_item(db_session, SKU)
    assert item is not None

    return item


@pytest.mark.parametrize(
    ("name", "unit", "expected_name", "expected_unit"),
    [
        ("new name", None, "new name", "GB-s"),
        (None, "s", "original name", "s"),
        ("new name", "s", "new name", "s"),
    ],
    ids=["name-only", "unit-only", "both"],
)
def test_update_item_keeps_the_field_the_operator_omitted(
    db_session: Session,
    run_command: Callable[..., None],
    stored_item: BillingItem,
    name: str | None,
    unit: str | None,
    expected_name: str,
    expected_unit: str,
) -> None:
    """A configuration entry describes an item completely, so the command fills the omitted
    field from the stored row.

    It used to send a partial entry instead and rely on the loader updating only the keys it
    found. That is what kept item entries from being validated at all, and getting it wrong
    here blanks a column rather than leaving it alone.
    """
    run_command(billing_admin.update_item, sku=SKU, name=name, unit=unit)

    updated = BillingItem.find_billing_item(db_session, SKU)
    assert updated is not None
    assert updated.name == expected_name
    assert updated.unit == expected_unit


def test_update_item_needs_at_least_one_field(run_command: Callable[..., None], stored_item: BillingItem) -> None:
    """handle_errors turns the ValueError into a red line and a non-zero exit."""
    with pytest.raises(SystemExit):
        run_command(billing_admin.update_item, sku=SKU, name=None, unit=None)


def test_update_item_rejects_an_unknown_sku(run_command: Callable[..., None]) -> None:
    with pytest.raises(SystemExit):
        run_command(billing_admin.update_item, sku="never-configured", name="n", unit=None)
