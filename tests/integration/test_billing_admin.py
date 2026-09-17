"""Tests for the admin CLI's update-item and recharge commands.

`update-item` because it is the command the strict configuration document forced a change on,
and `recharge` because it writes to the ledger. `add-item` sends a complete entry and is not
covered here, and never was. `set-price` is gone: a rate belongs to a policy covering every
SKU at once, so there is no single price to set.

The commands are driven through a click Context carrying the test's session, rather than
through CliRunner: the `cli` group opens its own Session on the process-wide engine, so
invoking through it would write outside the test's transaction and leave the rows behind.
"""

import io
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import click
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlmodel import col

from accounting_service import db
from accounting_service.models import (
    BillingEvent,
    BillingItem,
    CreditLedgerTransaction,
    TransactionType,
)
from dev import billing_admin

SKU = "cli-test-sku"

CONFIG = f"""---
items:
  - sku: "{SKU}"
    name: "original name"
    unit: "GB-s"
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


RECHARGE_WORKSPACE = "recharge-test-workspace"
RECHARGE_CONFIG = """---
items:
  - sku: "{sku}"
    name: "Rechargeable"
    unit: "s"
pricing_policy:
  valid_from: "2025-01-01T00:00:00Z"
  default_category: standard
  reason: "{reason}"
  rates:
    - sku: {sku}
      credits_per_unit: {rate}
  category_multipliers:
    - category: standard
      multiplier: 1
"""


def a_policy_rating(db_session: Session, sku: str, rate: str, reason: str = "calibration") -> None:
    db.insert_configuration(db_session, io.StringIO(RECHARGE_CONFIG.format(sku=sku, rate=rate, reason=reason)))


def an_event(db_session: Session, sku: str, *, when: datetime, quantity: float = 2.0) -> BillingEvent:
    """A recorded event carrying no debit, which is what the ingester leaves behind when it
    consumes a message with no policy loaded."""
    item = BillingItem.find_billing_item(db_session, sku)
    assert item is not None

    event = BillingEvent(  # pyright: ignore[reportCallIssue]
        event_start=when,
        event_end=when + timedelta(hours=1),
        item_id=item.uuid,
        user=None,
        workspace=RECHARGE_WORKSPACE,
        quantity=quantity,
    )
    db_session.add(event)
    db_session.flush()

    return event


def debits_for(db_session: Session, event: BillingEvent) -> list[CreditLedgerTransaction]:
    return list(
        db_session.execute(
            select(CreditLedgerTransaction).where(
                col(CreditLedgerTransaction.billing_event_id) == event.uuid,
                col(CreditLedgerTransaction.transaction_type) == TransactionType.DEBIT,
            )
        )
        .scalars()
        .all()
    )


@pytest.fixture
def recharge(run_command: Callable[..., None]) -> Callable[..., None]:
    """`recharge` with every option defaulted, because the callback is invoked directly and
    click therefore applies none of its own defaults."""

    def _run(**overrides: object) -> None:
        arguments: dict[str, object] = {
            "start": None,
            "end": None,
            "workspace": None,
            "sku": None,
            "policy": None,
            "batch": 1000,
            "commit": False,
        }
        arguments.update(overrides)
        run_command(billing_admin.recharge, **arguments)

    return _run


class TestRecharge:
    """Writing the debit an event never got.

    An event consumed before any policy existed was recorded and not charged, and the usage
    reads coalesce the absent ledger row to zero, so it reads as free rather than as unpriced.
    Every assertion here is on ledger rows rather than on a reported credit figure, because
    zero credits is exactly what an uncharged event already reports.
    """

    def test_a_dry_run_writes_nothing(self, db_session: Session, recharge: Callable[..., None]) -> None:
        a_policy_rating(db_session, SKU, "0.5")
        event = an_event(db_session, SKU, when=datetime(2025, 6, 1, tzinfo=UTC))

        recharge()

        assert debits_for(db_session, event) == []

    def test_an_uncharged_event_is_charged(self, db_session: Session, recharge: Callable[..., None]) -> None:
        a_policy_rating(db_session, SKU, "0.5")
        event = an_event(db_session, SKU, when=datetime(2025, 6, 1, tzinfo=UTC), quantity=4.0)

        recharge(commit=True)

        (debit,) = debits_for(db_session, event)
        # Stored negated: a balance is a plain SUM.
        assert debit.credits == Decimal("-2.0")
        assert debit.quantity == 4.0
        assert debit.occurred_at_utc == event.event_start_utc

    def test_an_already_charged_event_is_left_alone(self, db_session: Session, recharge: Callable[..., None]) -> None:
        a_policy_rating(db_session, SKU, "0.5")
        event = an_event(db_session, SKU, when=datetime(2025, 6, 1, tzinfo=UTC))

        recharge(commit=True)
        (first,) = debits_for(db_session, event)

        # A second calibration, so a re-charge would visibly differ if one happened.
        a_policy_rating(db_session, SKU, "9.0", reason="recalibration")
        recharge(commit=True)

        (only,) = debits_for(db_session, event)
        assert only.uuid == first.uuid
        assert only.credits == first.credits

    def test_running_it_twice_charges_once(self, db_session: Session, recharge: Callable[..., None]) -> None:
        """The property that makes an interrupted run safe to repeat."""
        a_policy_rating(db_session, SKU, "0.5")
        events = [an_event(db_session, SKU, when=datetime(2025, 6, day, tzinfo=UTC)) for day in (1, 2, 3)]

        recharge(commit=True)
        recharge(commit=True)

        assert [len(debits_for(db_session, event)) for event in events] == [1, 1, 1]

    def test_an_unrated_sku_is_skipped_and_the_others_are_still_charged(
        self, db_session: Session, recharge: Callable[..., None]
    ) -> None:
        """One SKU the calibration pass missed must not cost the batch its other charges."""
        a_policy_rating(db_session, SKU, "0.5")
        db_session.add(BillingItem(sku="unrated-sku", name="Unrated", unit="s"))
        db_session.flush()

        rated = an_event(db_session, SKU, when=datetime(2025, 6, 1, tzinfo=UTC))
        unrated = an_event(db_session, "unrated-sku", when=datetime(2025, 6, 2, tzinfo=UTC))

        recharge(commit=True)

        assert len(debits_for(db_session, rated)) == 1
        assert debits_for(db_session, unrated) == []

    def test_the_date_range_bounds_what_is_charged(self, db_session: Session, recharge: Callable[..., None]) -> None:
        a_policy_rating(db_session, SKU, "0.5")
        before = an_event(db_session, SKU, when=datetime(2025, 5, 31, tzinfo=UTC))
        within = an_event(db_session, SKU, when=datetime(2025, 6, 15, tzinfo=UTC))
        after = an_event(db_session, SKU, when=datetime(2025, 7, 1, tzinfo=UTC))

        recharge(start="2025-06-01", end="2025-07-01", commit=True)

        assert debits_for(db_session, before) == []
        assert len(debits_for(db_session, within)) == 1
        assert debits_for(db_session, after) == []

    def test_a_pinned_policy_prices_instead_of_the_one_resolution_picks(
        self, db_session: Session, recharge: Callable[..., None]
    ) -> None:
        """The escape hatch for a backfill that should not price under the current numbers."""
        a_policy_rating(db_session, SKU, "0.5")
        a_policy_rating(db_session, SKU, "9.0", reason="recalibration")
        event = an_event(db_session, SKU, when=datetime(2025, 6, 1, tzinfo=UTC), quantity=2.0)

        recharge(policy=1, commit=True)

        (debit,) = debits_for(db_session, event)
        assert debit.credits == Decimal("-1.0")

    def test_an_unknown_policy_version_is_refused(self, db_session: Session, recharge: Callable[..., None]) -> None:
        a_policy_rating(db_session, SKU, "0.5")
        an_event(db_session, SKU, when=datetime(2025, 6, 1, tzinfo=UTC))

        # `handle_errors` turns a business-rule ValueError into a red line and exit 1.
        with pytest.raises(SystemExit):
            recharge(policy=99, commit=True)

    def test_every_event_is_charged_when_the_batch_is_smaller_than_the_backlog(
        self, db_session: Session, recharge: Callable[..., None]
    ) -> None:
        """Paging is on the primary key and the caller removes rows as it charges them, so a
        batch boundary is where a paging mistake would drop an event."""
        a_policy_rating(db_session, SKU, "0.5")
        events = [an_event(db_session, SKU, when=datetime(2025, 6, day, tzinfo=UTC)) for day in range(1, 8)]

        recharge(batch=2, commit=True)

        assert [len(debits_for(db_session, event)) for event in events] == [1] * 7
