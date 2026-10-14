"""Tests for the admin CLI's update-item, recharge and workspaces commands.

`update-item` because it is the command the strict configuration document forced a change on,
`recharge` because it writes to the ledger, and `workspaces` because shell scripts parse its
output. `add-item` sends a complete entry and is not
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
from uuid import uuid4

import click
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlmodel import col

from accounting_service import db
from accounting_service.models import (
    BillableResourceConsumptionRateSample,
    BillingEvent,
    BillingItem,
    CreditLedgerTransaction,
    TransactionType,
    WorkspaceAccount,
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


def test_an_error_goes_to_stderr_and_leaves_stdout_empty(
    run_command: Callable[..., None], capsys: pytest.CaptureFixture[str]
) -> None:
    """Output is fed to shell loops, which would read an error on stdout as workspace names."""
    with pytest.raises(SystemExit):
        run_command(billing_admin.update_item, sku="never-configured", name="n", unit=None)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "never-configured" in captured.err


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


class TestWorkspaces:
    """Listing every workspace the service has heard of, from usage as well as from mappings."""

    @pytest.fixture
    def known(self, db_session: Session, stored_item: BillingItem) -> None:
        """One workspace heard of each way, and one heard of twice.

        `event-only` and `sample-only` have usage and no mapping, `mapped-only` has a mapping
        and no usage, and `both` has an event and a mapping.

        `mapped-only` has a grant, and `event-only` has a grant that was reversed.
        """
        when = datetime(2025, 6, 1, tzinfo=UTC)

        for workspace in ("event-only", "both"):
            db_session.add(
                BillingEvent(  # pyright: ignore[reportCallIssue]
                    event_start=when,
                    event_end=when + timedelta(hours=1),
                    item_id=stored_item.uuid,
                    workspace=workspace,
                    quantity=1.0,
                )
            )

        db_session.add(
            BillableResourceConsumptionRateSample(  # pyright: ignore[reportCallIssue]
                sample_time=when, item_id=stored_item.uuid, workspace="sample-only", rate=1.0
            )
        )

        for workspace in ("mapped-only", "both"):
            WorkspaceAccount.record_mapping(db_session, uuid4(), workspace)

        CreditLedgerTransaction.record_grant(db_session, workspace="mapped-only", credits=Decimal(100), reason="r")

        clawed_back = CreditLedgerTransaction.record_grant(
            db_session, workspace="event-only", credits=Decimal(100), reason="r"
        )
        db_session.flush()
        db_session.add(
            CreditLedgerTransaction(  # pyright: ignore[reportCallIssue]
                workspace="event-only",
                transaction_type=TransactionType.REVERSAL,
                credits=-clawed_back.credits,
                reverses_id=clawed_back.uuid,
                reason="r",
                occurred_at=when,
            )
        )

        db_session.flush()

    def listed(self, run_command: Callable[..., None], capsys: pytest.CaptureFixture[str], **arguments: object) -> str:
        run_command(billing_admin.list_workspaces, **arguments)
        return capsys.readouterr().out

    @pytest.mark.usefixtures("known")
    def test_every_source_is_listed_once_one_name_per_line(
        self, run_command: Callable[..., None], capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert (
            self.listed(run_command, capsys, unmapped=False, ungranted=False)
            == "both\nevent-only\nmapped-only\nsample-only\n"
        )

    @pytest.mark.usefixtures("known")
    def test_unmapped_lists_only_usage_with_no_account(
        self, run_command: Callable[..., None], capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert self.listed(run_command, capsys, unmapped=True, ungranted=False) == "event-only\nsample-only\n"

    def test_nothing_known_prints_nothing(
        self, run_command: Callable[..., None], capsys: pytest.CaptureFixture[str]
    ) -> None:
        """No header and no "none found" line, so a loop over the output runs zero times."""
        assert self.listed(run_command, capsys, unmapped=False, ungranted=False) == ""

    @pytest.mark.usefixtures("known")
    def test_ungranted_leaves_out_every_workspace_with_a_grant_even_a_reversed_one(
        self, run_command: Callable[..., None], capsys: pytest.CaptureFixture[str]
    ) -> None:
        """A reversal is a deliberate clawback, and granting again would undo it."""
        assert self.listed(run_command, capsys, unmapped=False, ungranted=True) == "both\nsample-only\n"

    @pytest.mark.usefixtures("known")
    def test_ungranted_and_unmapped_combine(
        self, run_command: Callable[..., None], capsys: pytest.CaptureFixture[str]
    ) -> None:
        assert self.listed(run_command, capsys, unmapped=True, ungranted=True) == "sample-only\n"

    def test_a_debit_does_not_count_as_a_grant(
        self,
        db_session: Session,
        run_command: Callable[..., None],
        recharge: Callable[..., None],
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """A new workspace may already have been charged for usage, and still needs its grant."""
        a_policy_rating(db_session, "ungranted-sku", "0.5")
        event = an_event(db_session, "ungranted-sku", when=datetime(2025, 6, 1, tzinfo=UTC))
        recharge(commit=True)
        capsys.readouterr()
        assert len(debits_for(db_session, event)) == 1

        assert self.listed(run_command, capsys, unmapped=False, ungranted=True) == f"{RECHARGE_WORKSPACE}\n"
