import json
from bisect import bisect_right
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from functools import wraps
from io import StringIO
from uuid import UUID

import rich_click as click
from rich.console import Console
from rich.table import Table
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlmodel import col

from accounting_service import db, models
from accounting_service.pricing import RateCard, UnratedSKUError, price_usage
from accounting_service.timestamps import as_utc

console = Console(stderr=False)


def handle_errors(fn: Callable) -> Callable:
    """The single place commands report a failure.

    Raise ValueError for a business-rule violation (bad input, SKU not found) and it prints in
    red and exits non-zero, as does an unexpected SQLAlchemyError, rather than a traceback.
    """

    @wraps(fn)
    def wrapper(*args: list, **kwargs: dict) -> None:
        try:
            fn(*args, **kwargs)
        except (ValueError, SQLAlchemyError) as e:
            console.print(f"[red]{e}[/red]")
            raise SystemExit(1) from None

    return wrapper


@click.group("billing-admin")
@click.pass_context
@click.rich_config(help_config=click.RichHelpConfiguration(text_markup="markdown", width=79))
def cli(ctx: click.Context) -> None:
    """
    Inspect billing items and credit rates, grant credits, and read the ledger.

    Rates are not set here. A pricing policy covers every rate at once (D3) and is minted by
    loading the configuration document, which is reviewed and versioned.

    `grant` and `set-category` are privileged writes with no HTTP endpoint yet, so they are
    reachable by anyone who can reach the database - the same footing as the rest of this tool.
    """
    ctx.obj = session = Session(db.get_engine())

    @ctx.call_on_close
    def close_client() -> None:
        session.close()


# noinspection unresolved-references
@cli.command("ls")
@click.pass_obj
@click.argument("sku", help="Show this SKU's rate in every policy instead of listing all items", required=False)
@handle_errors
def list_items(session: Session, sku: str | None) -> None:
    """
    Lists all billing items and their credit rate under the policy in force.

    Pass a SKU to show its rate in every policy instead.
    """

    if sku is None:
        _list_all_items(session)
    else:
        _list_item_history(session, sku)


def _list_item_history(session: Session, sku: str) -> None:
    if models.BillingItem.find_billing_item(session, sku) is None:
        raise ValueError(f"SKU [blue]{sku}[/blue] doesn't exist")

    # col() because SQLModel declares fields as plain annotations, so `PricingPolicy.version`
    # types as an `int` rather than as a column.
    query = (
        select(models.PricingPolicy, models.PricingPolicyRate)
        .join(models.PricingPolicyRate, col(models.PricingPolicy.uuid) == col(models.PricingPolicyRate.policy_id))
        .join(models.BillingItem, col(models.BillingItem.uuid) == col(models.PricingPolicyRate.item_id))
        .where(col(models.BillingItem.sku) == sku)
        .order_by(col(models.PricingPolicy.version))
    )

    rows = list(session.execute(query))

    if not rows:
        console.print(f"No policy rates [blue]{sku}[/blue]")

        return

    table = Table(title=f"Rate history for {sku}")
    table.add_column("Policy", justify="right")
    table.add_column("Credits/unit", justify="right")
    table.add_column("Valid From", justify="right")
    table.add_column("Configured At", justify="right")
    table.add_column("Corrects", justify="right")

    for policy, rate in rows:
        table.add_row(
            str(policy.version),
            str(rate.credits_per_unit),
            policy.valid_from.isoformat(),
            policy.configured_at.isoformat(),
            str(policy.corrects_id) if policy.corrects_id else None,
        )

    console.print(table)


def _list_all_items(session: Session) -> None:
    """Every item, with its rate under the policy that prices usage now.

    An item with no rate shows blank rather than being left out: a SKU nothing can charge for
    is the interesting case.
    """
    policy = models.PricingPolicy.resolve(session, datetime.now(UTC))
    rates = {rate.item.sku: rate.credits_per_unit for rate in policy.rates} if policy else {}

    items = session.execute(select(models.BillingItem).order_by(models.BillingItem.sku)).scalars()

    table = Table(title=f"Billing Items (policy v{policy.version})" if policy else "Billing Items (no policy)")
    table.add_column("SKU")
    table.add_column("Name")
    table.add_column("Unit", justify="right")
    table.add_column("Credits/unit", justify="right")

    for item in items:
        rate = rates.get(item.sku)
        table.add_row(item.sku, item.name, item.unit, str(rate) if rate is not None else None)

    console.print(table)


# noinspection unresolved-references,argument-list
@cli.command("add-item")
@click.pass_obj
@click.option("-s", "--sku", help="SKU to create", required=True)
@click.option("-n", "--name", help="The SKU name", type=str, required=True)
@click.option("-u", "--unit", help="The SKU unit", type=str, required=True)
@handle_errors
def add_item(session: Session, sku: str, name: str, unit: str) -> None:
    """
    Creates a new billing item.

    The item has no rate until a configuration document rates it, so until then it is a SKU
    nothing can be charged for.

    Fails if the SKU already exists; use `update-item` to change its name or unit.
    """
    if models.BillingItem.find_billing_item(session, sku) is not None:
        raise ValueError(f"SKU [blue]{sku}[/blue] already exists")

    configuration = {"items": [{"sku": sku, "name": name, "unit": unit}]}
    j = json.dumps(configuration)

    db.insert_configuration(session, StringIO(j))
    session.commit()
    console.print(f"[green]Added {sku} ({name}, {unit}). It has no rate until a policy is loaded.[/green]")


# noinspection unresolved-references
@cli.command("update-item")
@click.pass_obj
@click.option("-s", "--sku", help="SKU to change", required=True)
@click.option("-n", "--name", help="The SKU name", type=str, required=False)
@click.option("-u", "--unit", help="The SKU unit", type=str, required=False)
@handle_errors
def update_item(session: Session, sku: str, name: str | None, unit: str | None) -> None:
    """
    Updates the name and/or unit of an existing billing item.

    Provide at least one of --name or --unit. This does not change prices.
    """
    if name is None and unit is None:
        raise ValueError("Provide at least one of --name or --unit")

    existing = models.BillingItem.find_billing_item(session, sku)
    if existing is None:
        raise ValueError(f"SKU [blue]{sku}[/blue] doesn't exist")

    # A configuration entry describes an item completely, so the field the operator left out is
    # filled from the stored row rather than omitted from the document.
    configuration = {
        "items": [{"sku": sku, "name": name or existing.name, "unit": unit or existing.unit}],
    }
    j = json.dumps(configuration)

    db.insert_configuration(session, StringIO(j))
    session.commit()
    console.print(f"[green]Updated {sku}[/green]")


# noinspection unresolved-references
@cli.command("grant")
@click.pass_obj
@click.option("-w", "--workspace", help="Workspace to credit", required=True)
@click.option("-a", "--amount", help="Credits to add. Must be positive", required=True)
@click.option("-r", "--reason", help="Why. Recorded on the transaction for the audit log", required=True)
@click.option("--by", help="UUID of the hub admin responsible", type=str, required=False)
@handle_errors
def grant(session: Session, workspace: str, amount: str, reason: str, by: str | None) -> None:
    """
    Grants credits to a workspace.

    Nothing converts money into credits (D2): a user asks a hub admin, who runs this.

    The amount must be positive. Taking credits back is a reversal, which references the
    transaction it corrects and belongs to T17.

    A grant is not idempotent, so re-running this after an error you are unsure about will
    double the credits.
    """
    try:
        credits = Decimal(amount)
    except InvalidOperation:
        raise ValueError(f"[blue]{amount}[/blue] is not a number") from None

    if credits <= 0:
        raise ValueError(f"Grant amount must be positive, not [blue]{credits}[/blue]")

    transaction = models.CreditLedgerTransaction.record_grant(
        session,
        workspace=workspace,
        credits=credits,
        reason=reason,
        created_by=UUID(by) if by else None,
    )
    session.commit()

    balance = models.CreditLedgerTransaction.balance(session, workspace)
    console.print(f"[green]Granted {credits} credits to {workspace}. Balance is now {balance}.[/green]")
    console.print(f"Transaction {transaction.uuid}")


# noinspection unresolved-references
@cli.command("set-category")
@click.pass_obj
@click.option("-w", "--workspace", help="Workspace to categorise", required=True)
@click.option("-c", "--category", help="Category name, as used in the configuration document", required=True)
@click.option("--by", help="UUID of the hub admin responsible", type=str, required=False)
@handle_errors
def set_category(session: Session, workspace: str, category: str, by: str | None) -> None:
    """
    Sets which pricing category a workspace is charged under.

    The workspace service is the authority on a workspace's category and will send it over
    Pulsar. Until it does, nothing populates this table and every workspace prices under the
    policy's default category.

    An unrecognised category is not an error: a workspace whose category has no multiplier
    prices under the default (D6). This warns when it cannot find one, so a typo that changes
    nothing is at least visible.

    Charges already written keep the category they were priced under.
    """
    policy = models.PricingPolicy.resolve(session, datetime.now(UTC))

    if policy is None:
        raise ValueError("No pricing policy is loaded, so nothing would price this workspace")

    configured = {entry.category for entry in policy.category_multipliers}

    models.WorkspaceCategory.assign(session, workspace, category, updated_by=UUID(by) if by else None)
    session.commit()

    console.print(f"[green]{workspace} is now priced under category {category}.[/green]")

    if category not in configured:
        console.print(
            f"[yellow]Policy v{policy.version} has no multiplier for {category}, so usage will price "
            f"under the default category {policy.default_category}. Configured: "
            f"{', '.join(sorted(configured))}[/yellow]"
        )


# noinspection unresolved-references
@cli.command("ledger")
@click.pass_obj
@click.argument("workspace", help="Workspace whose ledger to read")
@click.option("-n", "--limit", help="How many transactions to show", type=int, default=20)
@handle_errors
def ledger(session: Session, workspace: str, limit: int) -> None:
    """
    Shows a workspace's most recent credit transactions and its balance.

    Newest first, ordered by when this service recorded them rather than by when the usage
    happened, so a backfilled event appears at the top.

    Every row is shown, including reversals. The usage endpoints net a reversal against the
    charge it corrects and hide the pair (D12); this is the raw ledger.
    """
    transactions = models.CreditLedgerTransaction.recent_transactions(session, workspace, limit=limit)
    balance = models.CreditLedgerTransaction.balance(session, workspace)

    table = Table(title=f"{workspace} - balance {balance} credits")
    # The transaction ID is here to be copied, so it comes first and folds rather than
    # truncating: Rich shortens whichever column it must, and a truncated UUID is no use.
    table.add_column("Transaction", overflow="fold")
    table.add_column("Recorded", overflow="fold")
    table.add_column("Type")
    table.add_column("SKU", overflow="fold")
    table.add_column("Quantity", justify="right")
    table.add_column("Category")
    table.add_column("Credits", justify="right")

    for transaction in transactions:
        credits = (
            f"[green]+{transaction.credits}[/green]"
            if transaction.credits > 0
            else f"[red]{transaction.credits}[/red]"
        )

        table.add_row(
            str(transaction.uuid),
            # Seconds, and no offset: every timestamp this service holds is UTC.
            transaction.recorded_at_utc.strftime("%Y-%m-%d %H:%M:%S"),
            transaction.transaction_type.value,
            transaction.item.sku if transaction.item else transaction.reason,
            str(transaction.quantity) if transaction.quantity is not None else None,
            transaction.category,
            credits,
        )

    console.print(table)

    if not transactions:
        console.print(f"No transactions for [blue]{workspace}[/blue]")


# noinspection unresolved-references
@cli.command("recharge")
@click.pass_obj
@click.option("--start", help="Only events whose `event_start` is at or after this ISO 8601 instant")
@click.option("--end", help="Only events whose `event_start` is before this ISO 8601 instant")
@click.option("-w", "--workspace", help="Only this workspace")
@click.option("-s", "--sku", help="Only this SKU")
@click.option("--policy", help="Charge under this policy version rather than the one resolution picks", type=int)
@click.option("--batch", help="Events per transaction", type=int, default=1000, show_default=True)
@click.option("--commit", help="Write the debits. Without this, nothing is written", is_flag=True)
@handle_errors
def recharge(
    session: Session,
    start: str | None,
    end: str | None,
    workspace: str | None,
    sku: str | None,
    policy: int | None,
    batch: int,
    commit: bool,
) -> None:
    """
    Charges billing events that carry no debit, reporting what it would do unless `--commit`.

    An event consumed before any policy existed was recorded and not charged: `_charge_event`
    logs and returns, and nothing revisits it. The usage reads coalesce the absent ledger row
    to zero (D12's netting works on rows that exist), so the event reads as free rather than
    as unpriced. This writes the missing debits.

    **Not a re-pricing.** Every event it touches has no debit at all. An event already charged
    is left alone, which makes this safe to re-run and safe to interrupt: the ledger's partial
    unique index is the arbiter, not a check this makes first. Changing a charge that exists
    is D8's job and needs a reversal, not this.

    By default each event is priced by the policy that resolution picks for its `event_start`,
    so a backfilled charge is indistinguishable from one the ingester would have written. Note
    what that means when policies share a `valid_from`: the most recently configured one wins
    for the whole period, not just for the part it was calibrated against. The dry run prints
    the split by policy version so this is visible before anything is written.

    `--policy` overrides that and charges everything under one version. It is the escape hatch
    for a backfill that should not price under the current calibration; the version used is
    recorded on every row either way, so the choice stays auditable.
    """
    started = _parse_instant(start, "--start")
    ended = _parse_instant(end, "--end")

    if started is not None and ended is not None and started >= ended:
        raise ValueError(f"--start [blue]{started}[/blue] is not before --end [blue]{ended}[/blue]")

    pinned = _find_policy_version(session, policy) if policy is not None else None

    if pinned is None and models.PricingPolicy.current(session) is None:
        raise ValueError("No pricing policy is loaded, so nothing would price these events")

    if batch < 1:
        raise ValueError(f"--batch must be at least 1, not [blue]{batch}[/blue]")

    charged: dict[tuple[int, str], tuple[int, Decimal]] = {}
    skipped: dict[str, int] = {}
    after: UUID | None = None
    seen = 0

    # Resolution is a query per call, and a backfill of any size cannot afford one per event.
    # Its answer depends only on which policies have started by the given instant, so it is
    # constant between consecutive `valid_from` boundaries: caching on the boundary an event
    # falls after asks the question once per calibration rather than once per timestamp.
    boundaries = sorted(
        as_utc(valid_from) for valid_from in session.execute(select(col(models.PricingPolicy.valid_from))).scalars()
    )
    policies: dict[int, models.PricingPolicy | None] = {}
    rate_cards: dict[UUID, RateCard] = {}
    categories: dict[str, str | None] = {}

    def policy_for(when: datetime) -> models.PricingPolicy | None:
        # -1 when `when` precedes every boundary, which is the bucket resolve() answers from
        # the earliest policy rather than from the policies in force (D10).
        bucket = bisect_right(boundaries, when) - 1

        if bucket not in policies:
            policies[bucket] = models.PricingPolicy.resolve(session, when)

        return policies[bucket]

    while True:
        events = models.BillingEvent.find_uncharged_events(
            session,
            start=started,
            end=ended,
            workspace=workspace,
            sku=sku,
            after=after,
            limit=batch,
        )

        if not events:
            break

        for event in events:
            after = event.uuid
            seen += 1

            in_force = pinned if pinned is not None else policy_for(event.event_start_utc)

            if in_force is None:
                skipped["no applicable policy"] = skipped.get("no applicable policy", 0) + 1
                continue

            if in_force.uuid not in rate_cards:
                rate_cards[in_force.uuid] = in_force.rate_card()

            if event.workspace not in categories:
                categories[event.workspace] = models.WorkspaceCategory.category_for(session, event.workspace)

            try:
                priced = price_usage(
                    rate_cards[in_force.uuid],
                    sku=event.item.sku,
                    quantity=event.quantity,
                    category=categories[event.workspace],
                )
            except UnratedSKUError:
                skipped[f"{event.item.sku}: unrated by v{in_force.version}"] = (
                    skipped.get(f"{event.item.sku}: unrated by v{in_force.version}", 0) + 1
                )
                continue
            except ValueError:
                # A negative or non-finite quantity. The ingester refuses these too.
                skipped[f"{event.item.sku}: unpriceable quantity"] = (
                    skipped.get(f"{event.item.sku}: unpriceable quantity", 0) + 1
                )
                continue

            count, total = charged.get((in_force.version, event.item.sku), (0, Decimal(0)))
            charged[(in_force.version, event.item.sku)] = (count + 1, total + priced.credits)

            if commit:
                models.CreditLedgerTransaction.record_usage_debit(session, event, priced, in_force.uuid)

        if commit:
            # Per batch rather than once at the end, so an interrupted run keeps the work it
            # did and the next run resumes from the events still uncharged.
            session.commit()

        if len(events) < batch:
            break

    _report_recharge(charged, skipped, seen, committed=commit, pinned=pinned)


def _parse_instant(value: str | None, flag: str) -> datetime | None:
    """An ISO 8601 instant, defaulted to UTC when it carries no offset, as the loader does."""
    if value is None:
        return None

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{flag} [blue]{value}[/blue] is not an ISO 8601 date or datetime") from None

    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _find_policy_version(session: Session, version: int) -> models.PricingPolicy:
    found = session.execute(
        select(models.PricingPolicy).where(col(models.PricingPolicy.version) == version)
    ).scalar_one_or_none()

    if found is None:
        raise ValueError(f"No pricing policy with version [blue]{version}[/blue]")

    return found


def _report_recharge(
    charged: dict[tuple[int, str], tuple[int, Decimal]],
    skipped: dict[str, int],
    seen: int,
    *,
    committed: bool,
    pinned: models.PricingPolicy | None,
) -> None:
    if not seen:
        console.print("[green]No uncharged events match.[/green]")
        return

    table = Table(title="Debits written" if committed else "Debits that would be written")
    table.add_column("Policy", justify="right")
    table.add_column("SKU")
    table.add_column("Events", justify="right")
    table.add_column("Credits", justify="right")

    # Plain decimal notation throughout: a rate of 0.000000007 per unit renders as an exponent
    # under str(), which is unreadable next to a whole number of events.
    for (version, item), (count, total) in sorted(charged.items()):
        table.add_row(f"v{version}", item, f"{count:,}", format(total, "f"))

    console.print(table)

    events = sum(count for count, _ in charged.values())
    credits = sum((total for _, total in charged.values()), Decimal(0))

    console.print(f"{events:,} of {seen:,} events, {format(credits, 'f')} credits.")

    if pinned is not None:
        console.print(
            f"[yellow]Every event priced under v{pinned.version} because --policy was given, "
            f"not under the policy resolution would pick for it.[/yellow]"
        )

    for reason, count in sorted(skipped.items()):
        console.print(f"[yellow]{count:,} skipped - {reason}[/yellow]")

    if not committed:
        console.print("[blue]Nothing was written. Re-run with --commit.[/blue]")


if __name__ == "__main__":
    cli()
