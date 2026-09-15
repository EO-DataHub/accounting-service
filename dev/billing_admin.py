import json
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


if __name__ == "__main__":
    cli()
