import json
from collections.abc import Callable
from datetime import UTC, datetime
from functools import wraps
from io import StringIO

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
    """The single place commands report a failure. Raise ValueError for a business-rule violation
    (bad input, SKU not found, etc.) and it prints in red and exits non-zero, same as an
    unexpected SQLAlchemyError (eg. a lost database connection) instead of a raw traceback."""

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
    Inspect billing items and credit rates, and create items, against the database.

    Rates are not set here. A pricing policy covers every rate at once (D3) and is minted by
    loading the configuration document, which is reviewed and versioned. `set-price` used to
    write one price row and has no meaning against a policy.
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

    # col() rather than the bare attribute. SQLModel declares fields as plain annotations, so
    # `PricingPolicy.version` is an `int` to a type checker and `BillingItem.sku == sku` is a
    # `bool`, neither of which is what these arguments want. col() hands back the underlying
    # column, which is what SQLAlchemy was getting all along.
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

    An item with no rate shows blank rather than being left out: a SKU nothing can charge
    for is the interesting case, not one to hide.
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

    The item has no rate until a policy rates it, which happens by loading a configuration
    document. Until then it is a SKU nothing can be charged for.

    Fails if the SKU already exists; use `update-item` to change its name or unit instead.
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

    # A configuration entry describes an item completely, so the field the operator left out
    # is filled from the stored row rather than omitted from the document. Sending a partial
    # entry and relying on the loader to update only the keys it found is what stopped item
    # entries being validated at all.
    configuration = {
        "items": [{"sku": sku, "name": name or existing.name, "unit": unit or existing.unit}],
    }
    j = json.dumps(configuration)

    db.insert_configuration(session, StringIO(j))
    session.commit()
    console.print(f"[green]Updated {sku}[/green]")


if __name__ == "__main__":
    cli()
