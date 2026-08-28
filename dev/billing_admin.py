import json
from collections.abc import Callable
from datetime import UTC, datetime
from functools import wraps
from io import StringIO

import rich_click as click
from rich.console import Console
from rich.table import Table
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

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
    Manage billing items and prices directly against the database.
    """
    ctx.obj = session = Session(db.engine)

    @ctx.call_on_close
    def close_client() -> None:
        session.close()


# noinspection unresolved-references
@cli.command("ls")
@click.pass_obj
@click.argument("sku", help="Show price history for this SKU instead of listing all items", required=False)
@handle_errors
def list_items(session: Session, sku: str | None) -> None:
    """
    Lists all billing items and their current prices.

    Pass a SKU to show its full price history instead.
    """

    if sku is None:
        _list_all_items(session)
    else:
        _list_item_history(session, sku)


def _list_item_history(session: Session, sku: str) -> None:
    if models.BillingItem.find_billing_item(session, sku) is None:
        raise ValueError(f"SKU [blue]{sku}[/blue] doesn't exist")

    query = (
        select(models.BillingItemPrice)
        .join(models.BillingItem, models.BillingItem.uuid == models.BillingItemPrice.item_id)
        .where(models.BillingItem.sku == sku)
        .order_by(models.BillingItemPrice.valid_from)
    )

    prices = list(session.execute(query).scalars())

    if not prices:
        console.print(f"No price history for [blue]{sku}[/blue]")
        return

    table = Table(title=f"Billing Item History for [blue]{sku}[/blue]")
    table.add_column("Price", justify="right")
    table.add_column("Valid From", justify="right")
    table.add_column("Valid Until", justify="right")
    table.add_column("Updated", justify="right")

    for price in prices:
        # noinspection string-conversion-without-dunder-method
        table.add_row(
            str(price.price),
            price.valid_from.isoformat(),
            price.valid_until.isoformat() if price.valid_until else None,
            price.configured_at.isoformat(),
        )

    console.print(table)


def _list_all_items(session: Session) -> None:
    now = datetime.now(UTC)

    query = (
        select(models.BillingItem, models.BillingItemPrice)
        .outerjoin(
            models.BillingItemPrice,
            and_(
                models.BillingItem.uuid == models.BillingItemPrice.item_id,
                models.BillingItemPrice.valid_from <= now,
                or_(
                    models.BillingItemPrice.valid_until == None,  # noqa: E711
                    models.BillingItemPrice.valid_until > now,
                ),
            ),
        )
        .order_by(models.BillingItem.sku)
    )

    table = Table(title="Billing Items")
    table.add_column("SKU")
    table.add_column("Name")
    table.add_column("Unit", justify="right")
    table.add_column("Current Price", justify="right")
    table.add_column("Valid From", justify="right")
    for item, price in session.execute(query):
        table.add_row(
            item.sku,
            item.name,
            item.unit,
            str(price.price) if price else None,
            price.valid_from.isoformat() if price and price.valid_from else None,
        )

    console.print(table)


# noinspection unresolved-references,argument-list
@cli.command("set-price")
@click.option("-s", "--sku", help="SKU to set price for", required=True)
@click.option("-p", "--price", help="The set price in credits per unit", type=float, required=True)
@click.option("--valid", help="The date and time from which the price is valid", type=click.DateTime(), required=True)
@handle_errors
def set_price(sku: str, price: float, valid: datetime) -> None:
    """
    Sets a price for an existing billing item.

    --valid must be later than the item's latest price, or match it exactly to correct that
    price. The SKU must already exist; use `add-item` to create one.
    """
    configuration = {"prices": [{"sku": sku, "price": price, "valid_from": valid.isoformat()}]}
    j = json.dumps(configuration)
    db.insert_configuration(StringIO(j))
    console.print(f"[green]Set {sku} to {price} from {valid.isoformat()}[/green]")


# noinspection unresolved-references,argument-list
@cli.command("add-item")
@click.pass_obj
@click.option("-s", "--sku", help="SKU to create", required=True)
@click.option("-n", "--name", help="The SKU name", type=str, required=True)
@click.option("-u", "--unit", help="The SKU unit", type=str, required=True)
@click.option("-p", "--price", help="The set price in credits per unit", type=float, required=True)
@click.option("--valid", help="The date and time from which the price is valid", type=click.DateTime(), required=True)
@handle_errors
def add_item(session: Session, sku: str, name: str, unit: str, price: float, valid: datetime) -> None:
    """
    Creates a new billing item with its initial price.

    Fails if the SKU already exists; use `update-item` to change its name or unit instead.
    """
    if models.BillingItem.find_billing_item(session, sku) is not None:
        raise ValueError(f"SKU [blue]{sku}[/blue] already exists")

    configuration = {
        "items": [{"sku": sku, "name": name, "unit": unit}],
        "prices": [{"sku": sku, "price": price, "valid_from": valid.isoformat()}],
    }
    j = json.dumps(configuration)

    db.insert_configuration(StringIO(j))
    console.print(f"[green]Added {sku} ({name}, {unit}) with price {price} from {valid.isoformat()}[/green]")


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

    if models.BillingItem.find_billing_item(session, sku) is None:
        raise ValueError(f"SKU [blue]{sku}[/blue] doesn't exist")

    # Either name or unit can be None, but the upsert functions just check for their existence in the database.
    # So remove the None values from the items dictionary.
    items = [{"sku": sku, "name": name, "unit": unit}]
    clean_items = [{k: v for k, v in items[0].items() if v is not None}]

    configuration = {
        "items": clean_items,
        "prices": [],
    }
    j = json.dumps(configuration)

    db.insert_configuration(StringIO(j))
    console.print(f"[green]Updated {sku}[/green]")


if __name__ == "__main__":
    cli()
