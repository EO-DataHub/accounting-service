import json
from datetime import UTC, datetime
from io import StringIO

import rich_click as click
from rich.console import Console
from rich.table import Table
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from accounting_service import db, models

console = Console(stderr=False)


@click.group("billing-admin")
@click.pass_context
@click.rich_config(help_config=click.RichHelpConfiguration(text_markup="markdown", width=79))
def cli(ctx: click.Context) -> None:
    ctx.obj = session = Session(db.engine)

    @ctx.call_on_close
    def close_client() -> None:
        session.close()


@cli.command("ls")
@click.pass_obj
@click.argument("sku", help="SKU to set price for", required=False)
def list_items(session: Session, sku: str | None) -> None:
    """
    Lists all billing items and their current prices.
    """

    if sku is None:
        _list_all_items(session)
    else:
        _list_item_history(session, sku)


def _list_item_history(session: Session, sku: str) -> None:
    query = (
        select(models.BillingItemPrice)
        .join(models.BillingItem, models.BillingItem.uuid == models.BillingItemPrice.item_id)
        .where(models.BillingItem.sku == sku)
        .order_by(models.BillingItemPrice.valid_from)
    )

    table = Table(title=f"Billing Item History for [blue]{sku}[/blue]")
    table.add_column("Price", justify="right")
    table.add_column("Valid From", justify="right")
    table.add_column("Valid Until", justify="right")
    table.add_column("Updated", justify="right")

    for price in session.execute(query).scalars():
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


@cli.command("set-price")
@click.option("-s", "--sku", help="SKU to set price for", required=True)
@click.option("-p", "--price", help="The set price in credits per unit", type=float, required=True)
@click.option("--valid", help="The date and time from which the price is valid", type=click.DateTime(), required=True)
def set_price(sku: str, price: float, valid: datetime) -> None:
    configuration = {"prices": [{"sku": sku, "price": price, "valid_from": valid.isoformat()}]}
    j = json.dumps(configuration)
    try:
        db.insert_configuration(StringIO(j))
    except ValueError as e:
        console.print(f"[red]{e}[/red]")


@cli.command("add-item")
@click.pass_obj
@click.option("-s", "--sku", help="SKU to create", required=True)
@click.option("-n", "--name", help="The SKU name", type=str, required=True)
@click.option("-u", "--unit", help="The SKU unit", type=str, required=True)
@click.option("-p", "--price", help="The set price in credits per unit", type=float, required=True)
@click.option("--valid", help="The date and time from which the price is valid", type=click.DateTime(), required=True)
def add_item(session: Session, sku: str, name: str, unit: str, price: float, valid: datetime) -> None:
    query = session.query(models.BillingItem).filter(models.BillingItem.sku == sku).one_or_none()

    if query is not None:
        console.print(f"[red]SKU [blue]{sku}[/blue] already exists[/red]")
        return

    configuration = {
        "items": [{"sku": sku, "name": name, "unit": unit}],
        "prices": [{"sku": sku, "price": price, "valid_from": valid.isoformat()}],
    }
    j = json.dumps(configuration)

    try:
        db.insert_configuration(StringIO(j))
    except ValueError as e:
        console.print(f"[red]{e}[/red]")


@cli.command("update-item")
@click.pass_obj
@click.option("-s", "--sku", help="SKU to change", required=True)
@click.option("-n", "--name", help="The SKU name", type=str, required=False)
@click.option("-u", "--unit", help="The SKU unit", type=str, required=False)
def update_item(session: Session, sku: str, name: str | None, unit: str | None) -> None:
    query = session.query(models.BillingItem).filter(models.BillingItem.sku == sku).one_or_none()

    if query is None:
        console.print(f"[red]SKU [blue]{sku}[/blue] doesn't exist[/red]")
        return

    # Either name or unit can be None, but the upsert functions just check for their existence in the database.
    # So remove the None values from the items dictionary.
    items = [{"sku": sku, "name": name, "unit": unit}]
    clean_items = [{k: v for k, v in items[0].items() if v is not None}]

    configuration = {
        "items": clean_items,
        "prices": [],
    }
    j = json.dumps(configuration)

    try:
        db.insert_configuration(StringIO(j))
    except ValueError as e:
        console.print(f"[red]{e}[/red]")


if __name__ == "__main__":
    cli()
