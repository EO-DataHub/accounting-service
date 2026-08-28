from datetime import UTC, datetime

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
def list_items(session: Session) -> None:
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
    table.add_column("Price", justify="right")
    table.add_column("Valid From")
    table.add_column("Valid Until")
    for item, price in session.execute(query):
        table.add_row(
            item.sku,
            item.name,
            item.unit,
            price.price if price else None,
            price.valid_from if price else None,
            price.valid_until if price else None,
        )

    console.print(table)


if __name__ == "__main__":
    cli()
