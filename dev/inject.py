import logging
import uuid
from datetime import UTC, datetime, timedelta

import pulsar
import rich_click as click
from eodhp_utils.pulsar import messages
from eodhp_utils.runner import setup_logging
from rich.console import Console
from rich.json import JSON
from rich.logging import RichHandler

from accounting_service.ingester.messager import AccountingIngesterMessager

console = Console(stderr=False)
logging.basicConfig(handlers=[RichHandler(show_time=False, show_path=False)], level=0, force=True)


@click.group()
@click.pass_context
@click.rich_config(help_config=click.RichHelpConfiguration(text_markup="markdown", width=79))
def cli(ctx: click.Context) -> None:
    pulsar_logger = logging.getLogger("pulsar")
    pulsar_logger.setLevel(logging.WARNING)
    console.print("Creating pulsar client")
    ctx.obj = client = pulsar.Client("pulsar://localhost:6650", logger=pulsar_logger)

    @ctx.call_on_close
    def close_client() -> None:
        console.print("Closing pulsar client")
        client.close()


@cli.command("billing-event")
@click.pass_obj
@click.option("-w", "--workspace", help="Workspace to send billing event to", required=True)
@click.option("-s", "--sku", help="SKU to send billing event for", default="cpu-seconds")
@click.option("-q", "--quantity", help="Quantity of the SKU to send billing event for", default=0.0004, type=float)
def billing_event(client: pulsar.Client, workspace: str, sku: str, quantity: float) -> None:
    setup_logging(verbosity=3, enable_otel_logging=True)

    console.print("Creating a [blue]billing-event[/blue] producer")
    producer = client.create_producer(topic="billing-events", schema=AccountingIngesterMessager.get_schema())
    now = datetime.now(tz=UTC)
    then = now - timedelta(minutes=5)
    bemsg = messages.BillingEvent(
        uuid=str(uuid.uuid4()),
        event_start=then.isoformat(),
        event_end=now.isoformat(),
        sku=sku,
        workspace=workspace,
        quantity=quantity,
    )

    console.print("Sending a [blue]billing-event[/blue]:")
    console.print(JSON.from_data(bemsg.__dict__))
    producer.send(bemsg)
    console.print("[blue]billing-event[/blue] sent")

    producer.close()


if __name__ == "__main__":
    cli()
