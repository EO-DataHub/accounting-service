import click
from eodhp_utils.runner import log_component_version, run, setup_logging

from accounting_service import db
from accounting_service.ingester.messager import (
    AccountingIngesterMessager,
    WorkspaceSettingsIngesterMessager,
)


@click.command
@click.option("--takeover", "-t", is_flag=True, default=False, help="Run in takeover mode.")
@click.option("-v", "--verbose", count=True)
@click.option("--pulsar-url")
def cli(takeover: bool, verbose: int, pulsar_url=None):
    setup_logging(verbosity=verbose)
    log_component_version("annotations_ingester")

    db.create_db_and_tables()

    run(
        {
            "billing-events": AccountingIngesterMessager(),
            "workspace-settings": WorkspaceSettingsIngesterMessager(),
        },
        "accounting-ingester",
        takeover_mode=takeover,
        pulsar_url=pulsar_url,
    )


if __name__ == "__main__":
    cli()
