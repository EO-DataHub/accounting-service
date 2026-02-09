import logging
from typing import Any

import click
from eodhp_utils.runner import log_component_version, run, setup_logging

from accounting_service import db
from accounting_service.ingester.messager import (
    AccountingIngesterMessager,
    ConsumptionSampleRateIngesterMessager,
    WorkspaceSettingsIngesterMessager,
)


def load_config_file(filename: str = "/etc/eodh/accounting.conf") -> None:
    try:
        with open(filename) as f:
            db.insert_configuration(f)
    except FileNotFoundError:
        logging.warning("Configuration file %s not found - not loading item or price data", filename)


@click.command
@click.option("--takeover", "-t", is_flag=True, default=False, help="Run in takeover mode.")
@click.option("-v", "--verbose", count=True)
@click.option("--pulsar-url")
@click.option("--config-file", default="/etc/eodh/accounting.conf")
def cli(takeover: bool, verbose: int, config_file: str, pulsar_url: str | None = None) -> None:
    setup_logging(verbosity=verbose)
    log_component_version("accounting-service")

    db.create_db_and_tables()
    load_config_file(config_file)

    messagers: dict[str, Any] = {
        "billing-events": AccountingIngesterMessager(),
        "workspace-settings": WorkspaceSettingsIngesterMessager(),
        "billing-events-consumption-rate-samples": ConsumptionSampleRateIngesterMessager(),
    }
    run(
        messagers,
        "accounting-ingester",
        takeover_mode=takeover,
        pulsar_url=pulsar_url,
    )


if __name__ == "__main__":
    cli()
