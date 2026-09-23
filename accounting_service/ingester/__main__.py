import logging
from typing import Any

import click
from eodhp_utils.runner import log_component_version, run, setup_logging

from accounting_service import db
from accounting_service.configuration import ConfigurationError
from accounting_service.ingester.messager import (
    AccountingIngesterMessager,
    ConsumptionSampleRateIngesterMessager,
    WorkspaceSettingsIngesterMessager,
)
from accounting_service.ingester.topics import TopicMessagers, messagers_by_topic, split_topics


def load_config_file(filename: str = "/etc/eodh/accounting.conf") -> None:
    try:
        with open(filename) as f, db.get_sessionmaker()() as session:
            db.insert_configuration(session, f)
            session.commit()
    except FileNotFoundError:
        logging.warning("Configuration file %s not found - not loading item or price data", filename)
    except ConfigurationError:
        # Deliberately fatal. The ingester loads this before it starts consuming, so carrying
        # on would price events against a config an operator has already got wrong.
        logging.fatal("Configuration file %s is not valid - refusing to start", filename)
        raise


@click.command
@click.option("--takeover", "-t", is_flag=True, default=False, help="Run in takeover mode.")
@click.option("-v", "--verbose", count=True)
@click.option("--pulsar-url")
@click.option("--config-file", default="/etc/eodh/accounting.conf")
@click.option(
    "--billing-events-topic",
    "billing_events_topics",
    envvar="PULSAR_TOPIC_BILLING_EVENTS",
    default="billing-events",
    callback=split_topics,
    show_default=True,
    show_envvar=True,
    help="Topic to read billing events from. A comma-separated list reads all of them.",
)
@click.option(
    "--workspace-settings-topic",
    "workspace_settings_topics",
    envvar="PULSAR_TOPIC_WORKSPACE_SETTINGS",
    default="workspace-settings",
    callback=split_topics,
    show_default=True,
    show_envvar=True,
    help="Topic to read workspace settings from. A comma-separated list reads all of them.",
)
@click.option(
    "--consumption-rate-samples-topic",
    "consumption_rate_samples_topics",
    envvar="PULSAR_TOPIC_CONSUMPTION_RATE_SAMPLES",
    default="billing-events-consumption-rate-samples",
    callback=split_topics,
    show_default=True,
    show_envvar=True,
    help="Topic to read consumption rate samples from. A comma-separated list reads all of them.",
)
def cli(
    takeover: bool,
    verbose: int,
    config_file: str,
    billing_events_topics: list[str],
    workspace_settings_topics: list[str],
    consumption_rate_samples_topics: list[str],
    pulsar_url: str | None = None,
) -> None:
    setup_logging(verbosity=verbose)
    log_component_version("accounting-service")

    load_config_file(config_file)

    messagers: TopicMessagers[Any] = messagers_by_topic(
        [
            (billing_events_topics, AccountingIngesterMessager()),
            (workspace_settings_topics, WorkspaceSettingsIngesterMessager()),
            (consumption_rate_samples_topics, ConsumptionSampleRateIngesterMessager()),
        ]
    )
    run(
        messagers,
        "accounting-ingester",
        takeover_mode=takeover,
        pulsar_url=pulsar_url,
    )


if __name__ == "__main__":
    cli()
