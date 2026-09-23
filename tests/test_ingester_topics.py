"""Tests for the ingester's configurable Pulsar topics.

The routing tests drive the real eodhp_utils Runner against a mocked Pulsar client, since
what matters is how the pinned eodhp-utils treats the keys it is given.
"""

from collections.abc import Iterator
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner, Result
from eodhp_utils.messagers import Messager
from eodhp_utils.runner import Runner

from accounting_service.ingester.__main__ import cli
from accounting_service.ingester.messager import (
    AccountingIngesterMessager,
    ConsumptionSampleRateIngesterMessager,
    WorkspaceSettingsIngesterMessager,
)
from accounting_service.ingester.topics import messagers_by_topic

TOPIC_ENV_VARS = (
    "PULSAR_TOPIC_BILLING_EVENTS",
    "PULSAR_TOPIC_WORKSPACE_SETTINGS",
    "PULSAR_TOPIC_CONSUMPTION_RATE_SAMPLES",
)


@pytest.fixture
def run() -> Iterator[Mock]:
    """Patches out everything the ingester CLI does apart from choosing its topics."""
    with (
        patch("accounting_service.ingester.__main__.setup_logging"),
        patch("accounting_service.ingester.__main__.load_config_file"),
        patch("accounting_service.ingester.__main__.run") as run,
    ):
        yield run


def invoke(env: dict[str, str | None]) -> Result:
    # Unset any topic variables the calling shell has, unless the test sets them.
    return CliRunner().invoke(cli, [], env={name: None for name in TOPIC_ENV_VARS} | env)


def test_default_topics_are_unchanged(run: Mock) -> None:
    result = invoke({})

    assert result.exit_code == 0, result.output
    messagers = run.call_args.args[0]
    assert list(messagers) == ["billing-events", "workspace-settings", "billing-events-consumption-rate-samples"]
    assert isinstance(messagers["billing-events"], AccountingIngesterMessager)
    assert isinstance(messagers["workspace-settings"], WorkspaceSettingsIngesterMessager)
    assert isinstance(messagers["billing-events-consumption-rate-samples"], ConsumptionSampleRateIngesterMessager)
    assert run.call_args.args[1] == "accounting-ingester"


def test_every_listed_topic_maps_to_the_same_messager(run: Mock) -> None:
    result = invoke(
        {
            "PULSAR_TOPIC_BILLING_EVENTS": " billing-events , persistent://public/billing/billing-events,",
            "PULSAR_TOPIC_WORKSPACE_SETTINGS": "persistent://public/workspaces/workspace-settings",
            "PULSAR_TOPIC_CONSUMPTION_RATE_SAMPLES": (
                "persistent://public/billing/billing-events-consumption-rate-samples"
            ),
        }
    )

    assert result.exit_code == 0, result.output
    messagers = run.call_args.args[0]
    assert list(messagers) == [
        "billing-events",
        "persistent://public/billing/billing-events",
        "persistent://public/workspaces/workspace-settings",
        "persistent://public/billing/billing-events-consumption-rate-samples",
    ]
    assert messagers["billing-events"] is messagers["persistent://public/billing/billing-events"]
    assert isinstance(messagers["billing-events"], AccountingIngesterMessager)


def test_blank_topic_list_is_refused(run: Mock) -> None:
    result = invoke({"PULSAR_TOPIC_BILLING_EVENTS": " , "})

    assert result.exit_code == 2
    assert "PULSAR_TOPIC_BILLING_EVENTS" in result.output
    run.assert_not_called()


def test_topics_for_different_messagers_must_differ_in_last_segment(run: Mock) -> None:
    # The runner routes on the last segment, so these two could not be told apart.
    result = invoke({"PULSAR_TOPIC_WORKSPACE_SETTINGS": "persistent://public/workspaces/billing-events"})

    assert result.exit_code == 2
    assert "persistent://public/workspaces/billing-events" in result.output
    run.assert_not_called()


def broker_topic_name(topic: str) -> str:
    """The name Pulsar reports on a message from `topic`: short names live in public/default."""
    return topic if "://" in topic else f"persistent://public/default/{topic}"


@pytest.mark.parametrize(
    "billing_topics",
    [
        pytest.param(["billing-events"], id="short name"),
        pytest.param(["persistent://public/billing/billing-events"], id="fully qualified only"),
        pytest.param(["billing-events", "persistent://public/billing/billing-events"], id="both during cutover"),
    ],
)
def test_runner_subscribes_to_listed_topics_and_routes_them_to_the_messager(billing_topics: list[str]) -> None:
    billing, settings, samples = (Mock(**{"consume.return_value": Messager.Failures()}) for _ in range(3))
    messagers = messagers_by_topic(
        [
            (billing_topics, billing),
            (["persistent://public/workspaces/workspace-settings"], settings),
            (["billing-events-consumption-rate-samples"], samples),
        ]
    )

    client = Mock()
    with patch("eodhp_utils.runner.get_pulsar_client", return_value=client):
        runner = Runner(messagers, "accounting-ingester")

    subscribed = [call.kwargs["topic"] for call in client.subscribe.call_args_list]
    assert subscribed == [
        *billing_topics,
        "persistent://public/workspaces/workspace-settings",
        "billing-events-consumption-rate-samples",
    ]

    for topic in billing_topics:
        consumer = Mock()
        msg = Mock(**{"topic_name.return_value": broker_topic_name(topic), "properties.return_value": {}})

        runner._listener(consumer, msg)

        billing.consume.assert_called_with(msg)
        consumer.acknowledge.assert_called_once_with(msg)

    assert billing.consume.call_count == len(billing_topics)
    settings.consume.assert_not_called()
    samples.consume.assert_not_called()


def test_topic_not_subscribed_to_has_no_messager() -> None:
    messagers = messagers_by_topic([(["persistent://public/billing/billing-events"], Mock())])

    with pytest.raises(KeyError):
        messagers["workspace-settings"]
