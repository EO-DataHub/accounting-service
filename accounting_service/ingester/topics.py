"""Which Pulsar topics the ingester reads, and which messager handles each one.

Each messager reads a comma-separated list of topics, so that while a topic moves to another
namespace the ingester can read the old and the new one at once:

    PULSAR_TOPIC_BILLING_EVENTS=billing-events,persistent://public/billing/billing-events
"""

from collections.abc import Iterable

import click


def split_topics(_ctx: click.Context, param: click.Parameter, value: str) -> list[str]:
    """Click callback turning a comma-separated option into a list of topics."""
    topics = list(dict.fromkeys(topic.strip() for topic in value.split(",") if topic.strip()))
    if not topics:
        raise click.BadParameter("at least one topic is needed", param=param)

    return topics


def short_name(topic: str) -> str:
    """The last path segment of a topic name, which is what the eodhp-utils runner routes on."""
    return topic.rsplit("/", 1)[-1]


class TopicMessagers[M](dict[str, M]):
    """Messagers keyed by topic, for eodhp_utils.runner.run.

    The eodhp-utils runner (as of 0.1.15) subscribes to every key, then looks up the messager
    for a message by the last path segment of the topic it arrived on. A short key such as
    `billing-events` is found that way, but a fully qualified one such as
    `persistent://public/billing/billing-events` is not, and the message would be left
    unacknowledged. Falling back to a match on the last segment lets a fully qualified name be
    used on its own, without also subscribing to the short name in public/default.
    """

    def __missing__(self, topic: str) -> M:
        name = short_name(topic)
        for key, messager in self.items():
            if short_name(key) == name:
                return messager

        raise KeyError(topic)


def messagers_by_topic[M](topics_and_messagers: Iterable[tuple[list[str], M]]) -> TopicMessagers[M]:
    """Maps every listed topic to its messager.

    Topics read by different messagers must not share a last path segment, since the runner
    would not be able to tell their messages apart.
    """
    result = TopicMessagers[M]()
    messager_by_name: dict[str, M] = {}
    for topics, messager in topics_and_messagers:
        for topic in topics:
            if messager_by_name.setdefault(short_name(topic), messager) is not messager:
                raise click.UsageError(
                    f"Topic {topic} has the same last path segment as a topic another messager reads."
                )
            result[topic] = messager

    return result
