"""Reading the output of PostgreSQL's `EXPLAIN (FORMAT JSON)` in tests."""

import json
from collections.abc import Iterator
from typing import Any


def plan_nodes(explain_output: Any) -> list[dict[str, Any]]:  # noqa: ANN401 - whatever the driver returns
    """Every node of the plan, the root first.

    `explain_output` is the single value an `EXPLAIN (FORMAT JSON)` returns: the JSON text, or
    the list the driver has already decoded it into.
    """
    document = json.loads(explain_output) if isinstance(explain_output, str | bytes) else explain_output
    if isinstance(document, list):
        (document,) = document
    return list(_walk(document["Plan"]))


def _walk(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield node
    for child in node.get("Plans", []):
        yield from _walk(child)
