"""Which workspaces `WorkspaceAccount.known_workspaces` finds, and what reading them costs.

Here rather than in tests/ because both questions are PostgreSQL's: what the set operations
over five tables return, and which plan the planner picks for them.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from accounting_service.models import (
    BillableResourceConsumptionRateSample,
    BillingEvent,
    BillingItem,
    CreditLedgerTransaction,
    TransactionType,
    WorkspaceAccount,
    WorkspaceCategory,
)
from tests.integration.explain import plan_nodes

WHEN = datetime(2025, 6, 1, tzinfo=UTC)


@pytest.fixture
def item(db_session: Session) -> BillingItem:
    stored = BillingItem(sku="known-sku", name="known", unit="GB")
    db_session.add(stored)
    db_session.flush()
    return stored


def add_event(session: Session, item: BillingItem, workspace: str) -> None:
    session.add(
        BillingEvent(  # pyright: ignore[reportCallIssue]
            event_start=WHEN,
            event_end=WHEN + timedelta(hours=1),
            item_id=item.uuid,
            workspace=workspace,
            quantity=1.0,
        )
    )


def add_sample(session: Session, item: BillingItem, workspace: str) -> None:
    session.add(
        BillableResourceConsumptionRateSample(  # pyright: ignore[reportCallIssue]
            sample_time=WHEN, item_id=item.uuid, workspace=workspace, rate=1.0
        )
    )


def add_reversed_grant(session: Session, workspace: str) -> None:
    grant = CreditLedgerTransaction.record_grant(session, workspace=workspace, credits=Decimal(10), reason="r")
    session.flush()
    session.add(
        CreditLedgerTransaction(  # pyright: ignore[reportCallIssue]
            workspace=workspace,
            transaction_type=TransactionType.REVERSAL,
            credits=-grant.credits,
            reverses_id=grant.uuid,
            reason="r",
            occurred_at=WHEN,
        )
    )


def known(session: Session, *, unmapped: bool = False, ungranted: bool = False) -> list[str]:
    session.flush()
    listed = WorkspaceAccount.known_workspaces(session, unmapped=unmapped, ungranted=ungranted)
    from_query = sorted(
        session.execute(WorkspaceAccount.known_workspaces_query(unmapped=unmapped, ungranted=ungranted)).scalars()
    )

    assert listed == from_query
    return listed


class TestEachSourceOnItsOwn:
    def test_usage_only(self, db_session: Session, item: BillingItem) -> None:
        add_event(db_session, item, "ws")
        add_event(db_session, item, "ws")

        assert known(db_session) == ["ws"]

    def test_rate_samples_only(self, db_session: Session, item: BillingItem) -> None:
        add_sample(db_session, item, "ws")
        add_sample(db_session, item, "ws")

        assert known(db_session) == ["ws"]

    def test_mapping_only(self, db_session: Session) -> None:
        WorkspaceAccount.record_mapping(db_session, uuid4(), "ws")

        assert known(db_session) == ["ws"]

    def test_ledger_only(self, db_session: Session) -> None:
        CreditLedgerTransaction.record_grant(db_session, workspace="ws", credits=Decimal(1), reason="r")
        CreditLedgerTransaction.record_grant(db_session, workspace="ws", credits=Decimal(2), reason="r")

        assert known(db_session) == ["ws"]

    def test_category_only(self, db_session: Session) -> None:
        WorkspaceCategory.assign(db_session, "ws", "academic")

        assert known(db_session) == ["ws"]

    def test_nothing_is_empty(self, db_session: Session) -> None:
        assert known(db_session) == []
        assert known(db_session, unmapped=True) == []
        assert known(db_session, ungranted=True) == []
        assert known(db_session, unmapped=True, ungranted=True) == []


class TestSeveralSources:
    def test_a_workspace_in_every_source_is_listed_once(self, db_session: Session, item: BillingItem) -> None:
        add_event(db_session, item, "ws")
        add_sample(db_session, item, "ws")
        WorkspaceAccount.record_mapping(db_session, uuid4(), "ws")
        CreditLedgerTransaction.record_grant(db_session, workspace="ws", credits=Decimal(1), reason="r")
        WorkspaceCategory.assign(db_session, "ws", "academic")

        assert known(db_session) == ["ws"]

    def test_many_workspaces_in_the_usage_tables_are_each_found(self, db_session: Session, item: BillingItem) -> None:
        """Interleaved names, several rows each, so a step from one name to the next that skips
        or repeats one shows up."""
        for workspace in ("b", "d", "f", "h"):
            for _ in range(3):
                add_event(db_session, item, workspace)
        for workspace in ("a", "d", "g", "h"):
            for _ in range(3):
                add_sample(db_session, item, workspace)

        assert known(db_session) == ["a", "b", "d", "f", "g", "h"]


@pytest.fixture
def every_source(db_session: Session, item: BillingItem) -> None:
    """One workspace per source, a mapped one with usage, a granted one with usage, and one
    whose only grant was reversed."""
    add_event(db_session, item, "event-only")
    add_sample(db_session, item, "sample-only")
    WorkspaceAccount.record_mapping(db_session, uuid4(), "mapped-only")
    CreditLedgerTransaction.record_grant(db_session, workspace="granted-only", credits=Decimal(1), reason="r")
    WorkspaceCategory.assign(db_session, "categorised-only", "academic")

    add_event(db_session, item, "mapped-with-usage")
    WorkspaceAccount.record_mapping(db_session, uuid4(), "mapped-with-usage")

    add_sample(db_session, item, "granted-with-usage")
    CreditLedgerTransaction.record_grant(db_session, workspace="granted-with-usage", credits=Decimal(1), reason="r")

    add_event(db_session, item, "reversed-with-usage")
    add_reversed_grant(db_session, "reversed-with-usage")


@pytest.mark.usefixtures("every_source")
class TestUnmappedAndUngranted:
    def test_default_lists_every_source(self, db_session: Session) -> None:
        assert known(db_session) == [
            "categorised-only",
            "event-only",
            "granted-only",
            "granted-with-usage",
            "mapped-only",
            "mapped-with-usage",
            "reversed-with-usage",
            "sample-only",
        ]

    def test_unmapped_is_usage_with_no_account(self, db_session: Session) -> None:
        assert known(db_session, unmapped=True) == [
            "event-only",
            "granted-with-usage",
            "reversed-with-usage",
            "sample-only",
        ]

    def test_ungranted_leaves_out_any_grant_even_a_reversed_one(self, db_session: Session) -> None:
        assert known(db_session, ungranted=True) == [
            "categorised-only",
            "event-only",
            "mapped-only",
            "mapped-with-usage",
            "sample-only",
        ]

    def test_unmapped_and_ungranted_combine(self, db_session: Session) -> None:
        assert known(db_session, unmapped=True, ungranted=True) == ["event-only", "sample-only"]

    def test_two_of_the_query_can_be_joined_in_one_statement(self, db_session: Session) -> None:
        """Workspaces both unmapped and ungranted, found by joining the two variants."""
        db_session.flush()
        unmapped = WorkspaceAccount.known_workspaces_query(unmapped=True).subquery()
        ungranted = WorkspaceAccount.known_workspaces_query(ungranted=True).subquery()
        both = select(unmapped.c.workspace).join(ungranted, unmapped.c.workspace == ungranted.c.workspace)

        assert sorted(db_session.execute(both).scalars()) == ["event-only", "sample-only"]


# Enough rows that reading them all costs far more than one probe per workspace, so the
# planner's choice reflects the query's shape rather than a table small enough to scan.
WORKSPACES = 10
ROWS_PER_TABLE = 20_000

USAGE_TABLES = {
    "billing_event": "billingevent_workspace_eventstart_index",
    "billing_resource_consumption_rate_sample": "billableresourceconsumptionratesample_workspace_time_index",
}


@pytest.fixture
def large_usage(db_session: Session, item: BillingItem) -> None:
    connection = db_session.connection()
    connection.execute(
        text(
            "INSERT INTO billing_event (uuid, event_start, event_end, item_id, workspace, quantity) "
            "SELECT gen_random_uuid(), "
            "  timestamptz '2025-01-01 00:00+00' + n * interval '1 minute', "
            "  timestamptz '2025-01-01 00:05+00' + n * interval '1 minute', "
            "  :item, 'ws' || lpad((n % :workspaces)::text, 3, '0'), 1.0 "
            "FROM generate_series(1, :rows) AS n"
        ),
        {"item": item.uuid, "workspaces": WORKSPACES, "rows": ROWS_PER_TABLE},
    )
    connection.execute(
        text(
            "INSERT INTO billing_resource_consumption_rate_sample (uuid, sample_time, item_id, workspace, rate) "
            "SELECT gen_random_uuid(), timestamptz '2025-01-01 00:00+00' + n * interval '1 minute', "
            "  :item, 'ws' || lpad((n % :workspaces)::text, 3, '0'), 1.0 "
            "FROM generate_series(1, :rows) AS n"
        ),
        {"item": item.uuid, "workspaces": WORKSPACES, "rows": ROWS_PER_TABLE},
    )
    connection.execute(text("ANALYZE billing_event"))
    connection.execute(text("ANALYZE billing_resource_consumption_rate_sample"))


def explain_analyze(session: Session, statement: Any) -> list[dict[str, Any]]:  # noqa: ANN401 - any statement
    compiled = statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    return plan_nodes(session.connection().exec_driver_sql(f"EXPLAIN (ANALYZE, FORMAT JSON) {compiled}").scalar_one())


@pytest.mark.usefixtures("large_usage")
class TestKnownWorkspacesPlan:
    @pytest.mark.parametrize(
        ("unmapped", "ungranted"),
        [(False, False), (True, False), (False, True), (True, True)],
    )
    def test_usage_tables_are_probed_per_workspace_not_read_whole(
        self, db_session: Session, unmapped: bool, ungranted: bool
    ) -> None:
        """No sequential scan of either usage table, an index scan on each, and the rows those
        index scans return add up to about one per workspace rather than the whole table."""
        statement = WorkspaceAccount.known_workspaces_query(unmapped=unmapped, ungranted=ungranted)
        nodes = explain_analyze(db_session, statement)

        assert len(WorkspaceAccount.known_workspaces(db_session, unmapped=unmapped, ungranted=ungranted)) == (
            WORKSPACES
        )

        for table, index in USAGE_TABLES.items():
            on_table = [node for node in nodes if node.get("Relation Name") == table]

            assert on_table, f"{table} is not read at all"
            assert not [node for node in on_table if node["Node Type"] == "Seq Scan"], f"{table} is scanned whole"

            index_scans = [node for node in on_table if node["Node Type"] in ("Index Scan", "Index Only Scan")]
            assert index_scans, f"{table} has no index scan"
            assert {node["Index Name"] for node in index_scans} == {index}

            rows_read = sum(node["Actual Rows"] * node["Actual Loops"] for node in on_table)
            assert rows_read <= 4 * (WORKSPACES + 1), f"{table}: {rows_read} rows read for {WORKSPACES} workspaces"
