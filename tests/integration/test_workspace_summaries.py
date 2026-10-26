"""The hub-admin list of every workspace (T23): `GET /accounting/workspaces`.

Here rather than in tests/ because what is asserted is a query over five tables: which
workspaces the union finds, which snapshot DISTINCT ON picks, and what each FILTERed sum
counts. The serialisation of a row is in tests/test_api_models.py.

`recorded_at` is set by hand wherever a snapshot is involved. Its default is the transaction
timestamp, and every row a test writes shares one transaction, so left alone they would all
fall on the same side of any cut.
"""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from accounting_service.models import (
    BillableResourceConsumptionRateSample,
    BillingItem,
    CreditBalanceSnapshot,
    CreditLedgerTransaction,
    PricingPolicy,
    TransactionType,
    WorkspaceAccount,
    WorkspaceCategory,
)
from tests.conftest import TOKEN_MEMBER, TOKEN_OWNER
from tests.integration.test_models import gen_billingitem_data

JANUARY = datetime(2025, 1, 1, tzinfo=UTC)
ACCOUNT = UUID("4b48ebea-bdb8-4bb9-bce9-a7853ad3965d")


@pytest.fixture
def policy(db_session: Session) -> PricingPolicy:
    """Only its default category matters here, so it rates nothing."""
    stored = PricingPolicy(
        version=1,
        valid_from=JANUARY,
        valid_until=None,
        corrects_id=None,
        default_category="standard",
        reason="test",
    )
    db_session.add(stored)
    db_session.flush()

    return stored


def a_row(
    workspace: str,
    transaction_type: TransactionType,
    credits: str,
    *,
    recorded_at: datetime = JANUARY,
    policy: PricingPolicy | None = None,
    reverses: CreditLedgerTransaction | None = None,
) -> CreditLedgerTransaction:
    """A ledger row with `recorded_at` chosen. A debit, or a reversal of one, needs `policy`."""
    return CreditLedgerTransaction(
        workspace=workspace,
        transaction_type=transaction_type,
        credits=Decimal(credits),
        recorded_at=recorded_at,
        occurred_at=recorded_at,
        policy_id=policy.uuid if policy else None,
        category="standard" if policy else None,
        reverses_id=reverses.uuid if reverses else None,
    )


def summaries(client: TestClient) -> dict[str, dict[str, Any]]:
    response = client.get("/accounting/workspaces")
    assert response.status_code == 200, response.text

    rows = response.json()
    assert [row["workspace"] for row in rows] == sorted(row["workspace"] for row in rows)

    return {row.pop("workspace"): row for row in rows}


class TestWhichWorkspacesAreListed:
    def test_every_source_once_sorted(self, client: TestClient, db_session: Session) -> None:
        """A workspace granted credits or categorised before it has used anything is listed:
        that is the idle workspace the usage reads cannot show.
        """
        gen_billingitem_data(db_session, [{"workspace": "event-only"}, {"workspace": "both"}])
        stored_item = BillingItem(sku="sample-sku", name="sample", unit="GB")
        db_session.add(stored_item)
        db_session.flush()
        db_session.add(
            BillableResourceConsumptionRateSample(  # pyright: ignore[reportCallIssue]
                sample_time=JANUARY, item_id=stored_item.uuid, workspace="sample-only", rate=1.0
            )
        )
        WorkspaceAccount.record_mapping(db_session, uuid4(), "mapped-only")
        WorkspaceAccount.record_mapping(db_session, uuid4(), "both")
        CreditLedgerTransaction.record_grant(db_session, workspace="granted-only", credits=Decimal(5), reason="r")
        WorkspaceCategory.assign(db_session, "categorised-only", "academic")
        db_session.flush()

        assert list(summaries(client)) == [
            "both",
            "categorised-only",
            "event-only",
            "granted-only",
            "mapped-only",
            "sample-only",
        ]

    def test_nothing_known_is_an_empty_list(self, client: TestClient) -> None:
        assert summaries(client) == {}

    def test_an_idle_workspace_reports_what_it_was_granted(self, client: TestClient, db_session: Session) -> None:
        CreditLedgerTransaction.record_grant(db_session, workspace="idle", credits=Decimal(100), reason="r")
        db_session.flush()

        row = summaries(client)["idle"]

        assert (row["balance"], row["total_granted"], row["last_usage"]) == ("100", "100", None)


class TestBalance:
    def test_matches_the_single_workspace_read_with_no_snapshot(
        self, client: TestClient, db_session: Session, policy: PricingPolicy
    ) -> None:
        db_session.add_all(
            [
                a_row("ws", TransactionType.GRANT, "100"),
                a_row("ws", TransactionType.DEBIT, "-30.5", policy=policy),
                a_row("other", TransactionType.GRANT, "7"),
            ]
        )
        db_session.flush()

        rows = summaries(client)

        assert rows["ws"]["balance"] == "69.5"
        assert Decimal(rows["ws"]["balance"]) == CreditLedgerTransaction.balance(db_session, "ws")
        assert rows["other"]["balance"] == "7"

    def test_matches_the_single_workspace_read_with_snapshots(
        self, client: TestClient, db_session: Session, policy: PricingPolicy
    ) -> None:
        """The latest whole-pool snapshot plus what was recorded after it. An older snapshot
        and a per-user one are both ignored, and so is everything the snapshot already holds.
        """
        cut = JANUARY + timedelta(days=10)
        db_session.add_all(
            [
                a_row("ws", TransactionType.GRANT, "100", recorded_at=cut - timedelta(days=1)),
                a_row("ws", TransactionType.DEBIT, "-1", recorded_at=cut + timedelta(days=1), policy=policy),
                CreditBalanceSnapshot(workspace="ws", as_of=cut - timedelta(days=5), balance=Decimal(-999)),
                CreditBalanceSnapshot(workspace="ws", as_of=cut, balance=Decimal(50)),
                CreditBalanceSnapshot(workspace="ws", user=uuid4(), as_of=cut, balance=Decimal(-999)),
            ]
        )
        db_session.flush()

        rows = summaries(client)

        assert rows["ws"]["balance"] == "49"
        assert Decimal(rows["ws"]["balance"]) == CreditLedgerTransaction.balance(db_session, "ws")

    def test_a_snapshot_with_nothing_after_it(self, client: TestClient, db_session: Session) -> None:
        db_session.add_all(
            [
                a_row("ws", TransactionType.GRANT, "100", recorded_at=JANUARY),
                CreditBalanceSnapshot(workspace="ws", as_of=JANUARY, balance=Decimal(100)),
            ]
        )
        db_session.flush()

        assert summaries(client)["ws"]["balance"] == "100"

    def test_every_row_shares_one_as_of(self, client: TestClient, db_session: Session) -> None:
        for workspace in ("a", "b"):
            CreditLedgerTransaction.record_grant(db_session, workspace=workspace, credits=Decimal(1), reason="r")
        db_session.flush()

        assert len({row["as_of"] for row in summaries(client).values()}) == 1


class TestTotalGranted:
    def test_a_reversed_grant_is_taken_off_and_a_reversed_debit_is_not_a_grant(
        self, client: TestClient, db_session: Session, policy: PricingPolicy
    ) -> None:
        kept = a_row("ws", TransactionType.GRANT, "100")
        clawed_back = a_row("ws", TransactionType.GRANT, "40")
        debit = a_row("ws", TransactionType.DEBIT, "-3", policy=policy)
        db_session.add_all([kept, clawed_back, debit])
        db_session.flush()
        db_session.add_all(
            [
                a_row("ws", TransactionType.REVERSAL, "-40", reverses=clawed_back),
                a_row("ws", TransactionType.REVERSAL, "3", reverses=debit, policy=policy),
            ]
        )
        db_session.flush()

        row = summaries(client)["ws"]

        assert row["total_granted"] == "100"
        assert row["balance"] == "100"

    def test_none_is_zero(self, client: TestClient, db_session: Session) -> None:
        gen_billingitem_data(db_session, [{"workspace": "ws"}])
        db_session.flush()

        assert summaries(client)["ws"]["total_granted"] == "0"


class TestAccountCategoryAndLastUsage:
    def test_assigned_and_mapped(self, client: TestClient, db_session: Session, policy: PricingPolicy) -> None:
        last = datetime(2025, 3, 5, 12, tzinfo=UTC)
        gen_billingitem_data(
            db_session,
            [
                {"workspace": "ws", "event_start": last - timedelta(days=2), "event_end": last - timedelta(days=1)},
                {"workspace": "ws", "event_start": last - timedelta(hours=1), "event_end": last},
            ],
        )
        WorkspaceAccount.record_mapping(db_session, ACCOUNT, "ws")
        WorkspaceCategory.assign(db_session, "ws", "academic")
        db_session.flush()

        row = summaries(client)["ws"]

        assert (row["account"], row["category"], row["category_assigned"], row["last_usage"]) == (
            str(ACCOUNT),
            "academic",
            True,
            "2025-03-05T12:00:00Z",
        )

    def test_unassigned_reports_the_policy_default(
        self, client: TestClient, db_session: Session, policy: PricingPolicy
    ) -> None:
        gen_billingitem_data(db_session, [{"workspace": "ws"}])
        db_session.flush()

        row = summaries(client)["ws"]

        assert (row["account"], row["category"], row["category_assigned"]) == (None, "standard", False)

    def test_unassigned_with_no_policy_is_null(self, client: TestClient, db_session: Session) -> None:
        gen_billingitem_data(db_session, [{"workspace": "ws"}])
        db_session.flush()

        row = summaries(client)["ws"]

        assert (row["category"], row["category_assigned"]) == (None, False)


class TestCost:
    def test_the_query_count_does_not_grow_with_the_estate(
        self,
        client: TestClient,
        db_session: Session,
        policy: PricingPolicy,
        counting_selects: Callable[[], Any],
    ) -> None:
        """One statement for the list, plus what resolving the policy costs, however many
        workspaces. Compared rather than counted, so the policy's eager loads can change.
        """

        def statements_for(workspaces: int) -> int:
            for number in range(workspaces):
                CreditLedgerTransaction.record_grant(
                    db_session, workspace=f"ws{number}", credits=Decimal(1), reason="r"
                )
            db_session.flush()

            with counting_selects() as statements:
                assert len(summaries(client)) == workspaces

            return len(statements)

        one = statements_for(1)

        assert statements_for(5) == one


class TestAccess:
    @pytest.mark.parametrize("token", [TOKEN_OWNER, TOKEN_MEMBER])
    def test_no_workspace_tier_reaches_it(
        self,
        client: TestClient,
        authenticate_as: Callable[[dict[str, Any]], None],
        token: dict[str, Any],
    ) -> None:
        authenticate_as(token)

        response = client.get("/accounting/workspaces")

        assert response.status_code == 401
        assert response.json() == {"detail": "Must be a hub admin"}

    def test_a_parameter_is_rejected_rather_than_ignored(self, client: TestClient) -> None:
        """There is no server-side filter, so one that looked like it worked would not have."""
        assert client.get("/accounting/workspaces?balance-below=10").status_code == 422
