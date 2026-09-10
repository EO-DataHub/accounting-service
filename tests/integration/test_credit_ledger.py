"""Tests for the credit ledger: what the ingester writes, and what reads it back.

These need a database. Most of them are about something only PostgreSQL can answer - whether
the partial unique index refuses a second debit, whether the check constraint refuses an
unpriced one, whether a native enum refuses an invalid value - and the rest drive the
ingester or the API end to end.

The pricing arithmetic itself is tested without a database in tests/test_pricing.py. What is
tested here is that the arithmetic reaches a row, that the row survives redelivery, and that
the charge can still be explained from what the row stores (T8, T9, T10, T13).
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from eodhp_utils.pulsar import messages
from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.exc import DataError, IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from sqlmodel import col

from accounting_service import models
from accounting_service.ingester.messager import AccountingIngesterMessager
from accounting_service.models import (
    BillingItem,
    CreditBalanceSnapshot,
    CreditLedgerTransaction,
    PricingPolicy,
    PricingPolicyCategoryMultiplier,
    PricingPolicyRate,
    TransactionType,
    WorkspaceCategory,
)
from tests.integration.conftest import bemsg_to_pulsar_msg

JANUARY = datetime(2025, 1, 1, tzinfo=UTC)
SKU = "cpu-seconds"
WORKSPACE = "test-workspace"

# 0.001 credits per CPU-second, doubled for a commercial workspace and halved for an academic
# one. Chosen so every product below is exact in decimal and readable in an assertion.
RATE = Decimal("0.001")


@pytest.fixture
def item(db_session: Session) -> BillingItem:
    billing_item = BillingItem(sku=SKU, name="CPU time", unit="s")
    db_session.add(billing_item)
    db_session.flush()

    return billing_item


@pytest.fixture
def policy(db_session: Session, item: BillingItem) -> PricingPolicy:
    """A policy in force since January, rating one SKU across three categories."""
    stored = PricingPolicy(
        version=1,
        valid_from=JANUARY,
        valid_until=None,
        corrects_id=None,
        default_category="standard",
        reason="test",
    )
    stored.rates = [
        PricingPolicyRate(item_id=item.uuid, credits_per_unit=RATE)  # pyright: ignore[reportCallIssue]
    ]
    stored.category_multipliers = [
        PricingPolicyCategoryMultiplier(category="standard", multiplier=Decimal(1)),  # pyright: ignore[reportCallIssue]
        PricingPolicyCategoryMultiplier(category="academic", multiplier=Decimal("0.5")),  # pyright: ignore[reportCallIssue]
        PricingPolicyCategoryMultiplier(category="commercial", multiplier=Decimal(2)),  # pyright: ignore[reportCallIssue]
    ]
    db_session.add(stored)
    db_session.commit()

    return stored


def a_usage_message(quantity: float = 3600.0, workspace: str = WORKSPACE) -> messages.BillingEvent:
    """One hour of CPU time, at a fixed time after the policy takes effect.

    A fixed time rather than a faked one, because the policy resolved for an event depends on
    when the usage happened and a date before `valid_from` would take D10's fallback path
    instead of the ordinary one.
    """
    message: messages.BillingEvent = messages.BillingEvent.get_fake()
    message.uuid = str(uuid4())
    message.sku = SKU
    message.workspace = workspace
    message.quantity = quantity
    message.event_start = (JANUARY + timedelta(days=30)).isoformat()
    message.event_end = (JANUARY + timedelta(days=30, hours=1)).isoformat()

    return message


def debits(session: Session, workspace: str = WORKSPACE) -> list[CreditLedgerTransaction]:
    return list(
        session.execute(
            select(CreditLedgerTransaction)
            .where(col(CreditLedgerTransaction.workspace) == workspace)
            .where(col(CreditLedgerTransaction.transaction_type) == TransactionType.DEBIT)
        )
        .scalars()
        .all()
    )


class TestTheIngesterCharges:
    """T9: a billing event arrives and a debit is written."""

    def test_a_usage_message_writes_one_debit(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        message = a_usage_message()

        failures = AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(message))

        assert not failures.any_permanent()
        assert not failures.any_temporary()

        (debit,) = debits(db_session)

        # 3600 seconds at 0.001 credits each, standard category, multiplier 1. Four decimal
        # places because nothing is rounded and the scale of a product is the sum of its
        # inputs' scales: str(3600.0) carries one and the rate carries three.
        assert debit.credits == Decimal("-3.6000")
        assert debit.workspace == WORKSPACE
        assert debit.quantity == 3600.0
        assert debit.category == "standard"
        assert debit.policy_id == policy.uuid
        assert debit.billing_event_id == UUID(str(message.uuid))

    def test_the_debit_is_negative_and_the_price_is_not(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        """The sign is the ledger's, so that a balance is a plain sum."""
        AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(a_usage_message()))

        (debit,) = debits(db_session)

        assert debit.credits < 0
        assert debit.transaction_type == TransactionType.DEBIT

    def test_the_debit_occurred_when_the_usage_started(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        """`occurred_at` is the usage time; `recorded_at` is now. A period filter needs the
        first and reconciliation needs the second, which is why both are stored."""
        AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(a_usage_message()))

        (debit,) = debits(db_session)

        assert debit.occurred_at_utc == JANUARY + timedelta(days=30)
        assert debit.recorded_at_utc > JANUARY + timedelta(days=30)

    def test_a_redelivered_message_does_not_charge_twice(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        """The case the partial unique index exists for. Pulsar can redeliver, and a second
        charge for the same usage is the failure that a balance cannot recover from."""
        message = a_usage_message()
        messager = AccountingIngesterMessager(session_factory=db_session_factory)

        messager.consume(bemsg_to_pulsar_msg(message))
        failures = messager.consume(bemsg_to_pulsar_msg(message))

        assert not failures.any_permanent()
        assert not failures.any_temporary()
        assert len(debits(db_session)) == 1

    def test_the_workspace_category_multiplies_the_charge(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        """T6's read-if-present, and the reason the multiplier is worth having: the same
        quantity of the same SKU costs a commercial workspace twice what it costs a standard
        one."""
        WorkspaceCategory.assign(db_session, WORKSPACE, "commercial")
        db_session.commit()

        AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(a_usage_message()))

        (debit,) = debits(db_session)

        assert debit.credits == Decimal("-7.2000")
        assert debit.category == "commercial"

    def test_an_unconfigured_category_prices_under_the_default(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        """D6: a category this service has no multiplier for is a pricing decision, not a
        validation failure. The category recorded is the one the charge was computed under."""
        WorkspaceCategory.assign(db_session, WORKSPACE, "never-configured")
        db_session.commit()

        AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(a_usage_message()))

        (debit,) = debits(db_session)

        assert debit.category == "standard"
        assert debit.credits == Decimal("-3.6000")

    def test_recategorising_does_not_change_a_charge_already_written(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        """The whole reason the category is stored on the row rather than joined to."""
        AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(a_usage_message()))

        WorkspaceCategory.assign(db_session, WORKSPACE, "commercial")
        db_session.commit()
        db_session.expire_all()

        (debit,) = debits(db_session)

        assert debit.category == "standard"
        assert debit.credits == Decimal("-3.6000")

    def test_an_unrated_sku_records_the_event_but_no_charge(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        """A collector emitting a SKU the last calibration pass did not cover.

        The event is kept, because a quantity that is dropped cannot be recovered while a
        charge can always be applied later from a stored quantity (T18). The message is not
        failed, because redelivering it would not help - nothing changes until somebody rates
        the SKU.
        """
        db_session.add(BillingItem(sku="unrated-sku", name="Unrated", unit="s"))
        db_session.commit()

        message = a_usage_message()
        message.sku = "unrated-sku"

        failures = AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(message))

        assert not failures.any_permanent()
        assert not failures.any_temporary()
        assert db_session.get(models.BillingEvent, UUID(str(message.uuid))) is not None
        assert debits(db_session) == []

    def test_no_policy_at_all_records_the_event_but_no_charge(
        self, db_session: Session, db_session_factory: sessionmaker[Session], item: BillingItem
    ) -> None:
        """The state of a fresh installation before any configuration is loaded."""
        message = a_usage_message()

        failures = AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(message))

        assert not failures.any_permanent()
        assert db_session.get(models.BillingEvent, UUID(str(message.uuid))) is not None
        assert debits(db_session) == []


class TestTheSchemaRefusesBadRows:
    """The constraints, which is the part of this only PostgreSQL can answer."""

    def test_a_second_original_debit_for_one_event_is_refused(
        self, db_session: Session, policy: PricingPolicy, item: BillingItem
    ) -> None:
        event = models.BillingEvent(
            event_start=JANUARY,
            event_end=JANUARY + timedelta(hours=1),
            item_id=item.uuid,
            user=None,
            workspace=WORKSPACE,
            quantity=1.0,
        )
        db_session.add(event)
        db_session.flush()

        for _ in range(2):
            db_session.add(
                CreditLedgerTransaction(
                    workspace=WORKSPACE,
                    transaction_type=TransactionType.DEBIT,
                    credits=Decimal(-1),
                    billing_event_id=event.uuid,
                    item_id=item.uuid,
                    quantity=1.0,
                    policy_id=policy.uuid,
                    category="standard",
                    occurred_at=JANUARY,
                )
            )

        with pytest.raises(IntegrityError):
            db_session.flush()

    def test_a_correction_against_a_charged_event_is_permitted(
        self, db_session: Session, policy: PricingPolicy, item: BillingItem
    ) -> None:
        """The reason the index is partial. A plain unique constraint on `billing_event_id`
        would make re-pricing impossible (T18)."""
        event = models.BillingEvent(
            event_start=JANUARY,
            event_end=JANUARY + timedelta(hours=1),
            item_id=item.uuid,
            user=None,
            workspace=WORKSPACE,
            quantity=1.0,
        )
        db_session.add(event)
        db_session.flush()

        db_session.add(
            CreditLedgerTransaction(
                workspace=WORKSPACE,
                transaction_type=TransactionType.DEBIT,
                credits=Decimal(-1),
                billing_event_id=event.uuid,
                item_id=item.uuid,
                quantity=1.0,
                policy_id=policy.uuid,
                category="standard",
                occurred_at=JANUARY,
            )
        )
        db_session.add(
            CreditLedgerTransaction(
                workspace=WORKSPACE,
                transaction_type=TransactionType.DEBIT,
                credits=Decimal(-2),
                billing_event_id=event.uuid,
                item_id=item.uuid,
                quantity=1.0,
                policy_id=policy.uuid,
                category="standard",
                occurred_at=JANUARY,
                correction_batch_id=uuid4(),
            )
        )

        db_session.flush()

        assert len(debits(db_session)) == 2

    def test_a_debit_without_a_policy_is_refused(self, db_session: Session) -> None:
        """`ck_credit_ledger_transaction_debit_is_priced`. An unpriced debit is a charge
        nothing can explain, which is the one thing this schema is built to prevent."""
        db_session.add(
            CreditLedgerTransaction(
                workspace=WORKSPACE,
                transaction_type=TransactionType.DEBIT,
                credits=Decimal(-1),
                policy_id=None,
                category=None,
                occurred_at=JANUARY,
            )
        )

        with pytest.raises(IntegrityError):
            db_session.flush()

    def test_a_grant_needs_no_policy(self, db_session: Session) -> None:
        """The reason those two columns are nullable, against the schema note's specification.
        A grant is not priced, so there is no policy that could honestly be recorded on it."""
        CreditLedgerTransaction.record_grant(db_session, WORKSPACE, Decimal(100), reason="test")

        db_session.flush()

        assert CreditLedgerTransaction.balance(db_session, WORKSPACE) == Decimal(100)

    def test_the_transaction_type_enum_refuses_an_invalid_value(self, db_session: Session) -> None:
        """A native enum, so the value set is enforced by the database rather than by Python.

        Written through raw SQL because the Python enum makes the mistake unrepresentable,
        which is the point: the guard is here for whatever writes to this table without going
        through these models.
        """
        with pytest.raises((DataError, IntegrityError)):
            db_session.execute(
                text(
                    "INSERT INTO credit_ledger_transaction "
                    "(uuid, workspace, transaction_type, credits, occurred_at, recorded_at) "
                    "VALUES (gen_random_uuid(), 'w', 'refund', -1, now(), now())"
                )
            )


class TestTheBalance:
    """T10."""

    def test_a_workspace_with_no_ledger_has_a_zero_balance(self, db_session: Session) -> None:
        """Zero rather than None. A workspace that has never spent anything has a balance,
        and it is nought."""
        assert CreditLedgerTransaction.balance(db_session, "never-seen") == Decimal(0)

    def test_a_grant_less_usage(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        CreditLedgerTransaction.record_grant(db_session, WORKSPACE, Decimal(100), reason="test")
        db_session.commit()

        AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(a_usage_message()))

        assert CreditLedgerTransaction.balance(db_session, WORKSPACE) == Decimal("96.4000")

    def test_spending_more_than_was_granted_goes_negative(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        """Reported, not refused. Nothing in this service blocks work on a balance (D4)."""
        AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(a_usage_message()))

        assert CreditLedgerTransaction.balance(db_session, WORKSPACE) < 0

    def test_another_workspace_does_not_count(
        self, db_session: Session, db_session_factory: sessionmaker[Session], policy: PricingPolicy
    ) -> None:
        CreditLedgerTransaction.record_grant(db_session, "other-workspace", Decimal(500), reason="test")
        db_session.commit()

        assert CreditLedgerTransaction.balance(db_session, WORKSPACE) == Decimal(0)

    def test_a_snapshot_is_added_to_only_what_came_after_it(self, db_session: Session) -> None:
        """The snapshot is an opening figure and the ledger supplies the delta.

        `recorded_at` is set explicitly on both rows rather than left to its default, and that
        is not merely convenience. `func.now()` is PostgreSQL's *transaction* timestamp, so
        every row written inside one transaction carries the same `recorded_at` and no
        ordering between them exists to test. Whatever writes snapshots (T16) inherits that:
        it cannot take its cut from "now", because rows it has just summed may share the
        instant it would cut at, and `recorded_at > as_of` would then drop them. It has to cut
        at the `recorded_at` of the newest row it included.
        """
        cut = datetime(2026, 1, 1, tzinfo=UTC)

        db_session.add(
            CreditLedgerTransaction(
                workspace=WORKSPACE,
                transaction_type=TransactionType.GRANT,
                credits=Decimal(100),
                reason="before the cut",
                occurred_at=cut - timedelta(hours=1),
                recorded_at=cut - timedelta(hours=1),
            )
        )
        db_session.add(CreditBalanceSnapshot(workspace=WORKSPACE, user=None, as_of=cut, balance=Decimal(100)))
        db_session.add(
            CreditLedgerTransaction(
                workspace=WORKSPACE,
                transaction_type=TransactionType.GRANT,
                credits=Decimal(5),
                reason="after the cut",
                occurred_at=cut + timedelta(hours=1),
                recorded_at=cut + timedelta(hours=1),
            )
        )
        db_session.commit()

        # 100 from the snapshot plus the 5 recorded after it. The 100 granted before the cut
        # is already inside the snapshot and must not be counted a second time.
        assert CreditLedgerTransaction.balance(db_session, WORKSPACE) == Decimal(105)

    def test_a_backfilled_event_is_not_lost_by_the_snapshot(self, db_session: Session) -> None:
        """Why the delta is taken on `recorded_at` and never on `occurred_at`.

        This row describes usage from before the snapshot's cut but was recorded after it. On
        `occurred_at` it would fall inside the snapshot's period, which does not contain it,
        and outside the delta - so it would vanish from the balance entirely.
        """
        cut = datetime(2026, 1, 1, tzinfo=UTC)

        db_session.add(CreditBalanceSnapshot(workspace=WORKSPACE, user=None, as_of=cut, balance=Decimal(100)))
        db_session.add(
            CreditLedgerTransaction(
                workspace=WORKSPACE,
                transaction_type=TransactionType.GRANT,
                credits=Decimal(7),
                reason="backfilled",
                occurred_at=cut - timedelta(days=30),
                recorded_at=cut + timedelta(hours=1),
            )
        )
        db_session.commit()

        assert CreditLedgerTransaction.balance(db_session, WORKSPACE) == Decimal(107)

    def test_the_latest_snapshot_wins(self, db_session: Session) -> None:
        """Snapshots accumulate, keyed on `as_of`. Reading an older one would double-count
        everything between the two."""
        earlier = datetime.now(UTC) - timedelta(hours=1)
        later = datetime.now(UTC)

        db_session.add(CreditBalanceSnapshot(workspace=WORKSPACE, user=None, as_of=earlier, balance=Decimal(10)))
        db_session.add(CreditBalanceSnapshot(workspace=WORKSPACE, user=None, as_of=later, balance=Decimal(70)))
        db_session.commit()

        assert CreditLedgerTransaction.balance(db_session, WORKSPACE) == Decimal(70)


class TestExplainingACharge:
    """T13, over HTTP. The endpoint recomputes the arithmetic from what the row stores."""

    def test_a_charge_explains_itself(
        self,
        client: TestClient,
        db_session: Session,
        db_session_factory: sessionmaker[Session],
        policy: PricingPolicy,
    ) -> None:
        AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(a_usage_message()))
        (debit,) = debits(db_session)

        response = client.get(f"/workspaces/{WORKSPACE}/accounting/ledger/{debit.uuid}")

        assert response.status_code == 200
        body = response.json()

        assert body["credits"] == "-3.6000"
        assert body["transaction_type"] == "debit"
        assert body["pricing"] == {
            "sku": SKU,
            "quantity": 3600.0,
            "credits_per_unit": "0.001",
            "category": "standard",
            "multiplier": "1",
            "policy_version": 1,
            "charge": "3.6000",
        }

    def test_the_explanation_reproduces_the_charge_that_was_stored(
        self,
        client: TestClient,
        db_session: Session,
        db_session_factory: sessionmaker[Session],
        policy: PricingPolicy,
    ) -> None:
        """The check on the whole scheme: the recomputed product must equal the magnitude of
        what the ledger recorded. If these ever diverge, a stored charge no longer follows
        from the policy said to have produced it."""
        WorkspaceCategory.assign(db_session, WORKSPACE, "academic")
        db_session.commit()

        AccountingIngesterMessager(session_factory=db_session_factory).consume(
            bemsg_to_pulsar_msg(a_usage_message(quantity=1234.5))
        )
        (debit,) = debits(db_session)

        body = client.get(f"/workspaces/{WORKSPACE}/accounting/ledger/{debit.uuid}").json()

        assert Decimal(body["pricing"]["charge"]) == -Decimal(body["credits"])
        assert body["pricing"]["category"] == "academic"
        assert body["pricing"]["multiplier"] == "0.5"

    def test_an_old_charge_still_explains_under_its_own_policy(
        self,
        client: TestClient,
        db_session: Session,
        db_session_factory: sessionmaker[Session],
        policy: PricingPolicy,
        item: BillingItem,
    ) -> None:
        """The point of pinning the policy version on every row (D8).

        A charge is written, then the rates are recalibrated. The explanation must quote the
        rate that priced it, not the rate in force when the question is asked.
        """
        AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(a_usage_message()))
        (debit,) = debits(db_session)

        recalibrated = PricingPolicy(
            version=2,
            valid_from=JANUARY,
            valid_until=None,
            corrects_id=None,
            default_category="standard",
            reason="ten times dearer",
        )
        recalibrated.rates = [
            PricingPolicyRate(item_id=item.uuid, credits_per_unit=Decimal("0.01"))  # pyright: ignore[reportCallIssue]
        ]
        recalibrated.category_multipliers = [
            PricingPolicyCategoryMultiplier(  # pyright: ignore[reportCallIssue]
                category="standard", multiplier=Decimal(1)
            )
        ]
        db_session.add(recalibrated)
        db_session.commit()

        body = client.get(f"/workspaces/{WORKSPACE}/accounting/ledger/{debit.uuid}").json()

        assert body["pricing"]["policy_version"] == 1
        assert body["pricing"]["credits_per_unit"] == "0.001"
        assert body["pricing"]["charge"] == "3.6000"

    def test_a_grant_has_no_pricing_to_explain(
        self, client: TestClient, db_session: Session, policy: PricingPolicy
    ) -> None:
        transaction = CreditLedgerTransaction.record_grant(
            db_session, WORKSPACE, Decimal(100), reason="opening balance"
        )
        db_session.commit()

        body = client.get(f"/workspaces/{WORKSPACE}/accounting/ledger/{transaction.uuid}").json()

        assert body["pricing"] is None
        assert body["transaction_type"] == "grant"
        assert body["credits"] == "100"
        assert body["reason"] == "opening balance"

    def test_a_transaction_in_another_workspace_is_not_found(
        self, client: TestClient, db_session: Session, policy: PricingPolicy
    ) -> None:
        """A transaction UUID is not a capability. 404 rather than 403, so the response does
        not confirm that the ID exists."""
        transaction = CreditLedgerTransaction.record_grant(
            db_session, "somebody-elses-workspace", Decimal(100), reason="test"
        )
        db_session.commit()

        response = client.get(f"/workspaces/{WORKSPACE}/accounting/ledger/{transaction.uuid}")

        assert response.status_code == 404

    def test_an_unknown_transaction_is_not_found(self, client: TestClient) -> None:
        response = client.get(f"/workspaces/{WORKSPACE}/accounting/ledger/{uuid4()}")

        assert response.status_code == 404


class TestTheBalanceEndpoint:
    """T10, over HTTP."""

    def test_it_reports_the_balance(
        self,
        client: TestClient,
        db_session: Session,
        db_session_factory: sessionmaker[Session],
        policy: PricingPolicy,
    ) -> None:
        CreditLedgerTransaction.record_grant(db_session, WORKSPACE, Decimal(100), reason="test")
        db_session.commit()

        AccountingIngesterMessager(session_factory=db_session_factory).consume(bemsg_to_pulsar_msg(a_usage_message()))

        response = client.get(f"/workspaces/{WORKSPACE}/accounting/balance")

        assert response.status_code == 200
        assert response.json()["balance"] == "96.4000"
        assert response.json()["workspace"] == WORKSPACE

    def test_an_untouched_workspace_reports_zero(self, client: TestClient) -> None:
        response = client.get(f"/workspaces/{WORKSPACE}/accounting/balance")

        assert response.status_code == 200
        assert response.json()["balance"] == "0"

    def test_the_balance_is_a_string_and_not_a_json_number(self, client: TestClient, db_session: Session) -> None:
        """Credits are exact decimals. A JSON number loses that on precisely the values where
        it matters, which is why `ExactDecimal` exists."""
        CreditLedgerTransaction.record_grant(db_session, WORKSPACE, Decimal("0.000000412"), reason="test")
        db_session.commit()

        assert client.get(f"/workspaces/{WORKSPACE}/accounting/balance").json()["balance"] == "0.000000412"
