import logging
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from uuid import UUID

from eodhp_utils.messagers import Messager, PulsarJSONMessager
from eodhp_utils.pulsar import messages
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from accounting_service import db, models
from accounting_service.pricing import UnratedSKUError, price_usage


class DBIngester:
    """
    Shared database handling for the ingester messagers.

    `session_factory` exists so a caller can supply the sessions this ingester opens. The
    ingester is not built by FastAPI, so it has no dependency injection of its own, and
    without this seam a test can only redirect it by patching a module global. Left unset,
    it uses the process-wide factory.
    """

    def __init__(self, session_factory: sessionmaker[Session] | None = None) -> None:
        # Messager's own arguments (s3_client, output_bucket, producer) all default and none
        # of these messagers use them, so there is nothing to forward. Add a parameter here
        # if that changes.
        super().__init__()
        self._session_factory = session_factory

    def _session(self) -> Session:
        # Resolved per call rather than in __init__, so constructing a messager needs no
        # database configuration.
        factory = self._session_factory or db.get_sessionmaker()
        return factory()

    def is_temporary_error(self, e: Exception) -> bool:
        if isinstance(e, OperationalError):
            return True

        return False

    def _add_observed_sku(self, msg: messages.BillingEvent | messages.BillingResourceConsumptionRateSample) -> None:
        with self._session() as session:
            models.BillingItem.ensure_sku_exists(session, str(msg.sku))
            session.commit()


def truncate_to_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


class AccountingIngesterMessager(DBIngester, PulsarJSONMessager[messages.BillingEvent, bytes]):
    """
    This Messager receives Pulsar messages containing billing events and updates the
    accounting DB.
    """

    def process_payload(self, obj: messages.BillingEvent) -> Sequence[Messager.Action]:
        try:
            uuid_ = self._try_record_event(obj)
        except IntegrityError:
            # This is /probably/ because the SKU in the message is unknown.
            #
            # To avoid the risk of data loss if we forget to configured an item in advance, we
            # create an empty item. This can be corrected later by an admin.
            logging.exception(
                "IntegrityError recording BillingEvent with sku %s - assuming missing BillingItem",
                obj.sku,
            )

            self._add_observed_sku(obj)
            uuid_ = self._try_record_event(obj)

        if uuid_:
            logging.debug("Recorded BillingEvent with uuid %s", str(uuid_))
        else:
            logging.info("Received duplicate BillingEvent uuid %s", obj.uuid)

        return []

    def _try_record_event(self, bemsg: messages.BillingEvent) -> UUID | None:
        """Record the event and charge for it, in one transaction.

        One transaction deliberately: an event and its debit land together or neither does.
        The alternative leaves usage recorded but uncharged after a crash between the two,
        which is a gap nothing would notice - the event reads back fine and the balance is
        quietly wrong.
        """
        with self._session() as session:
            uuid_ = models.BillingEvent.insert_from_message(session, bemsg)

            if uuid_ is not None:
                self._charge_event(session, uuid_, str(bemsg.sku))

            session.commit()

        return uuid_

    def _charge_event(self, session: Session, event_id: UUID, sku: str) -> None:
        """Price the event and write the debit (T9).

        Three things stop a charge being written, and all three record the event anyway and
        say so at error level rather than failing the message. Usage data is the thing that
        cannot be recovered if it is dropped - a charge can always be applied later from the
        stored quantity, which is what the re-pricing runner does (T18) - and none of the
        three is fixed by redelivering the message, so raising would wedge the consumer on a
        message it can never process.

        This is the same bargain the unknown-SKU path above already makes: record it, and make
        the omission loud enough to alert on.
        """
        event = session.get(models.BillingEvent, event_id)

        if event is None:
            # Inserted in this transaction two lines ago, so this cannot happen. Asserting it
            # rather than letting the None flow onwards, which would fail somewhere less
            # obvious.
            raise AssertionError(f"billing event {event_id} vanished within its own transaction")

        # Resolved per event rather than cached. The policy is the same for nearly every
        # message, so a cache would pay off, but it would also price under a stale policy for
        # its lifetime whenever another replica mints a new one - and pricing under the wrong
        # policy is the one error this design exists to prevent. Revisit with a cache keyed on
        # something that invalidates, not on a timeout.
        policy = models.PricingPolicy.resolve(session, event.event_start_utc)

        if policy is None:
            logging.error(
                "No pricing policy applies to BillingEvent %s at %s - recorded but not charged",
                event_id,
                event.event_start_utc.isoformat(),
            )
            return

        # None where the workspace has no assignment, which the rate card resolves to the
        # policy's default category (D6). Every workspace is in that state until the workspace
        # service starts sending the field (T6).
        category = models.WorkspaceCategory.category_for(session, event.workspace)

        try:
            priced = price_usage(policy.rate_card(), sku=sku, quantity=event.quantity, category=category)
        except UnratedSKUError:
            logging.error(
                "Pricing policy version %s holds no rate for SKU %s - BillingEvent %s recorded but not charged",
                policy.version,
                sku,
                event_id,
            )
            return
        except ValueError:
            # A quantity that is negative or not finite. A producer fault rather than a
            # transient one, so it is logged with the value that caused it.
            logging.exception(
                "Cannot price quantity %r of %s - BillingEvent %s recorded but not charged",
                event.quantity,
                sku,
                event_id,
            )
            return

        debit = models.CreditLedgerTransaction.record_usage_debit(session, event, priced, policy.uuid)

        if debit is None:
            # The partial unique index refused it, so this event already carries an original
            # debit. Reachable for an event stored before this code shipped, or one whose
            # debit was written by a concurrent consumer.
            logging.info("BillingEvent %s is already charged", event_id)
        else:
            logging.debug(
                "Charged %s credits to %s for BillingEvent %s under policy version %s, category %s",
                priced.credits,
                event.workspace,
                event_id,
                policy.version,
                priced.category,
            )


class WorkspaceSettingsIngesterMessager(DBIngester, PulsarJSONMessager[messages.WorkspaceSettings, bytes]):
    def process_payload(self, obj: messages.WorkspaceSettings) -> Sequence[Messager.Action]:
        with self._session() as session:
            recorded = models.WorkspaceAccount.record_mapping(session, UUID(str(obj.account)), str(obj.name))
            session.commit()

        if recorded:
            logging.info("Associated workspace %s with account %s", obj.name, obj.account)
        else:
            logging.debug("Ignoring WorkspaceSettings for %s, already known", obj.name)

        return []


class ConsumptionSampleRateIngesterMessager(
    DBIngester, PulsarJSONMessager[messages.BillingResourceConsumptionRateSample, bytes]
):
    """
    This Messager receives Pulsar messages containing consumption rate samples and adds them to
    the accounting DB. It also converts them to estimated BillingEvents periodically.
    """

    def process_payload(self, obj: messages.BillingResourceConsumptionRateSample) -> Sequence[Messager.Action]:
        self._record_event(obj)

        # We must convert previously recorded consumption rate data into billing events.
        # We do this in one hour windows.
        #
        # To do this accurately, we need complete consumption rate data extending at least one
        # sample beyoned the end of the window, so when we receive a message we generate up
        # to the start of the hour containing its timestamp.
        #
        # If a resource is deleted then part of the last hour of use may be uncharged.
        # To prevent this the relevant collector should listen for resource deletion events
        # from Pulsar and generate zero rate messages at that timepoint and an hour later.
        # None do this at present, but deletion is currently rare.
        msg_datetime = datetime.fromisoformat(str(obj.sample_time)).astimezone(UTC)
        generate_upto = truncate_to_hour(msg_datetime)
        self._generate_new_estimates(str(obj.workspace), str(obj.sku), generate_upto)

        return []

    def _record_event(self, msg: messages.BillingResourceConsumptionRateSample) -> None:
        try:
            uuid_ = self._try_record_event(msg)
        except IntegrityError:
            logging.exception(
                "IntegrityError recording %s with sku %s - assuming missing BillingItem",
                type(msg),
                msg.sku,
            )

            self._add_observed_sku(msg)
            uuid_ = self._try_record_event(msg)

        if uuid_:
            logging.debug("Recorded %s with uuid %s", type(msg), str(uuid_))
        else:
            logging.info("Received duplicate %s uuid %s", type(msg), msg.uuid)

    def _try_record_event(self, msg: messages.BillingResourceConsumptionRateSample) -> UUID | None:
        with self._session() as session:
            uuid_ = models.BillableResourceConsumptionRateSample.insert_from_message(session, msg)
            session.commit()

        return uuid_

    def _generate_new_estimates(self, workspace: str, sku: str, upto: datetime) -> None:
        """
        This generates BillingEvents with estimated resource consumption for one hour windows, each
        starting on the hour. The first will begin at the end time of the last generated
        BillingItem for this SKU and workspace if any exists, otherwise it will begin at the start
        of the hour in which the first observed consumption rate sample was taken.

        The last will end at the start of the clock hour containing `upto`.
        """
        logging.debug(
            "Generating BillingEvent estimates for workspace %s and sku %s up to %s",
            workspace,
            sku,
            upto,
        )

        with self._session() as session:
            item = models.BillingItem.find_billing_item(session, sku=sku)
            assert item is not None  # _record_event would have failed without it

            last_estimate = models.BillingEvent.find_latest_billing_event(session, workspace, sku)
            logging.debug(
                "Last estimated billing event for workspace %s and sku %s was %s",
                workspace,
                sku,
                last_estimate,
            )
            if last_estimate:
                # Continue estimating from after the last estimate.
                generate_from = last_estimate.event_end_utc
            else:
                # No prior estimates - estimate starting from when we first had consumption rate
                # data.
                earliest_sample = models.BillableResourceConsumptionRateSample.find_earliest(
                    session, workspace, item.uuid
                )
                assert earliest_sample is not None

                generate_from = truncate_to_hour(earliest_sample.sample_time_utc)

            generate_to = truncate_to_hour(generate_from + timedelta(hours=1))

            while generate_to <= upto:
                logging.debug(
                    "Generating BillingEvent estimates for workspace %s and sku %s for window %s to %s",
                    workspace,
                    sku,
                    generate_from,
                    generate_to,
                )
                consumption = models.BillableResourceConsumptionRateSample.calculate_consumption_for_interval(
                    session,
                    workspace,
                    sku,
                    generate_from,
                    generate_to,
                )

                session.add(
                    # item_id is set from the `item` relationship at flush. SQLModel's
                    # generated __init__ knows nothing about relationships, so pyright reads
                    # this as a missing argument.
                    models.BillingEvent(  # pyright: ignore[reportCallIssue]
                        uuid=uuid.uuid5(
                            uuid.UUID("67f9a35c-567c-4a30-b51d-2fc64328bd55"),
                            f"{workspace}-{sku}-{generate_from.isoformat()}",
                        ),
                        event_start=generate_from,
                        event_end=generate_to,
                        item=item,
                        user=None,
                        workspace=workspace,
                        quantity=consumption or 0,
                    )
                )

                generate_from = generate_to
                generate_to = truncate_to_hour(generate_from + timedelta(hours=1))

            session.commit()
