import logging
import uuid
from datetime import datetime, timedelta
from typing import Optional, Sequence
from uuid import UUID

from eodhp_utils.messagers import Messager, PulsarJSONMessager
from eodhp_utils.pulsar import messages
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from accounting_service import db, models


class DBIngester:
    def is_temporary_error(self, e: Exception):
        if isinstance(e, OperationalError):
            return True

        return False

    def _add_observed_sku(self, msg):
        with Session(db.engine) as session:
            session.add(models.BillingItem(sku=msg.sku, name="", unit=""))
            session.commit()


class AccountingIngesterMessager(DBIngester, PulsarJSONMessager[messages.BillingEvent, bytes]):
    """
    This Messager receives Pulsar messages containing billing events and updates the
    accounting DB.
    """

    def process_payload(self, bemsg: messages.BillingEvent) -> Sequence[Messager.Action]:
        try:
            uuid = self._try_record_event(bemsg)
        except IntegrityError:
            # This is /probably/ because the SKU in the message is unknown.
            #
            # To avoid the risk of data loss if we forget to configured an item in advance, we
            # create an empty item. This can be corrected later by an admin.
            logging.exception(
                "IntegrityError recording BillingEvent with sku %s - assuming missing BillingItem",
                bemsg.sku,
            )

            self._add_observed_sku(bemsg)
            uuid = self._try_record_event(bemsg)

        if uuid:
            logging.debug("Recorded BillingEvent with uuid %s", str(uuid))
        else:
            logging.info("Received duplicate BillingEvent uuid %s", bemsg.uuid)

        return []

    def _try_record_event(self, bemsg: messages.BillingEvent) -> Optional[UUID]:
        with Session(db.engine) as session:
            uuid = models.BillingEvent.insert_from_message(session, bemsg)
            session.commit()

        return uuid


class WorkspaceSettingsIngesterMessager(
    DBIngester, PulsarJSONMessager[messages.WorkspaceSettings, bytes]
):
    def process_payload(self, wsmsg: messages.WorkspaceSettings) -> Sequence[Messager.Action]:
        with Session(db.engine) as session:
            recorded = models.WorkspaceAccount.record_mapping(
                session, UUID(wsmsg.account), wsmsg.name
            )
            session.commit()

        if recorded:
            logging.info("Associated workspace %s with account %s", wsmsg.name, wsmsg.account)
        else:
            logging.debug("Ignoring WorkspaceSettings for %s, already known", wsmsg.name)

        return []


class ConsumptionSampleRateIngesterMessager(
    DBIngester, PulsarJSONMessager[messages.BillingResourceConsumptionRateSample, bytes]
):
    """
    This Messager receives Pulsar messages containing consumption rate samples and adds them to
    the accounting DB. It also converts them to estimated BillingEvents periodically.
    """

    def process_payload(
        self, msg: messages.BillingResourceConsumptionRateSample
    ) -> Sequence[Messager.Action]:
        self._record_event(msg)

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
        msg_datetime = datetime.fromisoformat(msg.sample_time)
        generate_upto = msg_datetime.replace(minute=0, second=0, microsecond=0, tzinfo=None)
        self._generate_new_estimates(msg.workspace, msg.sku, generate_upto)

        return []

    def _record_event(self, msg: messages.BillingResourceConsumptionRateSample):
        try:
            uuid = self._try_record_event(msg)
        except IntegrityError:
            logging.exception(
                "IntegrityError recording %s with sku %s - assuming missing BillingItem",
                type(msg),
                msg.sku,
            )

            self._add_observed_sku(msg)
            uuid = self._try_record_event(msg)

        if uuid:
            logging.debug("Recorded %s with uuid %s", type(msg), str(uuid))
        else:
            logging.info("Received duplicate %s uuid %s", type(msg), msg.uuid)

    def _try_record_event(
        self, msg: messages.BillingResourceConsumptionRateSample
    ) -> Optional[UUID]:
        with Session(db.engine) as session:
            uuid = models.BillableResourceConsumptionRateSample.insert_from_message(session, msg)
            session.commit()

        return uuid

    @staticmethod
    def _generate_new_estimates(workspace, sku, upto):
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

        with Session(db.engine) as session:
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
                generate_from = last_estimate.event_end
            else:
                # No prior estimates - estimate starting from when we first had consumption rate
                # data.
                earliest_sample = models.BillableResourceConsumptionRateSample.find_earliest(
                    session, workspace, item.uuid
                )
                assert earliest_sample is not None

                generate_from = earliest_sample.sample_time.replace(
                    minute=0, second=0, microsecond=0
                )

            generate_to = (generate_from + timedelta(hours=1)).replace(
                minute=0, second=0, microsecond=0
            )

            while generate_to <= upto:
                logging.debug(
                    "Generating BillingEvent estimates for workspace %s and sku %s for window %s to %s",
                    workspace,
                    sku,
                    generate_from,
                    generate_to,
                )
                consumption = (
                    models.BillableResourceConsumptionRateSample.calculate_consumption_for_interval(
                        session,
                        workspace,
                        sku,
                        generate_from,
                        generate_to,
                    )
                )

                session.add(
                    models.BillingEvent(
                        uuid=uuid.uuid5(
                            uuid.UUID("67f9a35c-567c-4a30-b51d-2fc64328bd55"),
                            f"{workspace}-{sku}-{generate_from.isoformat()}",
                        ),
                        event_start=generate_from,
                        event_end=generate_to,
                        item=item,
                        user=None,
                        workspace=workspace,
                        quantity=consumption,
                    )
                )

                generate_from = generate_to
                generate_to = (generate_from + timedelta(hours=1)).replace(
                    minute=0, second=0, microsecond=0
                )

            session.commit()
