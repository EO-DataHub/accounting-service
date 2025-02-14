import logging
from typing import Sequence
from uuid import UUID

from eodhp_utils.messagers import Messager, PulsarJSONMessager
from eodhp_utils.pulsar import messages
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session

from accounting_service import db, models


class AccountingIngesterMessager(PulsarJSONMessager[messages.BillingEvent]):
    """
    This Messager receives Pulsar messages containing billing events and updates the
    accounting DB.
    """

    def is_temporary_error(self, e: Exception):
        if isinstance(e, OperationalError):
            return True

        return False

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

        logging.debug("Recorded BillingEvent with uuid %s", str(uuid))

        return []

    def _try_record_event(self, bemsg: messages.BillingEvent) -> UUID:
        with Session(db.engine) as session:
            uuid = models.BillingEvent.insert_from_message(session, bemsg)
            session.commit()

        return uuid

    def _add_observed_sku(self, bemsg: messages.BillingEvent):
        with Session(db.engine) as session:
            session.add(models.BillingItem(sku=bemsg.sku, name="", unit=""))
            session.commit()
