from datetime import datetime
from uuid import UUID


def find_billing_events(
    workspace: str = None,
    account: UUID = None,
    start: datetime = None,
    end: datetime = None,
    after: UUID = None,
    limit: int = 5_000,
):
    """
    Find and return BillingEvents
    """
    pass
