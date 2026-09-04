"""Turning the timestamps this service handles into UTC.

Separate from models.py because neither function needs the ORM, and because putting them
there made anything that wanted them depend on the whole SQLAlchemy layer. consumption.py
needs them and must not import models.py, which imports it.
"""

from datetime import UTC, datetime


def as_utc(dt: datetime) -> datetime:
    """Return `dt` in UTC, treating a naive value as already being UTC.

    PostgreSQL returns an aware datetime in the connection's timezone, which is not
    necessarily UTC, so the conversion is real. A naive value reaches here when an object was
    built in Python and read back before any round trip.

    Do not use astimezone on its own. Given a naive datetime it assumes local time, so on a
    machine which is not on UTC it shifts the value by the local offset. That made the
    consumption-sample tests fail during British Summer Time and pass in winter.
    """
    return (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).astimezone(UTC)


def datetime_default_to_utc(dt: datetime | None) -> datetime | None:
    """Label a naive datetime as UTC without moving it. Passes None and aware values through.

    Distinct from as_utc: this only fills in a missing offset, and is what an incoming value
    wants. as_utc also converts an aware value, and is what an outgoing value wants.
    """
    if dt and not dt.tzinfo:
        return dt.replace(tzinfo=UTC)

    return dt
