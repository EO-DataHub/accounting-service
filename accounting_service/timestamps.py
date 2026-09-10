"""Turning the timestamps this service handles into UTC."""

from datetime import UTC, datetime


def as_utc(dt: datetime) -> datetime:
    """Return `dt` in UTC, treating a naive value as already being UTC.

    PostgreSQL returns an aware datetime in the connection's timezone, which is not
    necessarily UTC.

    Do not use astimezone on its own: given a naive datetime it assumes local time, so on a
    machine which is not on UTC it shifts the value by the local offset.
    """

    return (dt if dt.tzinfo else dt.replace(tzinfo=UTC)).astimezone(UTC)


def datetime_default_to_utc(dt: datetime | None) -> datetime | None:
    """Label a naive datetime as UTC without moving it. Passes None and aware values through.

    Distinct from as_utc, which also converts an aware value. This is what an incoming value
    wants; as_utc is what an outgoing value wants.
    """
    if dt and not dt.tzinfo:
        return dt.replace(tzinfo=UTC)

    return dt
