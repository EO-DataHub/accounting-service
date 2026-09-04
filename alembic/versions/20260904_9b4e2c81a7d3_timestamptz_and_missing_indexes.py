"""convert timestamp columns to timestamptz and reconcile indexes

Brings a database created by Base.metadata.create_all, from a models.py older than the
TIMESTAMP(timezone=True) convention, into line with what the models declare. That schema was
stamped with the baseline rather than migrated to it, so it was recorded as up to date while
five columns were still naive.

Five columns, all predating the convention:

    billing_event.event_start, billing_event.event_end
    billing_item_price.valid_from, .valid_until, .configured_at

billing_resource_consumption_rate_sample.sample_time is already correct: that table was added
after the convention, so it has never been naive.

Why the naive columns matter, beyond tidiness
---------------------------------------------

`AT TIME ZONE 'UTC'` is a type switch rather than a conversion, and it runs in opposite
directions depending on the column:

    timestamptz -> timestamp      (drops the offset, giving UTC wall time)
    timestamp   -> timestamptz    (reads the value as UTC, giving an instant)

The aggregation in find_billing_events is `date_trunc('day', event_start AT TIME ZONE 'UTC')`,
so on a naive column it computes a different type from the one the tests exercise.

It also makes the two expression indexes on billing_event impossible to create. On a naive
column the inner expression yields timestamptz, and `date_trunc(text, timestamptz)` is only
STABLE, so PostgreSQL refuses the index with "functions in index expression must be marked
IMMUTABLE". On a timestamptz column it yields timestamp, that overload is IMMUTABLE, and the
index is accepted. This is why the conversions must come before the index work below, and why
a database with these columns naive cannot have those indexes at all.

The USING clause is not optional
--------------------------------

A bare `ALTER COLUMN ... TYPE timestamptz` reads existing values in the *session* timezone,
which is what autogenerate emits:

    session TZ=UTC             naive 23:30 becomes 23:30+00:00
    session TZ=Europe/London   naive 23:30 becomes 22:30+00:00

`USING <column> AT TIME ZONE 'UTC'` states the interpretation instead of inheriting it, so the
result does not depend on how the connection happens to be configured. This assumes the stored
values are UTC, which is what the application has always written: every path goes through
datetime_default_to_utc or as_utc.

Safe to run anywhere
--------------------

Every step is conditional, so this is a no-op on a database built by the earlier revisions and
a repair on one that was stamped. It can be run twice.

Revision ID: 9b4e2c81a7d3
Revises: 7c3d5e9a1f42
Create Date: 2026-09-04

"""

import logging
from collections.abc import Sequence

import sqlalchemy as sa

# Needed because SQLModel maps str to sqlmodel.sql.sqltypes.AutoString, which autogenerate
# writes into revisions without importing.
import sqlmodel  # noqa: F401

from alembic import op

revision: str = "9b4e2c81a7d3"
down_revision: str | None = "7c3d5e9a1f42"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Columns which must be timestamptz and may still be naive.
NAIVE_CANDIDATES: list[tuple[str, str]] = [
    ("billing_event", "event_start"),
    ("billing_event", "event_end"),
    ("billing_item_price", "valid_from"),
    ("billing_item_price", "valid_until"),
    ("billing_item_price", "configured_at"),
]

# Indexes from an older models.py which nothing declares now.
STALE_INDEXES: list[str] = [
    "ix_billing_event_event_start",
    "workspace",  # on billing_event(event_start), despite the name
    "item",  # on billing_item_price(valid_from), despite the name
]

# Indexes the models declare. Written out rather than generated, because the two expression
# indexes are excluded from autogenerate by include_object in alembic/env.py and so are never
# emitted into a revision automatically.
DECLARED_INDEXES: list[tuple[str, str]] = [
    (
        "billingevent_workspace_eventstart_index",
        "CREATE INDEX IF NOT EXISTS billingevent_workspace_eventstart_index ON billing_event (workspace, event_start)",
    ),
    (
        "billingitemprice_item_validfrom_index",
        "CREATE INDEX IF NOT EXISTS billingitemprice_item_validfrom_index ON billing_item_price (item_id, valid_from)",
    ),
    (
        "billingevent_day_aggregate_index",
        "CREATE INDEX IF NOT EXISTS billingevent_day_aggregate_index ON billing_event ("
        "date_trunc('day', event_start AT TIME ZONE 'UTC'), "
        "(date_trunc('day', event_start AT TIME ZONE 'UTC') + '1 day'::interval), "
        "workspace, item_id)",
    ),
    (
        "billingevent_month_aggregate_index",
        "CREATE INDEX IF NOT EXISTS billingevent_month_aggregate_index ON billing_event ("
        "date_trunc('month', event_start AT TIME ZONE 'UTC'), "
        "(date_trunc('month', event_start AT TIME ZONE 'UTC') + '1 month'::interval), "
        "workspace, item_id)",
    ),
]

COLUMN_TYPE = sa.text(
    "SELECT data_type FROM information_schema.columns "
    "WHERE table_schema = current_schema() AND table_name = :table AND column_name = :column"
)


def upgrade() -> None:
    connection = op.get_bind()

    # First: the conversions. The expression indexes below cannot be created until these are
    # done, so the order is load-bearing rather than stylistic.
    for table, column in NAIVE_CANDIDATES:
        current = connection.execute(COLUMN_TYPE, {"table": table, "column": column}).scalar_one_or_none()

        if current is None:
            logging.warning("No %s.%s column to convert", table, column)
            continue

        if current == "timestamp with time zone":
            continue

        logging.info("Converting %s.%s from %s to timestamptz, reading values as UTC", table, column, current)
        op.execute(
            f'ALTER TABLE "{table}" ALTER COLUMN "{column}" TYPE timestamptz USING "{column}" AT TIME ZONE \'UTC\''
        )

    for index in STALE_INDEXES:
        # Quoted: two of these are named after a column and one collides with a keyword.
        op.execute(f'DROP INDEX IF EXISTS "{index}"')

    for name, statement in DECLARED_INDEXES:
        logging.debug("Ensuring index %s", name)
        op.execute(statement)


def downgrade() -> None:
    # Deliberately empty, as with 7c3d5e9a1f42.
    #
    # Converting back to a naive column would discard the offset, and would then force the two
    # expression indexes to be dropped because PostgreSQL will not keep them on a naive column.
    # The stale indexes this removes were left over from a models.py that no longer exists, so
    # recreating them would restore nothing anybody wants. Reaching the previous state means
    # restoring a backup, not running a downgrade.
    pass
