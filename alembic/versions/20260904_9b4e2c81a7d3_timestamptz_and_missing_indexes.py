"""convert timestamp columns to timestamptz and reconcile indexes

Repairs a database created by `create_all` from a models.py older than the
TIMESTAMP(timezone=True) convention and then stamped with the baseline rather than migrated,
so it was recorded as up to date while five columns were still naive:

    billing_event.event_start, billing_event.event_end
    billing_item_price.valid_from, .valid_until, .configured_at

The conversions must come before the index work. On a naive column the expression indexes on
billing_event cannot be created at all: `date_trunc(text, timestamptz)` is only STABLE, and
PostgreSQL refuses a non-IMMUTABLE index expression.

Every step is conditional, so this is a no-op on a database built by the earlier revisions and
a repair on one that was stamped. It can be run twice.

Revision ID: 9b4e2c81a7d3
Revises: 7c3d5e9a1f42
Create Date: 2026-09-04
"""

import logging
from collections.abc import Sequence

import sqlalchemy as sa

# SQLModel maps str to AutoString, which autogenerate writes into revisions without importing.
import sqlmodel.sql.sqltypes  # noqa: F401

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

# Indexes the models declare. Written out because the two expression indexes are excluded from
# autogenerate by include_object in alembic/env.py.
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

    # The conversions come first: the expression indexes below cannot be created until they are
    # done, so the order is load-bearing.
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
    # Deliberately empty, as with 7c3d5e9a1f42. Converting back to naive would discard the
    # offset and force the expression indexes to be dropped. Reaching the previous state means
    # restoring a backup.
    pass
