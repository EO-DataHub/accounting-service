"""rename constraints to the metadata naming convention

Databases created by `create_all` before Alembic owned the schema carry the constraint names
PostgreSQL invented (billing_event_pkey), where the naming convention expects pk_billing_event.
Alembic matches constraints by name, so until they agree autogenerate reports phantom
differences and any later revision naming a constraint fails.

Reads the current name from pg_constraint rather than assuming PostgreSQL's default, and does
nothing when the name is already right. Safe on a legacy database, on one built by the
baseline, and on a second run.

Indexes need no attention: SQLAlchemy's default is already the convention in use.

Revision ID: 7c3d5e9a1f42
Revises: 20fef2107e45
Create Date: 2026-08-20
"""

import logging
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "7c3d5e9a1f42"
down_revision: str | None = "20fef2107e45"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# (table, pg_constraint.contype, name the convention gives it), where
# p = primary key, f = foreign key, c = check. Every table below holds at most
# one constraint of each type listed, so discovery by type is unambiguous.
CONSTRAINTS: list[tuple[str, str, str]] = [
    ("workspace_account", "p", "pk_workspace_account"),
    ("billing_item", "p", "pk_billing_item"),
    ("billing_event", "p", "pk_billing_event"),
    ("billing_event", "f", "fk_billing_event_item_id_billing_item"),
    ("billing_event", "c", "ck_billing_event_start_before_end"),
    ("billing_item_price", "p", "pk_billing_item_price"),
    ("billing_item_price", "f", "fk_billing_item_price_item_id_billing_item"),
    ("billing_item_price", "c", "ck_billing_item_price_validity_order"),
    (
        "billing_resource_consumption_rate_sample",
        "p",
        "pk_billing_resource_consumption_rate_sample",
    ),
    (
        "billing_resource_consumption_rate_sample",
        "f",
        "fk_billing_resource_consumption_rate_sample_item_id_billing_item",
    ),
]

FIND_CONSTRAINT = sa.text(
    "SELECT conname FROM pg_constraint WHERE conrelid = CAST(:table AS regclass) AND contype = :contype"
)


def upgrade() -> None:
    connection = op.get_bind()

    for table, contype, target in CONSTRAINTS:
        found = list(connection.execute(FIND_CONSTRAINT, {"table": table, "contype": contype}).scalars())

        if len(found) > 1:
            # Ambiguous: this migration cannot tell which one the convention
            # means. Renaming a guess would be worse than stopping.
            raise RuntimeError(
                f"Expected at most one '{contype}' constraint on {table}, found {sorted(found)}. "
                f"Rename the one that should become {target} by hand, then re-run."
            )

        if not found:
            # Absent rather than misnamed, so there is nothing to rename. Left
            # alone: failing here would block a deployment for a problem this
            # migration cannot fix.
            logging.warning("No '%s' constraint on %s to rename to %s", contype, table, target)
            continue

        current = found[0]

        if current == target:
            continue

        logging.info("Renaming constraint %s on %s to %s", current, table, target)
        op.execute(f'ALTER TABLE "{table}" RENAME CONSTRAINT "{current}" TO "{target}"')


def downgrade() -> None:
    # Deliberately empty. The names replaced were whatever PostgreSQL invented
    # in each environment, so there is no single earlier state to restore.
    pass
