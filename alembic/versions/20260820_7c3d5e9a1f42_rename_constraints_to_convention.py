"""rename constraints to the metadata naming convention

Databases whose tables were created by Base.metadata.create_all, before Alembic
owned the schema, carry the constraint names PostgreSQL invented for them:
billing_event_pkey, billing_event_item_id_fkey, billing_event_check. The naming
convention on Base.metadata expects pk_billing_event,
fk_billing_event_item_id_billing_item and ck_billing_event_start_before_end.

Alembic matches constraints by name, so until the names agree, autogenerate
reports phantom differences and any later migration that names a constraint
fails against those databases.

This migration brings them into line. It reads the current name from
pg_constraint rather than assuming PostgreSQL's default, and does nothing when
the name is already correct, so it is safe on a legacy database, on one created
by the baseline revision, and on a second run.

Indexes need no attention: SQLAlchemy's built-in default for indexes is already
ix_%(column_0_label)s, which is the convention in use, and every other index in
this schema is named explicitly in models.py.

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


# (table, pg_constraint.contype, name the convention gives it)
#   p = primary key, f = foreign key, c = check
#
# Every table below holds at most one constraint of each type listed, which is
# what makes discovery by type unambiguous.
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
            # The constraint is absent rather than misnamed, so there is nothing
            # to rename. Left alone deliberately: failing a migration on a
            # database that differs in some other way would block a deployment
            # for a problem this migration cannot fix.
            logging.warning("No '%s' constraint on %s to rename to %s", contype, table, target)
            continue

        current = found[0]

        if current == target:
            continue

        logging.info("Renaming constraint %s on %s to %s", current, table, target)
        op.execute(f'ALTER TABLE "{table}" RENAME CONSTRAINT "{current}" TO "{target}"')


def downgrade() -> None:
    # Deliberately empty. The names this migration replaced were whatever
    # PostgreSQL happened to invent in each environment, so there is no single
    # earlier state to restore. Normalising names is treated as one-way.
    pass
