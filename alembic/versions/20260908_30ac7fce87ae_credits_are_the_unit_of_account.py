"""credits are the unit of account

Removes the last of the fiat pricing: the `billing_item_price` table, its index, and
`pricing_policy.credit_to_currency_rate`. `pricing_policy_rate` already holds what every SKU
costs in credits, which is the number the ledger charges.

Data loss is intended. `billing_item_price` rows are discarded; nothing consumes them and the
prices they held are superseded by policy rates covering the same SKUs.

The downgrade is deliberately empty, as with 7c3d5e9a1f42 and 9b4e2c81a7d3. Recreating the
table would bring it back empty, so the previous code would serve no prices while reporting
success. Reaching the previous state means restoring a backup.

Revision ID: 30ac7fce87ae
Revises: 9b12692d3f40
Create Date: 2026-09-08
"""

from collections.abc import Sequence

# SQLModel maps str to AutoString, which autogenerate writes into revisions without importing.
# The submodule is named explicitly because sqlmodel/__init__.py does not re-export `sql`.
import sqlmodel.sql.sqltypes  # noqa: F401

from alembic import op

revision: str = "30ac7fce87ae"
down_revision: str | None = "9b12692d3f40"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_index(op.f("billingitemprice_item_validfrom_index"), table_name="billing_item_price")
    op.drop_table("billing_item_price")
    op.drop_column("pricing_policy", "credit_to_currency_rate")


def downgrade() -> None:
    # Deliberately empty. See "Data loss is intended" above.
    pass
