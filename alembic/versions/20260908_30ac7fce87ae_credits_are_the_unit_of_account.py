"""credits are the unit of account

Removes the last of the fiat pricing: the `billing_item_price` table, its index, and
`pricing_policy.credit_to_currency_rate`.

Buying credits is out of scope for this service. A user asks a hub admin, who grants credits
(T15), so nothing here converts money into credits or back. `pricing_policy_rate` already
holds what every SKU costs in credits, which is the number the ledger charges, and
`GET /accounting/prices` now serves that instead of a price in pounds.

The exchange rate went with it. It was introduced as a reporting and calibration value under
D2, and it never acquired a reader: the scoping doc's own list of gap findings recorded that
"the exchange rate had no consumer" and answered it by nominating T11, which now serves
credits. Calibrating a credit rate against real AWS costs is arithmetic done by whoever
writes the configuration document; the document records the outcome, and the rate used to
reach it does not need storing. Whatever invoicing arrives later brings its own rates, VAT
and discounts, and its own versioning, so this service should not assert a money value it has
no authority over.

Data loss is intended
---------------------

`billing_item_price` rows are discarded. There are no consumers of the data, and the prices
it held are superseded by policy rates covering the same SKUs.

The downgrade is deliberately empty, as with 7c3d5e9a1f42 and 9b4e2c81a7d3. Recreating the
table is easy and pointless: it would come back empty, so the previous code would serve no
prices at all while reporting success. Autogenerate also offered to restore
`credit_to_currency_rate` as NOT NULL with no default, which fails outright on a
`pricing_policy` table that has any rows. Reaching the previous state means restoring a
backup, not running a downgrade.

Revision ID: 30ac7fce87ae
Revises: 9b12692d3f40
Create Date: 2026-09-08

"""

from collections.abc import Sequence

# Needed because SQLModel maps str to sqlmodel.sql.sqltypes.AutoString, which autogenerate
# writes into revisions without importing. The submodule is imported explicitly, not just
# `import sqlmodel`: sqlmodel/__init__.py does not re-export `sql`, so a type checker reports
# `"sql" is not a known attribute of module "sqlmodel"` on every column autogenerate emits.
# Unused in a revision that touches no str column; ruff is told to leave it alone rather than
# every revision needing a decision about it.
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
