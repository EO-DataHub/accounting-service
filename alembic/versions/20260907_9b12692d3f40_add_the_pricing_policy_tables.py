"""add the pricing policy tables

`pricing_policy`, `pricing_policy_rate` and `pricing_policy_category_multiplier` for T3.

The unique constraints on `(policy_id, item_id)` and `(policy_id, category)` are what make a
policy well formed: a second rate for the same SKU would make a charge depend on which row a
query returned first. The convention names a unique constraint after its first column only, so
both read as `uq_..._policy_id`.

The downgrade is real: these tables are new and empty, so dropping them loses nothing.

Revision ID: 9b12692d3f40
Revises: 9b4e2c81a7d3
Create Date: 2026-09-07
"""

from collections.abc import Sequence

import sqlalchemy as sa

# SQLModel maps str to AutoString, which autogenerate writes into revisions without importing.
import sqlmodel.sql.sqltypes

from alembic import op

revision: str = "9b12692d3f40"
down_revision: str | None = "9b4e2c81a7d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pricing_policy",
        sa.Column("uuid", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("valid_from", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("valid_until", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("configured_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("corrects_id", sa.Uuid(), nullable=True),
        sa.Column("credit_to_currency_rate", sa.Numeric(), nullable=False),
        sa.Column("default_category", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("reason", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.CheckConstraint(
            "valid_until IS NULL OR valid_from <= valid_until", name=op.f("ck_pricing_policy_validity_order")
        ),
        sa.ForeignKeyConstraint(
            ["corrects_id"], ["pricing_policy.uuid"], name=op.f("fk_pricing_policy_corrects_id_pricing_policy")
        ),
        sa.PrimaryKeyConstraint("uuid", name=op.f("pk_pricing_policy")),
        sa.UniqueConstraint("version", name=op.f("uq_pricing_policy_version")),
    )
    op.create_table(
        "pricing_policy_category_multiplier",
        sa.Column("uuid", sa.Uuid(), nullable=False),
        sa.Column("policy_id", sa.Uuid(), nullable=False),
        sa.Column("category", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("multiplier", sa.Numeric(), nullable=False),
        sa.ForeignKeyConstraint(
            ["policy_id"],
            ["pricing_policy.uuid"],
            name=op.f("fk_pricing_policy_category_multiplier_policy_id_pricing_policy"),
        ),
        sa.PrimaryKeyConstraint("uuid", name=op.f("pk_pricing_policy_category_multiplier")),
        sa.UniqueConstraint("policy_id", "category", name=op.f("uq_pricing_policy_category_multiplier_policy_id")),
    )
    op.create_table(
        "pricing_policy_rate",
        sa.Column("uuid", sa.Uuid(), nullable=False),
        sa.Column("policy_id", sa.Uuid(), nullable=False),
        sa.Column("item_id", sa.Uuid(), nullable=False),
        sa.Column("credits_per_unit", sa.Numeric(), nullable=False),
        sa.ForeignKeyConstraint(
            ["item_id"], ["billing_item.uuid"], name=op.f("fk_pricing_policy_rate_item_id_billing_item")
        ),
        sa.ForeignKeyConstraint(
            ["policy_id"], ["pricing_policy.uuid"], name=op.f("fk_pricing_policy_rate_policy_id_pricing_policy")
        ),
        sa.PrimaryKeyConstraint("uuid", name=op.f("pk_pricing_policy_rate")),
        sa.UniqueConstraint("policy_id", "item_id", name=op.f("uq_pricing_policy_rate_policy_id")),
    )


def downgrade() -> None:
    op.drop_table("pricing_policy_rate")
    op.drop_table("pricing_policy_category_multiplier")
    op.drop_table("pricing_policy")
