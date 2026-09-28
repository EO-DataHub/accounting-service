"""add the week and quarter aggregate indexes

Two more of `_aggregate_index` in models.py, for the periods TimeAggregation gained. Written by
hand, as day and month were in the baseline: both are listed in UNCOMPARED_INDEXES in
alembic/env.py, so autogenerate neither emits them nor notices when they are missing.

The expressions must be the ones `find_billing_events` groups by, which `_aggregate_index`
builds from `TimeAggregation.interval`. They are written out here rather than imported so that
the revision keeps describing what it did if the models later change.

Not CONCURRENTLY, like every index before them. Each build holds a lock that blocks writes to
billing_event while it runs, so the ingester's inserts wait for it rather than fail.

Revision ID: 5e8a1c7d2b94
Revises: 0b4b174175ad
Create Date: 2026-09-28
"""

from collections.abc import Sequence

from alembic import op

revision: str = "5e8a1c7d2b94"
down_revision: str | None = "0b4b174175ad"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        "CREATE INDEX billingevent_week_aggregate_index ON billing_event ("
        "date_trunc('week', event_start AT TIME ZONE 'UTC'), "
        "(date_trunc('week', event_start AT TIME ZONE 'UTC') + '1 week'::interval), "
        "workspace, item_id)"
    )
    op.execute(
        "CREATE INDEX billingevent_quarter_aggregate_index ON billing_event ("
        "date_trunc('quarter', event_start AT TIME ZONE 'UTC'), "
        "(date_trunc('quarter', event_start AT TIME ZONE 'UTC') + '3 months'::interval), "
        "workspace, item_id)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX billingevent_quarter_aggregate_index")
    op.execute("DROP INDEX billingevent_week_aggregate_index")
