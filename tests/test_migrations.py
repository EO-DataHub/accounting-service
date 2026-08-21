"""Integration tests for the Alembic migrations.

These need a real PostgreSQL instance. The unit tests run against SQLite, which
cannot express the expression indexes the migrations create, so every test here
skips unless SQL_DRIVER names PostgreSQL. A plain `pytest` run therefore skips
the file, and you opt in by pointing the SQL_* variables at a database - the
`db` service in docker-compose.yaml will do:

    SQL_DRIVER=postgresql+psycopg SQL_HOST=localhost SQL_PORT=5433 \\
    SQL_USER=accounting SQL_PASSWORD=changeme SQL_DATABASE=accounting \\
    SQL_SCHEMA=public uv run pytest -m integrationtest

Each test migrates a throwaway database from empty, so the database named by
SQL_DATABASE is read for its connection details only and is never modified.

The point of the index test is specific. alembic/env.py excludes two expression
indexes from autogenerate comparison, because PostgreSQL normalises their
expressions and Alembic would otherwise report them as changed forever. That
exclusion also stops autogenerate emitting them, so they are written by hand in
the baseline migration. Nothing else notices if a regenerated baseline drops
them - hence this test.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import Engine, create_engine, text

from accounting_service import db_settings, models
from alembic import command

pytestmark = pytest.mark.integrationtest

ALEMBIC_INI = Path(__file__).parent.parent / "alembic.ini"

TEST_DATABASE = "accounting_migrationtest"

# Kept in step with UNCOMPARED_INDEXES in alembic/env.py.
HAND_WRITTEN_INDEXES = {
    "billingevent_day_aggregate_index",
    "billingevent_month_aggregate_index",
}

NAMED_CHECK_CONSTRAINTS = {
    "ck_billing_event_start_before_end",
    "ck_billing_item_price_validity_order",
}


@pytest.fixture
def migrated_database() -> Iterator[Engine]:
    """Create an empty database, migrate it to head, and drop it afterwards."""
    if db_settings.is_sqlite():
        pytest.skip("Migrations need PostgreSQL, but SQL_DRIVER names SQLite")

    url = db_settings.get_db_url()

    # CREATE DATABASE and DROP DATABASE cannot run inside a transaction.
    admin_engine = create_engine(url, connect_args=db_settings.connect_args, isolation_level="AUTOCOMMIT")

    def drop() -> None:
        with admin_engine.connect() as connection:
            connection.execute(text(f'DROP DATABASE IF EXISTS "{TEST_DATABASE}" WITH (FORCE)'))

    drop()

    with admin_engine.connect() as connection:
        connection.execute(text(f'CREATE DATABASE "{TEST_DATABASE}"'))

    test_engine = create_engine(url.set(database=TEST_DATABASE), connect_args=db_settings.connect_args)

    config = Config(str(ALEMBIC_INI))

    try:
        with test_engine.begin() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")

        yield test_engine
    finally:
        test_engine.dispose()
        drop()
        admin_engine.dispose()


def test_migrations_create_every_model_table(migrated_database: Engine) -> None:
    """A database migrated from empty holds every table the models declare."""
    with migrated_database.connect() as connection:
        rows = connection.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()"))
        present = {row[0] for row in rows}

    missing = set(models.Base.metadata.tables) - present

    assert not missing, f"Migrations did not create: {sorted(missing)}"


def test_hand_written_expression_indexes_survive(migrated_database: Engine) -> None:
    """The indexes excluded from autogenerate are still created by the migrations.

    If this fails, a migration was probably regenerated without re-adding the
    hand-written op.create_index calls for these indexes. See the module
    docstring, alembic/env.py, and the comment above them in models.py.
    """
    with migrated_database.connect() as connection:
        rows = connection.execute(text("SELECT indexname FROM pg_indexes WHERE tablename = 'billing_event'"))
        present = {row[0] for row in rows}

    missing = HAND_WRITTEN_INDEXES - present

    assert not missing, f"Expression indexes absent after migration: {sorted(missing)}"


def test_check_constraints_keep_their_names(migrated_database: Engine) -> None:
    """Check constraints carry the names the metadata naming convention gives them.

    Autogenerate matches check constraints by name, so an anonymous or renamed
    constraint silently stops being tracked.
    """
    with migrated_database.connect() as connection:
        rows = connection.execute(text("SELECT conname FROM pg_constraint WHERE contype = 'c'"))
        present = {row[0] for row in rows}

    missing = NAMED_CHECK_CONSTRAINTS - present

    assert not missing, f"Named check constraints absent after migration: {sorted(missing)}"
