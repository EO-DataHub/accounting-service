"""Fixtures for the tests that need a real database.

These live here rather than in tests/conftest.py so that the tests above cannot reach
them. Nothing under tests/ outside this directory can request `db_session` or `client`,
which means the fast suite has no way to start a container even by accident - the same
reasoning as the old name-matching guard on the database URL, enforced by where the
fixture is defined instead of by a check at runtime.

What is left in tests/conftest.py is the token claim sets, which both halves need:
tests/test_authz.py resolves tiers from them directly, and test_api.py sends them over
HTTP.
"""

import os
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import Mock

import pytest
from eodhp_utils.pulsar import messages
from faker import Faker
from fastapi.testclient import TestClient

# noinspection PyPackageRequirements
from pulsar import Message
from sqlalchemy import Connection, event
from sqlalchemy.orm import Session, sessionmaker
from sqlmodel import SQLModel
from testcontainers.community.postgres import PostgresContainer

from accounting_service import db, db_settings
from accounting_service.app.app import app as fastapi_app
from accounting_service.app.authz import decode_jwt_token
from accounting_service.ingester.messager import (
    AccountingIngesterMessager,
    WorkspaceSettingsIngesterMessager,
)
from tests.conftest import TOKEN_HUB_ADMIN


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer]:
    """A throwaway PostgreSQL for the whole test session.

    A real PostgreSQL rather than SQLite, because models.py writes different SQL for each -
    date_trunc against datetime(), plus expression indexes SQLite cannot express - so testing
    the SQLite path proved nothing about production.

    A container rather than a shared instance, because the suite creates the schema and must
    not be able to reach anything real. That replaces the earlier name-matching guard: the
    tests cannot destroy a real database because they never learn how to reach one.
    """
    with PostgresContainer("postgres:17", driver="psycopg") as container:
        os.environ["SQL_DRIVER"] = "postgresql+psycopg"
        os.environ["SQL_HOST"] = container.get_container_host_ip()
        os.environ["SQL_PORT"] = str(container.get_exposed_port(5432))
        os.environ["SQL_USER"] = container.username
        os.environ["SQL_PASSWORD"] = container.password
        os.environ["SQL_DATABASE"] = container.dbname
        os.environ["SQL_SCHEMA"] = "public"

        # Nothing has read the settings or built the engine yet, because both are cached
        # functions rather than module-level values. This is where the container's address
        # takes effect.
        db_settings.get_settings.cache_clear()
        db.get_engine.cache_clear()
        db.get_sessionmaker.cache_clear()

        yield container


@pytest.fixture(scope="session")
def db_schema(postgres_container: PostgresContainer) -> None:
    """Create the schema once. The container starts empty, so nothing is dropped."""
    with db.get_engine().begin() as conn:
        SQLModel.metadata.create_all(conn)


@pytest.fixture
def db_connection(db_schema: None) -> Iterator[Connection]:
    """A connection with an open transaction that is rolled back when the test ends.

    This is SQLAlchemy's documented recipe for test suites. Everything a test does - through
    the fixture session, through the API, or through the ingester - happens inside this one
    transaction, and rolling it back returns the database to an empty schema.

    It is why tests no longer begin by deleting rows left behind by their predecessors.
    """
    connection = db.get_engine().connect()
    transaction = connection.begin()

    try:
        yield connection
    finally:
        transaction.rollback()
        connection.close()


@pytest.fixture
def db_session_factory(db_connection: Connection) -> sessionmaker[Session]:
    """A session factory whose sessions join the test's transaction.

    Pass this to DBIngester. Without it the ingester opens sessions on the process-wide
    engine, outside the test's transaction, and its commits survive the test.
    """
    return sessionmaker(bind=db_connection, join_transaction_mode="create_savepoint")


@pytest.fixture
def db_session(db_session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """A session joined to the test's transaction.

    join_transaction_mode="create_savepoint" turns the session's own commits into savepoint
    releases, so code under test can call commit() freely and the outer rollback still
    discards everything. A plain session.rollback() in teardown could not do that, which is
    why state used to leak between tests.
    """
    with db_session_factory() as session:
        yield session


def fake_event_known_times() -> tuple[messages.BillingEvent, datetime, datetime]:
    faker = Faker()

    ############# Setup
    bemsg: messages.BillingEvent = messages.BillingEvent.get_fake()

    start = faker.past_datetime("-30d", tzinfo=UTC)
    end = start + faker.time_delta("+10m")
    bemsg.event_start = start.isoformat()
    bemsg.event_end = end.isoformat()

    return bemsg, start, end


def msg_to_pulsar_msg(klass: type, inmsg: object) -> Message:
    # noinspection unresolved-references
    schema = klass.get_schema()

    testmsg = Mock()
    testmsg.data = Mock(return_value=schema.encode(inmsg))
    # noinspection protected-member
    msg = Message._wrap(testmsg)
    msg._schema = schema

    return msg


def bemsg_to_pulsar_msg(bemsg: messages.BillingEvent) -> Message:
    return msg_to_pulsar_msg(AccountingIngesterMessager, bemsg)


def wsmsg_to_pulsar_msg(bemsg: messages.WorkspaceSettings) -> Message:
    return msg_to_pulsar_msg(WorkspaceSettingsIngesterMessager, bemsg)


@pytest.fixture
def client(db_session: Session) -> Iterator[TestClient]:
    """A FastAPI test HTTP client, authenticated as a hub_admin.

    hub_admin is the default because it short-circuits every tier check, which is
    what the tests predating the tier work assume. Use the `authenticate_as`
    fixture to swap in a different token.

    The overrides are cleared on teardown. They live on the application object,
    which is shared by every test, so a token left installed here would leak into
    whichever test ran next.
    """

    def override_get_db() -> Iterator[Session]:
        try:
            yield db_session
        finally:
            pass

    fastapi_app.dependency_overrides[db.get_session] = override_get_db
    fastapi_app.dependency_overrides[decode_jwt_token] = lambda: TOKEN_HUB_ADMIN

    yield TestClient(fastapi_app)

    fastapi_app.dependency_overrides.clear()


@pytest.fixture
def counting_selects() -> Callable[[], AbstractContextManager[list[str]]]:
    """Record the SELECT statements issued inside a block.

    For asserting what a read path costs. A query per row is invisible to every other kind of
    test: the response is correct, the suite is green, and the cost only shows up under load.

        with counting_selects() as statements:
            client.get(...)
        assert len(statements) == 2
    """

    @contextmanager
    def _counting() -> Iterator[list[str]]:
        recorded: list[str] = []

        def before(
            _conn: object,
            _cursor: object,
            statement: str,
            _params: object,
            _context: object,
            _executemany: object,
        ) -> None:
            if statement.lstrip().upper().startswith("SELECT"):
                recorded.append(statement)

        engine = db.get_engine()
        event.listen(engine, "before_cursor_execute", before)
        try:
            yield recorded
        finally:
            event.remove(engine, "before_cursor_execute", before)

    return _counting


@pytest.fixture
def authenticate_as(client: TestClient) -> Callable[[dict[str, Any]], None]:
    """Replace the claims the API sees for the rest of the test.

    Overrides are consulted per request, so this can be called after the client
    exists and as many times as a test needs.

    This bypasses `decode_jwt_token` itself. A test covering a missing or
    malformed Authorization header must leave the override alone and let the real
    function run.
    """

    def _authenticate_as(claims: dict[str, Any]) -> None:
        fastapi_app.dependency_overrides[decode_jwt_token] = lambda: claims

    return _authenticate_as
