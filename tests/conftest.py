from typing import Type
from unittest.mock import Mock

import pytest
from eodhp_utils.pulsar import messages
from faker import Faker
from fastapi.testclient import TestClient
from pulsar import Message
from sqlalchemy.orm import scoped_session, sessionmaker

from accounting_service import db
from accounting_service.app import app
from accounting_service.ingester.messager import (
    AccountingIngesterMessager,
    WorkspaceSettingsIngesterMessager,
)


@pytest.fixture(scope="session")
def db_session_factory():
    """returns a SQLAlchemy scoped session factory"""
    db.drop_tables()
    db.create_db_and_tables()
    return scoped_session(sessionmaker(bind=db.engine))


@pytest.fixture(scope="function")
def db_session(db_session_factory):
    """yields a SQLAlchemy connection which is rollbacked after the test"""
    session_ = db_session_factory()

    yield session_

    session_.rollback()
    session_.close()


def fake_event_known_times():
    faker = Faker()

    ############# Setup
    bemsg: messages.BillingEvent = messages.BillingEvent.get_fake()

    start = faker.past_datetime("-30d")
    end = start + faker.time_delta("+10m")
    bemsg.event_start = start.isoformat()
    bemsg.event_end = end.isoformat()

    return bemsg, start, end


def msg_to_pulsar_msg(klass: Type, inmsg):
    schema = klass.get_schema()

    testmsg = Mock()
    testmsg.data = Mock(return_value=schema.encode(inmsg))
    msg = Message._wrap(testmsg)
    msg._schema = schema

    return msg


def bemsg_to_pulsar_msg(bemsg):
    return msg_to_pulsar_msg(AccountingIngesterMessager, bemsg)


def wsmsg_to_pulsar_msg(bemsg):
    return msg_to_pulsar_msg(WorkspaceSettingsIngesterMessager, bemsg)


@pytest.fixture()
def client(db_session):
    """This supplies a FastAPI test HTTP client"""

    def override_get_db():
        try:
            yield db_session
        finally:
            # db_session.close()
            pass

    app.app.dependency_overrides[db.get_session] = override_get_db

    yield TestClient(app.app)
