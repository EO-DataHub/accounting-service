"""The health route, over HTTP but with no database and no token."""

from fastapi.testclient import TestClient

from accounting_service.app.app import app
from accounting_service.db import get_session


def test_health_answers_without_a_token() -> None:
    response = TestClient(app).get("/accounting/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_health_is_not_cached() -> None:
    response = TestClient(app).get("/accounting/health")

    assert response.headers["Cache-Control"] == "no-store"


def test_health_does_not_open_a_session() -> None:
    def refuse() -> None:
        raise AssertionError("health opened a database session")

    app.dependency_overrides[get_session] = refuse
    try:
        assert TestClient(app).get("/accounting/health").status_code == 200
    finally:
        app.dependency_overrides.pop(get_session, None)


def test_health_is_not_published() -> None:
    assert "/accounting/health" not in app.openapi()["paths"]
