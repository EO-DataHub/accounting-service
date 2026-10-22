"""The API refuses to start when it cannot verify tokens (T24).

Lifespan runs only when TestClient is entered as a context manager, which the route tests
in tests/integration do not do, so this is the one place it is exercised.
"""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from accounting_service.app import authz
from accounting_service.app.app import app
from accounting_service.settings import Settings


@pytest.fixture(autouse=True)
def no_overrides() -> Iterator[None]:
    """Start from the real dependencies, whatever another test module installed."""
    saved = dict(app.dependency_overrides)
    app.dependency_overrides.clear()
    yield
    app.dependency_overrides.clear()
    app.dependency_overrides.update(saved)


def _configure(monkeypatch: pytest.MonkeyPatch, keycloak_base_url: str | None) -> None:
    settings = Settings.model_construct(KEYCLOAK_BASE_URL=keycloak_base_url)
    monkeypatch.setattr(authz, "get_settings", lambda: settings)


@pytest.fixture(autouse=True)
def fresh_verifier() -> Iterator[None]:
    """Each test starts and ends without a verifier built from another test's settings."""
    authz.verifier.cache_clear()
    yield
    authz.verifier.cache_clear()


def test_without_a_keycloak_url_the_api_does_not_start(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, None)

    with pytest.raises(RuntimeError, match="KEYCLOAK_BASE_URL"), TestClient(app):
        pass


def test_with_a_keycloak_url_the_api_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    _configure(monkeypatch, "https://eodh.example")

    with TestClient(app):
        pass


def test_overriding_the_decoder_does_not_skip_the_startup_check(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only replacing the lifespan, as dev/local_api.py does, starts the API without one."""
    _configure(monkeypatch, None)
    app.dependency_overrides[authz.decode_jwt_token] = lambda: {}

    with pytest.raises(RuntimeError, match="KEYCLOAK_BASE_URL"), TestClient(app):
        pass
