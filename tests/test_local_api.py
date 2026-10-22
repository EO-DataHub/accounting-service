"""The local entrypoint takes claims from the bearer token unchecked (T24)."""

import os
import subprocess
import sys
import types
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jwt
import pytest
from fastapi import HTTPException

from dev.local_token import local_claims, local_token

REPO_ROOT = Path(__file__).parent.parent


def _credentials(token: str) -> types.SimpleNamespace:
    """Stands in for HTTPAuthorizationCredentials; local_claims only reads .credentials."""
    return types.SimpleNamespace(credentials=token)


def test_a_minted_token_carries_the_claims_authz_reads() -> None:
    token = local_token(hub_admin=True, owner=("ws-owned",), member=("ws1", "ws2"))

    claims = local_claims(_credentials(token))

    assert claims["realm_access"] == {"roles": ["hub_admin"]}
    assert claims["workspaces-owned"] == ["ws-owned"]
    assert claims["workspaces"] == ["ws1", "ws2"]


def test_signature_expiry_audience_and_issuer_are_not_checked() -> None:
    """What the README promises for `make run`: any well-formed token is taken at its word."""
    token = jwt.encode(
        {"sub": "anyone", "exp": datetime.now(tz=UTC) - timedelta(days=1), "iss": "https://elsewhere.example"},
        "not-keycloaks-key-and-long-enough-for-hs256",
        algorithm="HS256",
    )

    assert local_claims(_credentials(token))["sub"] == "anyone"


def test_a_malformed_token_is_a_401() -> None:
    with pytest.raises(HTTPException) as raised:
        local_claims(_credentials("not-a-jwt"))

    assert raised.value.status_code == 401


def test_the_local_api_starts_without_a_keycloak_url() -> None:
    """In a subprocess, because importing dev.local_api replaces the shared app's lifespan
    and decoder for the rest of the session.
    """
    script = "from fastapi.testclient import TestClient\nfrom dev.local_api import app\nwith TestClient(app): pass\n"
    env = {**os.environ, "KEYCLOAK_BASE_URL": ""}

    result = subprocess.run([sys.executable, "-c", script], cwd=REPO_ROOT, env=env, capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
