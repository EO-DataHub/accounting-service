"""Tests for decode_jwt_token's signature verification.

test_authz.py covers workspace_authz/account_authz as pure functions over a claims dict, and
deliberately does not exercise decode_jwt_token's real path: every route test overrides it via
FastAPI's dependency_overrides (see tests/integration/conftest.py). This file is the one place
that checks the signature verification itself actually rejects a forged token, since a bug
here lets every other authorisation check in the app be bypassed with whatever claims an
attacker likes.

There is no private key for the real Keycloak instance, so most of these sign with a
throwaway RSA keypair and mock the verifier's JWKS client to hand back its public half, rather than
calling the real endpoint over the network.
"""

import types
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from fastapi import HTTPException
from jwt import PyJWKClientConnectionError

from accounting_service.app import authz
from accounting_service.settings import Settings

ISSUER = "https://eodh.example/keycloak/realms/eodhp"

PRIVATE_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)

# The actual document returned by https://eodatahub.org.uk/keycloak/realms/eodhp/protocol/
# openid-connect/certs on 2026-09-22. Keycloak publishes two keys here: one for verifying
# signatures (RS256, use=sig) and one for encryption (RSA-OAEP, use=enc). Kept verbatim so
# the shape-compatibility test below is against something real rather than a guess.
REAL_KEYCLOAK_JWKS: dict[str, object] = {
    "keys": [
        {
            "kid": "X2NoP9-FqRVXCHLFaUO7YcaPghYRQHjqWKAaofjXzE4",
            "kty": "RSA",
            "alg": "RS256",
            "use": "sig",
            "n": (
                "u_Q2kesd25lmI3BxK0FrKCqf-Dis4KPa0E5D8It57MgX3QvVN8lxTSYyCTDltny3iSTNj6G"
                "-esuNksGV0veGM6XBpqW0DO6i19_ENg7_qCYCObS6LWXhnkMnDAC3CS32dWW3-M520eOXz7"
                "pk3cPNim1hPU-Sde9-WMflmpOP6goygstN1Y_LTmU4HrMzdpsUhTCx9easPoOonGxlKjaMf"
                "tM8xdLdYLydxZEe-EEIsLsBGVB3NzBrDPvuWotCtaEHTbKojBFeCdEOKhSaL8UuhGyXVBU2"
                "NhU9z7h6xKKSbGTbTIAH3WR7vbADhY6SFp8ssHkYT8SikbCHodPUBo6xuQ"
            ),
            "e": "AQAB",
        },
        {
            "kid": "ihHUe8I6RsvhiyjXu2EzIEFraqzhrK6bFT5-vws5l9o",
            "kty": "RSA",
            "alg": "RSA-OAEP",
            "use": "enc",
            "n": (
                "xofsOwKx-jD4-iofnX5TNTs507i0VAYoYDpqIFYE0VDbe60kTPD1RN8VH1WCVLnLLRTKWoBY"
                "AwYaxIobr3oDAzh-VEqw3_sFkK2DxIqV7ReI8RKgpNR9EFARCiGa8KUpI5ClGVwSoPTTsmri4"
                "DiW1_vY2RWkcnh379fZA4AK4SOWMnDKVMtfE2AB35UGfCpz9xsr1OfLyQEis8fzANx8piHcV"
                "5FMQQujb1fBv67ybQNT3h5GtTxAMHkSTx-8UbBd1MvHkKCrSszYj5fPVECp0Xld61Smns6y0"
                "jbX7hG2YvzunIkFzISwCh7V_oQ6C5i5GQXq-nolOZMBJciE8s28Gw"
            ),
            "e": "AQAB",
        },
    ]
}


def _credentials(token: str) -> types.SimpleNamespace:
    """Stands in for HTTPAuthorizationCredentials; decode_jwt_token only reads .credentials."""
    return types.SimpleNamespace(credentials=token)


def _token(key: RSAPrivateKey, aud: str = "eodh", **claims: object) -> str:
    """A token as Keycloak would issue it. Pass a claim as None to leave it out."""
    defaults: dict[str, object] = {
        "sub": "test-user",
        "aud": aud,
        "iss": ISSUER,
        "exp": datetime.now(tz=UTC) + timedelta(minutes=5),
    }
    payload = {name: value for name, value in {**defaults, **claims}.items() if value is not None}
    return jwt.encode(payload, key, algorithm="RS256")


def _rejected(token: str) -> bool:
    with pytest.raises(HTTPException) as raised:
        authz.decode_jwt_token(_credentials(token))

    return raised.value.status_code == 401


@pytest.fixture(autouse=True)
def keycloak_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """A realm at a fixed address, whatever .env holds, so ISSUER is the one expected."""
    settings = Settings.model_construct(KEYCLOAK_BASE_URL="https://eodh.example")
    monkeypatch.setattr(authz, "get_settings", lambda: settings)
    authz.verifier.cache_clear()
    yield
    authz.verifier.cache_clear()


@pytest.fixture(autouse=True)
def signing_key(keycloak_settings: None) -> Iterator[MagicMock]:
    """Stands in for a real call to Keycloak: hands back our own throwaway public key."""
    with patch.object(authz.verifier().jwks, "get_signing_key_from_jwt") as get_signing_key:
        get_signing_key.return_value = types.SimpleNamespace(key=PRIVATE_KEY.public_key())
        yield get_signing_key


def test_a_genuinely_signed_token_is_accepted() -> None:
    token = _token(PRIVATE_KEY, **{"workspaces-owned": ["geodowd"]})

    result = authz.decode_jwt_token(_credentials(token))

    assert result["workspaces-owned"] == ["geodowd"]


def test_a_forged_signature_is_rejected() -> None:
    """This is the exact bug that shipped: verify_signature was False, so any signature -
    including one that is not cryptographically valid at all - was accepted.
    """
    header = jwt.utils.base64url_encode(b'{"alg":"RS256","typ":"JWT"}').decode()
    payload = jwt.utils.base64url_encode(b'{"sub":"attacker","workspaces-owned":["geodowd"],"aud":"eodh"}').decode()
    forged_signature = jwt.utils.base64url_encode(b"not-a-real-signature").decode()
    forged_token = f"{header}.{payload}.{forged_signature}"

    with pytest.raises(HTTPException) as raised:
        authz.decode_jwt_token(_credentials(forged_token))

    assert raised.value.status_code == 401


def test_a_token_signed_by_a_different_key_is_rejected() -> None:
    """Guards against accepting any valid-looking signature rather than specifically
    Keycloak's: a token signed end-to-end correctly, just with the wrong key.
    """
    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    token = _token(other_key)

    with pytest.raises(HTTPException) as raised:
        authz.decode_jwt_token(_credentials(token))

    assert raised.value.status_code == 401


def test_the_wrong_audience_is_rejected() -> None:
    """A genuinely signed token issued for a different client should not be accepted here."""
    token = _token(PRIVATE_KEY, aud="some-other-client")

    with pytest.raises(HTTPException) as raised:
        authz.decode_jwt_token(_credentials(token))

    assert raised.value.status_code == 401


def test_the_issuer_is_built_from_the_settings_that_build_the_jwks_url() -> None:
    config = authz.verifier()

    assert config.issuer == ISSUER
    assert config.jwks.uri == f"{ISSUER}/protocol/openid-connect/certs"


@pytest.mark.parametrize("base_url", ["https://eodh.example/", " https://eodh.example "])
def test_the_base_url_is_normalised_so_the_issuer_matches(base_url: str) -> None:
    """The issuer is compared exactly, so a stray slash would refuse every token."""
    settings = Settings(_env_file=None, KEYCLOAK_BASE_URL=base_url)  # pyright: ignore[reportCallIssue]

    assert settings.keycloak_issuer == ISSUER


def test_an_empty_base_url_counts_as_unset() -> None:
    """`KEYCLOAK_BASE_URL=` in an env file must stop the API starting, not pass the check."""
    assert Settings(_env_file=None, KEYCLOAK_BASE_URL="").KEYCLOAK_BASE_URL is None  # pyright: ignore[reportCallIssue]


def test_keycloak_unreachable_is_a_503_not_a_401(signing_key: MagicMock) -> None:
    """A 401 tells the client its token is bad, so it refreshes or logs out over an outage."""
    signing_key.side_effect = PyJWKClientConnectionError("connection refused")

    with pytest.raises(HTTPException) as raised:
        authz.decode_jwt_token(_credentials(_token(PRIVATE_KEY)))

    assert raised.value.status_code == 503


def test_another_realms_issuer_is_rejected() -> None:
    """Signed by a key this service trusts, but issued for a different realm."""
    assert _rejected(_token(PRIVATE_KEY, iss="https://eodh.example/keycloak/realms/master"))


def test_a_token_without_an_issuer_is_rejected() -> None:
    assert _rejected(_token(PRIVATE_KEY, iss=None))


def test_a_token_without_an_expiry_is_rejected() -> None:
    """PyJWT checks exp only when it is present unless told to require it."""
    assert _rejected(_token(PRIVATE_KEY, exp=None))


def test_a_token_expired_within_the_leeway_is_accepted() -> None:
    token = _token(PRIVATE_KEY, exp=datetime.now(tz=UTC) - timedelta(seconds=5))

    assert authz.decode_jwt_token(_credentials(token))["sub"] == "test-user"


def test_a_token_expired_beyond_the_leeway_is_rejected() -> None:
    expired = datetime.now(tz=UTC) - authz.JWT_LEEWAY - timedelta(seconds=30)

    assert _rejected(_token(PRIVATE_KEY, exp=expired))


def test_without_a_keycloak_url_verification_is_a_configuration_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing setting is the server's fault, not the caller's, so it is not a 401. The
    API refuses to start in this state (tests/test_startup.py), so no request sees it.
    """
    monkeypatch.setattr(authz, "get_settings", lambda: Settings.model_construct())
    authz.verifier.cache_clear()

    with pytest.raises(RuntimeError, match="KEYCLOAK_BASE_URL"):
        authz.verifier()


def test_the_real_keycloak_jwks_document_parses_and_picks_the_signing_key() -> None:
    """Keycloak publishes an encryption key (RSA-OAEP) alongside the signing key (RS256) in
    the same document. Confirms PyJWT handles that shape rather than choking on the entry
    this code was never meant to use, using the real response rather than a guess at its
    shape.
    """
    jwk_set = jwt.PyJWKSet.from_dict(REAL_KEYCLOAK_JWKS)
    signing_keys = [key for key in jwk_set.keys if key.public_key_use == "sig"]

    assert [key.key_id for key in signing_keys] == ["X2NoP9-FqRVXCHLFaUO7YcaPghYRQHjqWKAaofjXzE4"]
