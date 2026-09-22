"""Tests for decode_jwt_token's signature verification.

test_authz.py covers workspace_authz/account_authz as pure functions over a claims dict, and
deliberately does not exercise decode_jwt_token's real path: every route test overrides it via
FastAPI's dependency_overrides (see tests/integration/conftest.py). This file is the one place
that checks the signature verification itself actually rejects a forged token, since a bug
here lets every other authorisation check in the app be bypassed with whatever claims an
attacker likes.

There is no private key for the real Keycloak instance, so most of these sign with a
throwaway RSA keypair and mock _jwks_client() to hand back its public half, rather than
calling the real endpoint over the network.
"""

import types
from collections.abc import Iterator
from unittest.mock import patch

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from fastapi import HTTPException

from accounting_service.app import authz

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


def _token(key: RSAPrivateKey, aud: str = "account", **claims: object) -> str:
    return jwt.encode({"sub": "test-user", "aud": aud, **claims}, key, algorithm="RS256")


@pytest.fixture(autouse=True)
def mock_jwks() -> Iterator[None]:
    """Stands in for a real call to Keycloak: hands back our own throwaway public key."""
    with patch.object(authz, "_jwks_client") as mock_client:
        mock_client.return_value.get_signing_key_from_jwt.return_value = types.SimpleNamespace(
            key=PRIVATE_KEY.public_key()
        )
        yield


def test_a_genuinely_signed_token_is_accepted() -> None:
    token = _token(PRIVATE_KEY, **{"workspaces-owned": ["geodowd"]})

    result = authz.decode_jwt_token(_credentials(token))

    assert result["workspaces-owned"] == ["geodowd"]


def test_a_forged_signature_is_rejected() -> None:
    """This is the exact bug that shipped: verify_signature was False, so any signature -
    including one that is not cryptographically valid at all - was accepted.
    """
    header = jwt.utils.base64url_encode(b'{"alg":"RS256","typ":"JWT"}').decode()
    payload = jwt.utils.base64url_encode(b'{"sub":"attacker","workspaces-owned":["geodowd"],"aud":"account"}').decode()
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


def test_the_real_keycloak_jwks_document_parses_and_picks_the_signing_key() -> None:
    """Keycloak publishes an encryption key (RSA-OAEP) alongside the signing key (RS256) in
    the same document. Confirms PyJWT handles that shape rather than choking on the entry
    this code was never meant to use, using the real response rather than a guess at its
    shape.
    """
    jwk_set = jwt.PyJWKSet.from_dict(REAL_KEYCLOAK_JWKS)
    signing_keys = [key for key in jwk_set.keys if key.public_key_use == "sig"]

    assert [key.key_id for key in signing_keys] == ["X2NoP9-FqRVXCHLFaUO7YcaPghYRQHjqWKAaofjXzE4"]
