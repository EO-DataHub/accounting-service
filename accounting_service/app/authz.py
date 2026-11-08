from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
from functools import lru_cache
from typing import Annotated, Any
from uuid import UUID

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient, PyJWKClientConnectionError, PyJWTError

from accounting_service.settings import get_settings

bearer_scheme = HTTPBearer()

# The Keycloak client IDs platform tokens are issued for (the audience mappers on the eodh and
# eodh-workspaces clients, eodhp-argocd-deployment apps/keycloak/base/realms.yaml). This list is
# duplicated across the platform's services, so change them together.
JWT_AUDIENCE = ["eodh", "eodh-workspaces"]

# Tolerance on exp, nbf and iat for clock skew between these pods and Keycloak's.
JWT_LEEWAY = timedelta(seconds=10)


# How long a request waits on Keycloak for the JWKS document. PyJWKClient's default is 30
# seconds, which holds a worker thread for that long on every request while Keycloak is down.
JWKS_TIMEOUT_SECONDS = 5


@dataclass(frozen=True)
class Verifier:
    issuer: str
    jwks: PyJWKClient


@lru_cache
def verifier() -> Verifier:
    """The realm's issuer and a JWKS client for it, built once per process.

    The API's lifespan calls this, so a missing setting stops the process rather than
    turning every request into a 500 (T24). One client means the JWKS document is cached
    across requests rather than re-fetched from Keycloak on each. Building it does not
    contact Keycloak, so a Keycloak outage does not stop the API starting.
    """
    issuer = get_settings().keycloak_issuer
    if issuer is None:
        msg = "KEYCLOAK_BASE_URL must be set to verify JWTs. For a local run without Keycloak, use dev/local_api.py"
        raise RuntimeError(msg)

    return Verifier(
        issuer=issuer,
        jwks=PyJWKClient(f"{issuer}/protocol/openid-connect/certs", timeout=JWKS_TIMEOUT_SECONDS),
    )


class MinTier(IntEnum):
    MEMBER = 1
    ADMIN = 2
    OWNER = 3


def _claim_list(token_payload: dict[str, Any], claim: str) -> list[str]:
    """Claims are lists, but a single-element claim may arrive as a bare string.

    This avoids a substring match if the claim is a string and not a list.
    """

    value = token_payload.get(claim)
    if isinstance(value, str):
        return [value]

    if isinstance(value, list | tuple):
        # noinspection not-iterable
        return [item for item in value if isinstance(item, str)]

    return []


def resolve_workspace_tier(workspace: str, token_payload: dict[str, Any]) -> MinTier | None:
    """Highest tier the token holds in this workspace, or None for no access.

    Workspace owners are implicit admins.
    """

    if workspace in _claim_list(token_payload, "workspaces-owned"):
        return MinTier.OWNER

    if workspace in _claim_list(token_payload, "workspaces-admin"):
        return MinTier.ADMIN

    if workspace in _claim_list(token_payload, "workspaces"):
        return MinTier.MEMBER

    return None


def is_hub_admin(token_payload: dict[str, Any]) -> bool:
    realm_access = token_payload.get("realm_access")
    roles = realm_access.get("roles") if isinstance(realm_access, dict) else None
    return isinstance(roles, list) and "hub_admin" in roles


def workspace_authz(workspace: str, token_payload: dict[str, Any], min_tier: MinTier = MinTier.MEMBER) -> str:
    if is_hub_admin(token_payload):
        return workspace

    tier = resolve_workspace_tier(workspace, token_payload)

    if tier is None:
        raise HTTPException(status_code=401, detail="Access to this workspace is not allowed")

    if tier < min_tier:
        raise HTTPException(status_code=401, detail=f"Must be a workspace {min_tier.name.lower()}")

    return workspace


def hub_admin_authz(token_payload: dict[str, Any]) -> dict[str, Any]:
    """For reads that span every workspace, so that no workspace tier can grant them."""
    if not is_hub_admin(token_payload):
        raise HTTPException(status_code=401, detail="Must be a hub admin")

    return token_payload


def account_authz(account_id: UUID, token_payload: dict[str, Any]) -> UUID:
    if is_hub_admin(token_payload):
        return account_id

    if str(account_id) not in _claim_list(token_payload, "billing-accounts"):
        raise HTTPException(status_code=401, detail="Must be the account owner")

    return account_id


def decode_jwt_token(credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer_scheme)]) -> dict[str, Any]:
    # As this is used in dependency injection, FastAPI handles most of the failure modes.
    #
    # The signature is verified against Keycloak's own published key (fetched from
    # the realm's JWKS endpoint), rather than trusting an upstream gateway to have checked it: a
    # gateway sitting in front of the public path does not cover traffic that reaches this
    # service directly from elsewhere on the cluster network.
    #
    # exp is required, not just checked when present, and iss must be this realm's (T24).
    #
    # Failing to reach Keycloak is a 503, not a 401: a 401 tells the client its token is
    # bad, and a client that refreshes or logs out in response turns an outage into an
    # auth fault.
    config = verifier()
    try:
        signing_key = config.jwks.get_signing_key_from_jwt(credentials.credentials)
        return jwt.decode(
            credentials.credentials,
            signing_key.key,
            audience=JWT_AUDIENCE,
            issuer=config.issuer,
            algorithms=["RS256"],
            leeway=JWT_LEEWAY,
            options={"require": ["exp", "iss"]},
        )
    except PyJWKClientConnectionError as e:
        raise HTTPException(status_code=503, detail="Cannot reach the identity provider to verify the token") from e
    except PyJWTError as e:
        raise HTTPException(status_code=401, detail="Invalid JWT token") from e
