from enum import IntEnum
from functools import lru_cache
from typing import Annotated, Any
from uuid import UUID

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWKClient, PyJWTError

from accounting_service.settings import get_settings

bearer_scheme = HTTPBearer()

# The Keycloak client IDs platform tokens are issued for (the audience mappers on the eodh and
# eodh-workspaces clients, eodhp-argocd-deployment apps/keycloak/base/realms.yaml). This list is
# duplicated across the platform's services, so change them together.
JWT_AUDIENCE = ["eodh", "eodh-workspaces"]


@lru_cache
def _jwks_client() -> PyJWKClient:
    """One client per process, so the JWKS document is cached rather than re-fetched from
    Keycloak on every request. PyJWKClient does this caching internally, but only across
    calls on the same instance.
    """
    settings = get_settings()
    if not settings.keycloak_certs_url:
        msg = "KEYCLOAK_BASE_URL must be set to verify JWTs"
        raise RuntimeError(msg)

    return PyJWKClient(settings.keycloak_certs_url)


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
    # KEYCLOAK_CERTS_URL), rather than trusting an upstream gateway to have checked it: a
    # gateway sitting in front of the public path does not cover traffic that reaches this
    # service directly from elsewhere on the cluster network.
    try:
        signing_key = _jwks_client().get_signing_key_from_jwt(credentials.credentials)
        return jwt.decode(
            credentials.credentials,
            signing_key.key,
            audience=JWT_AUDIENCE,
            algorithms=["RS256"],
        )
    except PyJWTError as e:
        raise HTTPException(status_code=401, detail="Invalid JWT token") from e
