from enum import IntEnum
from typing import Annotated, Any
from uuid import UUID

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jwt import PyJWTError

bearer_scheme = HTTPBearer()


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
    """
    Highest tier the token holds in this workspace, or None for no access.

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
    # Settings `verify_signature` to False assumes that it has been verified further upstream.
    # This must be addressed because a forged token could be used and credits could be added without purchasing them.
    try:
        return jwt.decode(credentials.credentials, options={"verify_signature": False}, algorithms=["RS256"])
    except PyJWTError as e:
        raise HTTPException(status_code=401, detail="Invalid JWT token") from e
