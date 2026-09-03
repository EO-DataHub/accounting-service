from collections.abc import Callable
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends

from accounting_service.app.authz import MinTier, account_authz, decode_jwt_token, workspace_authz

TokenDep = Annotated[dict[str, Any], Depends(decode_jwt_token)]


def require_workspace(min_tier: MinTier = MinTier.MEMBER) -> Callable[..., str]:
    """Build a dependency asserting the caller holds `min_tier` in the workspace."""

    def dependency(workspace: str, token_payload: TokenDep) -> str:
        return workspace_authz(workspace, token_payload, min_tier)

    return dependency


def require_account(account_id: UUID, token_payload: TokenDep) -> UUID:
    return account_authz(account_id, token_payload)
