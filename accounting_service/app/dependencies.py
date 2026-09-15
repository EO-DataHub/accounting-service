from collections.abc import Callable
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, Response

from accounting_service.app.authz import MinTier, account_authz, decode_jwt_token, workspace_authz
from accounting_service.settings import get_settings

TokenDep = Annotated[dict[str, Any], Depends(decode_jwt_token)]


def require_workspace(min_tier: MinTier = MinTier.MEMBER) -> Callable[..., str]:
    """Build a dependency asserting the caller holds `min_tier` in the workspace."""

    def dependency(workspace: str, token_payload: TokenDep) -> str:
        return workspace_authz(workspace, token_payload, min_tier)

    return dependency


def require_account(account_id: UUID, token_payload: TokenDep) -> UUID:
    return account_authz(account_id, token_payload)


def require_token(token_payload: TokenDep) -> dict[str, Any]:
    """Assert a token was presented, without asking anything of its claims.

    For data that is the same for every caller but is not public. Every endpoint this service
    serves now needs a token: the ones with a workspace or an account in the path reach
    `decode_jwt_token` through their own authorisation dependency, and the rest carry this.
    Nothing here is anonymously readable, which is the whole of the rule.
    """
    return token_payload


def cache_control(max_age: int, *, vary: str) -> Callable[[Response], None]:
    """Build a dependency that sets cache headers on the response."""

    def dependency(response: Response) -> None:
        response.headers["Cache-Control"] = f"private,max-age={max_age}"
        response.headers["Vary"] = vary

    return dependency


# Usage data is specific to the caller and changes constantly, so it is cached briefly and
# varies on everything that selected it.
usage_data_cache = cache_control(get_settings().USAGE_CACHE_TIMEOUT, vary="Cookie,Authorization,Accept-Encoding")

# SKUs and prices are the same for every caller, so they cache for longer and vary only on
# the encoding.
global_data_cache = cache_control(get_settings().GLOBAL_CACHE_TIMEOUT, vary="Accept-Encoding")
