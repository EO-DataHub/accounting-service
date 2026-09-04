from collections.abc import Callable
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, Response

from accounting_service.app.authz import MinTier, account_authz, decode_jwt_token, workspace_authz

TokenDep = Annotated[dict[str, Any], Depends(decode_jwt_token)]


def require_workspace(min_tier: MinTier = MinTier.MEMBER) -> Callable[..., str]:
    """Build a dependency asserting the caller holds `min_tier` in the workspace."""

    def dependency(workspace: str, token_payload: TokenDep) -> str:
        return workspace_authz(workspace, token_payload, min_tier)

    return dependency


def require_account(account_id: UUID, token_payload: TokenDep) -> UUID:
    return account_authz(account_id, token_payload)


def cache_control(max_age: int, *, vary: str) -> Callable[[Response], None]:
    """Build a dependency that sets cache headers on the response.

    Declared at the route rather than called inside the handler, so the caching policy sits
    beside the path it applies to and cannot be forgotten part-way down a function body.

    These headers are discarded when a handler raises. An HTTPException builds its own
    response, so an error carries only the headers the exception itself sets - which is what
    keeps `GET /accounting/skus/{sku}` returning max-age=60 on a 404 rather than caching
    "SKU not known" for five minutes.
    """

    def dependency(response: Response) -> None:
        response.headers["Cache-Control"] = f"private,max-age={max_age}"
        response.headers["Vary"] = vary

    return dependency


# Usage data is specific to the caller and changes constantly, so it is cached briefly and
# varies on everything that selected it.
usage_data_cache = cache_control(5, vary="Cookie,Authorization,Accept-Encoding")

# SKUs and prices are the same for every caller, so they cache for longer and vary only on
# the encoding.
global_data_cache = cache_control(300, vary="Accept-Encoding")
