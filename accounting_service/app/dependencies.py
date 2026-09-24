from collections.abc import Callable
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends, HTTPException, Query, Response

from accounting_service.app.authz import MinTier, account_authz, decode_jwt_token, is_hub_admin, workspace_authz
from accounting_service.settings import get_settings
from accounting_service.timestamps import datetime_default_to_utc

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


def pricing_instant(
    token_payload: TokenDep,
    at: Annotated[
        datetime | None,
        Query(
            title="Instant to resolve the policy at",
            description=(
                "Serve the policy that priced usage at this RFC 3339 instant rather than the "
                "one pricing usage now. A timestamp with no offset is read as UTC. Restricted "
                "to hub admins: what prices usage now is the product read, and what priced it "
                "on some past date is an audit one."
            ),
            examples=["2025-02-12T13:34:22Z"],
        ),
    ] = None,
) -> datetime:
    """The instant a pricing read resolves at: `at` where it is given, otherwise now.

    Refused rather than ignored for a token without the role, because a caller who asked
    what priced usage in January and was silently told today's rates would read the answer
    as January's.
    """

    at = datetime_default_to_utc(at)

    if at is None:
        return datetime.now(UTC)

    if not is_hub_admin(token_payload):
        raise HTTPException(status_code=401, detail="'at' is restricted to hub admins")

    return at


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
