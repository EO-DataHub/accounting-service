"""Unsigned bearer tokens for the local API (`dev/local_api.py`): minting one, and reading
one back without checking it.

    curl -H "Authorization: Bearer $(uv run python -m dev.local_token --hub-admin)" ...

The token's algorithm is `none`, so only the local entrypoint accepts it: the real one
allows RS256 alone. The claim names are the ones `accounting_service/app/authz.py` reads.

This module has no import side effects, so tests can import `local_claims` without
installing it on the app, which only `dev/local_api.py` does.
"""

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

import jwt
import rich_click as click
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from accounting_service.app.authz import bearer_scheme


def local_token(
    *,
    hub_admin: bool = False,
    owner: tuple[str, ...] = (),
    admin: tuple[str, ...] = (),
    member: tuple[str, ...] = (),
    billing_account: tuple[UUID, ...] = (),
    sub: str = "local-dev",
) -> str:
    now = datetime.now(tz=UTC)
    claims: dict[str, Any] = {
        "sub": sub,
        "iat": now,
        "exp": now + timedelta(days=1),
        "realm_access": {"roles": ["hub_admin"] if hub_admin else []},
        "workspaces-owned": list(owner),
        "workspaces-admin": list(admin),
        "workspaces": list(member),
        "billing-accounts": [str(account) for account in billing_account],
    }

    return jwt.encode(claims, None, algorithm="none")


def local_claims(credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer_scheme)]) -> dict[str, Any]:
    """The token's claims, unchecked: no signature, issuer, audience or expiry."""
    try:
        return jwt.decode(credentials.credentials, options={"verify_signature": False})
    except jwt.PyJWTError as e:
        raise HTTPException(status_code=401, detail="Invalid JWT token") from e


@click.command()
@click.option("--hub-admin", is_flag=True, help="Grant the hub_admin realm role")
@click.option("--owner", multiple=True, metavar="WORKSPACE", help="Owner of this workspace; repeatable")
@click.option("--admin", multiple=True, metavar="WORKSPACE", help="Admin of this workspace; repeatable")
@click.option("--member", multiple=True, metavar="WORKSPACE", help="Member of this workspace; repeatable")
@click.option("--billing-account", multiple=True, type=click.UUID, help="Owner of this billing account; repeatable")
@click.option("--sub", default="local-dev", show_default=True, help="The token's subject")
def cli(
    hub_admin: bool,
    owner: tuple[str, ...],
    admin: tuple[str, ...],
    member: tuple[str, ...],
    billing_account: tuple[UUID, ...],
    sub: str,
) -> None:
    click.echo(
        local_token(
            hub_admin=hub_admin,
            owner=owner,
            admin=admin,
            member=member,
            billing_account=billing_account,
            sub=sub,
        )
    )


if __name__ == "__main__":
    cli()
