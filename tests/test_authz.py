"""Tests for the authorisation tiers.

No database, no HTTP client. These used to run through the API and therefore through
PostgreSQL, which was how they were written before authz.py existed as a module of its own:
`workspace_authz` and `account_authz` are functions over a dict, and nothing about a tier
decision needs a query.

tests/test_api.py keeps two cases that still go over HTTP, to show the dependency is wired
into the routes at all. Everything about which tier grants what is here.
"""

from typing import Any
from uuid import uuid4

import pytest
from fastapi import HTTPException

from accounting_service.app.authz import (
    MinTier,
    account_authz,
    is_hub_admin,
    resolve_workspace_tier,
    workspace_authz,
)
from tests.conftest import (
    TOKEN_ADMIN,
    TOKEN_HUB_ADMIN,
    TOKEN_MEMBER,
    TOKEN_NO_REALM_ACCESS,
    TOKEN_OWNER,
    TOKEN_SCALAR_CLAIM,
    TOKEN_STRANGER,
)

ACCOUNT = "4b48ebea-bdb8-4bb9-bce9-a7853ad3965d"


class TestResolveWorkspaceTier:
    """Which tier a token holds in a workspace, from its claims alone."""

    def test_the_owned_claim_gives_owner(self) -> None:
        assert resolve_workspace_tier("workspace2", TOKEN_OWNER) is MinTier.OWNER

    def test_the_admin_claim_gives_admin(self) -> None:
        assert resolve_workspace_tier("workspace1", TOKEN_ADMIN) is MinTier.ADMIN

    def test_the_workspaces_claim_gives_member(self) -> None:
        assert resolve_workspace_tier("workspace1", TOKEN_MEMBER) is MinTier.MEMBER

    def test_no_claim_for_the_workspace_gives_nothing(self) -> None:
        assert resolve_workspace_tier("workspace1", TOKEN_STRANGER) is None

    def test_the_highest_tier_wins_when_several_claims_match(self) -> None:
        """An owner listed as a member too is an owner, not a member."""
        both = {"workspaces": ["ws"], "workspaces-owned": ["ws"], "realm_access": {"roles": []}}

        assert resolve_workspace_tier("ws", both) is MinTier.OWNER

    def test_a_scalar_claim_is_not_substring_matched(self) -> None:
        """`workspace1` is a substring of `workspace1-prod`, and must not match it.

        The claim can arrive as a bare string rather than a list, and a plain `in` test
        against a string compares substrings. That would grant owner access to a workspace
        whose name is a prefix of one actually owned.
        """
        assert resolve_workspace_tier("workspace1", TOKEN_SCALAR_CLAIM) is None

    @pytest.mark.parametrize(
        "claim",
        [5, None, {"workspace1": True}, 12.5],
        ids=["integer", "null", "object", "float"],
    )
    def test_a_claim_of_the_wrong_shape_grants_nothing(self, claim: object) -> None:
        """The token is not signature-checked, so its contents are caller-controlled.

        Each of these used to raise, giving a 500 where a 401 belongs. An object was worse:
        iterating a dict yields its keys, so it granted access on them.
        """
        payload = {"workspaces-owned": claim, "realm_access": {"roles": []}}

        assert resolve_workspace_tier("workspace1", payload) is None


class TestIsHubAdmin:
    def test_the_realm_role_is_recognised(self) -> None:
        assert is_hub_admin(TOKEN_HUB_ADMIN) is True

    def test_a_token_without_the_role_is_not(self) -> None:
        assert is_hub_admin(TOKEN_MEMBER) is False

    def test_a_token_without_realm_access_is_not(self) -> None:
        """Issued by a client with no roles mapper. Used to raise KeyError."""
        assert is_hub_admin(TOKEN_NO_REALM_ACCESS) is False

    @pytest.mark.parametrize(
        "realm_access",
        ["hub_admin", ["hub_admin"], 7, None],
        ids=["string", "list", "integer", "null"],
    )
    def test_realm_access_of_the_wrong_shape_is_not(self, realm_access: object) -> None:
        assert is_hub_admin({"realm_access": realm_access}) is False

    def test_roles_as_a_bare_string_is_not_substring_matched(self) -> None:
        """A string is iterable, so `"hub_admin" in "hub_admin"` would be True."""
        assert is_hub_admin({"realm_access": {"roles": "hub_admin"}}) is False


class TestWorkspaceAuthz:
    """The gate: a minimum tier, with hub_admin above all of them."""

    @pytest.mark.parametrize(
        ("token", "workspace", "min_tier", "allowed"),
        [
            # hub_admin overrides every tier, including for a workspace it has no claim for.
            (TOKEN_HUB_ADMIN, "workspace1", MinTier.MEMBER, True),
            (TOKEN_HUB_ADMIN, "never-heard-of-it", MinTier.OWNER, True),
            # A member reaches its own workspace at MEMBER and no higher.
            (TOKEN_MEMBER, "workspace1", MinTier.MEMBER, True),
            (TOKEN_MEMBER, "workspace1", MinTier.ADMIN, False),
            (TOKEN_MEMBER, "workspace1", MinTier.OWNER, False),
            (TOKEN_MEMBER, "workspace2", MinTier.MEMBER, False),
            # Ordering: an owner satisfies every lower tier without being listed under them.
            (TOKEN_OWNER, "workspace2", MinTier.MEMBER, True),
            (TOKEN_OWNER, "workspace2", MinTier.ADMIN, True),
            (TOKEN_OWNER, "workspace2", MinTier.OWNER, True),
            # An admin satisfies MEMBER and ADMIN but not OWNER.
            (TOKEN_ADMIN, "workspace1", MinTier.MEMBER, True),
            (TOKEN_ADMIN, "workspace1", MinTier.ADMIN, True),
            (TOKEN_ADMIN, "workspace1", MinTier.OWNER, False),
            (TOKEN_STRANGER, "workspace1", MinTier.MEMBER, False),
        ],
    )
    def test_the_tier_ordering(self, token: dict[str, Any], workspace: str, min_tier: MinTier, allowed: bool) -> None:
        if allowed:
            assert workspace_authz(workspace, token, min_tier) == workspace
            return

        with pytest.raises(HTTPException) as raised:
            workspace_authz(workspace, token, min_tier)

        assert raised.value.status_code == 401

    def test_member_is_the_default_minimum(self) -> None:
        """Most endpoints take the default, so it needs to be the permissive one."""
        assert workspace_authz("workspace1", TOKEN_MEMBER) == "workspace1"

    def test_no_access_and_insufficient_tier_report_differently(self) -> None:
        """Both are 401, but the messages distinguish the two for whoever is debugging."""
        with pytest.raises(HTTPException) as stranger:
            workspace_authz("workspace1", TOKEN_STRANGER, MinTier.MEMBER)

        with pytest.raises(HTTPException) as outranked:
            workspace_authz("workspace1", TOKEN_MEMBER, MinTier.OWNER)

        assert stranger.value.detail != outranked.value.detail


class TestAccountAuthz:
    def test_a_listed_account_is_allowed(self) -> None:
        account = uuid4()
        token = {"billing-accounts": [str(account)], "realm_access": {"roles": []}}

        assert account_authz(account, token) == account

    def test_an_unlisted_account_is_refused(self) -> None:
        with pytest.raises(HTTPException) as raised:
            account_authz(uuid4(), TOKEN_MEMBER)

        assert raised.value.status_code == 401

    def test_hub_admin_reaches_any_account(self) -> None:
        account = uuid4()

        assert account_authz(account, TOKEN_HUB_ADMIN) == account

    def test_a_scalar_claim_is_not_substring_matched(self) -> None:
        """The same hazard as the workspace claims, on a claim holding UUID strings."""
        account = uuid4()
        token = {"billing-accounts": f"{account}-suffix", "realm_access": {"roles": []}}

        with pytest.raises(HTTPException):
            account_authz(account, token)
