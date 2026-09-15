"""Shared test fixtures and constants.

Only what both halves of the suite need. The database fixtures are in
tests/integration/conftest.py, so that the tests here cannot start a container: this
directory is the fast suite, and it has no route to PostgreSQL.
"""

from typing import Any

# Token claim sets for authorisation tests.
#
# The claim names match what the Keycloak realm emits, so 'workspaces-owned' and
# 'workspaces-admin' are hyphenated. See the client scopes in
# eodhp-argocd-deployment/apps/keycloak/base/realms.yaml.
#
# 'workspaces-admin' has no producer yet: it needs a client scope and a
# workspaces-per-user endpoint on eodhp-workspace-services. The resolver does not
# care where the claim came from, so the admin tier is testable now.

TOKEN_HUB_ADMIN: dict[str, Any] = {
    "workspaces": ["workspace1", "workspace2"],
    "workspaces-owned": ["workspace2"],
    "realm_access": {"roles": ["user", "hub_admin"]},
}

TOKEN_MEMBER: dict[str, Any] = {
    "workspaces": ["workspace1"],
    "realm_access": {"roles": ["user"]},
}

# Owns workspace2 but is deliberately not in its member list, so this token
# exercises tier ordering: OWNER satisfies a MEMBER check on its own.
TOKEN_OWNER: dict[str, Any] = {
    "workspaces": ["workspace1"],
    "workspaces-owned": ["workspace2"],
    "realm_access": {"roles": ["user"]},
}

# Same idea one tier down: admin of workspace1 without member listing.
TOKEN_ADMIN: dict[str, Any] = {
    "workspaces": [],
    "workspaces-admin": ["workspace1"],
    "realm_access": {"roles": ["user"]},
}

TOKEN_STRANGER: dict[str, Any] = {
    "workspaces": [],
    "realm_access": {"roles": ["user"]},
}

# A claim delivered as a bare string rather than a list. The Keycloak mapper is
# not known to guarantee a list for a single value, and a plain `in` test against
# a string matches substrings, which would grant access to 'workspace1' here.
TOKEN_SCALAR_CLAIM: dict[str, Any] = {
    "workspaces-owned": "workspace1-prod",
    "realm_access": {"roles": ["user"]},
}

# A token with no realm_access at all, as issued by a client with no roles mapper.
TOKEN_NO_REALM_ACCESS: dict[str, Any] = {
    "workspaces": ["workspace1"],
}
