"""The API with token verification switched off, for a local run without Keycloak.

    uvicorn dev.local_api:app --reload

Claims are read from the bearer token without checking its signature, issuer, audience or
expiry, so the caller chooses who they are. `dev/local_token.py` mints a token to suit.

This is an entrypoint rather than a setting on purpose (T24). Deployed configuration is
environment variables, and any value an environment variable can hold is a value a
deployment could set by mistake. Reaching this module means changing the command the
container runs, which no configuration change does. It is also left out of the image
(.dockerignore); compose mounts it.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from accounting_service.app.app import app
from accounting_service.app.authz import decode_jwt_token
from dev.local_token import local_claims

logger = logging.getLogger(__name__)


@asynccontextmanager
async def local_lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Replaces the API's lifespan, which refuses to start without KEYCLOAK_BASE_URL."""
    yield


app.router.lifespan_context = local_lifespan
app.dependency_overrides[decode_jwt_token] = local_claims

logger.warning("JWT verification is OFF: claims are taken from bearer tokens unchecked. Never deploy this entrypoint")

__all__ = ["app"]
