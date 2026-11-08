import os
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings
from sqlalchemy import URL

app_env = os.getenv("APP_ENV", "")

# `.env` when APP_ENV is unset, `.<APP_ENV>.env` otherwise. The name is built here rather than
# in model_config so the empty case does not produce `..env`.
env_file = f".{app_env}.env" if app_env else ".env"


class Settings(BaseSettings):
    SQL_DRIVER: str = "postgresql+psycopg"
    SQL_PORT: int | None = None
    SQL_PASSWORD: str | None = None
    SQL_USER: str | None = None
    SQL_DATABASE: str = "accounting"
    SQL_HOST: str | None = None
    SQL_SCHEMA: str = "public"

    USAGE_CACHE_TIMEOUT: int = 5
    GLOBAL_CACHE_TIMEOUT: int = 300

    # The platform domain, e.g. https://test.eodatahub.org.uk - the same value injected
    # elsewhere in the platform as platform.domain (see eodhp-argocd-deployment). Used to
    # build the realm's issuer URL and JWKS endpoint for JWT verification. The API refuses
    # to start without it; dev/local_api.py, which does not verify tokens, does not (T24).
    KEYCLOAK_BASE_URL: str | None = None
    KEYCLOAK_REALM: str = "eodhp"

    model_config = {"env_file": env_file}

    @field_validator("KEYCLOAK_BASE_URL", mode="before")
    @classmethod
    def _normalise_base_url(cls, value: object) -> object:
        """The issuer is compared with the token's `iss` exactly, so a trailing slash would
        refuse every token, and an empty value (`KEYCLOAK_BASE_URL=` in an env file) would
        pass the startup check and fail every request. Both are caught here instead.
        """
        if isinstance(value, str):
            return value.strip().rstrip("/") or None

        return value

    @property
    def keycloak_issuer(self) -> str | None:
        """The `iss` the realm's tokens carry. Keycloak's hostname is pinned to the platform
        domain, so this does not vary with the URL a token was requested through.
        """
        if self.KEYCLOAK_BASE_URL is None:
            return None

        return f"{self.KEYCLOAK_BASE_URL}/keycloak/realms/{self.KEYCLOAK_REALM}"


@lru_cache
def get_settings() -> Settings:
    """The process-wide settings, read on first use.

    Call `get_settings.cache_clear()` after changing the environment.
    """
    return Settings()


def get_db_url() -> URL:
    settings = get_settings()

    return URL.create(
        settings.SQL_DRIVER,
        username=settings.SQL_USER,
        password=settings.SQL_PASSWORD,
        host=settings.SQL_HOST,
        port=settings.SQL_PORT,
        database=settings.SQL_DATABASE,
        # timezone is pinned so the offset the driver applies does not vary by server. Values
        # are stored as timestamptz, so the instant is never wrong, only its labelling.
        query={"options": f"-c search_path={settings.SQL_SCHEMA} -c timezone=UTC"},
    )
