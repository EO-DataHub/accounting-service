"""Check the migrations against a throwaway database, in a subprocess.

`make check-migrations` runs this. It starts a PostgreSQL container, migrates it from empty to
head, confirms every index the models declare exists, and runs `alembic check` against the
result.

The subprocesses are the point, not an implementation detail: an in-process `alembic check`
cannot detect an empty target_metadata, because anything that has already imported
accounting_service.models has populated it as a side effect.

The index step exists because the aggregate indexes on billing_event are hidden from
autogenerate by include_object in alembic/env.py, so `alembic check` passes a database missing
them. It checks by name only: an index whose expressions differ from the model still passes.

It says nothing about a deployed database, whose schema may predate the migrations - run
`alembic check` against that directly.
"""

import os
import subprocess
import sys

from sqlalchemy import create_engine, text
from testcontainers.community.postgres import PostgresContainer

from accounting_service.models import metadata

# Match the oldest deployed PostgreSQL, not the newest available. Deployed databases are 14,
# where `UNIQUE NULLS NOT DISTINCT` is a syntax error; checking on 17 passed a revision that
# could not apply in production. Override with PG_IMAGE to try another version.
IMAGE = os.environ.get("PG_IMAGE", "postgres:14")

# Alembic's plugin registration lines say nothing useful and there are seven of them.
NOISE = "INFO  [alembic.runtime.plugins]"


def run(step: str, args: list[str], env: dict[str, str]) -> int:
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    print(f"--- alembic {step} (exit {result.returncode}) ---")
    for line in (result.stdout + result.stderr).splitlines():
        if NOISE not in line:
            print(f"  {line}")

    return result.returncode


def missing_indexes(url: str) -> set[str]:
    """Indexes the models declare that the migrated database does not have."""
    declared = {str(index.name) for table in metadata.tables.values() for index in table.indexes}

    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            present = set(connection.execute(text("SELECT indexname FROM pg_indexes")).scalars())
    finally:
        engine.dispose()

    return declared - present


def main() -> int:
    print(f"Starting {IMAGE}...")

    with PostgresContainer(IMAGE, driver="psycopg") as container:
        env = dict(os.environ) | {
            "SQL_DRIVER": "postgresql+psycopg",
            "SQL_HOST": container.get_container_host_ip(),
            "SQL_PORT": str(container.get_exposed_port(5432)),
            "SQL_USER": container.username,
            "SQL_PASSWORD": container.password,
            "SQL_DATABASE": container.dbname,
            "SQL_SCHEMA": "public",
        }

        if code := run("upgrade head", ["upgrade", "head"], env):
            print("\nThe migrations do not apply to an empty database.")
            return code

        print("--- declared indexes ---")
        if missing := missing_indexes(container.get_connection_url()):
            print(
                f"\nThe migrations do not create these indexes: {sorted(missing)}.\n"
                "Any in UNCOMPARED_INDEXES in alembic/env.py must be written into a revision "
                "by hand; autogenerate will not do it."
            )
            return 1
        print("  all present")

        if code := run("check", ["check"], env):
            print(
                "\nThe models and the migrations disagree. Either a model changed without a "
                "revision, or a revision does not describe what the models declare.\n"
                "`alembic revision --autogenerate -m '...'` will show the difference; read it "
                "before applying it."
            )
            return code

    print("\nMigrations apply cleanly and match the models.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
