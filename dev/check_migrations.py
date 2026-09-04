"""Check the migrations against a throwaway database, in a subprocess.

`make check-migrations` runs this. It starts a PostgreSQL container, migrates it from empty
to head, and runs `alembic check` against the result. Both steps run as subprocesses, which
is the point of the script rather than an implementation detail.

Running `alembic check` in-process cannot detect an empty target_metadata, because anything
that has already imported accounting_service.models has populated it as a side effect. That
fault reached the remote test database: an import cleanup removed the models import from
alembic/env.py, target_metadata was empty, and autogenerate reported every table as removed
- it would have generated a revision dropping the whole schema. Every in-process check said
it was fine.

Two things this does not cover. It says nothing about a deployed database, whose schema may
predate the migrations - use `alembic check` against that directly. And the two expression
indexes on billing_event are excluded from autogenerate by include_object in alembic/env.py,
so a database missing them passes: `tests/test_schema.py` asserts the models declare them,
and nothing asserts a database has them.
"""

import os
import subprocess
import sys

from testcontainers.community.postgres import PostgresContainer

IMAGE = "postgres:17"

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
