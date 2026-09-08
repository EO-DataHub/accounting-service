# accounting-service

Platform accounting for EODH: usage arrives over Pulsar, prices and usage go out over HTTP.
`README.md` covers what it does and how to start it. This file records what is easy to get
wrong.

## Commands

| Command | What it is for |
|---|---|
| `make test-unit` | 122 tests, no database, about a second. Run this on every change |
| `make test-integration` | The rest. Needs Docker; starts a throwaway PostgreSQL |
| `make testonce` | Everything |
| `make check` | The gate: ruff, format, pyright, validate-pyproject, check-migrations |
| `make check-migrations` | Applies every revision to an empty container, then `alembic check` |
| `make run` | The API, with reload |
| `make run-ingester` | Needs Pulsar on localhost:6650 |

Run `make check` before reporting a change as done.

## Layout

- `models.py` — every table, deliberately all in one module. `alembic/env.py` imports
  `metadata` from here, and a table declared elsewhere would need its own import in
  `env.py` to reach the metadata.
- `configuration.py`, `pricing.py`, `consumption.py` — the rules, as pure functions over
  frozen Pydantic models with no session. New logic belongs here rather than on a table
  class.
- `timestamps.py` — `as_utc`, `datetime_default_to_utc`.
- `db.py`, `db_settings.py` — engine and settings, cached and resolved on first use rather
  than at import.
- `app/` — routes, response models, authorisation, dependencies.
- `ingester/` — Pulsar messagers.
- `dev/` — `billing_admin.py` (admin CLI), `inject.py` (send test messages),
  `check_migrations.py`, and `accounting.conf` for local runs.

## Tests

`tests/` needs no database and must stay that way. The fixtures that start a container live
in `tests/integration/conftest.py`, so nothing outside that directory can reach one. Put a
new test in `tests/` unless it asserts something only PostgreSQL can answer, such as
whether a constraint refuses a write.

A test that drives the ingester must pass `session_factory=db_session_factory`, or its
writes land outside the test's transaction and survive the rollback.

## Schema and migrations

PostgreSQL only. No driver branching and no SQLite.

- Tables are SQLModel classes and there is no `Base`. `table=True` turns Pydantic
  validation off, so never spread an unvalidated dict into a table class.
- Declare timestamps with the `aware_timestamp()` factory. A bare `datetime` annotation
  becomes TIMESTAMP WITHOUT TIME ZONE, which discards the offset on write. This has been
  fixed three times; `tests/test_schema.py` now guards it.
- Constraint names come from the naming convention on `SQLModel.metadata`. Check
  constraints are the exception and must be named in the model, bare name only.
- Write a revision by autogenerating against a container already migrated to head, then
  reading what it produced. `dev/check_migrations.py` shows the container setup.
- Deployed databases are migrated, never recreated. Some were stamped rather than migrated,
  so `alembic check` against a deployed database can disagree with a local one.

## Type checking

Configured in `pyproject.toml` under `[tool.pyright]`. There was a `pyrightconfig.json`
alongside it, which pyright loads in preference and which made that whole section inert; it
has been folded in and removed.

`accounting_service` is held to `standard`, where a bad attribute or argument is an error.
`reportArgumentType` and `reportAttributeAccessIssue` are downgraded to warnings for `tests`,
`dev` and `alembic` only, through `executionEnvironments`. The package is clean under the
strict setting, so keep it that way rather than widening the downgrade to cover it.

Pyright cannot check SQLModel query expressions: fields are plain annotations rather than
`Mapped[...]`, so a `where(...)` clause looks like a bool and a row built with `item=obj`
looks like a missing `item_id`. `models.py` carries a file-level suppression with the
reasoning in its header. Put a suppression in the file that needs it, as narrowly as it will
go — a wrapper function with one `# pyright: ignore` beats a suppression covering a file.

## Docker

Pass `--build` after any dependency change: `docker compose up --build`. The compose file
mounts the source but not `.venv`, so a stale image runs current code against old
dependencies and dies on import.

`docker compose up` does not propagate a service's exit code, so a failed migration looks
like a successful run. Use `docker compose run --rm migrate` for one-shot tasks.

## Design docs

The design lives outside this repo, in its own git repository at
`~/Documents/Sparkgeo/Projects/eodh/Accounting and billing/`:

- `Credits ledger scoping.md` — the task list, T1 to T21, with a Status column
- `Credits ledger schema.md` — table specifications and the schema conventions
- `Credits ledger design decisions.md` — D1 to D12, each decision and why
- `ADR-001 Credit-based platform accounting.md` — the case for the credit model

Code comments cite decisions as D-numbers and tasks as T-numbers. When a task is finished,
mark it in the scoping doc's Status column.
