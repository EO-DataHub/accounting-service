# Changelog

## Unreleased

Two breaking changes to API responses. There is one client, `eodhp-workspace-ui`, and both
were taken deliberately while it is being reworked.

- **`price` on `GET /accounting/prices` is a string, not a number.** It was a `Decimal` in the
  database and a `float` on the wire, which loses exactness on the values where it matters.
  It is now an exact decimal string: `"0.000000412"` rather than `4.12e-07`, and `"0.10"`
  rather than `0.1`. Never scientific notation, and the stored scale is preserved. The back
  end keeps the precision; the client decides how to display it.
- **Timestamps include sub-second precision where the stored value has it.** `event_start`,
  `event_end` and `valid_from`/`valid_until` were truncated to whole seconds. Billing event
  timestamps arrive from Pulsar with microseconds, so the truncation was discarding real
  precision. `2025-06-01T09:30:15.654321Z` where it used to read `2025-06-01T09:30:15Z`.
  Both forms are ISO-8601 and `format: date-time`, so only a client parsing with a fixed
  format string is affected.

Also in this release, and not breaking:

- An invalid `time-aggregation` value is rejected with 422 instead of being ignored. Asking
  for `week` previously returned unaggregated rows and a 200. An empty value still means no
  aggregation.
- `limit=0` and negative limits are rejected with 422. `limit=0` previously returned 100 rows.
- Timestamps in responses are converted to UTC rather than being labelled `Z`. The reported
  time was an hour out whenever the database connection was not on UTC. The connection
  timezone is now pinned to UTC as well.
- Tests run against a throwaway PostgreSQL container, so they exercise the same SQL as
  production. The SQLite support they needed has been removed.

## v0.6.1

- Add a simple CLI tool to manage billing items

## v0.6.0

- Use Alembic for database migrations

## v0.5.3

- Remove 'authorization' from inputs in OpenAPI spec

## v0.5.2

- Fix some double-counting problems when time-aggregating
- Allow for the use of indexes when time-aggregating

## v0.5.1

- Add support for time aggregated results

## v0.5.0

- Remove 'user' field in the billing events in the API, which is never set.

## v0.4.0

- Add support for configuring prices and products via a file

## v0.3.0

- Add authorization support

## v0.2.0

- Add consumption rate sampling support - estimated billing events can be generated from
  them.

## v0.1.1

- When duplicate billing event UUIDs are seen the later ones are dropped. This allows billing
  collectors to generate UUIDs from unique event keys as an anti-duplicate strategy.
