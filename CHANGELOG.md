# Changelog

## Unreleased

Usage is now charged in credits. A billing event that arrives over Pulsar is priced against
the pricing policy in force and written to an append-only ledger, and a workspace has a
balance.

- **New: `GET /workspaces/{workspace}/accounting/balance`.** The workspace's credit balance,
  as an exact decimal string. Usage debits are negative and grants positive, so the balance is
  the sum of the ledger. A workspace with no transactions has a balance of zero. Any member
  may read it. A negative balance is reported, not refused: nothing here blocks work.
- **New: `GET /workspaces/{workspace}/accounting/ledger/{transaction}`.** One transaction, and
  for a charge the arithmetic behind it - the quantity metered, the credits per unit, the
  workspace category and its multiplier, and the pricing policy version all four came from.
  Those are recomputed from what the row stores, so a charge explains the same way months
  later, after the rates have been recalibrated and after the workspace has been moved to
  another category. `pricing` is null for a grant, which is not priced. A transaction in
  another workspace is a 404.
- **The ingester writes a debit for every billing event it records.** The event and its charge
  land in one transaction, and a unique index permits one original debit per event, so a
  redelivered message does not charge twice.
- **Three things record the event but no charge, at error level rather than failing the
  message**: no policy covers the usage time, the policy holds no rate for the SKU, or the
  quantity is negative or not finite. None is fixed by redelivering, and a stored quantity can
  always be charged later; a dropped quantity cannot be recovered. Worth an alert, on the same
  footing as the existing auto-created `BillingItem`.
- **Nothing is rounded.** A charge is stored as the product comes out, so a rate of `0.001`
  against a quantity of `3600.0` gives `3.6000` - four decimal places, because the scale of a
  product is the sum of its inputs' scales. Read paths preserve that scale rather than
  trimming it, as they already do for prices. Clients should format for display.
- **`billing-admin` gains `grant`, `set-category` and `ledger`.** `grant` adds credits to a
  workspace and records who and why; `set-category` sets which pricing category a workspace is
  charged under, until the workspace service starts sending it; `ledger` lists a workspace's
  recent transactions and its balance. All three are privileged writes with no HTTP endpoint
  yet, reachable by anyone who can reach the database - the same footing as the rest of that
  tool.

### Earlier in this release

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

- **`time-aggregation` is a closed set, strictly enforced.** `day` and `month` are accepted
  and the parameter may be omitted. Everything else is a 422, including an empty value:
  `?time-aggregation=` no longer means "no aggregation". Previously any unrecognised value,
  `week` included, was silently ignored and returned unaggregated rows with a 200, so a
  caller asking for weekly totals got daily rows and no indication of it.
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
