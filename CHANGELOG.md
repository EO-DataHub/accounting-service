# Changelog

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
