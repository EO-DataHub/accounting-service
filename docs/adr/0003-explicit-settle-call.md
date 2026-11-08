---
status: proposed
date: 2026-10-28
---

# Writers settle credit status with an explicit call

Every code path that changes a balance or an allowance calls `settle_credit_status(session,
workspaces)` before it commits. Each workspace it touched is passed once. The `record_*` methods
on the ledger do not settle by themselves, and no session hook or database trigger settles for
them.

The decision is deliberate. A reader may expect a hook or a trigger, because both make the call
impossible to forget. Do not convert it to either without reading the reasons below.

- **Inside each `record_*` method.** Rejected. `billing-admin recharge` can write thousands of
  debits for one workspace. It would take the lock and read the balance once per row, and it
  could record a transition that a later row in the same run cancels. A batch writer settles
  once per workspace and announces only the net outcome.
- **A `before_commit` session hook.** Rejected. A commit would quietly take locks and do extra
  work, and the project prefers explicit calls to conventions.
- **A database trigger.** Rejected. The rule would move into PL/pgSQL, out of reach of the pure
  functions in `consumption.py` and `pricing.py`.

## Consequences

A new writer can forget the call. An integration test is the guard. It drives each writer across
the allowance and asserts that a transition was recorded. A new writer needs a new case in that
test.
