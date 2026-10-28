---
title: Overdrawn workspaces and credit status transitions
status: proposed
task: T16, re-scoped
author: John Donovan
date: 2026-10-28
related:
  - ../../GLOSSARY.md
  - ../adr/0001-allowance-is-platform-credit-not-owner-budget.md
  - ../adr/0002-credit-status-transitions-via-outbox.md
  - ../adr/0003-explicit-settle-call.md
---

# Overdrawn workspaces and credit status transitions

`accounting-service` announces on Pulsar each time a workspace becomes overdrawn and each time it
returns to credit. Another developer builds the consumer that tells users. This plan covers the
announcement, the allowance that defines "overdrawn", and the message contract the consumer
builds against.

The terms *balance*, *allowance*, *default allowance*, *allowance override*, *overdrawn*, *in
credit*, *credit status transition*, *suspended* and *budget* have one meaning each, defined in
[`GLOSSARY.md`](../../GLOSSARY.md). This plan uses them in that meaning only.

## Goal

A workspace may spend below zero, down to its allowance. When its balance goes past that line,
the people responsible for it should hear about it. When a grant, a reversal or a raised
allowance brings it back, they should hear that too. Nothing is blocked (D4).

## Behaviour

- Every workspace has an allowance: an override if a `hub_admin` set one, or else the default
  allowance. The default is 0 at release.
- An override may be unlimited. The default may not.
- A workspace is overdrawn when `balance < -allowance`. At exactly `-allowance` it is in credit.
  A workspace with an unlimited allowance is always in credit. A workspace with no recorded
  transition is in credit.
- The status is worked out again whenever the balance or the allowance changes. If it differs
  from the last recorded status, the service records a transition and publishes it.
- Members of a workspace can read its effective allowance. Only a `hub_admin` can change an
  allowance, and every change needs a reason.
- In the front end, a `hub_admin` sets or removes an override from a row action on the hub-admin
  workspaces page, which asks for the reason. The default is set on the same page.

## Message contract

This section is the interface the consumer is built against. The plan owner decides any change
to it, and tells the consumer's developer.

**Topic:** `workspace-credit-status-transitions`. **Message key:** the workspace name.
**Class:** `WorkspaceCreditStatusTransition` in `eodhp_utils/pulsar/messages.py`, a
`pulsar.schema.Record` sent with a `JsonSchema`, like the other classes there.

| Field | Type | Value |
|---|---|---|
| `uuid` | string | The transition's ID. The same transition always has the same ID |
| `workspace` | string | The workspace name |
| `status` | string | The status the workspace entered: `overdrawn` or `in_credit` |
| `balance` | string | The balance at the transition, as a decimal string, unrounded, for example `"-1203.000000417"` |
| `allowance` | string or null | The allowance in force at the transition, as a non-negative decimal string. `null` when the allowance is unlimited |
| `at` | string | When the service recorded the transition, ISO 8601 in UTC |

Guarantees:

- **At least once.** A message can arrive more than once. Deduplicate on `uuid`. A consumer marks
  a `uuid` as seen only after it acts on the message, so a message it ignored is not seen.
- **Ordered per workspace.** The messages for one workspace arrive in the order the service
  recorded them. There is no order across workspaces.
- **Each message is a change.** For one workspace, each message's `status` differs from the one
  before it. With today's two values they alternate, and the first message for a workspace is
  always `overdrawn`, because a workspace starts in credit.
- **More statuses may follow.** A later release may add a value, such as one for a warn-at
  threshold (#221). A consumer ignores a `status` it does not recognise, and logs it at error
  level, because it means this service and the consumer are out of sync. When the consumer is
  updated, `billing-admin resend-status` sends the current status again, with its original
  `uuid`.
- **State, not event.** The latest message for a workspace is its current status. A consumer
  that restarts can rebuild its state from the latest message per workspace.
- **Facts only.** The message names no recipients. Who to tell, and how, belongs to the
  consumer (D9). This service holds no user identities.
- **Latency.** A transition reaches the topic within one relay interval of its commit (5 seconds
  by default), as long as the ingester is running.

`balance` and `allowance` are strings because credits are not rounded (D8). A float would
change the last digits.

## Interfaces the tests drive

The test agent writes tests from this plan alone, so this plan names the entry points. The names
are proposals. A change to a name is a change to this plan.

| Entry point | Kind | Purpose |
|---|---|---|
| `GET /workspaces/{workspace}/accounting/allowance` | HTTP | `{allowance, source}`. `source` is `default` or `override`. `allowance` is `null` when unlimited. Any member |
| `PUT /workspaces/{workspace}/accounting/allowance` | HTTP | Body `{allowance, reason}`. Sets an override. `allowance: null` sets it to unlimited. `hub_admin` |
| `DELETE /workspaces/{workspace}/accounting/allowance?reason=...` | HTTP | Removes the override. The reason is a query parameter, because some clients and proxies drop the body of a `DELETE`. `hub_admin` |
| `GET /accounting/allowance/default` | HTTP | `{allowance}`. Any valid token |
| `PUT /accounting/allowance/default` | HTTP | Body `{allowance, reason}`. `allowance` may not be `null`. `hub_admin` |
| `billing-admin set-allowance WORKSPACE AMOUNT --reason` | CLI | Sets an override. `AMOUNT` may be `unlimited`. `--default` in place of a workspace sets the default, which may not be `unlimited` |
| `billing-admin clear-allowance WORKSPACE --reason` | CLI | Removes an override |
| `billing-admin allowance WORKSPACE` | CLI | Prints the effective allowance and every change, newest first, with actor, time and reason. `--default` for the default |
| `billing-admin settle` | CLI | Works out every known workspace's status. Prints the transitions it would record. Writes only with `--commit` |
| `billing-admin credit-status WORKSPACE` | CLI | Prints the workspace's transitions, newest first, with `published_at` |
| `settle_credit_status(session, workspaces)` | Python | Records any transitions for the given workspaces. Does not commit (ADR 0003) |
| `TransitionRelay.tick(session, producer)` | Python | Sends unpublished transitions and marks them published. The ingester calls it on a timer |
| `TRANSITION_RELAY_INTERVAL` | Setting | Seconds between relay ticks. Default 5 |
| `billing-admin resend-status WORKSPACE` | CLI | Sends the workspace's latest transition again through the relay. `--all` in place of a workspace does it for every workspace with a transition |
| `GET /workspaces/{workspace}/accounting/balance` | HTTP | Adds `allowance`, `allowance_source`, `status` and `status_since` to the existing response. Any member |
| `GET /accounting/workspaces` | HTTP | Adds the same four fields to every row of the estate list from PR #67. `hub_admin` |

## Acceptance criteria

Each criterion is a behaviour a test can check. "Refused" means the status the service returns
for other authorisation failures, which is 401 today.

### Allowance

- **A1.** A workspace with no override reports the default allowance with `source: default`.
- **A2.** After a `hub_admin` sets an override, the workspace reports that value with `source:
  override`.
- **A3.** After the override is removed, the workspace reports the default with `source:
  default`. A later change to the default reaches it.
- **A4.** An override equal to the current default still reports `source: override`, and a
  later change to the default does not reach it.
- **A5.** A negative allowance is a 422 on any write, and nothing is recorded.
- **A6.** A write without a `reason`, or with an empty one, is a 422, and nothing is recorded.
- **A7.** A workspace owner, a workspace admin or a plain member who tries to set or remove an
  allowance is refused. A member can read their workspace's allowance. A non-member is refused.
- **A8.** Any valid token can read the default. Only a `hub_admin` can set it.
- **A9.** On a newly migrated database the default allowance is 0.
- **A10.** `billing-admin allowance` lists every set and removal for the workspace, including
  the ones that a later change replaced, each with its actor, time and reason.
- **A11.** A `hub_admin` can set an override to unlimited. The workspace then reports `allowance:
  null` with `source: override`, and a later change to the default does not reach it.
- **A12.** An unlimited default is a 422 on the HTTP write and an error from the CLI, and nothing
  is recorded.

### Status

- **S1.** A debit that takes a workspace from in credit to overdrawn records one transition to
  `overdrawn`. The transition carries the balance after the debit and the allowance in force.
- **S2.** A workspace whose balance is exactly `-allowance` is in credit. One more credit of
  debit makes it overdrawn.
- **S3.** More debits while the workspace is overdrawn record no transition.
- **S4.** A grant that brings an overdrawn workspace back within its allowance records one
  transition to `in_credit`. A grant that leaves it overdrawn records nothing.
- **S5.** Raising an overdrawn workspace's override until it is in credit records a transition
  to `in_credit`. Lowering an in-credit workspace's override until it is overdrawn records a
  transition to `overdrawn`.
- **S6.** Removing an override records a transition if the default gives a different status.
- **S7.** A change to the default records a transition for every workspace without an override
  whose status changes, and none for workspaces with an override.
- **S8.** A negative balance within the allowance is in credit and records no transition.
- **S9.** If the transaction that wrote a debit, grant or allowance change rolls back, no
  transition is recorded.
- **S10.** Every writer settles. Each of these, taking a workspace across its allowance, records
  a transition: an ingested billing event, `billing-admin grant`, `billing-admin recharge`, the
  allowance endpoints and the allowance commands.
- **S11.** `billing-admin recharge` records at most one transition per workspace per run, even
  when it writes many debits for that workspace.
- **S12.** Two concurrent transactions on one workspace, which together take it across its
  allowance, record exactly one transition. *PostgreSQL only: an integration test.*
- **S13.** `billing-admin credit-status` lists every transition recorded for the workspace,
  newest first, with whether and when each one was published.
- **S14.** A workspace with an unlimited override records no transition to `overdrawn`, whatever
  its balance. Setting an overdrawn workspace's override to unlimited records a transition to
  `in_credit` with `allowance: null`.

### Publication

- **P1.** A relay tick sends every unpublished transition and marks each one published.
- **P2.** A relay tick sends the transitions in the order they were recorded, with the workspace
  name as the message key.
- **P3.** A published message has exactly the fields in the message contract. `balance` and
  `allowance` keep every digit of the stored value.
- **P4.** A transition already marked published is not sent again by a later tick.
- **P5.** If a send fails, that transition stays unpublished and no later transition is sent in
  the same tick. The next tick sends it first. The failure is logged at error level.
- **P6.** Transitions recorded by the API and by `billing-admin` are sent by the ingester's relay.
- **P7.** Two relays ticking at once do not send the same transition. *PostgreSQL only: an
  integration test.*
- **P8.** The relay interval comes from `TRANSITION_RELAY_INTERVAL`, default 5 seconds.
- **P9.** `billing-admin resend-status WORKSPACE` makes the next relay tick send that
  workspace's latest transition again, with the same `uuid` and fields. It records no new
  transition, and `billing-admin credit-status` still shows when the transition was first
  published. For a workspace with no transition, the command fails and nothing is sent.
- **P10.** `billing-admin resend-status --all` does the same for every workspace with a
  transition, and sends only the latest transition of each.

### Reads for the pages

The hub-admin workspaces page shows every workspace's allowance and status. The workspace pages
show the same for one workspace, and a later suspension banner (#247) reads them too.

- **R1.** The balance response carries `allowance` and `allowance_source`, equal to what the
  allowance read returns for the same workspace, with `allowance: null` when unlimited.
- **R2.** The balance response carries `status`, the status of the workspace's latest
  transition, or `in_credit` if it has none.
- **R3.** The balance response carries `status_since`, the `at` of the latest transition, or
  `null` if the workspace has none.
- **R4.** Every row of `GET /accounting/workspaces` carries the same four fields with the same
  values as the balance response for that workspace. The list is still one query for the estate,
  not one per workspace.

### Go-live

- **G1.** `billing-admin settle` without `--commit` prints the transitions it would record and
  records nothing.
- **G2.** `billing-admin settle --commit` records them. A second run records nothing.
- **G3.** `billing-admin settle` covers every workspace that `billing-admin workspaces` lists.

## Stages

Each stage ends in its own commit and is complete when its criteria pass and `make check` is
clean.

1. **Message contract.** Add `WorkspaceCreditStatusTransition` and its `get_fake()` to
   `eodhp-utils`. Release it, and raise this service's `eodhp-utils` version. The dependency
   change needs the owner's agreement first (CLAUDE.md). Share this stage's pull request with
   the consumer's developer, who builds against it. *Criteria: P3, checked against the class.*
2. **Allowance.** History table, a migration that seeds a default of 0, effective-allowance
   resolution as a pure function, the reads, and `billing-admin allowance`. A removed override
   and an unlimited override are different rows in the history, so a null allowance cannot mark
   both. *Criteria: A1, A9, A10, and the read halves of A7 and A8.*
3. **Credit status.** Transition table with `published_at`, `settle_credit_status` with the
   per-workspace advisory lock, and the covering index `(workspace, recorded_at) INCLUDE
   (credits)` on the ledger. Call it from the ingester, `grant` and `recharge`. Add `billing-admin
   credit-status`. *Criteria: S1–S4, S8–S13.*
4. **Allowance writes.** The `PUT` and `DELETE` endpoints, `set-allowance` and `clear-allowance`.
   Each settles before commit, and a change to the default settles every workspace without an
   override. *Criteria: A2–A8, A11, A12, S5–S7, S14, the allowance half of S10.*
5. **Reads for the pages.** The four fields on the balance response and on `GET
   /accounting/workspaces`. Starts after PR #67 merges. *Criteria: R1–R4.*
6. **Relay.** `TransitionRelay`, the ingester timer, the producer configuration, the setting
   and `billing-admin resend-status`. *Criteria: P1–P10.*
7. **Go-live sweep.** `billing-admin settle`. *Criteria: G1–G3.*
8. **Documentation.** Update the eodhp-guide design docs: amend D4, D5 and D9, replace
   `workspace_budget` and `budget_breach_notification` in the schema doc, record in D4 that a
   future block reads status over HTTP rather than from Pulsar, set T16's status in
   the scoping doc, and update `pending-backend-endpoints.md`. Add the setting and topic to the
   README, and mark this plan done.

## Out of scope

- **The consumer:** recipients, email, in-app banners. That is the other developer's work.
- **The budget:** owner-set spending targets, warn-at thresholds, per-user thresholds and the
  alerts toggle. ADR 0001 defers them under that name. Until then, a "near limit" marker on the
  hub-admin page is worked out by the page from `balance` and `allowance`, and nothing is
  announced for it.
- **Suspending a workspace** when it is overdrawn. That is #247 in eodhp-sparkgeo-pm. Its design
  has OPAL poll an internal endpoint, `GET /internal/suspended-workspaces`, rather than consume
  the topic. The endpoint reads the latest transition per workspace from the table this plan
  builds. Pulsar carries notifications (#221), and the table is the source of truth for
  enforcement. The endpoint needs its own access design: every endpoint this service serves
  verifies a token today, and a test walks the routes to keep it so.
- **Balance snapshots.** The covering index comes first. Snapshots are a separate task if the
  balance read is still slow at real volumes.
- **Unpriced events.** A workspace with uncharged usage shows a higher balance than it should,
  and so can be in credit when it is overdrawn. `billing-admin recharge` fixes the balance, and
  its settle call announces the result.

## Release

1. Deploy the consumer, or accept that the first messages go to a topic with nobody reading it.
2. Run `billing-admin recharge` to clear any backlog of unpriced events.
3. Check that the default allowance is 0, the agreed figure. The migration seeds it.
4. Run `billing-admin settle` and read who would be announced.
5. Run `billing-admin settle --commit`.

Every workspace has been granted 1,000 credits, and none was past a default of 0 on
2026-10-28. Step 2 can change that: the test database holds a large backlog of unpriced events,
and charging it lowers balances. Step 4 shows who it would announce.

## Risks

| Risk | Mitigation |
|---|---|
| A new ledger writer forgets to settle, and its changes go unannounced | S10 lists every writer. A new writer adds a case to that test (ADR 0003) |
| The balance read on every debit is slow for a workspace with a long ledger | The covering index makes it an index-only scan. Measure on the test database after stage 3 |
| Charging the backlog of unpriced events makes many workspaces overdrawn at release | Release steps 2 to 5: recharge, check the figure, dry-run, then commit |
| The ingester is down, so no transition reaches the topic | Transitions wait in the outbox and go out when it restarts. Nothing is lost |
| Lowering the default sends many messages at once | Expected. The consumer handles a burst on the topic |
| A front end already built against the owner-set Budget page has no backend | ADR 0001 records the change. Tell the front-end team when this plan is agreed |

## Open questions

- **Is the consumer live before release step 5?** If not, the go-live messages wait on the topic
  until it subscribes, if the subscription is created first, or are lost.
- **Should the front-end team see the plan now,** given that it removes the owner's control of
  the limit from the Budget page?
