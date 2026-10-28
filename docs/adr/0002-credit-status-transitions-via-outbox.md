---
status: proposed
date: 2026-10-28
amends: D4 and D9 in eodhp-guide credits-ledger-design-decisions.md
---

# Credit status transitions are published through an outbox

A workspace is in credit or overdrawn. The service works out the status again whenever either
input changes: the balance, through any ledger write, or the allowance, through any set or
removal. It publishes only the transitions, on the `workspace-credit-status-transitions` topic.
Each message states the status entered, not the event that caused it. Delivery is at least once,
and each workspace's messages arrive in order.

The notification is the only control (D4), so a lost message is the worst failure. The
transition row is therefore written in the same database transaction as the change that caused
it, with `published_at` empty. A relay in the ingester polls for unpublished rows every few
seconds and sends them, oldest first, with the workspace as the message key. A send that fails
stays unpublished, and the relay stops at that row so that no later row overtakes it.

A per-workspace advisory lock, held to commit, serialises the check. Without it, two concurrent
writes could both record the same transition, or neither could see the crossing they caused
together. The latest transition row is the current status, so the database holds no second copy
of it.

## Considered options

- **Send after commit.** Rejected. A crash between the commit and the send loses the message
  for good, because the recorded status no longer differs from the real one.
- **Send before commit.** Rejected. A failed commit announces a transition that did not happen.
- **Send on every check and let the consumer deduplicate.** Rejected. One message per debit
  floods the topic, and every consumer has to repeat the same work.
- **Announce only "became overdrawn".** Rejected. A consumer cannot then clear a banner or lift a
  future provisioning block without asking this service.
- **Relay after each consumed message, with no timer.** Rejected. A grant on a quiet platform
  would wait for unrelated traffic, and a user would see a positive balance with the overdrawn
  warning still showing.
- **A mutable status row as the lock.** Rejected. It duplicates the latest transition.

## Consequences

- This service becomes a Pulsar producer for the first time. The message class goes in
  `eodhp-utils`, so the contract is a change to a shared library.
- D9's "resource type" field is gone, because D5 removed per-type limits. D9's statement that
  nobody is building a consumer is no longer true.
- The API and `billing-admin` hold no Pulsar producer. Their transitions wait for the ingester's
  next poll, so the ingester must be running for any transition to reach the topic.
- Consumers deduplicate on the message `uuid`, and mark a `uuid` as seen only after acting on it.
- A consumer logs a `status` it does not recognise as an error and ignores it. Once the consumer
  is updated, `billing-admin resend-status` repeats the latest transition with its original
  `uuid`. A re-send is one more at-least-once delivery, not a new transition.
- D4 expected a future block to consume this topic. The suspension design (#247 in
  eodhp-sparkgeo-pm) instead has OPAL poll an internal endpoint. That endpoint reads the latest
  transition per workspace from the same table, so the table, not the topic, is the source of
  truth for enforcement. The topic is for notifications.
