---
status: proposed
date: 2026-10-28
supersedes: D5 in eodhp-guide credits-ledger-design-decisions.md, in part
---

# The allowance is platform credit, not an owner's budget

A workspace is overdrawn when its balance goes past its allowance: how far below zero the
platform lets it go. Only a `hub_admin` sets an allowance. A workspace without an override
inherits the default allowance live, so a change to the default reaches it at once. Every set and
every removal is a new row with an actor and a reason. Nothing is overwritten.

D5 and the schema doc combined two things in `workspace_budget`. The first was the permitted
negative balance (`limit_credits`). The second was the owner's own controls: a warn-at threshold,
per-user thresholds and an alerts toggle. They answer different questions, different people set
them, and they change for different reasons. That is the argument D6 makes for keeping category
assignment out of the pricing policy. This decision keeps the first as the **allowance** and
defers the second as a future **budget** feature, under that name, with its own storage.

## Considered options

- **The owner sets the allowance** (D5, and the Budget page in the UX design). Rejected. An owner
  who can raise their own allowance can grant themselves credit, and only a `hub_admin` grants
  credit (T15).
- **The default is copied into each workspace when it is first seen.** Rejected. Every workspace
  then looks like an override that nobody chose. A record of "the owner set 100" becomes
  indistinguishable from "the default was 100 in March".
- **The default lives in the pricing policy.** Rejected. A change would mint a policy version
  that claims every rate changed.
- **Mutable allowance rows.** Rejected. Removing an override would delete the only record that
  it existed.

## Consequences

- The UX Budget page, as designed, has no backend. Its limit is now a `hub_admin` setting, and
  its warn-at threshold and per-user thresholds wait for the budget feature.
- Lowering the default can make many workspaces overdrawn at once, and each one is announced.
- An allowance is never negative and has no "unlimited" value. Zero means "overdrawn as soon as
  the balance is below zero".
