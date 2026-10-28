# Accounting

Platform accounting for EODH: what each workspace has consumed, what it was charged in credits,
and whether it has spent more credit than the platform extends to it.

## Credit

**Balance**:
The sum of every credit movement a workspace has had: grants in, charges out, corrections either
way. It may be negative.
_Avoid_: Funds, wallet

**Allowance**:
How far below zero the platform lets a workspace's balance go, in credits. Never negative; zero
means none.
_Avoid_: Limit, overdraft limit, credit limit, budget

**Default allowance**:
The allowance of every workspace that has no override. Changing it changes those workspaces'
allowance at once.
_Avoid_: Platform limit, budget policy

**Allowance override**:
An allowance set for one workspace, replacing the default allowance for it.
_Avoid_: Workspace budget, custom limit

**Overdrawn**:
A workspace whose balance is below the negative of its allowance. Sitting exactly at that line is
not overdrawn.
_Avoid_: Over budget, in breach, negative (a negative balance within the allowance is not
overdrawn)

**In credit**:
A workspace that is not overdrawn. A negative balance within the allowance is still in credit.
_Avoid_: Recovered, solvent

**Credit status transition**:
A workspace moving from in credit to overdrawn, or back. Every transition is announced.
_Avoid_: Breach, alert, notification

**Budget**:
Reserved for a future feature: a spending target a workspace owner sets for themselves. Separate
from the allowance, which the platform sets.
_Avoid_: Using it to mean the allowance
