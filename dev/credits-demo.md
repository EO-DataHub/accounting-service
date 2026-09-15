# Credits demo script

Six steps that show a grant, a charge, a recalibration, and a second charge under the new
rate. Everything runs from the CLI against the local compose stack.

The point of the demo is step 5 against step 2: the same usage costs twice as much after the
policy changes, and the first charge does not move.

## Before you start

You need two terminals. One runs the stack, the other runs the commands.

In terminal 1, start everything including Pulsar and leave it running:

```
docker compose --profile messaging up
```

Wait for the ingester to log that it is consuming. The first Pulsar start takes a minute.

In terminal 2, point the host tools at the local database:

```
cd ~/Projects/eodh/accounting-service
unset APP_ENV
export SQL_HOST=localhost SQL_PORT=5433 SQL_USER=accounting SQL_PASSWORD=changeme
export SQL_DATABASE=accounting SQL_SCHEMA=public
```

`APP_ENV=testing` and `APP_ENV=prod` both point at the remote test database. `grant` writes to
whatever database it reaches and does not ask first, so check this before step 1.

Confirm the rates loaded, and note the policy version in the table title:

```
uv run billing-admin ls
```

`cpu-seconds` should read 0.001 credits per unit under policy v1.

## 1. Grant credits

```
uv run billing-admin grant --workspace workspace1 --amount 1000 --reason "Demo"
```

Balance is 1000.

If you have run the demo before, the balance carries on from where it was. The ledger is
append-only, so there is no reset short of recreating the database.

## 2. Send a billing event

One hour of CPU time:

```
uv run inject billing-event --workspace workspace1 --sku cpu-seconds --quantity 3600
```

Watch terminal 1. The ingester logs the event and the charge.

## 3. Check the balance

```
uv run billing-admin ledger workspace1
```

One grant of +1000 and one debit of -3.6, balance 996.4. The debit is 3600 units at 0.001
credits each, under category `standard` with a multiplier of 1.

Copy the debit's transaction ID. Step 6 uses it.

## 4. Update the pricing policy

Rates are not set one at a time. Edit the policy in `dev/accounting.conf` and double the CPU
rate:

```yaml
pricing_policy:
  ...
  rates:
    - sku: cpu-seconds
      credits_per_unit: 0.002
```

The ingester loads the file when it starts, so restart it:

```
docker compose restart ingester
```

The loader compares the document with the policy in force. The rate changed, so it mints
version 2. Confirm:

```
uv run billing-admin ls
```

The title now reads policy v2 and `cpu-seconds` reads 0.002.

`uv run billing-admin ls cpu-seconds` shows the rate in both versions, side by side.

## 5. Send the same event again

```
uv run inject billing-event --workspace workspace1 --sku cpu-seconds --quantity 3600
```

## 6. Check the balance again

```
uv run billing-admin ledger workspace1
```

The second debit is -7.2, and the balance is 989.2. Same usage, twice the credits, because the
event priced under v2.

The first debit is still -3.6. Ask the API why, using the transaction ID from step 3:

```
export TOKEN=$(python3 -c "import json;print(json.load(open('http-client.env.json'))['dev']['jwt'])")
curl -H "Authorization: Bearer $TOKEN" \
  localhost:8000/workspaces/workspace1/accounting/ledger/$TRANSACTION
```

The `pricing` object reports 0.001 credits per unit and policy version 1. Recalibrating does
not rewrite what a past charge cost.

The balance endpoint takes the same token:

```
curl -H "Authorization: Bearer $TOKEN" localhost:8000/workspaces/workspace1/accounting/balance
```

The token in `http-client.env.json` is an unsigned hub admin token that holds `workspace1`,
which is why the demo uses that workspace rather than `my-workspace`.

## Optional: make the category matter

Set the workspace to `academic`, which the local configuration halves:

```
uv run billing-admin set-category --workspace workspace1 --category academic
uv run inject billing-event --workspace workspace1 --sku cpu-seconds --quantity 3600
uv run billing-admin ledger workspace1
```

The charge is -3.6: 3600 units at 0.002, halved. Charges already written keep the category
they priced under.

## Afterwards

Put `credits_per_unit` for `cpu-seconds` back to 0.001 in `dev/accounting.conf`, or the change
follows you into the next run. Restarting the ingester with the original file mints version 3,
which is correct: a policy is never edited, only superseded.

To start from an empty ledger:

```
docker compose down -v
```

That deletes the database volume. The next `up` migrates a fresh database.
