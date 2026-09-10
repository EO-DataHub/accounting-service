# UK EO Data Hub Platform: accounting service

This is the EODH accounting service, which

- receives accounting information from around the system via Pulsar,
- maintains a record of this information in a PostgreSQL database,
- serves accounting information to authorized users,
- loads and serves pricing information.

# Development of this component

## Getting started

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and run:

```commandline
make setup
```

## Building and testing

A number of `make` targets are defined:

- `make test`: run tests continuously
- `make testonce`: run tests once
- `make format`: lint and reformat
- `make check`: run type checking and linting in check mode
- `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild VERSION=1.2.3` for a release image)
- `make dockerpush`: push a `latest` Docker image (again, you can add `VERSION=1.2.3`)

## Managing dependencies

Dependencies are specified in `pyproject.toml`. After changing them, run `uv sync` to update the lockfile and
virtual environment.

## Running the server locally

Different `.env` files are supported. Set the environment variable `APP_ENV` to use a named `.i<env name>.env` file, or unset it to use the plain `.env` file.

For instance, to use `.testing.env` execute:

```commandline
APP_ENV=testing uv run ...
```

## Database migrations

This service manages its schema with [Alembic](https://alembic.sqlalchemy.org/). Migrations live in `alembic/versions/`.

`docker compose up` applies migrations for you: a `migrate` service runs `alembic upgrade head` before `api` and `ingester` start. A deployed environment applies migrations the same way, through a Kubernetes Job that runs before the new version starts.

To change the schema, edit the models in `accounting_service/models.py`, then generate a migration against your local database:

```commandline
SQL_DRIVER=postgresql+psycopg SQL_HOST=localhost SQL_PORT=5433 uv run alembic revision --autogenerate -m "describe the change"
```

Set `SQL_DRIVER` to `postgresql+psycopg`, even if you normally use SQLite. Some indexes are PostgreSQL-only, and autogenerate silently leaves them out under any other driver. Review the generated file before committing it - autogenerate does not always get everything right.

# Management of this Component

## Adding BillingItems (SKUs) and credit rates

### Add or update a billing item

The ingester creates a BillingItem automatically when it receives a Pulsar message for a SKU it does not know. The new item has no `name` or `unit`, so it does not display correctly in UIs. This logs an exception, but it is not a service failure.

To set the `name` and `unit` for the permanent catalog of known items, add the SKU to `accounting.conf`:

- Locally: edit `dev/accounting.conf`.
- In a deployed environment: edit the `products-prices` ConfigMap for that environment in `eodhp-argocd-deployment`.

```yaml
items:
  - sku: my-sku
    name: My product
    unit: "GB-s"
```

The ingester loads this file on startup and updates any item with a matching SKU, including one it created automatically. Redeploy the ingester to apply a change.

For a one-off fix that should not wait for a redeploy - eg. correcting a stub item right after it appears - use `billing-admin` instead:

```commandline
uv run billing-admin update-item --sku my-sku --name "My product" --unit "GB-s"
```

`billing-admin add-item` creates an item with no rate. Both commands connect directly to the database, so point them at the right one first, eg. through a `kubectl port-forward`, the same way you would for `alembic`.

### Change a credit rate

Rates are not set one at a time. A pricing policy covers every rate and every category multiplier together, and loading `accounting.conf` either matches the policy already in force or mints a new version of it. There is no `set-price` command, because a single price row has no meaning against a policy.

To change a rate, edit the `pricing_policy` section of the configuration document and restart the ingester:

```yaml
pricing_policy:
  valid_from: "2025-01-01T00:00:00Z"
  default_category: standard
  reason: "Why this calibration happened"
  rates:
    - sku: my-sku
      credits_per_unit: 0.001
  category_multipliers:
    - category: standard
      multiplier: 1
```

Restarting with the file unchanged writes nothing. A new version is minted only when a rate, a multiplier, the default category or `valid_from` changes. Rewording `reason` is not a calibration and mints nothing.

Nothing converts credits to money. A user who needs credits asks a hub admin, who grants them.

Run `uv run billing-admin ls` to see every item with its rate under the policy in force, or `uv run billing-admin ls my-sku` for that SKU's rate in every policy.

## Credits and the ledger

Every billing event the ingester records is priced and written to the credit ledger as one debit. The ledger is append-only: nothing updates or deletes a row, and a correction is a new row referencing the one it corrects. Debits are negative and grants positive, so a balance is the sum of the ledger.

Each debit stores the quantity metered, the pricing policy version that priced it, and the workspace category resolved at the time. That is what lets a charge be explained months later, after the rates have changed and after the workspace has moved to a different category.

### Read a workspace's credits

```commandline
uv run billing-admin ledger my-workspace
```

This shows the recent transactions and the balance. Over HTTP:

```commandline
GET /workspaces/my-workspace/accounting/balance
GET /workspaces/my-workspace/accounting/ledger/{transaction}
```

The second returns one transaction and, for a charge, the arithmetic behind it. Both need a token holding membership of the workspace.

### Grant credits

```commandline
uv run billing-admin grant --workspace my-workspace --amount 1000 --reason "Pilot allocation"
```

The reason is recorded on the transaction. A grant is not idempotent, so running the command twice grants twice.

### Set a workspace's pricing category

```commandline
uv run billing-admin set-category --workspace my-workspace --category commercial
```

The category selects which multiplier applies to every rate. The workspace service is the authority on it and will send it over Pulsar; until then nothing populates the table and every workspace prices under the policy's `default_category`.

A category with no multiplier in the policy is not an error. Usage prices under the default instead, and the command warns when it cannot find a multiplier for what you set.

Recategorising changes what happens next. Charges already written keep the category they were priced under.

### Walk through the whole path locally

This exercises configuration, pricing, the ledger and the read endpoints. Start the whole stack, which includes Pulsar, the ingester and the API:

```commandline
docker compose --profile messaging up
```

`billing-admin` and `inject` run on the host, so point them at the local database first. Check where they are pointing before you write anything:

```commandline
export SQL_HOST=localhost SQL_PORT=5433 SQL_USER=accounting SQL_PASSWORD=changeme
export SQL_DATABASE=accounting SQL_SCHEMA=public
```

Environment variables take priority over `.env`, so this overrides whatever that file holds. `grant` and `set-category` write to whichever database they reach, and neither asks for confirmation.

1. Grant the workspace some credits.

   ```commandline
   uv run billing-admin grant --workspace my-workspace --amount 1000 --reason "Demo"
   ```

2. Send an hour of CPU time.

   ```commandline
   uv run inject billing-event --workspace my-workspace --sku cpu-seconds --quantity 3600
   ```

3. Read the ledger. One debit, and a balance below 1000.

   ```commandline
   uv run billing-admin ledger my-workspace
   ```

4. Ask why the charge was what it was. Take the transaction ID from the table above. The endpoint needs a token; `http-client.env.json` holds an unsigned one for a hub admin, which passes every tier check.

   ```commandline
   export TOKEN=$(python3 -c "import json;print(json.load(open('http-client.env.json'))['dev']['jwt'])")
   curl -H "Authorization: Bearer $TOKEN" \
     localhost:8000/workspaces/my-workspace/accounting/ledger/$TRANSACTION
   ```

   The `pricing` object gives the quantity, the credits per unit, the category, the multiplier and the policy version.

   The balance endpoint takes the same token:

   ```commandline
   curl -H "Authorization: Bearer $TOKEN" localhost:8000/workspaces/my-workspace/accounting/balance
   ```

5. Make the category matter. Set the workspace to `academic`, which the local configuration halves, and send the same usage again.

   ```commandline
   uv run billing-admin set-category --workspace my-workspace --category academic
   uv run inject billing-event --workspace my-workspace --sku cpu-seconds --quantity 3600
   uv run billing-admin ledger my-workspace
   ```

   The second charge is half the first.

6. Recalibrate. Change `cpu-seconds` to `0.002` in `dev/accounting.conf` and restart the ingester, which loads the file and mints a new policy version.

   ```commandline
   docker compose restart ingester
   uv run inject billing-event --workspace my-workspace --sku cpu-seconds --quantity 3600
   uv run billing-admin ledger my-workspace
   ```

   The third charge uses the new rate. Run step 4 against the first transaction again: it still reports the rate and the policy version that priced it.

The last step is the point of versioning a policy. The past does not move when the rates do.

## Incompatible Schema

If you get an incompatible schema error and are sure it's safe to upgrade then you can delete the schema in a cluster.

- Install pulsar admin tools:
  - wget https://archive.apache.org/dist/pulsar/pulsar-4.0.1/apache-pulsar-4.0.1-bin.tar.gz
  - tar xf apache-pulsar-4.0.1-bin.tar.gz
  - sudo apt install openjdk-17-jre
- Forward Pulsar ports:
  - kubectl port-forward service/pulsar-proxy -n pulsar 8080:8080 # Admin port
  - kubectl port-forward service/pulsar-proxy -n pulsar 6650:6650
- Delete schema:
  - ./apache-pulsar-4.0.1/bin/pulsar-admin schemas delete persistent://public/default/billing-events

## Adding a Test Entry

Send a fake billing event onto Pulsar, to exercise the ingester without a real event source:

- Forward Pulsar ports: `kubectl port-forward service/pulsar-proxy -n pulsar 6650:6650` (skip this if you are running `docker compose --profile messaging up` locally).
- `uv run inject billing-event --workspace my-workspace` (add `--sku` and `--quantity` to change what it sends; both default to a CPU-time reading).

To associate a workspace with an account, which the ingester needs before it will attribute usage: `uv run inject workspace-settings --workspace my-workspace`.

`inject` does not yet send consumption rate samples, so the one-hour windowing in `ConsumptionSampleRateIngesterMessager` has to be exercised through the tests. Run `uv run inject --help` for what is available.
