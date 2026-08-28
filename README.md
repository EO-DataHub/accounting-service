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

## Database migrations

This service manages its schema with [Alembic](https://alembic.sqlalchemy.org/). Migrations live in `alembic/versions/`.

`docker compose up` applies migrations for you: a `migrate` service runs `alembic upgrade head` before `api` and `ingester` start. A deployed environment applies migrations the same way, through a Kubernetes Job that runs before the new version starts.

To change the schema, edit the models in `accounting_service/models.py`, then generate a migration against your local database:

```commandline
SQL_DRIVER=postgresql+psycopg SQL_HOST=localhost SQL_PORT=5433 uv run alembic revision --autogenerate -m "describe the change"
```

Set `SQL_DRIVER` to `postgresql+psycopg`, even if you normally use SQLite. Some indexes are PostgreSQL-only, and autogenerate silently leaves them out under any other driver. Review the generated file before committing it - autogenerate does not always get everything right.

# Management of this Component

## Adding BillingItems (SKUs) and Prices

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

`billing-admin add-item` creates a brand new item with its initial price in one step. Both connect directly to the database, so point them at the right one first, eg. through a `kubectl port-forward`, the same way you would for `alembic`.

### Add or change a price

`accounting.conf` never sets prices in a deployed environment: the ConfigMap always ships an empty `prices` list. Use `billing-admin` instead:

```commandline
uv run billing-admin set-price --sku my-sku --price 12.34 --valid 2025-01-01T00:00:00Z
```

`--valid` must be later than the item's current price, or match it exactly to correct that price - `billing-admin` rejects anything else. Under the hood this inserts a row into `billing_item_price` and, if it is replacing a price, sets `valid_until` on the old one. It never updates a price in place, so the price history stays intact.

Run `uv run billing-admin ls` to see all items and their current price, or `uv run billing-admin ls my-sku` for one item's full price history.

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

To also send a `WorkspaceSettings` message, or a message `inject` does not cover, edit and run `tests/send_test_message.py` instead - see the setup notes at the top of that file.
