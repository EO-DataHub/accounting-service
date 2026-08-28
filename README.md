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

# Management of this Component

## Adding BillingItems (SKUs) and Prices

### Add or update a billing item

The ingester creates a BillingItem automatically when it receives a Pulsar message for a SKU it does not know. The new item has no `name` or `unit`, so it does not display correctly in UIs. This logs an exception, but it is not a service failure.

To set the `name` and `unit`, add the SKU to `accounting.conf`:

- Locally: edit `dev/accounting.conf`.
- In a deployed environment: edit the `products-prices` ConfigMap for that environment in `eodhp-argocd-deployment`.

```yaml
items:
  - sku: my-sku
    name: My product
    unit: "GB-s"
```

The ingester loads this file on startup and updates any item with a matching SKU, including one it created automatically. Redeploy the ingester to apply a change.

### Add or change a price

In a deployed environment, `accounting.conf` never sets prices: the ConfigMap always ships an empty `prices` list. To add a price, connect to the database and insert a row into `billing_item_price`:

```sql
INSERT INTO billing_item_price (uuid, item_id, price, valid_from, configured_at)
VALUES (gen_random_uuid(), (SELECT uuid FROM billing_item WHERE sku = 'my-sku'), 12.34, '2025-01-01T00:00:00Z', now());
```

Never update an existing price. Set `valid_until` on the old price, then insert a new price with a matching `valid_from`:

```sql
UPDATE billing_item_price SET valid_until = '2025-02-01T00:00:00Z' WHERE uuid = '...';
```

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

- Forward Pulsar ports:
  - kubectl port-forward service/pulsar-proxy -n pulsar 6650:6650
- Run test message sender - edit it first if you need a particular message:
  - PYTHONPATH=. python ./tests/send_test_message.py
