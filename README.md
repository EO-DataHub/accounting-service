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

The service will automatically add any new BillingItems it sees from Pulsar (note that it will first log an SQL exception and this does not represent a service failure). However, it cannot set the `name` or `unit` fields which are necessary for proper display in UIs.

Prices cannot be added automatically.

To update items, connect to the database and `begin; UPDATE BillingItem SET name=..., unit=... WHERE sku=...`, check that one row was affected and then commit.

To add prices, insert into BillingItemPrice instead. Do not update prices. Instead, set valid_until in the old price and create a new price with a matching valid_from.

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
