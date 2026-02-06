"""This file is for manual testing

Setup with Docker: docker compose up

Setup without Docker:

Run `sudo -u postgres psql` and then use SQL:
  create user accounting;
  create database accounting owner accounting;
  alter user accounting with password 'changeme';

Edit .env:
    #SQL_DRIVER="sqlite+pysqlite"

    # Example for PostgreSQL
    SQL_DRIVER="postgresql+psycopg"
    SQL_USER="accounting"
    SQL_PASSWORD="changeme"
    SQL_HOST="localhost"
    SQL_PORT=5432
    SQL_SCHEMA="public"

Run Pulsar:
  docker run -it \
        -p 6650:6650 \
        -p 8080:8080 \
        --mount source=pulsardata,target=/pulsar/data \
        --mount source=pulsarconf,target=/pulsar/conf \
        apachepulsar/pulsar:4.0.2 \
        bin/pulsar standalone

Run the ingester:
  PYTHONPATH=. python -m accounting_service.ingester --pulsar-url pulsar://localhost



Finally, whichever setup you use, send a message:
  PYTHONPATH=. python ./tests/send_test_message.py

"""

import uuid

import pulsar
from eodhp_utils.pulsar import messages

client = pulsar.Client("pulsar://localhost:6650")

billing_producer = client.create_producer("billing-events", schema=messages.generate_billingevent_schema())
workspace_producer = client.create_producer("workspace-settings", schema=messages.generate_workspacesettings_schema())

wsmsg = messages.WorkspaceSettings.get_fake()
wsmsg.name = "test-workspace"

workspace_producer.send(wsmsg)

bemsg = messages.BillingEvent(
    uuid=str(uuid.uuid4()),
    event_start="2025-01-17T06:42:34.987619",
    event_end="2025-01-17T06:48:34.987619",
    sku="testsku",
    workspace="test-workspace",
    quantity=0.0004,
)

billing_producer.send(bemsg)

client.close()
