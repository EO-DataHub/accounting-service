#!/bin/bash

# Grant 1000 credits only to accounts that haven't already received a Welcome grant.
export APP_ENV=prod

for w in $(uv run billing-admin workspaces --ungranted); do
  uv run billing-admin grant -w "$w" -a 1000 -r "Welcome"
done
