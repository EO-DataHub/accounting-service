#!/usr/bin/env bash
#
# Grant credits to every workspace that has never had a grant of any kind. A workspace that has
# had any other grant, such as a pilot allocation, gets nothing from this.
#
# Usage: grant-to-new.sh <env> [--commit] [--amount N] [--reason TEXT]
#
# <env> picks the settings file .<env>.env, as APP_ENV does. Without --commit, this prints the
# database it would write to and the workspaces it would grant to, and writes nothing.
#
# Grants cannot be undone, but a workspace drops off the list once it has one. If this stops
# partway, run it again and it grants only to the workspaces it missed.

set -euo pipefail

usage() {
    echo "usage: $0 <env> [--commit] [--amount N] [--reason TEXT]" >&2
    exit 2
}

[[ $# -ge 1 ]] || usage
env=$1
shift

commit=false
amount=1000
reason="Welcome"

while [[ $# -gt 0 ]]; do
    case $1 in
        --commit) commit=true; shift ;;
        --amount) [[ $# -ge 2 ]] || usage; amount=$2; shift 2 ;;
        --reason) [[ $# -ge 2 ]] || usage; reason=$2; shift 2 ;;
        *) usage ;;
    esac
done

cd "$(dirname "$0")"

# pydantic-settings skips a missing env file silently and connects using the defaults instead.
if [[ ! -f ".$env.env" ]]; then
    echo "No .$env.env here, so APP_ENV=$env would fall back to the default settings." >&2
    exit 1
fi

export APP_ENV=$env

# The file name does not prove where it points, and exported SQL_* variables override the file,
# so show what the settings actually resolve to.
target=$(uv run python -c '
from accounting_service.settings import get_settings
s = get_settings()
print(f"{s.SQL_USER}@{s.SQL_HOST}:{s.SQL_PORT}/{s.SQL_DATABASE}, schema {s.SQL_SCHEMA}")
')

# Assigned rather than looped over directly, so a failure stops the script under `set -e`
# instead of expanding to nothing.
listed=$(uv run billing-admin workspaces --ungranted)

if [[ -z $listed ]]; then
    echo "Target: $target"
    echo "No workspace is without a grant. Nothing to do."
    exit 0
fi

mapfile -t workspaces <<< "$listed"

echo "Target: $target"
echo "Would grant $amount credits, reason \"$reason\", to ${#workspaces[@]} workspaces:"
printf '  %s\n' "${workspaces[@]}"

if [[ $commit != true ]]; then
    echo "Dry run. Pass --commit to grant."
    exit 0
fi

read -r -p "Type '$env' to grant to these ${#workspaces[@]} workspaces: " answer
if [[ $answer != "$env" ]]; then
    echo "Not confirmed. Nothing granted." >&2
    exit 1
fi

for workspace in "${workspaces[@]}"; do
    uv run billing-admin grant --workspace "$workspace" --amount "$amount" --reason "$reason"
done
