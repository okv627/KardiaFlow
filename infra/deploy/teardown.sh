#!/usr/bin/env bash
# Safe teardown of the KardiaFlow dev environment.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
infra_root="$here/.."

ENV_FILE="$infra_root/.env"
[[ -f "$ENV_FILE" ]] || {
  echo "ERROR: .env not found at $ENV_FILE" >&2; exit 1; }

# shellcheck source=/dev/null
source "$ENV_FILE"

echo "Initiating teardown of RG '$RG' and workspace '$WORKSPACE' …"

# Delete Databricks workspace (also removes its managed RG)
az databricks workspace delete \
  --resource-group "$RG" \
  --name "$WORKSPACE" --yes || true

# Delete parent resource group (non‑blocking)
az group delete --name "$RG" --yes --no-wait || true

echo "Teardown request submitted. Azure will finish deleting resources in the background."
