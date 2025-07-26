#!/usr/bin/env bash
# Teardown for KardiaFlow dev env (Standard or Premium).
# - Deletes Access Connectors (Premium / UC)
# - Deletes Databricks workspace and waits
# - Strips RG locks
# - Deletes Databricks managed RG (dbstorage… etc.)
# - Deletes main RG

set -euo pipefail

# ─── Paths and Environment ────────────────────────────────────────────────
here="$(cd "$(dirname "$0")" && pwd)"
infra_root="$here/.."
env_file="$infra_root/.env"

[[ -f "$env_file" ]] && source "$env_file"

: "${RG:?Set RG in infra/.env}"
: "${WORKSPACE:?Set WORKSPACE in infra/.env}"
SUB="${SUB:-}"
MANAGED_RG="${MANAGED_RG:-}"

[[ -n "$SUB" ]] && az account set --subscription "$SUB" >/dev/null

# ─── Helpers ───────────────────────────────────────────────────────────────
rg_exists()         { [[ "$(az group exists --name "$1" -o tsv 2>/dev/null)" == "true" ]]; }
ws_exists()         { az databricks workspace show -g "$RG" -n "$WORKSPACE" &>/dev/null; }
remove_rg_locks()   { az lock list --resource-group "$1" -o tsv --query "[].id" | xargs -r -L1 az lock delete --ids; }
wait_until_gone()   { local rg="$1"; for _ in {1..90}; do rg_exists "$rg" || return 0; sleep 10; done; return 1; }

discover_managed_rg() {
  local id ac_rg sa_rg

  if ws_exists; then
    id="$(az databricks workspace show -g "$RG" -n "$WORKSPACE" \
          --query 'managedResourceGroupId' -o tsv 2>/dev/null || true)"
    [[ -n "$id" ]] && echo "${id##*/}" && return
  fi

  if rg_exists "${WORKSPACE}-managed"; then
    echo "${WORKSPACE}-managed"
    return
  fi

  ac_rg="$(az resource list --resource-type Microsoft.Databricks/accessConnectors \
          -o tsv --query "[].resourceGroup" 2>/dev/null | sort -u || true)"
  if [[ -n "$ac_rg" && "$(wc -l <<< "$ac_rg")" -eq 1 ]]; then
    echo "$ac_rg"
    return
  fi

  sa_rg="$(az storage account list -o tsv \
          --query "[?contains(name,'dbstorage')].resourceGroup" 2>/dev/null | sort -u || true)"
  if [[ -n "$sa_rg" && "$(wc -l <<< "$sa_rg")" -eq 1 ]]; then
    echo "$sa_rg"
    return
  fi

  echo ""
}

delete_access_connectors() {
  local rg="$1"
  [[ -n "$rg" ]] && rg_exists "$rg" || return 0

  local ids
  ids="$(az resource list -g "$rg" --resource-type Microsoft.Databricks/accessConnectors \
        -o tsv --query "[].id" 2>/dev/null || true)"
  [[ -z "$ids" ]] && return 0

  echo "Deleting Access Connectors in RG '$rg'..."
  while IFS= read -r id; do
    [[ -n "$id" ]] && az resource delete --ids "$id" || true
  done <<< "$ids"
}

# ─── Begin Teardown ────────────────────────────────────────────────────────

echo "Teardown: RG=$RG WORKSPACE=$WORKSPACE"

# Discover managed RG if needed
if [[ -z "$MANAGED_RG" ]]; then
  MANAGED_RG="$(discover_managed_rg || true)"
fi
echo "Discovered MANAGED_RG=${MANAGED_RG:-<unknown>}"

# Delete access connectors in both main and managed RG
delete_access_connectors "$RG"
delete_access_connectors "$MANAGED_RG"

# Delete workspace
if ws_exists; then
  echo "Deleting Databricks workspace '$WORKSPACE'..."
  az databricks workspace delete -g "$RG" -n "$WORKSPACE" --yes || true

  for _ in {1..90}; do
    if ! ws_exists; then
      echo "Workspace deleted."
      break
    fi
    sleep 10
  done
fi

# Delete managed RG (if exists)
if [[ -n "$MANAGED_RG" && "$(rg_exists "$MANAGED_RG")" == "true" ]]; then
  echo "Deleting managed RG '$MANAGED_RG'..."
  remove_rg_locks "$MANAGED_RG" || true

  # Proactively delete any lingering dbstorage accounts
  az storage account list -g "$MANAGED_RG" -o tsv --query "[].name" 2>/dev/null | while read -r sa; do
    [[ -n "$sa" ]] && az storage account delete -g "$MANAGED_RG" -n "$sa" --yes || true
  done

  az group delete --name "$MANAGED_RG" --yes --no-wait || true

  if ! wait_until_gone "$MANAGED_RG"; then
    echo "WARNING: Managed RG '$MANAGED_RG' still present; check locks or protection policies."
  fi
fi

# Delete main RG
if rg_exists "$RG"; then
  echo "Deleting main RG '$RG'..."
  remove_rg_locks "$RG" || true
  az group delete --name "$RG" --yes --no-wait || true

  if ! wait_until_gone "$RG"; then
    echo "WARNING: RG '$RG' still present; check locks or protection policies."
  fi
fi

echo "Teardown complete. (NetworkWatcherRG is intentionally left intact.)"
