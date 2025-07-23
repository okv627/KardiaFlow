#!/usr/bin/env bash
set -euo pipefail

# --- 1. Load env ---
here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=/dev/null
source "$here/.env"

# --- 2. Require DATABRICKS_PAT ---
: "${DATABRICKS_PAT:?ERROR: Set DATABRICKS_PAT in infra/.env}"

# --- 3. Azure scope ---
az account set --subscription "$SUB"

# --- 4. Resolve workspace URL ---
DB_HOST="$(az deployment group show \
  --resource-group "$RG" \
  --name "$DEPLOY" \
  --query 'properties.outputs.databricksUrl.value' -o tsv)"

export DATABRICKS_HOST="https://${DB_HOST}"
export DATABRICKS_TOKEN="$DATABRICKS_PAT"
echo "Databricks host: $DATABRICKS_HOST"

# --- 5. Configure Databricks CLI (v0) non‑interactively ---
# Writes/updates ~/.databrickscfg entry for $PROFILE
databricks configure --token \
  --host  "$DATABRICKS_HOST" \
  --token "$DATABRICKS_TOKEN" \
  --profile "$PROFILE"

# --- 6. Create secret scope (ignore if exists) ---
databricks secrets create-scope "$PROFILE" \
  --initial-manage-principal users \
  --profile "$PROFILE" 2>/dev/null || true

# --- 7. Generate a 24h SAS token for ADLS 'raw' container ---
SAS_EXPIRY="$(date -u -d '+1 day' '+%Y-%m-%dT%H:%MZ' 2>/dev/null || gdate -u -d '+1 day' '+%Y-%m-%dT%H:%MZ')"
CONN_STR="$(az storage account show-connection-string \
  --resource-group "$RG" \
  --name "$ADLS" -o tsv)"

RAW_SAS="$(az storage container generate-sas \
  --connection-string "$CONN_STR" \
  --name            "$CONT" \
  --permissions     rl \
  --expiry          "$SAS_EXPIRY" \
  --https-only      \
  --output          tsv)"

# --- 8. Store the SAS in Databricks secrets (v0 syntax) ---
echo -n "$RAW_SAS" | databricks secrets put-secret \
  "$PROFILE" adls_raw_sas \
  --profile "$PROFILE"

echo "SAS stored in scope '$PROFILE' as key 'adls_raw_sas' (expires $SAS_EXPIRY UTC)"