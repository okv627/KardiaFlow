#!/usr/bin/env bash
# Generate a write‑enabled SAS token for the ADLS 'raw' container
# and store it as a Databricks secret.
set -euo pipefail

# ───────────── 0. Locate .env & repo root ─────────────
here="$(cd "$(dirname "$0")" && pwd)"       # …/infra/deploy
infra_root="$here/.."                       # …/infra
repo_root="$infra_root/.."                  # project root

ENV_FILE="$infra_root/.env"
[[ -f "$ENV_FILE" ]] || {
  echo "ERROR: .env not found at $ENV_FILE" >&2; exit 1; }

# shellcheck source=/dev/null
source "$ENV_FILE"
: "${DATABRICKS_PAT:?ERROR: Set DATABRICKS_PAT in infra/.env}"

# ───────────── 1. Azure scope ─────────────
az account set --subscription "$SUB"

# ───────────── 2. Resolve Databricks URL ─────────────
DB_HOST="$(az deployment group show \
  --resource-group "$RG" \
  --name "$DEPLOY" \
  --query 'properties.outputs.databricksUrl.value' -o tsv)"

export DATABRICKS_HOST="https://${DB_HOST}"
export DATABRICKS_TOKEN="$DATABRICKS_PAT"
echo "Databricks host: $DATABRICKS_HOST"

# ───────────── 3. Configure Databricks CLI ─────────────
databricks configure --token \
  --host  "$DATABRICKS_HOST" \
  --token "$DATABRICKS_TOKEN" \
  --profile "$PROFILE" >/dev/null

# ───────────── 4. Create secret scope (idempotent) ─────────────
databricks secrets create-scope "$PROFILE" \
  --initial-manage-principal users \
  --profile "$PROFILE" 2>/dev/null || true

# ───────────── 5. Generate SAS token ─────────────
SAS_EXPIRY="$(date -u -d '+180 days' '+%Y-%m-%dT%H:%MZ' 2>/dev/null || \
             gdate -u -d '+180 days' '+%Y-%m-%dT%H:%MZ')"

CONN_STR="$(az storage account show-connection-string \
  --resource-group "$RG" \
  --name "$ADLS" -o tsv)"

RAW_SAS="$(az storage container generate-sas \
  --connection-string "$CONN_STR" \
  --name            "$CONT" \
  --permissions     racwdl \
  --expiry          "$SAS_EXPIRY" \
  --https-only      -o tsv)"

# ───────────── 6. Store SAS in Databricks ─────────────
echo -n "$RAW_SAS" | databricks secrets put-secret \
  "$PROFILE" adls_raw_sas \
  --profile "$PROFILE"

echo "SAS stored in scope '$PROFILE' as key 'adls_raw_sas' (expires $SAS_EXPIRY UTC)"
