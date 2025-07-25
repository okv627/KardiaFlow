#!/usr/bin/env bash
# Build kflow wheel and push it to /Workspace/Shared/libs
set -euo pipefail

# ───────────── 0. Locate .env & repo root ─────────────
here="$(cd "$(dirname "$0")" && pwd)"       # …/infra/deploy
infra_root="$here/.."                       # …/infra
repo_root="$infra_root/.."                  # project root
cd "$repo_root"

ENV_FILE="$infra_root/.env"
[[ -f "$ENV_FILE" ]] || {
  echo "ERROR: .env not found at $ENV_FILE" >&2; exit 1; }

# shellcheck source=/dev/null
source "$ENV_FILE"
: "${DATABRICKS_PAT:?ERROR: Set DATABRICKS_PAT in infra/.env}"

# ───────────── 1. Resolve kflow version ─────────────
KFLOW_VER="$(python - <<'PY'
import sys, tomllib, pathlib
with open("pyproject.toml", "rb") as f:
    print(tomllib.load(f)["project"]["version"])
PY
)"
wheel_glob="dist/kflow-${KFLOW_VER}-py3-none-any.whl"

# ───────────── 2. Build wheel ─────────────
python -m pip install --quiet --upgrade build setuptools wheel
python -m build --wheel >/dev/null

[[ -e $wheel_glob ]] || {
  echo "ERROR: Wheel not found at $wheel_glob" >&2; exit 1; }

wheel_path="$(ls $wheel_glob | head -1)"
wheel_name="$(basename "$wheel_path")"

# ───────────── 3. Databricks auth ─────────────
DB_HOST="$(az deployment group show \
  --resource-group "$RG" \
  --name "$DEPLOY" \
  --query 'properties.outputs.databricksUrl.value' -o tsv)"

export DATABRICKS_HOST="https://${DB_HOST}"
export DATABRICKS_TOKEN="$DATABRICKS_PAT"

databricks configure --token \
  --host  "$DATABRICKS_HOST" \
  --token "$DATABRICKS_TOKEN" \
  --profile "$PROFILE" >/dev/null

# ───────────── 4. Upload wheel ─────────────
WS_DEST_DIR="/Workspace/Shared/libs"
WS_DEST_PATH="${WS_DEST_DIR}/${wheel_name}"

databricks workspace mkdirs "$WS_DEST_DIR" --profile "$PROFILE" 2>/dev/null || true

databricks workspace import \
  --file "$wheel_path" \
  "$WS_DEST_PATH" \
  --format RAW --overwrite \
  --profile "$PROFILE"

echo "Uploaded $wheel_name to $WS_DEST_PATH – kflow$KFLOW_VER ready."
