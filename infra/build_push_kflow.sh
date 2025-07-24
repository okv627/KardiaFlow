#!/usr/bin/env bash
set -euo pipefail

# ------------------------ 0. Locate repo & env ------------------------
here="$(cd "$(dirname "$0")" && pwd)"
repo_root="$here/.."
cd "$repo_root"

# shellcheck source=/dev/null
source "$here/.env"

: "${DATABRICKS_PAT:?ERROR: Set DATABRICKS_PAT in infra/.env}"

# ------------------------ 1. Resolve version --------------------------
KFLOW_VER="$(python - <<'PY'
import sys
from pathlib import Path
if sys.version_info < (3, 11):
    import tomli as tomllib
else:
    import tomllib
with open("pyproject.toml", "rb") as f:
    data = tomllib.load(f)
print(data["project"]["version"])
PY
)"

wheel_glob="dist/kflow-${KFLOW_VER}-py3-none-any.whl"

# ------------------------ 2. Build wheel ------------------------------
python -m pip install --upgrade build setuptools wheel >/dev/null
python -m build --wheel >/dev/null

if ! ls $wheel_glob >/dev/null 2>&1; then
  echo "ERROR: Wheel not found at $wheel_glob"
  exit 1
fi

wheel_path="$(ls $wheel_glob | head -1)"
wheel_name="$(basename "$wheel_path")"

# ------------------------ 3. Databricks auth --------------------------
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

# ------------------------ 4. Upload wheel -----------------------------
WS_DEST_DIR="/Workspace/Shared/libs"
WS_DEST_PATH="${WS_DEST_DIR}/${wheel_name}"

databricks workspace mkdirs "$WS_DEST_DIR" --profile "$PROFILE" >/dev/null 2>&1 || true

databricks workspace import \
  --file "$wheel_path" \
  "$WS_DEST_PATH" \
  --format RAW \
  --overwrite \
  --profile "$PROFILE"

echo "Uploaded wheel to $WS_DEST_PATH"
echo "kflow $KFLOW_VER ready."