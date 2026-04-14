#!/usr/bin/env bash
set -euo pipefail

REQ_FILE="/opt/airflow/requirements.txt"
DBT_DIR="/opt/dbt"
STATE_DIR="/home/airflow/.local/share/esg-bootstrap"
HASH_FILE="${STATE_DIR}/runtime.sha256"
TMP_REQ_FILE="/tmp/esg-runtime-requirements.txt"

mkdir -p "${STATE_DIR}" \
         "/opt/airflow/gx/uncommitted/validations" \
         "/opt/airflow/gx/uncommitted/data_docs/local_site"

hash_inputs() {
  sha256sum \
    "${REQ_FILE}" \
    "${DBT_DIR}/packages.yml" \
    "${DBT_DIR}/package-lock.yml" \
    "${DBT_DIR}/profiles.yml" \
    | sha256sum | awk '{print $1}'
}

DESIRED_HASH="$(hash_inputs)"
CURRENT_HASH="$(cat "${HASH_FILE}" 2>/dev/null || true)"

if [[ "${DESIRED_HASH}" != "${CURRENT_HASH}" ]]; then
  echo "[bootstrap] Installing Python runtime dependencies..."
  grep -Ev '^(dbt-core|dbt-postgres|openlineage-airflow|openlineage-dbt)==|^(dbt-core|dbt-postgres|openlineage-airflow|openlineage-dbt)>=' "${REQ_FILE}" > "${TMP_REQ_FILE}"
  python -m pip install --no-cache-dir --upgrade pip
  python -m pip install --no-cache-dir -r "${TMP_REQ_FILE}"
  python -m pip install --no-cache-dir --no-deps dbt-postgres==1.8.0
  python -m pip install --no-cache-dir \
    "dbt-core==1.8.0" \
    "dbt-adapters>=0.1.0a2,<2.0" \
    "dbt-common>=1.0.2,<2.0" \
    "agate>=1.7.0,<2.0"
  python -m pip install --no-cache-dir apache-airflow-providers-openlineage

  echo "[bootstrap] Installing dbt packages..."
  dbt deps --project-dir "${DBT_DIR}" --profiles-dir "${DBT_DIR}"

  printf '%s' "${DESIRED_HASH}" > "${HASH_FILE}"
  echo "[bootstrap] Runtime ready."
else
  echo "[bootstrap] Runtime already up to date."
fi
