#!/usr/bin/env bash
set -euo pipefail

bash /opt/airflow/bootstrap/bootstrap.sh

airflow db migrate

if airflow users list | grep -q "${AIRFLOW_ADMIN_USER}"; then
  echo "[bootstrap] Airflow admin user already exists."
else
  airflow users create \
    --username "${AIRFLOW_ADMIN_USER}" \
    --firstname Admin \
    --lastname ESG \
    --role Admin \
    --email admin@banque.fr \
    --password "${AIRFLOW_ADMIN_PASSWORD}"
fi
