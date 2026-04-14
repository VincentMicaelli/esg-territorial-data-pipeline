#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXPORT_DIR="${ROOT_DIR}/livrable/exports"

mkdir -p "${EXPORT_DIR}"

docker compose exec -T postgres bash -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --csv -c "SELECT * FROM gold.gold_esg_iris_final ORDER BY iris_code"' \
  > "${EXPORT_DIR}/gold_esg_iris_final.csv"

docker compose exec -T postgres bash -lc \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" --csv -c "SELECT * FROM gold.gold_esg_bassin_vie_final ORDER BY bv_code"' \
  > "${EXPORT_DIR}/gold_esg_bassin_vie_final.csv"

echo "Exports written to ${EXPORT_DIR}"
