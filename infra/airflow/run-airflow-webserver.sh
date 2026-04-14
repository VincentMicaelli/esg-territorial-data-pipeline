#!/usr/bin/env bash
set -euo pipefail

bash /opt/airflow/bootstrap/bootstrap.sh
exec airflow webserver
