"""
DAG Silver : transformations dbt après succès du DAG Bronze.
Déclenché automatiquement après dag_bronze_ingestion (sensor).
"""
from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "email": ["data-engineering@banque.fr"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

def _run_dbt_silver(**context) -> None:
    """Lance dbt run sur les modèles Silver et enregistre le résultat."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    import sqlalchemy
    from sqlalchemy import text
    from ingestion.config import PG_CONN_STRING

    run_id = context["run_id"]
    dbt_dir = "/opt/dbt"

    cmd = [
        "dbt", "run",
        "--select", "silver",
        "--project-dir", dbt_dir,
        "--profiles-dir", dbt_dir,
        "--vars", f'{{"pipeline_run_id": "{run_id}"}}',
    ]
    logger.info(f"Lancement dbt silver: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run silver échoué (rc={result.returncode}):\n{result.stderr}")

    logger.info("dbt Silver terminé avec succès")


def _run_dbt_tests_silver(**context) -> None:
    """Lance les tests dbt sur les modèles Silver."""
    dbt_dir = "/opt/dbt"
    cmd = [
        "dbt", "test",
        "--select", "silver",
        "--indirect-selection", "cautious",
        "--project-dir", dbt_dir,
        "--profiles-dir", dbt_dir,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info(result.stdout)

    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt test silver échoué:\n{result.stderr}")


with DAG(
    dag_id="silver_transform_esg_territorial",
    default_args=DEFAULT_ARGS,
    description="Transformations dbt Silver (nettoyage, référentiel territorial)",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "dbt", "esg"],
) as dag:

    run_silver = PythonOperator(
        task_id="dbt_run_silver",
        python_callable=_run_dbt_silver,
    )
    test_silver = PythonOperator(
        task_id="dbt_test_silver",
        python_callable=_run_dbt_tests_silver,
    )
    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_esg",
        trigger_dag_id="gold_esg_territorial",
        wait_for_completion=False,
    )

    run_silver >> test_silver >> trigger_gold
