"""
DAG Gold : construction des indicateurs ESG finaux.
Déclenché après Silver avec succes.
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
    "execution_timeout": timedelta(hours=1),
}


def _run_dbt_gold(**context) -> None:
    dbt_dir = "/opt/dbt"
    run_id  = context["run_id"]

    cmd = [
        "dbt", "run",
        "--select", "gold",
        "--project-dir", dbt_dir,
        "--profiles-dir", dbt_dir,
        "--vars", f'{{"pipeline_run_id": "{run_id}"}}',
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run gold échoué:\n{result.stderr}")


def _run_dbt_tests_gold(**context) -> None:
    dbt_dir = "/opt/dbt"
    cmd = ["dbt", "test", "--select", "gold",
           "--project-dir", dbt_dir, "--profiles-dir", dbt_dir]
    result = subprocess.run(cmd, capture_output=True, text=True)
    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt test gold échoué:\n{result.stderr}")


def _push_pipeline_metrics(**context) -> None:
    """Pousse les métriques finales vers Prometheus Pushgateway."""
    import sqlalchemy
    from sqlalchemy import text
    from ingestion.config import PG_CONN_STRING

    engine = sqlalchemy.create_engine(PG_CONN_STRING)
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT
                    COUNT(*) AS nb_iris,
                    ROUND(AVG(pct_completude)::numeric, 1) AS completude_moy,
                    ROUND(AVG(score_esg_0_100)::numeric, 1) AS score_esg_moy
                FROM gold.gold_esg_iris_final
            """)).fetchone()
        logger.info(
            f"Gold finale: {row.nb_iris} IRIS | "
            f"complétude={row.completude_moy}% | "
            f"score ESG moyen={row.score_esg_moy}"
        )
    finally:
        engine.dispose()


with DAG(
    dag_id="gold_esg_territorial",
    default_args=DEFAULT_ARGS,
    description="Construction des indicateurs ESG Gold par IRIS et bassin de vie",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "dbt", "esg", "livrable"],
) as dag:

    run_gold   = PythonOperator(task_id="dbt_run_gold",   python_callable=_run_dbt_gold)
    test_gold  = PythonOperator(task_id="dbt_test_gold",  python_callable=_run_dbt_tests_gold)
    metrics    = PythonOperator(task_id="push_metrics",   python_callable=_push_pipeline_metrics)

    # Trigger le DAG qualité après Gold
    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_check",
        trigger_dag_id="quality_check_esg_territorial",
        wait_for_completion=False,
    )

    run_gold >> test_gold >> metrics >> trigger_quality
