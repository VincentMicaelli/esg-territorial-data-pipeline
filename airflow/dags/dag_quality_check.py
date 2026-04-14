"""
DAG Qualité : Great Expectations sur la table Gold finale.
Génère un rapport HTML.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "email": ["data-engineering@banque.fr"],
    "email_on_failure": True,
    "retries": 0,
    "execution_timeout": timedelta(hours=1),
}


def _run_great_expectations(**context) -> None:
    """Exécute les suites GX sur la table Gold finale."""
    import great_expectations as gx
    from great_expectations.core.batch import BatchRequest
    from ingestion.config import PG_CONN_STRING

    context_gx = gx.get_context(
        context_root_dir="/opt/airflow/gx",
        runtime_environment={"GX_PG_CONN_STRING": PG_CONN_STRING},
    )

    suite_name = "gold_esg_final_suite"
    batch_req = BatchRequest(
        datasource_name="esg_postgres",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="gold.gold_esg_iris_final",
    )

    if suite_name not in context_gx.list_expectation_suite_names():
        raise ValueError(
            f"Suite Great Expectations introuvable: {suite_name}. "
            "Vérifier le montage du dossier great_expectations dans /opt/airflow/gx."
        )

    validator = context_gx.get_validator(
        batch_request=batch_req,
        expectation_suite_name=suite_name,
    )

    results = validator.validate()

    success_pct = results["statistics"]["success_percent"]
    logger.info(f"GX Validation: {success_pct:.1f}% de tests passés")

    if success_pct < 90:
        raise ValueError(
            f"Qualité insuffisante: seulement {success_pct:.1f}% des contrôles GX passés "
            f"(seuil: 90%). Consulter le rapport GX pour détail."
        )

    logger.info("GX Validation réussie (>= 90%)")


with DAG(
    dag_id="quality_check_esg_territorial",
    default_args=DEFAULT_ARGS,
    description="Contrôle qualité Great Expectations sur la table ESG Gold finale",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality", "gx", "esg"],
) as dag:

    quality_check = PythonOperator(
        task_id="run_great_expectations",
        python_callable=_run_great_expectations,
    )
