"""
DAG Bronze : téléchargement et chargement de toutes les sources ouvertes + interne.
Déclenchement manuel via Airflow ou `make run-bronze`.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-engineering@banque.fr"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=3),
}

def _ingest_open_source(source_id: str, table_name: str, **context) -> None:
    """Wrapper pour les sources ouvertes INSEE/ADEME."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    
    from ingestion.downloaders.insee_generic import (
        downloader_s01, downloader_s02, downloader_s03, downloader_s04, downloader_s05,
        downloader_s09,
        downloader_s11,
    )
    from ingestion.loaders.pg_loader import load_from_minio_to_bronze

    downloaders = {
        "S01": downloader_s01,
        "S02": downloader_s02,
        "S03": downloader_s03,
        "S04": downloader_s04,
        "S05": downloader_s05,
        "S09": downloader_s09,
        "S11": downloader_s11,
    }
    downloader = downloaders[source_id]

    run_id  = context["run_id"]
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    
    # DL + Upload MinIO + log
    result = downloader.run(
        pipeline_run_id = run_id,
        dag_id = dag_id,
        task_id = task_id,
    )
    
    # Chargement PGSQL Bronze
    load_from_minio_to_bronze(
        minio_path= result["minio_path"],
        schema="bronze",
        table=table_name,
        sep=result.get("normalized_separator", ","),
        encoding=result.get("normalized_encoding", "utf-8"),
        if_exists="replace",
    )
    
    logger.info(
        f"[{source_id}] Pipeline Bronze complet: "
        f"{result['row_count']:,} lignes -> bronze.{table_name}"
    )

def _ingest_internal_source(**context) -> None:
    """Chargement du fichier interne cartographie_bassin_vie.csv."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from ingestion.downloaders.internal_bv import load_internal_bv_file
    from ingestion.loaders.pg_loader import load_from_minio_to_bronze

    run_id  = context["run_id"]
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id

    result = load_internal_bv_file(
        pipeline_run_id=run_id,
        dag_id=dag_id,
        task_id=task_id,
    )

    load_from_minio_to_bronze(
        minio_path=result["minio_path"],
        schema="bronze",
        table="raw_internal_bv_iris",
        sep=result.get("normalized_separator", ","),
        encoding=result.get("normalized_encoding", "utf-8"),
        if_exists="replace",
    )

with DAG(
    dag_id = "bronze_ingestion_esg_territorial",
    default_args = DEFAULT_ARGS,
    description = "Ingestion manuelle de toutes les sources ESG territorial (open data + interne)",
    schedule_interval=None,
    start_date = datetime(2024, 1, 1),
    catchup = False,
    max_active_runs = 1,
    tags = ["bronze", "esg", "territorial"],
    doc_md="""
## DAG Bronze – Ingestion ESG Territorial

Télécharge et charge dans PostgreSQL Bronze :
| Source | Contenu | Millésime |
|--------|---------|-----------|
| S01 | FILOSOFI revenus/pauvreté IRIS | 2021 |
| S02 | RP Population IRIS | 2022 |
| S03 | RP Activité IRIS | 2022 |
| S04 | RP Logement IRIS | 2022 |
| S05 | BPE Équipements IRIS | 2024 |
| S09 | Passoires énergétiques IRIS | 2022 |
| S11 | ZRR communes | 2021 |
| S12 | Cartographie interne BV/IRIS | v1 |

**Audit** : `bronze.ingestion_log`
**Monitoring** : Grafana > dashboard "ESG Pipeline"
**Alertes** :Slack #data-alerts
    """,
) as dag:
    
    start = DummyOperator(task_id="start")
    end   = DummyOperator(task_id="end")

    t_s01 = PythonOperator(
        task_id="ingest_s01_filosofi",
        python_callable=_ingest_open_source,
        op_kwargs={"source_id": "S01", "table_name": "raw_filosofi_iris"},
    )
    t_s02 = PythonOperator(
        task_id="ingest_s02_rp_population",
        python_callable=_ingest_open_source,
        op_kwargs={"source_id": "S02", "table_name": "raw_rp_population_iris"},
    )
    t_s03 = PythonOperator(
        task_id="ingest_s03_rp_activite",
        python_callable=_ingest_open_source,
        op_kwargs={"source_id": "S03", "table_name": "raw_rp_activite_iris"},
    )
    t_s04 = PythonOperator(
        task_id="ingest_s04_rp_logement",
        python_callable=_ingest_open_source,
        op_kwargs={"source_id": "S04", "table_name": "raw_rp_logement_iris"},
    )
    t_s05 = PythonOperator(
        task_id="ingest_s05_bpe",
        python_callable=_ingest_open_source,
        op_kwargs={"source_id": "S05", "table_name": "raw_bpe_iris"},
    )
    t_s09 = PythonOperator(
        task_id="ingest_s09_passoires_energetiques",
        python_callable=_ingest_open_source,
        op_kwargs={"source_id": "S09", "table_name": "raw_passoires_energetiques_iris"},
    )
    t_s11 = PythonOperator(
        task_id="ingest_s11_zrr",
        python_callable=_ingest_open_source,
        op_kwargs={"source_id": "S11", "table_name": "raw_zrr_communes"},
    )
    t_s12 = PythonOperator(
        task_id="ingest_s12_internal_bv",
        python_callable=_ingest_internal_source,
    )
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id="silver_transform_esg_territorial",
        wait_for_completion=False,
    )

    # Toutes les sources en parallele, puis déclenchement automatique de Silver.
    start >> [t_s01, t_s02, t_s03, t_s04, t_s05, t_s09, t_s11, t_s12] >> end >> trigger_silver
