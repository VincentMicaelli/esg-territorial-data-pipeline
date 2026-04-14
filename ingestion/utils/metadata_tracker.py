"""
Enregistre chaque ingestion dans bronze.ingestion_log.
Chaque appel crée une ligne immuable d'audit.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import sqlalchemy
from sqlalchemy import text

from ingestion.config import PG_CONN_STRING

logger = logging.getLogger(__name__)

def log_ingestion(
    source_id: str,
    source_name: str,
    file_name: str,
    minio_path: str,
    pipeline_run_id: str,
    status: str = "SUCCESS",
    millesime: Optional[str] = None,
    source_url: Optional[str] = None,
    file_size_bytes: Optional[int] = None,
    row_count: Optional[int] = None,
    checksum_md5: Optional[str] = None,
    dag_id: Optional[str] = None,
    task_id: Optional[str] = None,
    error_message: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Insère une ligne dans bronze.ingestion_log.
    Appelé après chaque téléchargement, qu'il soit réussi ou échoué.
    """
    engine = sqlalchemy.create_engine(PG_CONN_STRING, pool_pre_ping=True)
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO bronze.ingestion_log (
                        source_id, source_name, source_url, millesime,
                        file_name, minio_path, file_size_bytes, row_count,
                        checksum_md5, ingested_at, pipeline_run_id, dag_id, task_id,
                        status, error_message, notes
                    ) VALUES (
                        :source_id, :source_name, :source_url, :millesime,
                        :file_name, :minio_path, :file_size_bytes, :row_count,
                        :checksum_md5, NOW(), :pipeline_run_id, :dag_id, :task_id,
                        :status, :error_message, :notes
                    )
                """),
                {
                    "source_id": source_id,
                    "source_name": source_name,
                    "source_url": source_url,
                    "millesime": millesime,
                    "file_name": file_name,
                    "minio_path": minio_path,
                    "file_size_bytes": file_size_bytes,
                    "row_count": row_count,
                    "checksum_md5": checksum_md5,
                    "pipeline_run_id": pipeline_run_id,
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "status": status,
                    "error_message": error_message,
                    "notes": notes,
                },
            )
        logger.info(f"[audit] Ingestion loguée — source={source_id} status={status} rows={row_count}")
    except Exception as exc:
        # Ne jamais faire planter l'ingestion à cause du logging
        logger.error(f"[audit] ÉCHEC écriture ingestion_log: {exc}")
    finally:
        engine.dispose()
