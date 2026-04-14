"""
Chargeur du fichier interne cartographie_bassin_vie.csv.
"""
from __future__ import annotations

import logging
import os
import time

import pandas as pd

from ingestion.config import SOURCE_CATALOG, INTERNAL_BV_FILE_PATH
from ingestion.loaders.minio_loader import (
    build_bronze_key,
    build_timestamped_filename,
    upload_bytes,
)
from ingestion.utils.metadata_tracker import log_ingestion
from ingestion.utils.source_normalizer import CSV_ENCODING, CSV_SEPARATOR, normalize_dataframe_columns
from ingestion.utils.validators import compute_md5

logger = logging.getLogger(__name__)


def _read_internal_bv_csv(file_path: str, separator: str, preferred_encoding: str) -> pd.DataFrame:
    """Lit le fichier interne avec fallback sur les encodages Windows courants."""
    encodings_to_try = [preferred_encoding, "cp1252", "latin-1"]
    last_error: UnicodeDecodeError | None = None

    for encoding in encodings_to_try:
        try:
            return pd.read_csv(
                file_path,
                sep=separator,
                encoding=encoding,
                dtype=str,
                low_memory=False,
            )
        except UnicodeDecodeError as exc:
            last_error = exc

    if last_error is not None:
        raise last_error
    raise ValueError(f"Impossible de lire le fichier interne: {file_path}")


def load_internal_bv_file(
    pipeline_run_id: str,
    dag_id: str = "",
    task_id: str = "",
    file_path: str = INTERNAL_BV_FILE_PATH,
) -> dict:
    """
    Charge le fichier interne BV/IRIS dans MinIO Bronze et PostgreSQL Bronze.
    Vérifie la présence des colonnes code_iris et code_bv.
    """
    source = SOURCE_CATALOG["S12"]
    start_ts = time.time()

    if not os.path.exists(file_path):
        error_msg = (
            f"Fichier interne introuvable: {file_path}. "
            f"Vérifier que le volume Docker est correctement monté "
            f"(voir docker-compose.yml section 'volumes' du service airflow-scheduler)."
        )
        logger.error(error_msg)
        log_ingestion(
            source_id="S12", source_name=source.name,
            file_name=os.path.basename(file_path), minio_path="",
            pipeline_run_id=pipeline_run_id, status="FAILED",
            error_message=error_msg,
        )
        raise FileNotFoundError(error_msg)

    # Lecture et validation
    df = _read_internal_bv_csv(
        file_path=file_path,
        separator=source.separator,
        preferred_encoding=source.encoding,
    )

    # Normaliser les noms de colonnes (minuscules, underscores)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Mapping des noms de colonnes du fichier réel vers le schéma attendu
    column_mapping = {
        "iris": "code_iris",
        "code_bassin_de_vie": "code_bv",
        "libelle_bassin_de_vie": "lib_bv",
        "lib_iris": "lib_iris",
        # Si les colonnes sont déjà au bon format, pas de renommage
        "code_iris": "code_iris",
        "code_bv": "code_bv",
    }
    df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns}, inplace=True)

    required_cols = {"code_iris", "code_bv"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"Colonnes manquantes dans cartographie_bassin_vie.csv: {missing}. "
            f"Colonnes disponibles: {list(df.columns)}"
        )

    row_count  = len(df)
    logger.info(f"[S12] Fichier interne chargé: {row_count} lignes")

    # Upload MinIO
    df = normalize_dataframe_columns(df)
    normalized_content = df.to_csv(index=False, sep=CSV_SEPARATOR).encode(CSV_ENCODING)

    checksum = compute_md5(normalized_content)
    filename = build_timestamped_filename("raw_internal_bv_iris", source.millesime)
    minio_key = build_bronze_key("S12", source.millesime, filename)
    full_path = f"s3://bronze/{minio_key}"

    upload_bytes(
        content=normalized_content, bucket="bronze", key=minio_key,
        content_type="text/csv; charset=utf-8",
        metadata={
            "source_id": "S12", "millesime": source.millesime,
            "checksum_md5": checksum, "pipeline_run_id": pipeline_run_id,
        },
    )

    duration = round(time.time() - start_ts, 2)
    log_ingestion(
        source_id="S12", source_name=source.name,
        file_name=filename, minio_path=full_path,
        pipeline_run_id=pipeline_run_id, dag_id=dag_id, task_id=task_id,
        status="SUCCESS", millesime=source.millesime,
        file_size_bytes=len(normalized_content), row_count=row_count,
        checksum_md5=checksum,
        notes=f"Fichier interne chargé depuis {file_path}",
    )

    return {
        "source_id": "S12",
        "minio_path": full_path,
        "row_count": row_count,
        "checksum_md5": checksum,
        "status": "SUCCESS",
        "normalized_separator": CSV_SEPARATOR,
        "normalized_encoding": CSV_ENCODING,
    }
