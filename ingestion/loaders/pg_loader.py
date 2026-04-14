"""
Charge les fichiers depuis MinIO vers les tables Bronze de PostgreSQL.
Règle fondamentale : AUCUNE transformation. Les données sont chargées telles quelles.
"""
from __future__ import annotations

import io
import logging
from typing import Optional

import pandas as pd
import sqlalchemy
from sqlalchemy import text

from ingestion.config import PG_CONN_STRING
from ingestion.loaders.minio_loader import download_bytes
from ingestion.utils.source_normalizer import normalize_dataframe_columns

logger = logging.getLogger(__name__)

# Colonnes à forcer en str pour eviter que pandas interprete les codes INSEE comme int
STRING_COLUMNS = {
    "IRIS", "IRISB", "COM", "CODGEO", "DEP", "REG",
    "code_iris", "code_bv", "code_commune", "code_insee",
    "DEPCOM", "ARR", "UU2020", "GRD_QUART", "TRIRIS",
}

def _get_dtype_overrides(df_columns: list[str]) -> dict:
    """Retourne le dict dtype pour forcer les colonnes géographiques en str."""
    return {
        col: str
        for col in df_columns
        if col.upper() in STRING_COLUMNS or col.lower() in {c.lower() for c in STRING_COLUMNS}
    }
    
def load_from_minio_to_bronze(
    minio_path: str,
    schema: str,
    table: str,
    sep: str = ",",
    encoding: str = "utf-8",
    if_exists: str = "replace",  # idempotent
    chunksize: int = 50_000,
) -> int:
    """
    Télécharge un CSV normalisé depuis MinIO et le charge dans PostgreSQL.
    Retourne le nombre de lignes chargées.

    Args:
        minio_path: Ex: 's3://bronze/S01/2021/raw_filosofi_2021.csv'
        schema: Schéma PG cible
        table: Nom de la table PG cible
        sep: Séparateur CSV
        encoding: Encodage du fichier
        if_exists: 'replace' (idempotence) ou 'append'
        chunksize: Nombre de lignes par batch d'insert
    """
    # Extraire bucket et key depuis le chemin s3://
    path_clean = minio_path.replace("s3://", "")
    bucket, key = path_clean.split("/", 1)
    
    logger.info(f"Telechargement depuis MinIO: {minio_path}")
    content = download_bytes(bucket=bucket, key=key)
    
    # Lecture pandas (Detection colonnes)
    df_sample = pd.read_csv(
        io.BytesIO(content), sep=sep, encoding=encoding, nrows=5, low_memory=False
    )
    dtype_map = _get_dtype_overrides(list(df_sample.columns))
    
    df = pd.read_csv(
        io.BytesIO(content),
        sep = sep,
        encoding = encoding,
        dtype = dtype_map,
        low_memory = False,
        na_values = ["s", "nd", "ND", "S", "nc", "NC", "", " ", "n.d.", "N/A"],
        keep_default_na = True,
    )
    
    df = normalize_dataframe_columns(df)
    
    row_count = len(df)
    logger.info(f"DataFrame chargé: {row_count:,} lignes, {len(df.columns)} colonnes")
    
    # Chargement par chunks
    engine = sqlalchemy.create_engine(
        PG_CONN_STRING,
        pool_pre_ping = True,
        pool_size = 5,
        max_overflow = 10,
    )
    
    try:
        with engine.begin() as conn:
            df.to_sql(
                name= table,
                schema=schema,
                con=conn,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                method="multi"
            )
        logger.info(f"Table {schema}.{table} chargée avec succès: {row_count:,} lignes")
    finally:
        engine.dispose()
        
    return row_count

def get_last_row_count(source_id: str) -> Optional[int]:
    """
    Retourne le nombre de lignes du dernier chargement réussi pour une source.
    Utilisé pour détecter les variations anormales de volumétrie.
    """
    engine = sqlalchemy.create_engine(PG_CONN_STRING, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT row_count
                    FROM bronze.ingestion_log
                    WHERE source_id = :source_id
                      AND status = 'SUCCESS'
                      AND row_count IS NOT NULL
                    ORDER BY ingested_at DESC
                    LIMIT 1
                """),
                {"source_id": source_id},
            )
            row = result.fetchone()
            return int(row[0]) if row else None
    except Exception:
        return None
    finally:
        engine.dispose()
