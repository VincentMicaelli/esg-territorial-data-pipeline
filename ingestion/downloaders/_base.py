"""
Classe de base abstraite pour tous les téléchargeurs.
Garantit une interface uniforme et une gestion d'erreur cohérente.
"""
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from ingestion.config import DataSource
from ingestion.loaders.minio_loader import (
    build_bronze_key,
    build_timestamped_filename,
    upload_bytes,
)
from ingestion.utils.source_normalizer import normalize_source_content
from ingestion.utils.metadata_tracker import log_ingestion
from ingestion.utils.validators import compute_md5

logger = logging.getLogger(__name__)

# Timeout HTTP : 30s connexion, 300s lecture
HTTP_TIMEOUT = (30, 300)

class BaseDownloader(ABC):
    """
    Classe de base pour les téléchargeurs de données ouvertes.
    Implémente : téléchargement HTTP avec retry, normalisation tabulaire,
    upload MinIO, validation schéma, logging d'audit.
    """

    def __init__(self, source: DataSource):
        self.source = source
        self.logger = logging.getLogger(f"{__name__}.{source.source_id}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=10, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _http_get(self, url: str) -> bytes:
        """Télécharge une URL avec retry automatique (3 tentatives, backoff exponentiel)."""
        self.logger.info(f"GET {url}")
        resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={
            "User-Agent": "ESG-Territorial-Pipeline/1.0 (banque-reseau; data-engineering@banque.fr)"
        })
        resp.raise_for_status()
        return resp.content

    def run(self, pipeline_run_id: str, dag_id: str = "", task_id: str = "") -> dict:
        """
        Exécute le cycle complet : download → validate → upload MinIO → log.
        Retourne un dict de métadonnées pour le log d'audit.
        """
        start_ts = time.time()
        filename = build_timestamped_filename(
            prefix=f"raw_{self.source.source_id.lower()}",
            millesime=self.source.millesime,
            extension="csv",
        )
        minio_key = build_bronze_key(self.source.source_id, self.source.millesime, filename)
        full_minio_path = f"s3://bronze/{minio_key}"

        try:
            # ── 1. Téléchargement ────────────────────────────────────────────
            raw_content = self._download()

            # ── 2. Normalisation tabulaire ──────────────────────────────────
            normalized = normalize_source_content(self.source, raw_content)
            content = normalized.csv_bytes

            # ── 3. Checksum MD5 ──────────────────────────────────────────────
            checksum = compute_md5(content)

            row_count = normalized.row_count

            # ── 4. Upload MinIO ──────────────────────────────────────────────
            upload_bytes(
                content=content,
                bucket="bronze",
                key=minio_key,
                content_type="text/csv; charset=utf-8",
                metadata={
                    "source_id": self.source.source_id,
                    "source_name": self.source.name,
                    "millesime": self.source.millesime,
                    "checksum_md5": checksum,
                    "row_count": str(row_count),
                    "pipeline_run_id": pipeline_run_id,
                    "detected_format": normalized.detected_format,
                },
            )

            duration = round(time.time() - start_ts, 2)
            self.logger.info(
                f"[{self.source.source_id}] ✓ Ingestion réussie: "
                f"{row_count:,} lignes | {len(content):,} bytes | {duration}s"
            )

            # ── 5. Log d'audit ───────────────────────────────────────────────
            log_ingestion(
                source_id=self.source.source_id,
                source_name=self.source.name,
                file_name=filename,
                minio_path=full_minio_path,
                pipeline_run_id=pipeline_run_id,
                dag_id=dag_id,
                task_id=task_id,
                status="SUCCESS",
                millesime=self.source.millesime,
                source_url=self.source.url,
                file_size_bytes=len(content),
                row_count=row_count,
                checksum_md5=checksum,
            )

            return {
                "source_id": self.source.source_id,
                "minio_path": full_minio_path,
                "row_count": row_count,
                "checksum_md5": checksum,
                "file_size_bytes": len(content),
                "status": "SUCCESS",
                "normalized_separator": normalized.separator,
                "normalized_encoding": normalized.encoding,
                "detected_format": normalized.detected_format,
            }

        except Exception as exc:
            duration = round(time.time() - start_ts, 2)
            error_msg = str(exc)
            self.logger.error(f"[{self.source.source_id}] ✗ Ingestion échouée ({duration}s): {error_msg}")

            log_ingestion(
                source_id=self.source.source_id,
                source_name=self.source.name,
                file_name=filename,
                minio_path=full_minio_path,
                pipeline_run_id=pipeline_run_id,
                dag_id=dag_id,
                task_id=task_id,
                status="FAILED",
                millesime=self.source.millesime,
                source_url=self.source.url,
                error_message=error_msg,
            )
            raise

    @abstractmethod
    def _download(self) -> bytes:
        """Télécharge le fichier source. Retourne le contenu brut en bytes."""
