"""
Upload et download de fichiers depuis/vers MinIO.
Utilise boto3 (S3-compatible).
"""
from __future__ import annotations

import io
import logging
import unicodedata
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from ingestion.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

logger = logging.getLogger(__name__)


def _sanitize_metadata_value(value: object) -> str:
    """
    S3/MinIO metadata must be ASCII-only.
    Keep values readable while stripping accents and unsupported punctuation.
    """
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    return ascii_value.strip() or "n-a"

def get_minio_client():
    """Retourne un client boto3 config pour MinIO"""
    return boto3.client(
        "s3",
        endpoint_url = f"http://{MINIO_ENDPOINT}",
        aws_access_key_id = MINIO_ACCESS_KEY,
        aws_secret_access_key = MINIO_SECRET_KEY,
        region_name = "us-east-1", #Fake, MinIO ne verifie pas ca
    )
    
def ensure_bucket(bucket: str) -> None:
    """Cree le bucket si il n'existe pas"""
    client = get_minio_client()
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)
        logger.info(f"Bucket '{bucket}' cree dans MinIO")
        
def upload_bytes(
    content: bytes,
    bucket: str,
    key: str,
    content_type: str = "application/octet-stream",
    metadata: Optional[dict[str, str]] = None,
) -> str:
    """
    Upload un contenu binaire dans MinIO.
    Retourne le chemin complet s3://bucket/key.
    """
    ensure_bucket(bucket)
    client = get_minio_client()
    
    extra_args: dict = {"ContentType": content_type}
    if metadata:
        extra_args["Metadata"] = {
            str(k): _sanitize_metadata_value(v)
            for k, v in metadata.items()
        }
        
    client.put_object(
        Bucket = bucket,
        Key = key,
        Body = content,
        **extra_args,
    )
    full_path = f"s3://{bucket}/{key}"
    logger.info(f"Fichier upload: {full_path} ({len(content):,} bytes)")
    return full_path

def download_bytes(bucket: str, key: str) -> bytes:
    """Télécharge un fichier depuis MinIO et retourne son contenu binaire."""
    client = get_minio_client()
    obj = client.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read()
    logger.debug(f"Fichier téléchargé: s3://{bucket}/{key} ({len(content):,} bytes)")
    return content

def build_bronze_key(source_id: str, millesime: str, filename: str) -> str:
    """
    Construit la clé MinIO pour la couche Bronze.
    Format: {source_id}/{millesime}/{filename}
    """
    return f"{source_id}/{millesime}/{filename}"

def build_timestamped_filename(
    prefix: str, millesime: str, extension: str = "csv"
) -> str:
    """
    Génère un nom de fichier horodaté pour le Bronze.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{millesime}_{ts}.{extension}"
