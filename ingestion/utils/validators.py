"""
Validation basique des fichiers avant chargement.
Ces contrôles sont intentionnellement légers (Bronze = brut).
La qualité fine est gérée par Great Expectations sur Silver/Gold.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Optional

import pandas as pd

from ingestion.config import DataSource

logger = logging.getLogger(__name__)


def compute_md5(content: bytes) -> str:
    """Calcule le MD5 d'un contenu binaire."""
    return hashlib.md5(content).hexdigest()


def validate_csv_schema(
    df: pd.DataFrame,
    source: DataSource,
    raise_on_error: bool = True,
) -> list[str]:
    """
    Vérifie que les colonnes clés attendues sont présentes.
    Retourne la liste des erreurs (vide si OK).
    """
    errors: list[str] = []

    # Contrôle des colonnes clés
    df_cols_lower = {c.lower() for c in df.columns}
    for col in source.key_columns:
        if col.lower() not in df_cols_lower:
            errors.append(f"Colonne attendue manquante: '{col}'")

    # Contrôle du volume
    n = len(df)
    if n < source.expected_min_rows:
        errors.append(
            f"Nombre de lignes trop faible: {n} < {source.expected_min_rows} (minimum attendu)"
        )
    if n > source.expected_max_rows:
        errors.append(
            f"Nombre de lignes suspect: {n} > {source.expected_max_rows} (maximum attendu)"
        )

    if errors:
        for err in errors:
            logger.error(f"[{source.source_id}] Validation échouée: {err}")
        if raise_on_error:
            raise ValueError(
                f"Validation source {source.source_id} échouée: {'; '.join(errors)}"
            )

    return errors


def validate_row_count_delta(
    new_count: int,
    previous_count: Optional[int],
    source_id: str,
    max_delta_pct: float = 0.20,
) -> None:
    """
    Alerte si le nombre de lignes change de plus de max_delta_pct par rapport
    au dernier chargement connu. N'échoue pas — logue seulement un warning.
    """
    if previous_count is None or previous_count == 0:
        return

    delta = abs(new_count - previous_count) / previous_count
    if delta > max_delta_pct:
        logger.warning(
            f"[{source_id}] Variation importante du nombre de lignes: "
            f"{previous_count} → {new_count} ({delta:.1%} de variation, seuil={max_delta_pct:.0%}). "
            f"Vérifier si une mise à jour de source a eu lieu."
        )