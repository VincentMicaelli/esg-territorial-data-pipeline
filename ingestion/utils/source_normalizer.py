"""
Normalisation des sources hétérogènes vers un DataFrame tabulaire et un CSV UTF-8.
"""
from __future__ import annotations

import io
import re
import tempfile
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd

from ingestion.config import DataSource
from ingestion.utils.validators import validate_csv_schema

CSV_SEPARATOR = ","
CSV_ENCODING = "utf-8"

ZIP_SIGNATURE = b"PK\x03\x04"
OLE_SIGNATURE = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
SQLITE_SIGNATURE = b"SQLite format 3\x00"


@dataclass
class NormalizedSource:
    dataframe: pd.DataFrame
    csv_bytes: bytes
    detected_format: str
    row_count: int
    separator: str = CSV_SEPARATOR
    encoding: str = CSV_ENCODING


def normalize_source_content(source: DataSource, content: bytes) -> NormalizedSource:
    detected_format = detect_source_format(source, content)
    dataframe = load_source_to_dataframe(source, content, detected_format)
    dataframe = normalize_dataframe_columns(dataframe)
    validate_csv_schema(dataframe, source, raise_on_error=True)

    csv_bytes = dataframe.to_csv(index=False, sep=CSV_SEPARATOR).encode(CSV_ENCODING)
    return NormalizedSource(
        dataframe=dataframe,
        csv_bytes=csv_bytes,
        detected_format=detected_format,
        row_count=len(dataframe),
    )


def detect_source_format(source: DataSource, content: bytes) -> str:
    hinted_format = source.file_format.lower()
    url_path = source.url.lower()

    if content.startswith(SQLITE_SIGNATURE):
        return "gpkg"

    if content.startswith(OLE_SIGNATURE):
        return "xls"

    if content.startswith(ZIP_SIGNATURE):
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            names = [name.lower() for name in archive.namelist()]
            if "[content_types].xml" in names and any(name.startswith("xl/") for name in names):
                return "xlsx"
            if any(name.endswith(".shp") for name in names):
                return "zip_shp"
            if any(name.endswith((".csv", ".txt")) for name in names):
                return "zip_csv"
        return hinted_format

    if url_path.endswith(".gpkg"):
        return "gpkg"
    if url_path.endswith(".xlsx"):
        return "xlsx"
    if url_path.endswith(".xls"):
        return "xls"
    if url_path.endswith(".csv"):
        return "csv"
    if url_path.endswith(".geojson") or hinted_format == "geojson":
        return "geojson"

    return hinted_format


def load_source_to_dataframe(
    source: DataSource,
    content: bytes,
    detected_format: str,
) -> pd.DataFrame:
    if detected_format == "csv":
        return pd.read_csv(
            io.BytesIO(content),
            sep=source.separator,
            encoding=source.encoding,
            dtype=str,
            low_memory=False,
        )

    if detected_format == "zip_csv":
        member_name, raw_bytes = _read_zip_member(
            content,
            pattern=source.archive_member_pattern,
            suffixes=(".csv", ".txt"),
        )
        source_name = member_name.lower()
        separator = source.separator
        if source_name.endswith(".txt") and separator == ",":
            separator = ";"
        return pd.read_csv(
            io.BytesIO(raw_bytes),
            sep=separator,
            encoding=source.encoding,
            dtype=str,
            low_memory=False,
        )

    if detected_format in {"xls", "xlsx"}:
        return _read_excel(content, source)

    if detected_format == "gpkg":
        return _read_gpkg(content, source)

    if detected_format == "zip_shp":
        return _read_zipped_shapefile(content, source)

    raise ValueError(f"Format non supporté pour {source.source_id}: {detected_format}")


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed_columns: list[str] = []
    seen: dict[str, int] = {}

    for column in df.columns:
        normalized = _normalize_column_name(str(column))
        if not normalized:
            normalized = "col"
        seen[normalized] = seen.get(normalized, 0) + 1
        if seen[normalized] > 1:
            normalized = f"{normalized}_{seen[normalized]}"
        renamed_columns.append(normalized)

    normalized_df = df.copy()
    normalized_df.columns = renamed_columns
    return normalized_df


def _normalize_column_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", name)
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    normalized = normalized.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    return normalized.strip("_")


def _read_excel(content: bytes, source: DataSource) -> pd.DataFrame:
    excel_file = pd.ExcelFile(io.BytesIO(content))
    if source.sheet_name is not None:
        return pd.read_excel(excel_file, sheet_name=source.sheet_name)

    last_error: Exception | None = None
    for sheet_name in excel_file.sheet_names:
        for header_row in range(0, 10):
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, header=header_row, dtype=str)
            except Exception as exc:
                last_error = exc
                continue

            if df.empty:
                continue

            preview = normalize_dataframe_columns(df.head(10))
            normalized_keys = {column.lower() for column in source.key_columns}
            if not normalized_keys or normalized_keys.issubset(set(preview.columns)):
                return df

    if last_error is not None:
        raise last_error

    raise ValueError(
        f"Aucune feuille exploitable trouvée pour la source {source.source_id}: {excel_file.sheet_names}"
    )


def _read_gpkg(content: bytes, source: DataSource) -> pd.DataFrame:
    with tempfile.NamedTemporaryFile(suffix=".gpkg") as tmp_file:
        tmp_file.write(content)
        tmp_file.flush()
        layers = fiona.listlayers(tmp_file.name)
        layer_name = source.layer_name or layers[0]
        gdf = gpd.read_file(tmp_file.name, layer=layer_name)

    if source.geometry_handling != "keep" and "geometry" in gdf.columns:
        gdf = gdf.drop(columns=["geometry"])

    return pd.DataFrame(gdf)


def _read_zipped_shapefile(content: bytes, source: DataSource) -> pd.DataFrame:
    with tempfile.TemporaryDirectory() as tmp_dir:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            archive.extractall(tmp_dir)

        pattern = re.compile(source.archive_member_pattern, re.IGNORECASE) if source.archive_member_pattern else None
        shapefiles = sorted(Path(tmp_dir).rglob("*.shp"))
        if pattern is not None:
            shapefiles = [path for path in shapefiles if pattern.search(str(path))]

        if not shapefiles:
            raise ValueError(f"Aucun shapefile trouvé dans l'archive pour {source.source_id}")

        gdf = gpd.read_file(shapefiles[0])
        if source.geometry_handling != "keep" and "geometry" in gdf.columns:
            gdf = gdf.drop(columns=["geometry"])
        return pd.DataFrame(gdf)


def _read_zip_member(
    content: bytes,
    pattern: str | None,
    suffixes: tuple[str, ...],
) -> tuple[str, bytes]:
    regex = re.compile(pattern, re.IGNORECASE) if pattern else None
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        members = [
            member for member in archive.namelist()
            if member.lower().endswith(suffixes)
        ]
        if regex is not None:
            members = [member for member in members if regex.search(member)]
        if not members:
            raise ValueError(f"Aucun fichier {suffixes} trouvé dans l'archive")
        member_name = members[0]
        with archive.open(member_name) as member_file:
            return member_name, member_file.read()
