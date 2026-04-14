from __future__ import annotations

import io
import tempfile
import unittest
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from ingestion.config import DataSource
from ingestion.utils.source_normalizer import normalize_source_content


class SourceNormalizerTests(unittest.TestCase):
    def test_zip_csv_is_normalized_to_dataframe_and_csv(self) -> None:
        csv_bytes = b"IRIS;P22_POP\n010010000;859\n010020000;120\n"
        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("sample.csv", csv_bytes)

        source = DataSource(
            source_id="TEST_ZIP_CSV",
            name="Test ZIP CSV",
            url="https://example.test/sample.zip",
            millesime="2024",
            file_format="zip_csv",
            separator=";",
            expected_min_rows=1,
            expected_max_rows=10,
            key_columns=["IRIS", "P22_POP"],
        )

        normalized = normalize_source_content(source, archive_buffer.getvalue())

        self.assertEqual(normalized.detected_format, "zip_csv")
        self.assertEqual(normalized.row_count, 2)
        self.assertListEqual(list(normalized.dataframe.columns), ["iris", "p22_pop"])
        self.assertIn("010010000", normalized.csv_bytes.decode("utf-8"))

    def test_excel_sheet_detection_reads_sample_zrr_file(self) -> None:
        source = DataSource(
            source_id="TEST_XLS",
            name="Test XLS",
            url="https://example.test/zrr.xls",
            millesime="2021",
            file_format="xls",
            expected_min_rows=1,
            expected_max_rows=100_000,
            key_columns=["CODGEO"],
        )

        content = Path("data/diffusion-zonages-zrr-cog2021.xls").read_bytes()
        normalized = normalize_source_content(source, content)

        self.assertEqual(normalized.detected_format, "xls")
        self.assertGreater(normalized.row_count, 1)
        self.assertIn("codgeo", normalized.dataframe.columns)

    def test_gpkg_is_flattened_without_geometry(self) -> None:
        source = DataSource(
            source_id="TEST_GPKG",
            name="Test GPKG",
            url="https://example.test/test.gpkg",
            millesime="2022",
            file_format="gpkg",
            expected_min_rows=1,
            expected_max_rows=10,
            key_columns=["district", "nombre", "taux"],
            layer_name="districts_with_result_final",
            geometry_handling="drop",
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            gpkg_path = Path(tmp_dir) / "sample.gpkg"
            gdf = gpd.GeoDataFrame(
                {
                    "district": ["010010000"],
                    "nombre": [10.5],
                    "taux": [12.3],
                    "city": ["01001"],
                },
                geometry=[Point(0, 0)],
                crs="EPSG:4326",
            )
            gdf.to_file(gpkg_path, layer="districts_with_result_final", driver="GPKG")

            normalized = normalize_source_content(source, gpkg_path.read_bytes())

        self.assertEqual(normalized.detected_format, "gpkg")
        self.assertEqual(normalized.row_count, 1)
        self.assertNotIn("geometry", normalized.dataframe.columns)
        self.assertListEqual(
            list(normalized.dataframe.columns),
            ["district", "nombre", "taux", "city"],
        )


if __name__ == "__main__":
    unittest.main()
