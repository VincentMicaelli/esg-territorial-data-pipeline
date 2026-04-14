from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from ingestion.loaders.pg_loader import load_from_minio_to_bronze


class PgLoaderTests(unittest.TestCase):
    @patch("ingestion.loaders.pg_loader.pd.DataFrame.to_sql", autospec=True)
    @patch("ingestion.loaders.pg_loader.sqlalchemy.create_engine")
    @patch("ingestion.loaders.pg_loader.download_bytes")
    def test_load_from_minio_uses_normalized_csv_headers(
        self,
        mock_download_bytes: MagicMock,
        mock_create_engine: MagicMock,
        mock_to_sql: MagicMock,
    ) -> None:
        mock_download_bytes.return_value = (
            "Code commune,Libellé commune,Value\n01001,Test,42\n".encode("utf-8")
        )

        connection_ctx = MagicMock()
        connection_ctx.__enter__.return_value = MagicMock()
        connection_ctx.__exit__.return_value = False

        fake_engine = MagicMock()
        fake_engine.begin.return_value = connection_ctx
        mock_create_engine.return_value = fake_engine

        row_count = load_from_minio_to_bronze(
            minio_path="s3://bronze/S11/2021/raw_s11_2021.csv",
            schema="bronze",
            table="raw_zrr_communes",
            sep=",",
            encoding="utf-8",
        )

        written_df = mock_to_sql.call_args[0][0]
        self.assertEqual(row_count, 1)
        self.assertListEqual(written_df.columns.tolist(), ["code_commune", "libelle_commune", "value"])


if __name__ == "__main__":
    unittest.main()
