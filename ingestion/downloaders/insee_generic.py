"""
Téléchargeur générique des sources ouvertes.
Le type réel du fichier est détecté et normalisé dans BaseDownloader.
"""
from __future__ import annotations

from ingestion.config import SOURCE_CATALOG
from ingestion.downloaders._base import BaseDownloader


class OpenDataDownloader(BaseDownloader):
    """Téléchargeur HTTP générique pour les sources du catalogue."""

    def _download(self) -> bytes:
        return self._http_get(self.source.url)


# Instances ready
downloader_s01 = OpenDataDownloader(SOURCE_CATALOG["S01"])
downloader_s02 = OpenDataDownloader(SOURCE_CATALOG["S02"])
downloader_s03 = OpenDataDownloader(SOURCE_CATALOG["S03"])
downloader_s04 = OpenDataDownloader(SOURCE_CATALOG["S04"])
downloader_s05 = OpenDataDownloader(SOURCE_CATALOG["S05"])
downloader_s09 = OpenDataDownloader(SOURCE_CATALOG["S09"])
downloader_s11 = OpenDataDownloader(SOURCE_CATALOG["S11"])
