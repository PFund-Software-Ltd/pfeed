from __future__ import annotations

from pfeed.sources.crypto_hft_data.source import CryptoHftDataSource


class CryptoHftDataMixin:
    data_source: CryptoHftDataSource

    @staticmethod
    def _create_data_source() -> CryptoHftDataSource:
        return CryptoHftDataSource()
