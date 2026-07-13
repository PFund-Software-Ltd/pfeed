"""High-level client for CryptoHFTData historical market data."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfeed.sources.crypto_hft_data.market_feed import CryptoHftDataMarketFeed

from pfeed.data_client import DataClient
from pfeed.sources.crypto_hft_data.mixin import CryptoHftDataMixin

__all__ = ["CryptoHftData"]


class CryptoHftData(CryptoHftDataMixin, DataClient):
    """Access CryptoHFTData discovery and historical market-data feeds."""

    market_feed: CryptoHftDataMarketFeed

    def list_exchanges(self) -> list[str]:
        """Return exchanges available to the configured account."""
        return self.data_source.get_batch_api().list_exchanges()

    def list_symbols(self, exchange: str, data_type: str | None = None) -> list[str]:
        """Return symbols available for an exchange and optional data type."""
        return self.data_source.get_batch_api().list_symbols(exchange, data_type)
