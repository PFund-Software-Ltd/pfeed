# pyright: reportArgumentType=false
"""CryptoHFTData source metadata and SDK construction."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast

if TYPE_CHECKING:
    from cryptohftdata import CryptoHFTDataClient

    from pfeed.sources.crypto_hft_data.product import CryptoHftDataProduct

import os

from pfeed.enums import (
    DataAccessType,
    DataCategory,
    DataProviderType,
    DataSource,
    DataType,
)
from pfeed.sources.data_provider_source import DataProviderSource
from pfeed.sources.source_metadata import SourceMetadata

__all__ = ["CryptoHftDataSource"]


class CryptoHftDataSource(DataProviderSource):
    """Historical crypto data distributed by CryptoHFTData."""

    NAME: ClassVar[DataSource] = DataSource.CRYPTO_HFT_DATA
    METADATA: ClassVar[SourceMetadata] = SourceMetadata(
        data_origin="https://cryptohftdata.com",
        data_categories={
            DataCategory.MARKET_DATA: {
                DataType.TICK: [
                    "CRYPTO",
                    "PERPETUAL",
                    "INVERSE-PERPETUAL",
                    "FUTURE",
                    "INVERSE-FUTURE",
                ],
            },
        },
        provider_type=DataProviderType.DISTRIBUTOR,
        access_type=DataAccessType.FREE_TIER,
        api_key_required=False,
        docs_url="https://cryptohftdata.com/docs",
    )

    def _get_api_key(self) -> str | None:
        return (
            os.getenv("CRYPTOHFTDATA_API_KEY")
            or os.getenv("CRYPTO_HFT_DATA_API_KEY")
            or os.getenv("CHD_API_KEY")
        )

    def get_batch_api(self) -> CryptoHFTDataClient:
        """Create the official SDK lazily so the dependency remains optional."""
        if self._batch_api is None:
            try:
                from cryptohftdata import CryptoHFTDataClient
            except ImportError as exc:
                raise ImportError(
                    'CryptoHFTData support requires `pip install "pfeed[cryptohftdata]"`'
                ) from exc
            self._batch_api = CryptoHFTDataClient(api_key=self._api_key)
        return cast("CryptoHFTDataClient", self._batch_api)

    def create_product(
        self, basis: str, symbol: str = "", **specs: Any
    ) -> CryptoHftDataProduct:
        exchange = specs.pop("exchange", None)
        if not exchange:
            raise ValueError("exchange is required for CryptoHFTData products")

        from pfund.entities.products import ProductFactory

        Product = cast(
            "type[CryptoHftDataProduct]", ProductFactory(source=self.name, basis=basis)
        )
        return Product(
            source=self.name,
            exchange=str(exchange),
            basis=basis,
            specs=specs,
            symbol=symbol,
        )
