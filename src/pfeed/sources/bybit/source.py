# pyright: reportArgumentType=false
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast

if TYPE_CHECKING:
    from pfund.entities.products.product_bybit import BybitProduct

from pfund.enums.env import Environment

from pfeed.enums import (
    DataAccessType,
    DataCategory,
    DataProviderType,
    DataSource,
    DataType,
)
from pfeed.sources.bybit.batch_api import BatchAPI
from pfeed.sources.bybit.stream_api import StreamAPI
from pfeed.sources.data_provider_source import DataProviderSource
from pfeed.sources.source_metadata import SourceMetadata

__all__ = ["BybitSource"]


class BybitSource(DataProviderSource):
    NAME: ClassVar[DataSource] = DataSource.BYBIT
    METADATA: ClassVar[SourceMetadata] = SourceMetadata(
        data_origin="https://www.bybit.com",
        data_categories={
            DataCategory.MARKET_DATA: {
                DataType.TICK: [
                    "PERPETUAL",
                    "INVERSE-PERPETUAL",
                    "FUTURE",
                    "INVERSE-FUTURE",
                    "CRYPTO",
                ],
            },
        },
        provider_type=DataProviderType.EXCHANGE,
        access_type=DataAccessType.FREE,
        api_key_required=False,
        start_date="2020-01-01",
    )

    def get_batch_api(self) -> BatchAPI:
        if self._batch_api is None:
            self._batch_api = BatchAPI()
        return self._batch_api

    def get_stream_api(self, env: Environment | str | None = None) -> StreamAPI:
        if self._stream_api is None:
            assert env is not None, "env must be provided when creating stream API"
            self._stream_api = StreamAPI(env=env)
        return self._stream_api

    def create_product(
        self, basis: str, symbol: str = "", **specs: Any
    ) -> BybitProduct:
        from pfund.brokers.crypto.exchanges import Bybit

        return cast("BybitProduct", Bybit.create_product(basis, symbol=symbol, **specs))
