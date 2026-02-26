from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar
if TYPE_CHECKING:
    from pfund.entities.products.product_bybit import BybitProduct
    from pfund.enums import Environment

from pfeed.sources.data_provider_source import DataProviderSource
from pfeed.enums import DataSource
from pfeed.sources.bybit.batch_api import BatchAPI
from pfeed.sources.bybit.stream_api import StreamAPI


__all__ = ["BybitSource"]


class BybitSource(DataProviderSource):
    name: ClassVar[DataSource] = DataSource.BYBIT

    def __init__(self):
        from pfund.brokers.crypto.exchanges import Bybit
        super().__init__()
        self._exchange = Bybit(env='LIVE')
        self.batch_api: BatchAPI = self.create_batch_api(env='BACKTEST')
        self.stream_api: StreamAPI = self.create_stream_api(env='LIVE')
    
    def create_batch_api(self, env: Environment | str) -> BatchAPI:
        self.batch_api = BatchAPI(env)
        return self.batch_api
        
    def create_stream_api(self, env: Environment | str) -> StreamAPI:
        self.stream_api = StreamAPI(env=env)
        return self.stream_api
    
    def create_product(self, basis: str, symbol: str='', **specs) -> BybitProduct:
        return self._exchange.create_product(basis, symbol=symbol, **specs)

    # TODO: backfill here?
    def backfill(self):
        pass