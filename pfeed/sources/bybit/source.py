from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund._typing import tEnvironment
    from pfund.products.product_bybit import BybitProduct

from pfund.exchanges import Bybit
from pfeed.sources.base_source import BaseSource
from pfeed.enums import DataSource
from pfeed.sources.bybit.batch_api import BatchAPI
from pfeed.sources.bybit.stream_api import StreamAPI


__all__ = ["BybitSource"]


class BybitSource(BaseSource):
    name = DataSource.BYBIT

    def __init__(self):
        super().__init__()
        self._exchange = Bybit(env='LIVE')
        self.batch_api: BatchAPI = self.create_batch_api(env='BACKTEST')
        self.stream_api: StreamAPI = self.create_stream_api(env='LIVE')
    
    def create_batch_api(self, env: tEnvironment) -> BatchAPI:
        self.batch_api = BatchAPI(env)
        return self.batch_api
        
    def create_stream_api(self, env: tEnvironment) -> StreamAPI:
        self.stream_api = StreamAPI(env=env)
        return self.stream_api
    
    def create_product(self, basis: str, symbol: str='', **specs) -> BybitProduct:
        return self._exchange.create_product(basis, symbol=symbol, **specs)

    # TODO: backfill here?
    def backfill(self):
        pass