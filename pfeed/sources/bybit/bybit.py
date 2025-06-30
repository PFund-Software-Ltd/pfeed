from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.sources.bybit.market_feed import BybitMarketFeed
    from pfeed.sources.bybit.source import BybitSource
    from pfeed.sources.bybit.batch_api import BatchAPI
    from pfeed.sources.bybit.stream_api import StreamAPI

from pfeed.data_client import DataClient


__all__ = ['Bybit']


class Bybit(DataClient):
    data_source: BybitSource
    market_feed: BybitMarketFeed
    
    @property
    def batch_api(self) -> BatchAPI:
        return self.data_source.batch_api
    batch = batch_api
    
    @property
    def stream_api(self) -> StreamAPI:
        return self.data_source.stream_api
    stream = stream_api
