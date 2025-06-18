from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import tDataTool
    from pfeed.enums import DataCategory
    from pfeed.sources.bybit.batch_api import BatchAPI
    from pfeed.sources.bybit.stream_api import StreamAPI


from pfeed.sources.bybit.source import BybitSource
from pfeed.sources.bybit.market_feed import BybitMarketFeed    


__all__ = ['Bybit']


class Bybit:
    def __init__(
        self, 
        data_tool: tDataTool='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=True,
        use_prefect: bool=False,
        use_deltalake: bool=False,
    ):
        params = {k: v for k, v in locals().items() if k not in ['self']}
        self.data_source = BybitSource()
        self.name = self.data_source.name
        self.market_feed = BybitMarketFeed(data_source=self.data_source, **params)
    
    @property
    def market(self) -> BybitMarketFeed:
        return self.market_feed
    
    @property
    def batch_api(self) -> BatchAPI:
        return self.data_source.batch_api
    
    @property
    def stream_api(self) -> StreamAPI:
        return self.data_source.stream_api
    
    @property
    def data_categories(self) -> list[DataCategory]:
        return self.data_source.data_categories