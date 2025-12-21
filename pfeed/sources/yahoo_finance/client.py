from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import tDataTool
    from pfeed.sources.yahoo_finance.market_feed import YahooFinanceMarketFeed
    from pfeed.sources.yahoo_finance.news_feed import YahooFinanceNewsFeed

from pfeed.data_client import DataClient
from pfeed.sources.yahoo_finance.mixin import YahooFinanceMixin

    
__all__ = ['YahooFinance']


class YahooFinance(YahooFinanceMixin, DataClient):
    market_feed: YahooFinanceMarketFeed
    news_feed: YahooFinanceNewsFeed

    def __init__(
        self, 
        data_tool: tDataTool='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=False,
        use_prefect: bool=False,
        use_deltalake: bool=False,
    ):
        super().__init__(
            data_tool=data_tool,
            pipeline_mode=pipeline_mode,
            use_ray=use_ray,
            use_prefect=use_prefect,
            use_deltalake=use_deltalake,
        )
    