from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.typing import tDATA_TOOL
    
from enum import StrEnum
    
from pfeed.sources.yahoo_finance.source import YahooFinanceSource
from pfeed.feeds.yahoo_finance.market_feed import YahooFinanceMarketFeed
from pfeed.feeds.yahoo_finance.news_feed import YahooFinanceNewsFeed


__all__ = ['YahooFinance']

class YFCategory(StrEnum):
    market = 'market'
    news = 'news'

YFDataFeed = YahooFinanceMarketFeed | YahooFinanceNewsFeed


class YahooFinance:
    def __init__(
        self, 
        data_tool: tDATA_TOOL='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=False,
        use_prefect: bool=False,
        use_bytewax: bool=False,
        use_deltalake: bool=False,
    ):
        params = {k: v for k, v in locals().items() if k not in ['self']}
        params['data_source'] = YahooFinanceSource()
        self.market = YahooFinanceMarketFeed(**params)
        self.news = YahooFinanceNewsFeed(**params)

    @property
    def data_source(self) -> YahooFinanceSource:
        return self.market.data_source
    
    @property
    def api(self):
        return self.market.api
    
    @property
    def categories(self) -> list[YFCategory]:
        return [category.value for category in YFCategory]