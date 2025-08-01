from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed._typing import tDataTool

from pfeed.sources.yahoo_finance.source import YahooFinanceSource
from pfeed.sources.yahoo_finance.market_feed import YahooFinanceMarketFeed
from pfeed.sources.yahoo_finance.news_feed import YahooFinanceNewsFeed
    
    
__all__ = ['YahooFinance']
YahooFinanceDataFeed = YahooFinanceMarketFeed | YahooFinanceNewsFeed


class YahooFinance:
    def __init__(
        self, 
        data_tool: tDataTool='polars', 
        pipeline_mode: bool=False,
        use_ray: bool=False,
        use_prefect: bool=False,
        use_deltalake: bool=False,
    ):
        params = {k: v for k, v in locals().items() if k not in ['self']}
        self.data_source = YahooFinanceSource()
        self.name = self.data_source.name
        self.market_feed = YahooFinanceMarketFeed(data_source=self.data_source, **params)
        self.news_feed = YahooFinanceNewsFeed(data_source=self.data_source, **params)
    
    @property
    def market(self) -> YahooFinanceMarketFeed:
        return self.market_feed
    
    @property
    def news(self) -> YahooFinanceNewsFeed:
        return self.news_feed

    @property
    def api(self):
        return self.data_source.api
    
    @property
    def data_categories(self) -> list[str]:
        return self.data_source.data_categories