from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.sources.yahoo_finance.market_feed import YahooFinanceMarketFeed
    from pfeed.sources.yahoo_finance.news_feed import YahooFinanceNewsFeed

from pfeed.data_client import DataClient
from pfeed.sources.yahoo_finance.mixin import YahooFinanceMixin

    
__all__ = ['YahooFinance']


class YahooFinance(YahooFinanceMixin, DataClient):
    market_feed: YahooFinanceMarketFeed
    news_feed: YahooFinanceNewsFeed
