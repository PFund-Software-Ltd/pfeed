from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from pfeed.sources.yahoo_finance.market_feed import YahooFinanceMarketFeed
    # from pfeed.sources.yahoo_finance.news_feed import YahooFinanceNewsFeed

from pfeed.client import DataClient
from pfeed.sources.yahoo_finance.source import YahooFinanceSource


class YahooFinance(DataClient):
    DataSource: ClassVar[type[YahooFinanceSource]] = YahooFinanceSource
    data_source: YahooFinanceSource

    market_feed: YahooFinanceMarketFeed
    # TODO: not ready
    # news_feed: YahooFinanceNewsFeed
