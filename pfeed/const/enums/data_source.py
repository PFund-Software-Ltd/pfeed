from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfeed.feeds.base_feed import BaseFeed
    
from enum import StrEnum


class DataSource(StrEnum):
    YAHOO_FINANCE = 'YAHOO_FINANCE'
    DATABENTO = 'DATABENTO'
    BYBIT = 'BYBIT'
    BINANCE = 'BINANCE'
    
    # TODO
    @property
    def feed_class(self) -> type[BaseFeed]:
        """Returns the corresponding Feed class for this data source."""
        from pfeed.feeds.bybit_feed import BybitFeed
        from pfeed.feeds.yahoo_finance_feed import YahooFinanceFeed
        return {
            DataSource.BYBIT: BybitFeed,
            DataSource.YAHOO_FINANCE: YahooFinanceFeed,
            # DataSource.DATABENTO: DatabentoFeed,
            # DataSource.BINANCE: BinanceFeed,
        }[self]
