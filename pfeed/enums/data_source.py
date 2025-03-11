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
    FINANCIAL_MODELING_PREP = 'FINANCIAL_MODELING_PREP'
    
    @property
    def feed_class(self) -> type[BaseFeed]:
        """Returns the corresponding Feed class for this data source."""
        import pfeed as pe
        feed_name = {
            DataSource.BYBIT: 'Bybit',
            DataSource.YAHOO_FINANCE: 'YahooFinance',
            # DataSource.DATABENTO: 'Databento',
            # DataSource.BINANCE: 'Binance',
            DataSource.FINANCIAL_MODELING_PREP: 'FinancialModelingPrep',
        }[self]
        return getattr(pe, feed_name)
