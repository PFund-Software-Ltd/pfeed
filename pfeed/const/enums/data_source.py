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
        from pfeed.feeds import (
            Bybit,
            YahooFinance,
            FinancialModelingPrep
        )
        return {
            DataSource.BYBIT: Bybit,
            DataSource.YAHOO_FINANCE: YahooFinance,
            # DataSource.DATABENTO: DatabentoFeed,
            # DataSource.BINANCE: BinanceFeed,
            DataSource.FINANCIAL_MODELING_PREP: FinancialModelingPrep,
        }[self]
