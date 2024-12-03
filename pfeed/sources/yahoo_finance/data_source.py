import yfinance

from pfeed.sources.base_data_source import BaseDataSource


__all__ = ["YahooFinanceDataSource"]


class YahooFinanceDataSource(BaseDataSource):
    def __init__(self):
        super().__init__('YAHOO_FINANCE')
        self.api = yfinance
