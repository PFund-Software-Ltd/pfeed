# pyright: reportUninitializedInstanceVariable=false, reportUnusedParameter=false
from __future__ import annotations
from typing import Any
    
from pfeed.sources.yahoo_finance.source import YahooFinanceSource


class YahooFinanceMixin:
    data_source: YahooFinanceSource
    _yfinance_kwargs: dict[str, Any] | None = None

    @staticmethod
    def _create_data_source() -> YahooFinanceSource:
        return YahooFinanceSource()
