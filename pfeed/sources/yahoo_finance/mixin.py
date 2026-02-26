# pyright: reportUninitializedInstanceVariable=false, reportUnusedParameter=false
from __future__ import annotations
import types
from typing import TYPE_CHECKING, Any
if TYPE_CHECKING:
    from pfeed.sources.yahoo_finance.stream_api import StreamAPI
    
from pfeed.sources.yahoo_finance.source import YahooFinanceSource


class YahooFinanceMixin:
    data_source: YahooFinanceSource
    _yfinance_kwargs: dict[str, Any] | None = None

    @property
    def batch_api(self) -> types.ModuleType:
        return self.data_source.batch_api
    
    @property
    def stream_api(self) -> StreamAPI:
        return self.data_source.stream_api
    
    @staticmethod
    def _create_data_source() -> YahooFinanceSource:
        return YahooFinanceSource()
