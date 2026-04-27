from __future__ import annotations
import types
from typing import TYPE_CHECKING, Any, ClassVar, cast
if TYPE_CHECKING:
    from pfund.enums import Environment
    from pfeed.sources.yahoo_finance.product import YahooFinanceProduct

import yfinance

from pfeed.enums import DataSource
from pfeed.sources.data_provider_source import DataProviderSource
from pfeed.sources.yahoo_finance.stream_api import StreamAPI


__all__ = ["YahooFinanceSource"]


class YahooFinanceSource(DataProviderSource):
    name: ClassVar[DataSource] = DataSource.YAHOO_FINANCE
    
    def __init__(self):
        super().__init__()
        self._batch_api: types.ModuleType | None = None
        self._stream_api: StreamAPI | None = None
    
    @property
    def batch_api(self) -> types.ModuleType:
        assert self._batch_api is not None, 'batch_api is not initialized'
        return self._batch_api
    
    @property
    def stream_api(self) -> StreamAPI:
        assert self._stream_api is not None, 'stream_api is not initialized'
        return self._stream_api
    
    def create_batch_api(self, env: Environment | str):
        self._batch_api = yfinance
    
    def create_stream_api(self, env: Environment | str):
        '''Creates or reuses existing stream API for the given environment'''
        if self._stream_api is None:
            self._stream_api = StreamAPI()

    def create_product(self, basis: str, symbol: str='', **specs: Any) -> YahooFinanceProduct:
        from pfund.entities.products import ProductFactory
        Product = cast(
            "type[YahooFinanceProduct]", 
            ProductFactory(source=self.name, basis=basis)
        )
        product = Product(
            source=self.name,
            basis=basis,
            specs=specs,
            symbol=symbol,
        )
        return product
