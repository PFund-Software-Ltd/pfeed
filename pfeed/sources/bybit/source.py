from __future__ import annotations
from typing import TYPE_CHECKING, ClassVar, Any, cast
if TYPE_CHECKING:
    from pfund.entities.products.product_bybit import BybitProduct

from pfund.enums import Environment
from pfeed.sources.data_provider_source import DataProviderSource
from pfeed.enums import DataSource
from pfeed.sources.bybit.batch_api import BatchAPI
from pfeed.sources.bybit.stream_api import StreamAPI


__all__ = ["BybitSource"]


class BybitSource(DataProviderSource):
    name: ClassVar[DataSource] = DataSource.BYBIT

    def __init__(self):
        from pfund.brokers.crypto.exchanges import Bybit
        super().__init__()
        self._exchange = Bybit(env='LIVE')
        self._batch_api: BatchAPI | None = None
        self._stream_api: StreamAPI | None = None
        
    @property
    def batch_api(self) -> BatchAPI:
        assert self._batch_api is not None, 'batch_api is not initialized'
        return self._batch_api
    
    @property
    def stream_api(self) -> StreamAPI:
        assert self._stream_api is not None, 'stream_api is not initialized'
        return self._stream_api
    
    def create_batch_api(self, env: Environment | str):
        '''Creates or reuses existing batch API for the given environment'''
        env = Environment[env.upper()]
        if self._batch_api is None or self._batch_api.env != env:
            self._batch_api = BatchAPI(env=env)
        
    def create_stream_api(self, env: Environment | str):
        '''Creates or reuses existing stream API for the given environment'''
        env = Environment[env.upper()]
        if self._stream_api is None or self._stream_api.env != env:
            self._stream_api = StreamAPI(env=env)
    
    def create_product(self, basis: str, name: str='', symbol: str='', **specs: Any) -> BybitProduct:
        from pfund.entities.products.product_bybit import BybitProduct  # pyright: ignore[reportUnusedImport]
        return cast(BybitProduct, self._exchange.create_product(basis, name=name, symbol=symbol, **specs))
