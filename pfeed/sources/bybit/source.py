from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.enums import Environment
    from pfund.products.product_crypto import CryptoProduct

from pfeed.sources.base_source import BaseSource
from pfeed.enums import DataSource


__all__ = ["BybitSource"]


class BybitSource(BaseSource):
    name = DataSource.BYBIT
    
    def __init__(self, env: Environment):
        from pfeed.sources.bybit.batch_api import BatchAPI
        from pfeed.sources.bybit.stream_api import StreamAPI
        super().__init__()
        from pfund.exchanges import Bybit
        self._exchange = Bybit(env=env)
        self.batch_api = BatchAPI(self._exchange._rest_api)
        self.stream_api = StreamAPI(self._exchange._ws_api)
    
    def create_product(self, basis: str, symbol: str='', **specs) -> CryptoProduct:
        return self._exchange.create_product(basis, symbol=symbol, **specs)

    # TODO: backfill here?
    def backfill(self):
        pass