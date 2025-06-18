from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_crypto import CryptoProduct

from pfeed.sources.base_source import BaseSource
from pfeed.enums import DataSource


__all__ = ["BybitSource"]


class BybitSource(BaseSource):
    name = DataSource.BYBIT
    
    def __init__(self):
        from pfund.exchanges import Bybit
        from pfeed.sources.bybit.batch_api import BatchAPI
        from pfeed.sources.bybit.stream_api import StreamAPI
        super().__init__()
        self.exchange: Bybit = Bybit(env='LIVE')
        self.batch_api = BatchAPI()
        self.stream_api = StreamAPI(self.exchange)
    
    def create_product(self, basis: str, symbol: str='', **specs) -> CryptoProduct:
        from pfeed.utils.utils import validate_product
        validate_product(basis)
        return self.exchange.create_product(basis, **specs)
