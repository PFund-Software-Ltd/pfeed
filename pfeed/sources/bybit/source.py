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
        self.batch_api = BatchAPI()
        self.stream_api = StreamAPI(env=env)
    
    def create_product(self, basis: str, symbol: str='', **specs) -> CryptoProduct:
        from pfund.exchanges import Bybit
        # FIXME: is validate_product still needed?
        from pfeed.utils.utils import validate_product
        validate_product(basis)
        return Bybit.create_product(basis, **specs)
