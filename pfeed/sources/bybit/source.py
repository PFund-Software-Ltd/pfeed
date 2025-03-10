from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.exchanges.exchange_base import BaseExchange
    from pfund.products.product_base import BaseProduct

from pfeed.sources.base_source import BaseSource


__all__ = ["BybitSource"]


class BybitSource(BaseSource):
    def __init__(self):
        from pfeed.sources.bybit.api import BybitAPI
        super().__init__('BYBIT')
        self._exchange: BaseExchange = self._create_exchange()
        self._exchange.load_all_product_mappings()
        self.adapter = self._exchange.adapter
        self.api = BybitAPI(self._exchange)
    
    def create_product(self, product_basis: str, symbol: str='', **product_specs) -> BaseProduct:
        from pfeed.utils.utils import validate_product
        validate_product(product_basis)
        return self._exchange.create_product(product_basis, **product_specs)
        
    @staticmethod
    def _create_exchange():
        from pfund.exchanges.bybit.exchange import Exchange
        return Exchange(env='LIVE')
    
    def get_products_by_type(self, product_type: str) -> list[str]:
        '''Get products by product type
        e.g. if ptype='PERP', return all perpetual products
        '''
        pdts = []
        category: str = self._exchange._derive_product_category(product_type)
        for epdt in self.api.get_epdts_by_ptype(product_type):
            pdt = self.adapter(epdt, group=category)
            is_mapping_exists = (pdt != epdt)
            # NOTE: mapping may not exist if the product has been delisted
            if is_mapping_exists:
                pdts.append(pdt)
        return pdts
