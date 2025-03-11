from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct

from pfeed.sources.base_source import BaseSource


class TradFiSource(BaseSource):
    def create_product(self, product_basis: str, symbol='', **product_specs) -> BaseProduct:
        from pfund.products.product_base import BaseProduct
        from pfund.products.product_stock import StockProduct
        from pfund.products.product_option import OptionProduct
        from pfund.products.product_future import FutureProduct
        from pfund.products.product_crypto import CryptoProduct
        from pfund.products.product_fx import FXProduct
        from pfund.enums import TradFiProductType
        from pfeed.utils.utils import validate_product

        product_basis = product_basis.upper()
        validate_product(product_basis)
        base_asset, quote_asset, product_type = product_basis.split('_')
        ptype = TradFiProductType[product_type.upper()]
        if ptype == TradFiProductType.STK:
            Product = StockProduct
        elif ptype == TradFiProductType.OPT:
            Product = OptionProduct
        elif ptype == TradFiProductType.FUT:
            Product = FutureProduct
        elif ptype == TradFiProductType.FX:
            Product = FXProduct
        elif ptype == TradFiProductType.CRYPTO:
            Product = CryptoProduct
        # EXTEND: add other product types, e.g. ETF, FX, etc.
        else:
            Product = BaseProduct
        # HACK: use 'IB' as a placeholder for broker, and change it to data source's name
        product = Product(
            bkr='IB',  
            base_asset=base_asset,
            quote_asset=quote_asset,
            type=ptype,
            specs=product_specs,
            symbol=symbol
        )
        product.bkr = self.name.value
        return product