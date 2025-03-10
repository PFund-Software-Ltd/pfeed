from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct


from pfeed.sources.tradfi_source import TradFiSource


__all__ = ["YahooFinanceSource"]


class YahooFinanceSource(TradFiSource):
    def __init__(self):
        import yfinance
        super().__init__('YAHOO_FINANCE')
        self.api = yfinance

    def create_product(self, product_basis: str, symbol='', **product_specs) -> BaseProduct:
        product = super().create_product(product_basis, symbol=symbol, **product_specs)
        if not symbol:
            symbol = self._create_symbol(product)
            product.set_symbol(symbol)
        return product
    
    def _create_symbol(self, product: BaseProduct) -> str:
        '''Creates symbol based on product type'''
        from pfund.enums import TradFiProductType
        ptype = product.type
        if ptype == TradFiProductType.STK:
            symbol = product.symbol  # using the default symbol created in product creation
        elif ptype == TradFiProductType.OPT:
            symbol = product.symbol  # using the default symbol created in product creation
        elif ptype == TradFiProductType.FUT:
            symbol = product.base_asset + '=F'  # e.g. ES=F
        elif ptype in (TradFiProductType.ETF, TradFiProductType.MTF):
            symbol = product.base_asset  # e.g. SPY
        elif ptype == TradFiProductType.FX:
            symbol = product.base_asset + product.quote_asset + '=X'  # e.g. EURUSD=X
        elif ptype == TradFiProductType.CRYPTO:
            symbol = product.base_asset + '-' + product.quote_asset  # e.g. BTC-USD
        elif ptype == TradFiProductType.INDEX:
            symbol = "^" + product.base_asset  # e.g. ^GSPC
        else:
            raise ValueError(f'Unsupported product type: {ptype}')
        return symbol
