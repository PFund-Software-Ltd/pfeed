from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pfund.products.product_base import BaseProduct

import yfinance

from pfeed.enums import DataSource
from pfeed.sources.tradfi_data_provider_source import TradFiDataProviderSource
from pfeed.sources.yahoo_finance.stream_api import StreamAPI


__all__ = ["YahooFinanceSource"]


class YahooFinanceSource(TradFiDataProviderSource):
    name = DataSource.YAHOO_FINANCE
    # hard-coded mappings for convenience
    MAPPINGS = {
        'HYPE_USD_CRYPTO': 'HYPE32196-USD',
    }
    
    def __init__(self):
        super().__init__()
        self.batch_api = yfinance
        self.stream_api = StreamAPI()

    def create_product(self, basis: str, symbol='', **specs) -> BaseProduct:
        product = super().create_product(basis, symbol=symbol, **specs)
        if not symbol:
            # if not specified, derive symbol used in yahoo finance
            symbol = self._create_symbol(product)
            product.symbol = symbol
        return product
    
    # conceptually, this should be put inside sth like YahooFinanceProduct, but since yahoo finance is not a trading venue, put it here instead
    def _create_symbol(self, product: BaseProduct) -> str:
        if product.is_stock():
            symbol = product.symbol  # using the default symbol created in product creation
        elif product.is_option():
            symbol = product.symbol  # using the default symbol created in product creation
        elif product.is_future():
            symbol = product.base_asset + '=F'  # e.g. ES=F
        elif product.is_etf() or product.is_mutual_fund():
            symbol = product.base_asset  # e.g. SPY
        elif product.is_forex():
            symbol = product.base_asset + product.quote_asset + '=X'  # e.g. EURUSD=X
        elif product.is_crypto():
            if str(product.basis) in self.MAPPINGS:
                symbol = self.MAPPINGS[str(product.basis)]
            else:
                symbol = product.base_asset + '-' + product.quote_asset  # e.g. BTC-USD
        elif product.is_index():
            symbol = "^" + product.base_asset  # e.g. ^GSPC
        else:
            raise ValueError(f'Unsupported product: {product}')
        return symbol
