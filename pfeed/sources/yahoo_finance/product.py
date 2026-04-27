from __future__ import annotations
from typing import ClassVar

from pfund.entities.products.product_base import BaseProduct


class YahooFinanceProduct(BaseProduct):
    # EXTEND: hard-coded mappings for convenience
    MAPPINGS: ClassVar[dict[str, str]] = {
        'HYPE_USD_CRYPTO': 'HYPE32196-USD',
    }
    def _create_symbol(self) -> str:
        if self.is_stock():
            from pfund.entities.products.mixins.stock import StockMixin
            symbol = StockMixin._create_symbol(self)
        elif self.is_option():
            from pfund.entities.products.mixins.option import OptionMixin
            symbol = OptionMixin._create_symbol(self)
        elif self.is_future():
            symbol = self.base_asset + '=F'  # e.g. ES=F
        elif self.is_etf() or self.is_mutual_fund():
            symbol = self.base_asset  # e.g. SPY
        elif self.is_forex():
            symbol = self.base_asset + self.quote_asset + '=X'  # e.g. EURUSD=X
        elif self.is_crypto():
            if str(self.basis) in self.MAPPINGS:
                symbol = self.MAPPINGS[str(self.basis)]
            else:
                symbol = self.base_asset + '-' + self.quote_asset  # e.g. BTC-USD
        elif self.is_index():
            symbol = "^" + self.base_asset  # e.g. ^GSPC
        else:
            raise ValueError(f'Unsupported product: {self}')
        return symbol
