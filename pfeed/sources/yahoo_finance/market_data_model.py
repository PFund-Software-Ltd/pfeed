from typing import ClassVar

from pfund.products.product_ibkr import IBKRProduct
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.sources.yahoo_finance.market_data_handler import YahooFinanceMarketDataHandler


class YahooFinanceMarketDataModel(MarketDataModel):
    product: IBKRProduct
    data_handler_class: ClassVar[type[YahooFinanceMarketDataHandler]] = YahooFinanceMarketDataHandler
