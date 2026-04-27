from typing import ClassVar

from pfeed.sources.yahoo_finance.product import YahooFinanceProduct
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.sources.yahoo_finance.market_data_handler import YahooFinanceMarketDataHandler


class YahooFinanceMarketDataModel(MarketDataModel):
    product: YahooFinanceProduct
    data_handler_class: ClassVar[type[YahooFinanceMarketDataHandler]] = YahooFinanceMarketDataHandler
