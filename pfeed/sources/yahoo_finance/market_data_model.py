from pfund.products.product_ib import IBProduct
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.sources.yahoo_finance.market_data_handler import YahooFinanceMarketDataHandler


class YahooFinanceMarketDataModel(MarketDataModel):
    product: IBProduct

    @property
    def data_handler_class(self) -> type[YahooFinanceMarketDataHandler]:
        return YahooFinanceMarketDataHandler