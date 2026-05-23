from pfeed.sources.yahoo_finance.product import YahooFinanceProduct
from pfeed.data_models.market_data_model import MarketDataModel


class YahooFinanceMarketDataModel(MarketDataModel):
    product: YahooFinanceProduct
