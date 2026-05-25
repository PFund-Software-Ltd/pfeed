from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.sources.yahoo_finance.product import YahooFinanceProduct


class YahooFinanceMarketDataModel(MarketDataModel):
    product: YahooFinanceProduct
