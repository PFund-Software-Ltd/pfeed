from pfund.products.product_ib import IBProduct
from pfeed.data_models.market_data_model import MarketDataModel


class YahooFinanceMarketDataModel(MarketDataModel):
    product: IBProduct
