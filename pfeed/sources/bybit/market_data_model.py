from pfund.products.product_bybit import BybitProduct
from pfeed.data_models.market_data_model import MarketDataModel


class BybitMarketDataModel(MarketDataModel):
    product: BybitProduct