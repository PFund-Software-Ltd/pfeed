from pfund.venues.bybit.product import BybitProduct

from pfeed.data_models.market_data_model import MarketDataModel


class BybitMarketDataModel(MarketDataModel):
    product: BybitProduct
