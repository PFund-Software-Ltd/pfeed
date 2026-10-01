from pfund.venues.ibkr.product import InteractiveBrokersProduct

from pfeed.data_models.market_data_model import MarketDataModel


# TODO: use Generic ProductT
class InteractiveBrokersMarketDataModel(MarketDataModel):
    product: InteractiveBrokersProduct
