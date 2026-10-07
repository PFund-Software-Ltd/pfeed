from pfund.venues.ibkr.product import InteractiveBrokersProduct

from pfeed.market.data_model import MarketDataModel


# TODO: use Generic ProductT
class InteractiveBrokersMarketDataModel(MarketDataModel):
    product: InteractiveBrokersProduct
