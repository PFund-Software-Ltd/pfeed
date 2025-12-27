from pfund.products.product_bybit import BybitProduct
from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.sources.bybit.market_data_handler import BybitMarketDataHandler


class BybitMarketDataModel(MarketDataModel):
    product: BybitProduct

    @property
    def data_handler_class(self) -> type[BybitMarketDataHandler]:
        return BybitMarketDataHandler