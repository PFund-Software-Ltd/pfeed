from pfeed.data_models.market_data_model import MarketDataModel
from pfeed.sources.crypto_hft_data.product import CryptoHftDataProduct


class CryptoHftDataMarketDataModel(MarketDataModel):
    product: CryptoHftDataProduct
