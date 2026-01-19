from pfeed.data_models.market_data_model import MarketDataModel


class RetrieveMarketDataModel(MarketDataModel):
    '''
    Data model for retrieving market data used by retrieve().
    '''
    auto_resample: bool = True
    