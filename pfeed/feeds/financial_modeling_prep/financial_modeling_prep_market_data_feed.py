from pfeed.feeds.market_data_feed import MarketDataFeed


class FinancialModelingPrepMarketDataFeed(MarketDataFeed):
    def __init__(self, api_key: str):
        super().__init__()
        self._api_key = api_key
