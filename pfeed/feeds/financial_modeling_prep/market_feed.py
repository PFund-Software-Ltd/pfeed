from pfeed.feeds.market_feed import MarketFeed


class FinancialModelingPrepMarketFeed(MarketFeed):
    def __init__(self, api_key: str):
        super().__init__()
        self._api_key = api_key
