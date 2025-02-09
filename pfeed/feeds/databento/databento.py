from pfeed.feeds.databento.market_feed import DatabentoMarketFeed


class Databento:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.market = DatabentoMarketFeed(api_key)

