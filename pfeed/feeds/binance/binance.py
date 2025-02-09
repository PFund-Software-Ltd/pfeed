from pfeed.feeds.binance.market_feed import BinanceMarketFeed


class Binance:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.market = BinanceMarketFeed(api_key)

