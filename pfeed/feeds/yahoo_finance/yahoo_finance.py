from pfeed.feeds.yahoo_finance.market_feed import YahooFinanceMarketFeed


__all__ = ['YahooFinance']


class YahooFinance:
    def __init__(self):
        self.market = YahooFinanceMarketFeed()


