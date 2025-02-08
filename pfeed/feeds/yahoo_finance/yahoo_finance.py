from pfeed.feeds.yahoo_finance.yahoo_finance_market_data_feed import YahooFinanceMarketDataFeed


class YahooFinance:
    def __init__(self):
        self.market = YahooFinanceMarketDataFeed()
        