from pfeed.feeds.market_feed import MarketFeed


# download data date by date to avoid memory issue (pandas) 
# and it is the least risky in case of download failure of bulk data
def download(...):
    pass


class DatabentoMarketFeed(MarketFeed):
    def __init__(self, api_key: str):
        super().__init__()
        self._api_key = api_key
