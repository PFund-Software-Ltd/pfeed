from pfeed.feeds.fundamental_feed import FundamentalFeed


class FinancialModelingPrepFundamentalFeed(FundamentalFeed):
    def __init__(self, api_key: str):
        super().__init__()
        self._api_key = api_key
