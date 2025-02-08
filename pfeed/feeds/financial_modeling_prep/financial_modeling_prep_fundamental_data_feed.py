from pfeed.feeds.fundamental_data_feed import FundamentalDataFeed


class FinancialModelingPrepFundamentalDataFeed(FundamentalDataFeed):
    def __init__(self, api_key: str):
        super().__init__()
        self._api_key = api_key
