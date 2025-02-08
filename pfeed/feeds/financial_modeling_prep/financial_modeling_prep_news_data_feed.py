from pfeed.feeds.news_data_feed import NewsDataFeed


class FinancialModelingPrepNewsDataFeed(NewsDataFeed):
    def __init__(self, api_key: str):
        super().__init__()
        self._api_key = api_key
